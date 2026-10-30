package e2e

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/luci1900/shardlite/e2e/e2eapp"
	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
)

// Every protocol through the real node and app binaries, with the durability
// check at the moment each response, event or message reaches the client.
func TestProtocolsThroughTheNode(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	v := newVerifier(t, s)
	mustHave := func(what, shard string, tokens ...string) {
		t.Helper()
		if !v.has(shard, tokens...) {
			t.Fatalf("%s reached the client, but blob storage doesn't have %v for %s", what, tokens, shard)
		}
	}

	s.create(t, "e2e.users/u1", "e2e.wallet/w1", "e2e.wallet/w2", "e2e.wallet/w3", "e2e.wallet/w4", "e2e.wallet/w5", "e2e.wallet/w7", "e2e.wallet/w9")

	// Plain HTTP.
	st, body, err := s.shardRequest("POST", "e2e.users/u1", "/add", "t1")
	if err != nil || st != 200 || jsonField(t, body, "count") != float64(1) {
		t.Fatalf("add: %d %q %v", st, body, err)
	}
	if st, body, _ := s.shardRequest("GET", "e2e.users/u1", "/count", ""); st != 200 || jsonField(t, body, "count") != float64(1) {
		t.Fatalf("count: %d %q", st, body)
	}
	if st, body, _ := s.request("GET", at("e2e.users/u1", "/tokens")+"?shardlite-target=e2e.users/u1", "", ""); st != 200 || body != "t1" {
		t.Fatalf("tokens via the query parameter: %d %q", st, body)
	}

	// A client can't create a shard: one that doesn't exist gets 404 and leaves
	// nothing behind.
	if st, body, _ := s.shardRequest("GET", "e2e.users/fresh", "/count", ""); st != 404 {
		t.Fatalf("count of a shard that doesn't exist: %d %q", st, body)
	}
	if _, _, err := v.files.GetManifest(t.Context(), shardName("e2e.users/fresh")); !isNotFound(err) {
		t.Fatalf("a client's request created e2e.users/fresh: %v", err)
	}

	// Sync on: the response comes after the write is in blob storage.
	if st, _, _ := s.shardRequest("POST", "e2e.wallet/w1", "/add", "pay-1"); st != 200 {
		t.Fatalf("add: %d", st)
	}
	mustHave("a response", "e2e.wallet/w1", "pay-1")

	// Server-sent events.
	q, _ := http.NewRequest("GET", "http://"+s.pub+at("e2e.wallet/w2", "/events")+"?n=3&prefix=sse", nil)
	q.Header.Set("Shardlite-Target", "e2e.wallet/w2")
	resp, err := http.DefaultClient.Do(q)
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(resp.Body)
	for i := 1; i <= 3; i++ {
		line, err := br.ReadString('\n')
		if err != nil || line != fmt.Sprintf("data: sse-%d\n", i) {
			t.Fatalf("event %d: %q %v", i, line, err)
		}
		mustHave("an event", "e2e.wallet/w2", fmt.Sprintf("sse-%d", i))
		br.ReadString('\n')
	}
	resp.Body.Close()

	// WebSocket.
	ctx, cancel := timeout(30 * time.Second)
	defer cancel()
	ws, err := dialWS(ctx, s.pub, "e2e.wallet/w3")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		tok := fmt.Sprintf("ws-%d", i)
		if echo, err := wsEcho(ctx, ws, tok); err != nil || echo != "ok:"+tok {
			t.Fatalf("echo: %q %v", echo, err)
		}
		mustHave("an echo", "e2e.wallet/w3", tok)
	}
	ws.Close(websocket.StatusNormalClosure, "")

	// gRPC: unary and server-streaming, with trailers, over h2c on both sides of the node.
	cc := dialGRPC(t, s.pub)
	gctx := onShard(ctx, "e2e.wallet/w4")
	var trailer metadata.MD
	out, err := unary(gctx, cc, "Add", "rpc-1", grpc.Trailer(&trailer))
	if err != nil || out != "ok:rpc-1" || trailer.Get("x-app-trailer")[0] != "t-rpc-1" {
		t.Fatalf("unary: %q %v %v", out, trailer, err)
	}
	mustHave("a unary reply", "e2e.wallet/w4", "rpc-1")
	stream, err := open(gctx, cc, "Watch", true, false)
	if err != nil || send(stream, "watch 3") != nil || stream.CloseSend() != nil {
		t.Fatalf("open stream: %v", err)
	}
	for n := 0; ; n++ {
		m, err := recv(stream)
		if err == io.EOF {
			if n != 3 {
				t.Fatalf("stream: %d messages", n)
			}
			break
		} else if err != nil {
			t.Fatalf("stream: %v", err)
		}
		mustHave("a streamed reply", "e2e.wallet/w4", strings.TrimPrefix(m, "ok:"))
	}
	// Errors the node makes are gRPC statuses.
	_, err = unary(onShard(ctx, "e2e.typo/1"), cc, "Add", "x")
	if status.Code(err) != codes.NotFound {
		t.Fatalf("undeclared kind over gRPC: %v", err)
	}
	// A kind that isn't public gets what an undeclared kind gets.
	_, err = unary(onShard(ctx, "e2e.audit/1"), cc, "Add", "x")
	if status.Code(err) != codes.NotFound {
		t.Fatalf("private kind over gRPC: %v", err)
	}

	// A shardless login creates the user's shard and answers with a URL naming it.
	st, body, _ = s.request("POST", "/login", "", "alice")
	url, _ := jsonField(t, body, "url").(string)
	if st != 200 || url != "/e2e.users/alice/me?shardlite-target=e2e.users%2Falice" {
		t.Fatalf("login: %d %q", st, body)
	}
	if st, body, _ = s.request("GET", url, "", ""); st != 200 || jsonField(t, body, "shard") != "e2e.users/alice" {
		t.Fatalf("following the URL: %d %q", st, body)
	}

	// A call to another shard through the control API, gated at the caller.
	st, _, _ = s.request("POST", at("e2e.wallet/w5", "/transfer")+"?to=e2e.wallet/w6&token=x1", "e2e.wallet/w5", "")
	if st != 200 {
		t.Fatalf("transfer: %d", st)
	}
	mustHave("a transfer", "e2e.wallet/w5", "debit:x1")
	mustHave("a transfer", "e2e.wallet/w6", "credit:x1")
	// A durable send: the row, the call and its reminder are one transaction on
	// the sender. The node wakes the sender's reminders handler once that is
	// durable, and the handler calls the receiver, which does its work once.
	if st, _, _ := s.request("POST", at("e2e.wallet/w7", "/notify")+"?to=e2e.wallet/w8&token=m1", "e2e.wallet/w7", ""); st != 200 {
		t.Fatalf("notify: %d", st)
	}
	mustHave("a notify", "e2e.wallet/w7", "sent:m1")
	eventually(t, "the send to reach its receiver", func() bool { return v.has("e2e.wallet/w8", "msg:m1") })

	// A reminder: set with a row in one transaction, and the node wakes the
	// shard's reminders handler two seconds later, which handles and deletes it.
	if st, _, _ := s.request("POST", at("e2e.wallet/w9", "/remind")+"?key=r1&in=2", "e2e.wallet/w9", ""); st != 200 {
		t.Fatalf("remind: %d", st)
	}
	mustHave("a remind", "e2e.wallet/w9", "set:r1")
	eventually(t, "the reminder to fire", func() bool { return v.has("e2e.wallet/w9", "fired:r1") })

	// Internal endpoints refuse requests that didn't come from another shard.
	if st, _, _ := s.request("POST", "/credit", "e2e.wallet/w6", "x2"); st != 403 {
		t.Fatalf("/credit from a client: %d", st)
	}

	// Refusals.
	if st, _, _ := s.shardRequest("GET", "e2e.audit/1", "/count", ""); st != 404 { // not public, like an undeclared kind
		t.Fatalf("private kind: %d", st)
	}
	if st, _, _ := s.shardRequest("GET", "e2e.typo/1", "/count", ""); st != 404 {
		t.Fatalf("undeclared kind: %d", st)
	}
	if st, _, _ := s.request("POST", "/_shardlite/reminders", "e2e.wallet/w6", ""); st != 404 {
		t.Fatalf("the reminders wake from a client: %d", st)
	}
	if st, _, _ := s.request("POST", "/_shardlite/sync", "", ""); st != 404 {
		t.Fatalf("node API from a client: %d", st)
	}
	if st, _, _ := s.control("POST", "/_shardlite/sync", "", ""); st != 204 {
		t.Fatalf("sync: %d", st)
	}
	// A completed sync means the sync-off shard's write is durable too.
	if !v.has("e2e.users/u1", "t1") {
		t.Fatal("sync returned but e2e.users/u1's write is not in blob storage")
	}

	// A new declaration of the app's (the file its ready route serves) is seen
	// at the node's next poll, without a restart.
	if st, _, _ := s.shardRequest("POST", "e2e.extra/1", "/add", "e"); st != 404 {
		t.Fatalf("a kind that isn't declared yet: %d", st)
	}
	declaration := strings.Replace(declarationJSON, `"audit"`, `"extra": {"public": true}, "audit"`, 1)
	if err := os.WriteFile(s.declarePath(), []byte(declaration), 0o644); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the new kind", func() bool {
		st, _, _ := s.control("POST", at("e2e.extra/1", "/create"), "e2e.extra/1", "")
		return st == 204
	})
	if st, _, _ := s.shardRequest("POST", "e2e.extra/1", "/add", "e"); st != 200 {
		t.Fatalf("the new kind from a client: %d", st)
	}

	// Graceful stop: every shard is let go of (the app closes its handles when idle) and the lease is released.
	eventually(t, "the app to close its databases", func() bool { return s.appDebug().OpenDBs == 0 })
	if err := s.node.term(30 * time.Second); err != nil {
		t.Fatalf("node exit: %v\n%s", err, s.node.out.String())
	}
	if l, err := v.files.Leases.Get(t.Context(), "n1"); err != nil || !l.Released {
		t.Fatalf("lease after shutdown: %+v %v\n%s", l, err, s.node.out.String())
	}
	for _, shard := range v.shards() {
		if _, ok, err := v.tokens(shard); err != nil || !ok {
			t.Fatalf("restore %s: %v (ok=%v)", shard, err, ok)
		}
	}
}

// A request whose Shardlite-Target is missing or names another shard still gets
// the answer of the shard it is for. The app works the shard out from the path
// and relays the request to it, marked as the client's request it is, and the
// hint only decides what the node opens first.
func TestRequestsForAnotherShardAreRelayed(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	v := newVerifier(t, s)
	const p1, p2, p3 = "e2e.wallet/p1", "e2e.wallet/p2", "e2e.wallet/p3"
	s.create(t, p1, p2, p3)
	me := func(t *testing.T, shard, hint string) (caller string, hops int) {
		t.Helper()
		st, body, err := s.request("GET", at(shard, "/me"), hint, "")
		if err != nil || st != 200 || jsonField(t, body, "shard") != shard {
			t.Fatalf("me with the hint %q: %d %q %v", hint, st, body, err)
		}
		caller, _ = jsonField(t, body, "caller").(string)
		h, _ := jsonField(t, body, "hops").(string)
		hops, _ = strconv.Atoi(h)
		return caller, hops
	}

	// A right hint costs nothing: the app uses the open file and it is no call.
	n := s.relayCount()
	caller, hops := me(t, p1, p1)
	if caller != "" {
		t.Fatalf("a request with a right hint arrived from %q", caller)
	}
	s.checkRelays(t, n)

	t.Run("no hint", func(t *testing.T) {
		n := s.relayCount()
		// It arrives as the client's request would, with no caller, but one hop further on.
		if c, h := me(t, p1, ""); c != "" || h != hops+1 {
			t.Fatalf("relayed: caller %q, hops %d, want none and %d", c, h, hops+1)
		}
		s.checkRelays(t, n, e2eapp.RelayRec{For: p1})
		if st, body, err := s.request("POST", at(p2, "/add"), "", "none"); err != nil || st != 200 || jsonField(t, body, "shard") != p2 || jsonField(t, body, "count") != float64(1) {
			t.Fatalf("add: %d %q %v", st, body, err)
		}
		if !v.has(p2, "none") {
			t.Fatal("the relayed write was not durable when the answer came")
		}
	})

	t.Run("a hint naming another shard", func(t *testing.T) {
		n := s.relayCount()
		if st, body, err := s.request("POST", at(p2, "/add"), p3, "other"); err != nil || st != 200 || jsonField(t, body, "shard") != p2 {
			t.Fatalf("add: %d %q %v", st, body, err)
		}
		// The node opened the shard the hint names and sent the request to the
		// app with it open, so the hint did activate p3. The app relayed the
		// request on, and the write landed in the shard it was for.
		s.checkRelays(t, n, e2eapp.RelayRec{Opened: p3, For: p2})
		if !v.has(p2, "other") || v.has(p3, "other") {
			t.Fatal("the write did not land in the shard the path names")
		}
		// A hint is still checked by the node: a kind clients can't name is refused.
		if st, _, _ := s.request("GET", at(p2, "/me"), "e2e.audit/1", ""); st != 404 {
			t.Fatalf("a hint naming a private kind: %d", st)
		}
		s.checkRelays(t, n, e2eapp.RelayRec{Opened: p3, For: p2})
	})

	t.Run("a stream", func(t *testing.T) {
		n := s.relayCount()
		q, _ := http.NewRequest("GET", "http://"+s.pub+at(p2, "/events")+"?n=3&prefix=rel", nil)
		q.Header.Set("Shardlite-Target", p3)
		resp, err := http.DefaultClient.Do(q)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		br := bufio.NewReader(resp.Body)
		for i := 1; i <= 3; i++ {
			line, err := br.ReadString('\n')
			if err != nil || line != fmt.Sprintf("data: rel-%d\n", i) {
				t.Fatalf("event %d: %q %v", i, line, err)
			}
			if !v.has(p2, fmt.Sprintf("rel-%d", i)) {
				t.Fatalf("event %d reached the client before its commit was durable", i)
			}
			br.ReadString('\n')
		}
		s.checkRelays(t, n, e2eapp.RelayRec{Opened: p3, For: p2})
	})
}

// A relay is the client's request, so it reaches no more than the client could
// have by naming the shard itself, with or without a hint: not a kind that isn't
// public, and not a shard that doesn't exist, which a call would have created.
func TestRelaysKeepTheClientsLimits(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	v := newVerifier(t, s)
	const p1, audit, random = "e2e.wallet/p1", "e2e.audit/1", "e2e.wallet/6f1c0d9a4e7b"
	s.create(t, p1, audit)
	base := s.appDebug().Served // the app serves the creation of the private one

	for _, tc := range []struct{ name, shard string }{{"a private kind", audit}, {"an id that does not exist", random}} {
		for _, hint := range []string{"", p1} {
			for _, req := range []struct{ method, route, body string }{{"GET", "/count", ""}, {"POST", "/add", "leak"}} {
				name := fmt.Sprintf("%s, %s %s, hint %q", tc.name, req.method, req.route, hint)
				n := s.relayCount()
				if st, _, err := s.request(req.method, at(tc.shard, req.route), hint, req.body); err != nil || st != 404 {
					t.Fatalf("%s: %d %v", name, st, err)
				}
				// The app did relay it and the node refused: nothing was served on the shard.
				s.checkRelays(t, n, e2eapp.RelayRec{Opened: hint, For: tc.shard})
				if got := s.appDebug().Served[tc.shard]; got != base[tc.shard] {
					t.Fatalf("%s: the app served %d requests on %s", name, got-base[tc.shard], tc.shard)
				}
			}
		}
	}
	if slices.Contains(v.shards(), random) {
		t.Fatal("a relay created a shard")
	}
	if v.has(audit, "leak") {
		t.Fatal("a relay wrote to a shard of a private kind")
	}
	// The shard of the private kind is there and the app can reach it: it is the
	// limits that refused the client.
	if st, _, err := s.control("GET", at(audit, "/count"), audit, ""); err != nil || st != 200 {
		t.Fatalf("a call to the private kind: %d %v", st, err)
	}
}

func eventually(t testing.TB, what string, cond func() bool) {
	t.Helper()
	apptest.Eventually(t, 20*time.Second, what, cond)
}

func isNotFound(err error) bool { return errors.Is(err, blob.ErrNotFound) }
