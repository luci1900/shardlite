package e2e

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/luci1900/shardlite/internal/storage/blob"
	"github.com/luci1900/shardlite/internal/test/apptest"
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

	s.create(t, "users/u1", "wallet/w1", "wallet/w2", "wallet/w3", "wallet/w4", "wallet/w5", "wallet/w7", "wallet/w9", "wallet/l1")

	// Plain HTTP.
	st, body, err := s.shardRequest("POST", "users/u1", "/add", "t1")
	if err != nil || st != 200 || jsonField(t, body, "count") != float64(1) {
		t.Fatalf("add: %d %q %v", st, body, err)
	}
	if st, body, _ := s.shardRequest("GET", "users/u1", "/count", ""); st != 200 || jsonField(t, body, "count") != float64(1) {
		t.Fatalf("count: %d %q", st, body)
	}
	if st, body, _ := s.request("GET", at("users/u1", "/tokens")+"?shardlite-target=users/u1", "", ""); st != 200 || body != "t1" {
		t.Fatalf("tokens via the query parameter: %d %q", st, body)
	}

	// A client can't create a shard: one that doesn't exist gets 404 and leaves
	// nothing behind.
	if st, body, _ := s.shardRequest("GET", "users/fresh", "/count", ""); st != 404 {
		t.Fatalf("count of a shard that doesn't exist: %d %q", st, body)
	}
	if _, _, err := v.files.GetManifest(t.Context(), shardName("users/fresh")); !isNotFound(err) {
		t.Fatalf("a client's request created users/fresh: %v", err)
	}

	// Sync on: the response comes after the write is in blob storage.
	if st, _, _ := s.shardRequest("POST", "wallet/w1", "/add", "pay-1"); st != 200 {
		t.Fatalf("add: %d", st)
	}
	mustHave("a response", "wallet/w1", "pay-1")

	// Server-sent events.
	q, _ := http.NewRequest("GET", "http://"+s.pub+at("wallet/w2", "/events")+"?n=3&prefix=sse", nil)
	q.Header.Set("Shardlite-Target", "wallet/w2")
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
		mustHave("an event", "wallet/w2", fmt.Sprintf("sse-%d", i))
		br.ReadString('\n')
	}
	resp.Body.Close()

	// WebSocket.
	ctx, cancel := timeout(30 * time.Second)
	defer cancel()
	ws, err := dialWS(ctx, s.pub, "wallet/w3")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		tok := fmt.Sprintf("ws-%d", i)
		if echo, err := wsEcho(ctx, ws, tok); err != nil || echo != "ok:"+tok {
			t.Fatalf("echo: %q %v", echo, err)
		}
		mustHave("an echo", "wallet/w3", tok)
	}
	ws.Close(websocket.StatusNormalClosure, "")

	// gRPC: unary and server-streaming, with trailers, over h2c on both sides of the node.
	cc := dialGRPC(t, s.pub)
	gctx := onShard(ctx, "wallet/w4")
	var trailer metadata.MD
	out, err := unary(gctx, cc, "Add", "rpc-1", grpc.Trailer(&trailer))
	if err != nil || out != "ok:rpc-1" || trailer.Get("x-app-trailer")[0] != "t-rpc-1" {
		t.Fatalf("unary: %q %v %v", out, trailer, err)
	}
	mustHave("a unary reply", "wallet/w4", "rpc-1")
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
		mustHave("a streamed reply", "wallet/w4", strings.TrimPrefix(m, "ok:"))
	}
	// Errors the node makes are gRPC statuses.
	_, err = unary(onShard(ctx, "typo/1"), cc, "Add", "x")
	if status.Code(err) != codes.NotFound {
		t.Fatalf("undeclared kind over gRPC: %v", err)
	}
	// A kind that isn't public gets what an undeclared kind gets.
	_, err = unary(onShard(ctx, "audit/1"), cc, "Add", "x")
	if status.Code(err) != codes.NotFound {
		t.Fatalf("private kind over gRPC: %v", err)
	}

	// A shardless login creates the user's shard and answers with a URL naming it.
	st, body, _ = s.request("POST", "/login", "", "alice")
	url, _ := jsonField(t, body, "url").(string)
	if st != 200 || url != "/users/alice/me?shardlite-target=users%2Falice" {
		t.Fatalf("login: %d %q", st, body)
	}
	if st, body, _ = s.request("GET", url, "", ""); st != 200 || jsonField(t, body, "shard") != "users/alice" {
		t.Fatalf("following the URL: %d %q", st, body)
	}

	// A call to another shard through the control API, gated at the caller.
	st, _, _ = s.request("POST", at("wallet/w5", "/transfer")+"?to=wallet/w6&token=x1", "wallet/w5", "")
	if st != 200 {
		t.Fatalf("transfer: %d", st)
	}
	mustHave("a transfer", "wallet/w5", "debit:x1")
	mustHave("a transfer", "wallet/w6", "credit:x1")
	// The app passes the context of the request it serves on to its call, so the
	// hop limit (32) sees the whole chain. Serving hop 31, the call is hop 32, the
	// last one allowed. Serving hop 32, it would be hop 33, and the node refuses
	// it with 508, which the app answers as a bad gateway.
	for _, c := range []struct {
		hops int
		want int
	}{{30, 200}, {31, 502}} {
		q, _ := http.NewRequest("POST", "http://"+s.ctl+at("wallet/w5", "/transfer")+fmt.Sprintf("?to=wallet/w6&token=h%d", c.hops), nil)
		q.Header.Set("Shardlite-Target", "wallet/w5")
		q.Header.Set("Shardlite-Context", fmt.Sprintf(`shard="wallet/w6", hops=%d`, c.hops))
		resp, err := http.DefaultClient.Do(q)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != c.want || (c.want == 502 && !strings.Contains(string(b), "508")) {
			t.Fatalf("a transfer at hop %d: %d %q, want %d", c.hops+1, resp.StatusCode, b, c.want)
		}
	}
	// Lists: two appends on the list's shard and one relayed by a call from
	// another shard, which records the value before it calls.
	for _, v := range []string{"a", "b"} {
		if st, body, _ := s.request("POST", at("wallet/l1", "/append")+"?list=x", "wallet/l1", v); st != 200 {
			t.Fatalf("append %s: %d %q", v, st, body)
		}
	}
	if st, body, _ := s.request("POST", at("wallet/w5", "/relay")+"?to=wallet/l1&list=x", "wallet/w5", "c"); st != 200 {
		t.Fatalf("relay: %d %q", st, body)
	}
	if st, body, _ := s.shardRequest("GET", "wallet/l1", "/list?list=x", ""); st != 200 || strings.TrimSpace(body) != `["a","b","c"]` {
		t.Fatalf("list: %d %q", st, body)
	}
	if st, body, _ := s.shardRequest("GET", "wallet/l1", "/list?list=empty", ""); st != 200 || strings.TrimSpace(body) != `[]` {
		t.Fatalf("an empty list: %d %q", st, body)
	}
	mustHave("a relay", "wallet/w5", "relay:c")
	// A durable send: the row, the call and its reminder are one transaction on
	// the sender. The node wakes the sender's reminders handler once that is
	// durable, and the handler calls the receiver, which does its work once.
	if st, _, _ := s.request("POST", at("wallet/w7", "/notify")+"?to=wallet/w8&token=m1", "wallet/w7", ""); st != 200 {
		t.Fatalf("notify: %d", st)
	}
	mustHave("a notify", "wallet/w7", "sent:m1")
	apptest.Eventually(t, apptest.Timeout, "the send to reach its receiver", func() bool { return v.has("wallet/w8", "msg:m1") })

	// A reminder: set with a row in one transaction, and the node wakes the
	// shard's reminders handler two seconds later, which handles and deletes it.
	if st, _, _ := s.request("POST", at("wallet/w9", "/remind")+"?key=r1&in=2", "wallet/w9", ""); st != 200 {
		t.Fatalf("remind: %d", st)
	}
	mustHave("a remind", "wallet/w9", "set:r1")
	apptest.Eventually(t, apptest.Timeout, "the reminder to fire", func() bool { return v.has("wallet/w9", "fired:r1") })

	// Internal endpoints refuse requests that didn't come from another shard.
	if st, _, _ := s.request("POST", "/credit", "wallet/w6", "x2"); st != 403 {
		t.Fatalf("/credit from a client: %d", st)
	}

	// Refusals.
	if st, _, _ := s.shardRequest("GET", "audit/1", "/count", ""); st != 404 { // not public, like an undeclared kind
		t.Fatalf("private kind: %d", st)
	}
	if st, _, _ := s.shardRequest("GET", "typo/1", "/count", ""); st != 404 {
		t.Fatalf("undeclared kind: %d", st)
	}
	if st, _, _ := s.request("POST", "/_shardlite/reminders", "wallet/w6", ""); st != 404 {
		t.Fatalf("the reminders wake from a client: %d", st)
	}
	if st, _, _ := s.request("POST", "/_shardlite/sync", "", ""); st != 404 {
		t.Fatalf("node API from a client: %d", st)
	}
	if st, _, _ := s.control("POST", "/_shardlite/sync", "", ""); st != 204 {
		t.Fatalf("sync: %d", st)
	}
	// A completed sync means the sync-off shard's write is durable too.
	if !v.has("users/u1", "t1") {
		t.Fatal("sync returned but users/u1's write is not in blob storage")
	}

	// A new declaration of the app's (the file its ready route serves) is seen
	// at the node's next poll, without a restart.
	if st, _, _ := s.shardRequest("POST", "extra/1", "/add", "e"); st != 404 {
		t.Fatalf("a kind that isn't declared yet: %d", st)
	}
	declaration := strings.Replace(declarationJSON, `"audit"`, `"extra": {"public": true}, "audit"`, 1)
	if err := os.WriteFile(s.declarePath(), []byte(declaration), 0o644); err != nil {
		t.Fatal(err)
	}
	apptest.Eventually(t, apptest.Timeout, "the new kind", func() bool {
		st, _, _ := s.control("POST", at("extra/1", "/create"), "extra/1", "")
		return st == 204
	})
	if st, _, _ := s.shardRequest("POST", "extra/1", "/add", "e"); st != 200 {
		t.Fatalf("the new kind from a client: %d", st)
	}

	// Graceful stop: every shard is let go of (the app closes each request's database when the request ends) and the lease is released.
	apptest.Eventually(t, apptest.Timeout, "the app to finish its requests and close their databases", func() bool { return s.appDebug().Serving == 0 })
	if err := s.node.Term(30 * time.Second); err != nil {
		t.Fatalf("node exit: %v\n%s", err, s.node.Output())
	}
	if l, err := v.files.Leases.Get(t.Context(), "n1"); err != nil || !l.Released {
		t.Fatalf("lease after shutdown: %+v %v\n%s", l, err, s.node.Output())
	}
	for _, shard := range v.shards() {
		if _, ok, err := v.tokens(shard); err != nil || !ok {
			t.Fatalf("restore %s: %v (ok=%v)", shard, err, ok)
		}
	}
}

// A request whose Shardlite-Target is missing or names another shard than the
// one its path is for is refused by the app with 400, and nothing is written to
// either shard. The client fixes its hint. A request with the right hint is
// served.
func TestRequestsForAnotherShardAreRefused(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	v := newVerifier(t, s)
	const p1, p2, p3 = "wallet/p1", "wallet/p2", "wallet/p3"
	s.create(t, p1, p2, p3)

	// The right hint is served, as the client's request: it has no caller.
	st, body, err := s.request("GET", at(p1, "/me"), p1, "")
	if err != nil || st != 200 || jsonField(t, body, "shard") != p1 || jsonField(t, body, "caller") != "" {
		t.Fatalf("me with the right hint: %d %q %v", st, body, err)
	}
	if st, body, err := s.request("POST", at(p2, "/add"), p2, "right"); err != nil || st != 200 || jsonField(t, body, "shard") != p2 || !v.has(p2, "right") {
		t.Fatalf("add with the right hint: %d %q %v", st, body, err)
	}
	served := s.appDebug().Served

	for _, tc := range []struct{ name, hint string }{{"no hint", ""}, {"a hint naming another shard", p3}} {
		t.Run(tc.name, func(t *testing.T) {
			if st, body, err := s.request("POST", at(p2, "/add"), tc.hint, "wrong"); err != nil || st != http.StatusBadRequest {
				t.Fatalf("add: %d %q %v, want 400", st, body, err)
			}
			if st, body, err := s.request("GET", at(p2, "/me"), tc.hint, ""); err != nil || st != http.StatusBadRequest {
				t.Fatalf("me: %d %q %v, want 400", st, body, err)
			}
			// A stream is refused before it starts.
			q, _ := http.NewRequest("GET", "http://"+s.pub+at(p2, "/events")+"?n=3&prefix=wrong", nil)
			if tc.hint != "" {
				q.Header.Set("Shardlite-Target", tc.hint)
			}
			resp, err := http.DefaultClient.Do(q)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("events: %d, want 400", resp.StatusCode)
			}
			if v.has(p2, "wrong") || v.has(p3, "wrong") || v.has(p2, "wrong-1") || v.has(p3, "wrong-1") {
				t.Fatal("a refused request wrote to a shard")
			}
			// The app never saw the request as one for an opened shard, so it counted
			// nothing: it refused before serving.
			if got := s.appDebug().Served; !maps.Equal(got, served) {
				t.Fatalf("the app served %v, had served %v", got, served)
			}
		})
	}

	// A hint is still checked by the node: a kind clients can't name is refused.
	if st, _, _ := s.request("GET", at(p2, "/me"), "audit/1", ""); st != 404 {
		t.Fatalf("a hint naming a private kind: %d", st)
	}
}

func isNotFound(err error) bool { return errors.Is(err, blob.ErrNotFound) }
