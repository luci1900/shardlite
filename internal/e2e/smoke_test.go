package e2e

import (
	"bufio"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/e2eapp"
)

// Every protocol through the real node and app binaries, with the durability
// check at the moment each response, event or message reaches the client.
func TestProtocolsThroughTheNode(t *testing.T) {
	s := newStack(t)
	v := newVerifier(t, s)
	mustHave := func(what, shard string, tokens ...string) {
		t.Helper()
		if !v.has(shard, tokens...) {
			t.Fatalf("%s reached the client, but blob storage doesn't have %v for %s", what, tokens, shard)
		}
	}

	// Plain HTTP.
	st, body, err := s.request("POST", "/add", "users/u1", "t1")
	if err != nil || st != 200 || jsonField(t, body, "count") != float64(1) {
		t.Fatalf("add: %d %q %v", st, body, err)
	}
	if st, body, _ := s.request("GET", "/count", "users/u1", ""); st != 200 || jsonField(t, body, "count") != float64(1) {
		t.Fatalf("count: %d %q", st, body)
	}
	if st, body, _ := s.request("GET", "/tokens?shardlite-target=users/u1", "", ""); st != 200 || body != "t1" {
		t.Fatalf("tokens via the query parameter: %d %q", st, body)
	}

	// A shard that is only read is created on activation and deleted when it
	// goes idle, so it leaves nothing durable behind.
	if st, body, _ := s.request("GET", "/count", "users/fresh", ""); st != 200 || jsonField(t, body, "count") != float64(0) {
		t.Fatalf("count of a new shard: %d %q", st, body)
	}
	eventually(t, "the shard that was only read to be deleted", func() bool {
		_, _, err := v.files.GetManifest(t.Context(), "users/fresh")
		return isNotFound(err)
	})

	// Sync on: the response comes after the write is in blob storage.
	if st, _, _ := s.request("POST", "/add", "wallet/w1", "pay-1"); st != 200 {
		t.Fatalf("add: %d", st)
	}
	mustHave("a response", "wallet/w1", "pay-1")

	// Server-sent events.
	q, _ := http.NewRequest("GET", "http://"+s.pub+"/events?n=3&prefix=sse", nil)
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
	ws, err := e2eapp.DialWS(s.pub, "/ws", http.Header{"Shardlite-Target": {"wallet/w3"}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		tok := fmt.Sprintf("ws-%d", i)
		ws.WriteText(tok)
		if echo, err := ws.ReadText(); err != nil || echo != "ok:"+tok {
			t.Fatalf("echo: %q %v", echo, err)
		}
		mustHave("an echo", "wallet/w3", tok)
	}
	ws.Close()

	// An ephemeral kind is served over HTTP and WebSocket like any other, and
	// ships nothing: its shard has an ephemeral manifest with no files.
	if st, body, err := s.request("POST", "/add", "lobby/p1", "e-1"); err != nil || st != 200 || jsonField(t, body, "count") != float64(1) {
		t.Fatalf("add to an ephemeral shard: %d %q %v", st, body, err)
	}
	if st, body, _ := s.request("GET", "/count", "lobby/p1", ""); st != 200 || jsonField(t, body, "count") != float64(1) {
		t.Fatalf("count of an ephemeral shard: %d %q", st, body)
	}
	ews, err := e2eapp.DialWS(s.pub, "/ws", http.Header{"Shardlite-Target": {"lobby/p2"}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		tok := fmt.Sprintf("ews-%d", i)
		ews.WriteText(tok)
		if echo, err := ews.ReadText(); err != nil || echo != "ok:"+tok {
			t.Fatalf("echo from an ephemeral shard: %q %v", echo, err)
		}
	}
	ews.Close()
	for _, shard := range []string{"lobby/p1", "lobby/p2"} {
		if m, _, err := v.files.GetManifest(t.Context(), shard); err != nil || !m.Ephemeral || len(m.Files) != 0 || m.Journal != "" {
			t.Fatalf("manifest of %s: %+v %v, want an ephemeral one with no files", shard, m, err)
		}
	}

	// gRPC: unary and server-streaming, with trailers, over h2c on both sides of the node.
	g := &e2eapp.GRPCClient{HTTP: e2eapp.NewH2CClient(), Base: "http://" + s.pub}
	ctx, cancel := e2eapp.WithTimeout(20 * time.Second)
	defer cancel()
	h := http.Header{"Shardlite-Target": {"wallet/w4"}}
	msgs, err := g.Call(ctx, "/e2e.Shards/Add", h, []byte("rpc-1"), nil)
	if err != nil || len(msgs) != 1 || string(msgs[0]) != "ok:rpc-1" {
		t.Fatalf("unary: %q %v", msgs, err)
	}
	mustHave("a unary reply", "wallet/w4", "rpc-1")
	n := 0
	_, err = g.Call(ctx, "/e2e.Shards/Watch", h, []byte("watch 3"), func(m []byte) {
		n++
		mustHave("a streamed reply", "wallet/w4", strings.TrimPrefix(string(m), "ok:"))
	})
	if err != nil || n != 3 {
		t.Fatalf("stream: %d messages, %v", n, err)
	}
	// Errors the node makes are gRPC statuses.
	var se *e2eapp.GRPCStatusError
	_, err = g.Call(ctx, "/e2e.Shards/Add", http.Header{"Shardlite-Target": {"typo/1"}}, []byte("x"), nil)
	if !asStatus(err, &se) || se.Code != 3 {
		t.Fatalf("undeclared kind over gRPC: %v", err)
	}
	// A kind that isn't public gets what an undeclared kind gets.
	_, err = g.Call(ctx, "/e2e.Shards/Add", http.Header{"Shardlite-Target": {"audit/1"}}, []byte("x"), nil)
	if !asStatus(err, &se) || se.Code != 3 {
		t.Fatalf("private kind over gRPC: %v", err)
	}

	// A shardless login answers with a URL naming the shard.
	st, body, _ = s.request("POST", "/login", "", "alice")
	url, _ := jsonField(t, body, "url").(string)
	if st != 200 || url != "/me?shardlite-target=users%2Falice" {
		t.Fatalf("login: %d %q", st, body)
	}
	if st, body, _ = s.request("GET", url, "", ""); st != 200 || jsonField(t, body, "shard") != "users/alice" {
		t.Fatalf("following the URL: %d %q", st, body)
	}

	// A call to another shard through the control API, gated at the caller.
	st, _, _ = s.request("POST", "/transfer?to=wallet/w6&token=x1", "wallet/w5", "")
	if st != 200 {
		t.Fatalf("transfer: %d", st)
	}
	mustHave("a transfer", "wallet/w5", "debit:x1")
	mustHave("a transfer", "wallet/w6", "credit:x1")
	// Internal endpoints refuse requests that didn't come from another shard.
	if st, _, _ := s.request("POST", "/credit", "wallet/w6", "x2"); st != 403 {
		t.Fatalf("/credit from a client: %d", st)
	}

	// Refusals.
	if st, _, _ := s.request("GET", "/count", "audit/1", ""); st != 400 { // not public, like an undeclared kind
		t.Fatalf("private kind: %d", st)
	}
	if st, _, _ := s.request("GET", "/count", "typo/1", ""); st != 400 {
		t.Fatalf("undeclared kind: %d", st)
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

	// Kinds are reloaded on SIGHUP.
	if st, _, _ := s.request("POST", "/add", "extra/1", "e"); st != 400 {
		t.Fatalf("a kind that isn't declared yet: %d", st)
	}
	kinds := strings.Replace(kindsJSON, `"audit"`, `"extra": {"public": true}, "audit"`, 1)
	if err := os.WriteFile(filepath.Join(s.dir, "kinds.json"), []byte(kinds), 0o644); err != nil {
		t.Fatal(err)
	}
	s.node.signal(syscall.SIGHUP)
	eventually(t, "the new kind", func() bool { st, _, _ := s.request("POST", "/add", "extra/1", "e"); return st == 200 })

	// Delete: refused while the app has the shard open, done once it has closed it.
	eventually(t, "delete", func() bool {
		st, _, _ := s.control("DELETE", "/_shardlite/shards/users/u1", "", "")
		return st == 204
	})
	if _, _, err := v.files.GetManifest(t.Context(), "users/u1"); !isNotFound(err) {
		t.Fatalf("manifest after delete: %v", err)
	}

	// Graceful stop: every shard is let go of (the app closes its handles when idle) and the lease is released.
	time.Sleep(time.Second)
	if err := s.node.term(30 * time.Second); err != nil {
		t.Fatalf("node exit: %v\n%s", err, s.node.out.String())
	}
	if l, err := v.files.Leases.Get(t.Context(), "n1"); err != nil || !l.Released {
		t.Fatalf("lease after shutdown: %+v %v", l, err)
	}
	for _, shard := range v.shards() {
		if _, ok, err := v.tokens(shard); err != nil || !ok {
			t.Fatalf("restore %s: %v (ok=%v)", shard, err, ok)
		}
	}
}

func asStatus(err error, target **e2eapp.GRPCStatusError) bool {
	se, ok := err.(*e2eapp.GRPCStatusError)
	if ok {
		*target = se
	}
	return ok
}

func eventually(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func isNotFound(err error) bool { return errors.Is(err, blob.ErrNotFound) }
