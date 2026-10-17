package node

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
)

// echoes makes the node's app answer every request with its name and the body it got.
func (n *clusterNode) echoes() *atomic.Int32 {
	var served atomic.Int32
	name := n.env.Name
	n.serve(func(w http.ResponseWriter, q *http.Request) {
		b, _ := io.ReadAll(q.Body)
		served.Add(1)
		w.Header().Set("X-Served-By", name)
		fmt.Fprintf(w, "%s got %q", name, b)
	})
	return &served
}

// post sends body to target through nd's public listener.
func post(t *testing.T, nd *clusterNode, target, body string) (*http.Response, string) {
	t.Helper()
	return send(t, "POST", "http://"+nd.Public+"/", map[string]string{"Shardlite-Target": target}, body)
}

// moveShard takes a shard from one node to another: the first deletes it (its app
// keeps no file) and the second creates it again.
func moveShard(t *testing.T, from, to *clusterNode, target string) {
	t.Helper()
	if resp, body := send(t, "DELETE", "http://"+from.Control+"/_shardlite/shards/"+target, nil, ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete on %s: %d %q", from.env.Name, resp.StatusCode, body)
	}
	ask(t, to, target)
}

// peer starts a fake node's peer listener (h2c, as the nodes' are) and returns its address.
func peer(t *testing.T, h http.HandlerFunc) string {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	srv.Config.Protocols = new(http.Protocols)
	srv.Config.Protocols.SetHTTP1(true)
	srv.Config.Protocols.SetUnencryptedHTTP2(true)
	srv.Start()
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String()
}

// believe makes the node think node owns target and listens at addr, as it
// would after the shard was there once.
func (n *clusterNode) believe(target, node, addr string) {
	m := n.m.Load()
	m.addrs.mu.Lock()
	if m.addrs.leases == nil {
		m.addrs.leases = map[string]*coord.NodeLease{}
	}
	m.addrs.leases[node] = &coord.NodeLease{NodeInfo: coord.NodeInfo{Name: node, Address: addr}, Renewed: time.Now(), TTL: time.Hour}
	m.addrs.mu.Unlock()
	m.owners.Add(target, node)
}

// moved421 is a peer that has read the whole request, then says the shard is not
// its own and names owner (and sends the header with no value if owner is empty).
func moved421(t *testing.T, owner string, read func([]byte)) string {
	return peer(t, func(w http.ResponseWriter, q *http.Request) {
		b, _ := io.ReadAll(q.Body)
		if read != nil {
			read(b)
		}
		w.Header().Set(hdrOwner, owner)
		w.WriteHeader(http.StatusMisdirectedRequest)
	})
}

// A shard that moved: the forward to its old owner gets 421 naming the new one,
// the request is sent there once with its body, and the cache names it.
func TestStaleOwnerIsCorrected(t *testing.T) {
	c := newCluster(t, 3)
	n0, n1, n2 := c.nodes[0], c.nodes[1], c.nodes[2]
	n0stale := n0.echoes()
	n2.echoes()
	post(t, n0, "users/m", "")
	if resp, _ := post(t, n1, "users/m", ""); resp.Header.Get("X-Served-By") != "n0" {
		t.Fatalf("before the move: %v", resp.Header)
	}
	moveShard(t, n0, n2, "users/m")
	before := n0stale.Load()
	resp, body := post(t, n1, "users/m", "hello")
	if resp.StatusCode != 200 || resp.Header.Get("X-Served-By") != "n2" || body != `n2 got "hello"` {
		t.Fatalf("after the move: %d %v %q", resp.StatusCode, resp.Header, body)
	}
	if n0stale.Load() != before {
		t.Fatal("the old owner's app saw the request")
	}
	if owner, _ := n1.m.Load().owners.Get("users/m"); owner != "n2" {
		t.Fatalf("the cache names %q", owner)
	}
}

// A 421 that names no owner: the forwarding node asks for itself.
func TestMovedWithoutAnOwner(t *testing.T) {
	c := newCluster(t, 3)
	n1, n2 := c.nodes[1], c.nodes[2]
	n2.echoes()
	ask(t, n2, "users/w")
	n1.believe("users/w", "ghost", moved421(t, "", nil))
	resp, body := post(t, n1, "users/w", "hi")
	if resp.StatusCode != 200 || body != `n2 got "hi"` {
		t.Fatalf("%d %q", resp.StatusCode, body)
	}
	if owner, _ := n1.m.Load().owners.Get("users/w"); owner != "n2" {
		t.Fatalf("the cache names %q", owner)
	}
}

// A stale entry for a shard this node owns: the old owner's 421 names this
// node, which serves the request itself and drops the entry.
func TestStaleEntryNamingThisNode(t *testing.T) {
	c := newCluster(t, 2)
	n0, n1 := c.nodes[0], c.nodes[1]
	stale := n0.echoes()
	n1.echoes()
	ask(t, n1, "users/t")
	n1.m.Load().owners.Add("users/t", "n0")
	resp, body := post(t, n1, "users/t", "mine")
	if resp.StatusCode != 200 || resp.Header.Get("X-Served-By") != "n1" || body != `n1 got "mine"` {
		t.Fatalf("%d %v %q", resp.StatusCode, resp.Header, body)
	}
	if stale.Load() != 0 {
		t.Fatal("the other node's app saw the request")
	}
	if owner, ok := n1.m.Load().owners.Get("users/t"); ok {
		t.Fatalf("the cache still names %q", owner)
	}
}

// A body within the limit is sent again as it was, and one past it is answered 503.
func TestBodyReplayLimit(t *testing.T) {
	c := newCluster(t, 3)
	n1, n2 := c.nodes[1], c.nodes[2]
	n2.echoes()
	ask(t, n2, "users/b")
	for _, tc := range []struct {
		body   string
		status int
	}{{strings.Repeat("a", replayLimit), 200}, {strings.Repeat("b", replayLimit+1), 503}} {
		var read atomic.Int32
		addr := moved421(t, "n2", func(b []byte) { read.Add(int32(len(b))) })
		n1.believe("users/b", "ghost", addr)
		resp, body := post(t, n1, "users/b", tc.body)
		if int(read.Load()) != len(tc.body) {
			t.Fatalf("the first owner read %d bytes of %d", read.Load(), len(tc.body))
		}
		switch {
		case resp.StatusCode != tc.status:
			t.Fatalf("a body of %d bytes: %d %q", len(tc.body), resp.StatusCode, body)
		case tc.status == 200 && body != fmt.Sprintf("n2 got %q", tc.body):
			t.Fatalf("the body arrived as %q", body)
		case tc.status == 503 && resp.Header.Get("Retry-After") == "":
			t.Fatal("503 without Retry-After")
		}
	}
}

// A gated call that is retried after a 421: nothing of its body leaves before
// the caller's commits are durable, and the new owner gets all of it.
func TestRetriedCallIsGatedAndIntact(t *testing.T) {
	c := newClusterWith(t, 2, manualFlush)
	n0, n1 := c.nodes[0], c.nodes[1]
	t.Cleanup(func() { cycling(func() { n0.Stop(); n1.Stop() }, n0, n1) })
	app := apptest.Start(t)
	paths := make(chan string, 2)
	n0.serve(func(w http.ResponseWriter, q *http.Request) { paths <- q.Header.Get("Shardlite-Path") })
	got := make(chan string, 1)
	n1.serve(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path != "/call" {
			return
		}
		b, _ := io.ReadAll(q.Body)
		got <- string(b)
		io.WriteString(w, "answer")
	})
	var caller string
	cycling(func() {
		send(t, "GET", "http://"+n0.Public+"/", map[string]string{"Shardlite-Target": "wallet/c"}, "")
		caller = <-paths
		send(t, "GET", "http://"+n1.Public+"/", map[string]string{"Shardlite-Target": "wallet/d"}, "")
	}, n0, n1)
	app.Must(t, "open", caller, caller)
	defer app.Must(t, "close", caller)
	app.Must(t, "insert", caller, "1", "64")
	before := n0.cycles()
	atFirst := make(chan uint64, 1)
	n0.believe("wallet/d", "ghost", moved421(t, "n1", func([]byte) { atFirst <- n0.cycles() }))
	answered := make(chan string, 1)
	go func() {
		q, _ := http.NewRequest("POST", "http://"+n0.Control+"/call", strings.NewReader("the call"))
		q.Header.Set("Shardlite-Target", "wallet/d")
		q.Header.Set("Shardlite-Caller", "wallet/c")
		resp, err := clusterClient.Do(q)
		if err != nil {
			answered <- err.Error()
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		answered <- fmt.Sprintf("%d %s", resp.StatusCode, b)
	}()
	for done := false; !done; {
		select {
		case cycles := <-atFirst:
			if cycles == before {
				t.Fatal("the call left the node before the caller's commit was durable")
			}
			done = true
		case a := <-answered:
			t.Fatalf("answered before the first forward was read: %s", a)
		case <-time.After(20 * time.Millisecond):
			n0.cycle()
		}
	}
	for {
		select {
		case b := <-got:
			if b != "the call" {
				t.Fatalf("the new owner got %q", b)
			}
		case a := <-answered:
			if a != "200 answer" {
				t.Fatalf("the call: %s", a)
			}
			return
		case <-time.After(20 * time.Millisecond):
			n0.cycle()
			n1.cycle()
		}
	}
}

// A shard that moves again before the retry: the second 421 is answered 503, and
// the request isn't sent a third time.
func TestMovesAgainBeforeTheRetry(t *testing.T) {
	c := newCluster(t, 4)
	n0, n1, n2, n3 := c.nodes[0], c.nodes[1], c.nodes[2], c.nodes[3]
	n0.echoes()
	n2.echoes()
	last := n3.echoes()
	post(t, n0, "users/m", "")
	post(t, n1, "users/m", "") // n1 caches n0
	moveShard(t, n0, n2, "users/m")
	// The retry's address read for n2 is held while the shard moves to n3.
	var held atomic.Bool
	blocked, moved := make(chan struct{}), make(chan struct{})
	n1.onStore(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if !after && op == blob.OpGet && strings.HasSuffix(key, coord.Key("n2")) && held.CompareAndSwap(false, true) {
			close(blocked)
			select {
			case <-moved:
			case <-ctx.Done():
			}
		}
		return nil
	})
	answered := make(chan *http.Response, 1)
	go func() {
		resp, _ := post(t, n1, "users/m", "x")
		answered <- resp
	}()
	<-blocked
	moveShard(t, n2, n3, "users/m")
	seen := last.Load()
	close(moved)
	resp := <-answered
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("a shard that moved twice: %d %v", resp.StatusCode, resp.Header)
	}
	if last.Load() != seen {
		t.Fatal("the request was sent a third time")
	}
	if owner, _ := n1.m.Load().owners.Get("users/m"); owner != "n3" {
		t.Fatalf("the cache names %q", owner)
	}
}

// A 421 that comes with the deadline gone: the retry isn't started.
func TestRetryIsNotStartedPastTheDeadline(t *testing.T) {
	c := newCluster(t, 3)
	n0, n1, n2 := c.nodes[0], c.nodes[1], c.nodes[2]
	n0.echoes()
	served := n2.echoes()
	post(t, n0, "users/dl", "")
	post(t, n1, "users/dl", "")
	moveShard(t, n0, n2, "users/dl")
	before := served.Load()
	// n1 has to read n2's address for the retry, and that waits for the deadline.
	n1.onStore(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if !after && op == blob.OpGet && strings.HasSuffix(key, coord.Key("n2")) {
			<-ctx.Done()
		}
		return nil
	})
	resp := grpcRequest(t, "http://"+n1.Public+"/svc/M", "users/dl", map[string]string{"Grpc-Timeout": "300m"})
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Grpc-Status") != "4" {
		t.Fatalf("a retry past the deadline: %d %v", resp.StatusCode, resp.Header)
	}
	if served.Load() != before {
		t.Fatal("the retry reached the new owner")
	}
}

// What a view of a replayed body reads: the same bytes every time, until more
// was read than is kept.
func TestReplayBody(t *testing.T) {
	b := newReplayBody(strings.NewReader("0123456789"), 6)
	buf := make([]byte, 4)
	v1 := b.view()
	if n, _ := v1.Read(buf); string(buf[:n]) != "0123" {
		t.Fatalf("first read %q", buf[:n])
	}
	v2 := b.view()
	if n, _ := v2.Read(buf); string(buf[:n]) != "0123" || !b.replayable() {
		t.Fatalf("a second view read %q", buf[:n])
	}
	all, err := io.ReadAll(v1) // 4 more bytes: past the 6 kept
	if string(all) != "456789" || err != nil {
		t.Fatalf("rest %q %v", all, err)
	}
	if b.replayable() {
		t.Fatal("replayable past the limit")
	}
	if _, err := io.ReadAll(b.view()); err == nil {
		t.Fatal("a new view read a body that was not kept")
	}
	if _, err := io.ReadAll(v2); err != errNotKept { // it has read all that was kept, and the rest is gone
		t.Fatalf("a view at the end of the kept bytes: %v", err)
	}
}
