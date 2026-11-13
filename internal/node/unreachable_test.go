package node

import (
	"context"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/test/apptest"
)

// A killed owner: every request for its shards gets 503 with Retry-After: 1,
// gRPC ones UNAVAILABLE, each forward is counted as unanswered, and the owner
// cache keeps its entry while the lease scan counts the owner live.
func TestKilledOwnerFailsFast(t *testing.T) {
	c := newCluster(t, 2)
	n0, n1 := c.nodes[0], c.nodes[1]
	n0.create("users/k")
	ask(t, n0, "users/k")
	ask(t, n1, "users/k") // n1 has the owner and its address cached
	waitForLive(t, n1, 2)
	n0.Kill()

	var g sync.WaitGroup
	for range 20 {
		g.Go(func() {
			resp, body := post(t, n1, "users/k", "hi")
			if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "1" || !strings.Contains(body, "does not answer") {
				t.Errorf("%d (Retry-After %q) %q", resp.StatusCode, resp.Header.Get("Retry-After"), body)
			}
			wantReason(t, resp, reasonOwnerUnreachable)
		})
	}
	g.Wait()
	resp := grpcRequest(t, "http://"+n1.Public+"/svc/M", "users/k", nil)
	resp.Body.Close()
	if got := resp.Header.Get("Grpc-Status"); got != "14" {
		t.Fatalf("gRPC: grpc-status %q, want 14 (UNAVAILABLE)", got)
	}
	wantReason(t, resp, reasonOwnerUnreachable)
	if got := n1.m.Load().Stats.Unanswered.Load(); got != 21 {
		t.Fatalf("%d forwards counted as unanswered, want 21", got)
	}
	if owner, ok := n1.m.Load().owners.Get(shardName("users/k")); !ok || owner != "n0" {
		t.Fatalf("the cache names %q (%v), want n0 until the lease scan finds it dead", owner, ok)
	}
}

// An owner cache entry that names a node the lease scan already counts dead is
// what a request leaves behind when it adds its entry just after the scan
// emptied them. The forward to it gets 503 and the entry goes, so the next
// request claims the shard.
func TestEntryNamingADeadNodeGoesWithItsForward(t *testing.T) {
	c := newClusterEnv(t, 2, map[string]string{"SHARDLITE_LEASE_TTL": "1s"}, func(o *ShardsetOptions) {
		o.Leases.Every = 50 * time.Millisecond
	})
	n0, n1 := c.nodes[0], c.nodes[1]
	n0.Kill()
	m := n1.m.Load()
	waitCounter(t, "n0 to be adopted", &m.Stats.Adoptions, 1)
	if m.isLive("n0") {
		t.Fatal("the lease scan counts n0 live after it was adopted")
	}
	const name = "users/stale"
	n1.create(name)
	m.owners.Add(shardName(name), "n0")
	if resp, body := post(t, n1, name, ""); resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "1" || !strings.Contains(body, "does not answer") {
		t.Fatalf("the forward to the dead node: %d (Retry-After %q) %q", resp.StatusCode, resp.Header.Get("Retry-After"), body)
	}
	if got := m.Stats.Unanswered.Load(); got != 1 {
		t.Fatalf("%d forwards counted as unanswered, want 1", got)
	}
	if owner, ok := m.owners.Get(shardName(name)); ok {
		t.Fatalf("the cache still names %s", owner)
	}
	if resp, body := post(t, n1, name, ""); resp.StatusCode != 200 {
		t.Fatalf("after the entry went: %d %q", resp.StatusCode, body)
	}
}

// silentPeer is a node that accepts connections and never reads or answers on
// them, as a paused process's listener does. accepted is sent a value for each.
func silentPeer(t *testing.T) (addr string, accepted <-chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan struct{}, 16)
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		for _, c := range conns {
			c.Close()
		}
		mu.Unlock()
	})
	return ln.Addr().String(), ch
}

// A paused owner (its lease valid, its listener accepting but nothing answering)
// gets 503 after the answer timeout, even for a body larger than HTTP/2 lets
// out before the owner reads, and requests for another owner's shards are
// answered meanwhile.
func TestPausedOwnerFailsAfterTheAnswerTimeout(t *testing.T) {
	c := newCluster(t, 2)
	n0, n1 := c.nodes[0], c.nodes[1]
	n0.echoes()
	n0.create("users/q")
	ask(t, n0, "users/q")
	addr, accepted := silentPeer(t)
	reg := lease.NewRegistry(blob.WithPrefix(c.mem, n1.env.Prefix), "sleepy", lease.RegistryOptions{TTL: time.Hour})
	if _, err := reg.Acquire(context.Background(), lease.NodeInfo{Name: "sleepy", Address: addr}); err != nil {
		t.Fatal(err)
	}
	n1.believe("users/p", "sleepy", addr)

	type result struct {
		status int
		retry  string
	}
	paused := make(chan result, 1)
	go func() {
		resp, _ := post(t, n1, "users/p", strings.Repeat("x", 256<<10))
		paused <- result{resp.StatusCode, resp.Header.Get("Retry-After")}
	}()
	<-accepted
	if resp, body := post(t, n1, "users/q", "other"); resp.StatusCode != 200 || body != `n0 got "other"` {
		t.Fatalf("another owner's shard: %d %q", resp.StatusCode, body)
	}
	select {
	case r := <-paused:
		t.Fatalf("the paused owner's request ended (%d) before another owner's was answered", r.status)
	default:
	}
	if r := <-paused; r.status != http.StatusServiceUnavailable || r.retry != "1" {
		t.Fatalf("the paused owner's request: %d (Retry-After %q)", r.status, r.retry)
	}
	if got := n1.m.Load().Stats.Unanswered.Load(); got != 1 {
		t.Fatalf("%d forwards counted as unanswered, want 1", got)
	}
}

// A slow app on an owner that answers is not an unreachable owner: its node
// acknowledges the forward at once, and the app takes as long as it needs.
func TestSlowAppIsNotUnreachable(t *testing.T) {
	c := newCluster(t, 2)
	n0, n1 := c.nodes[0], c.nodes[1]
	n0.create("users/s")
	ask(t, n0, "users/s")
	ask(t, n1, "users/s")
	release := make(chan struct{})
	n0.serve(func(w http.ResponseWriter, q *http.Request) {
		select {
		case <-release:
			w.Write([]byte("late"))
		case <-q.Context().Done():
		}
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		if resp, body := post(t, n1, "users/s", ""); resp.StatusCode != 200 || body != "late" {
			t.Errorf("%d %q", resp.StatusCode, body)
		}
	}()
	// Past the answer timeout: the forward's own deadline is far off.
	answered := time.AfterFunc(1500*time.Millisecond, func() { close(release) })
	defer answered.Stop()
	<-done
}

// A killed owner with commits only in its journal: requests for its shard get
// 503 until it is adopted, and then the shard is claimed on the node the next
// request reached, which serves it with every commit.
func TestAdoptedOwnersShardIsClaimed(t *testing.T) {
	c := newClusterEnv(t, 2, map[string]string{"SHARDLITE_LEASE_TTL": "1s"}, func(o *ShardsetOptions) {
		o.Leases.Every = 50 * time.Millisecond
	})
	n0, n1 := c.nodes[0], c.nodes[1]
	app := apptest.Start(t)
	paths := make(chan string, 1)
	n0.serve(func(w http.ResponseWriter, q *http.Request) { paths <- q.Header.Get("Shardlite-File") })
	n0.create("wallet/a")
	ask(t, n0, "wallet/a")
	path := <-paths
	app.Must(t, "open", "a", path)
	app.Must(t, "insert", "a", "5", "64")
	if resp, body := send(t, "POST", "http://"+n0.Control+"/_shardlite/sync", nil, ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("sync: %d %q", resp.StatusCode, body)
	}
	n0.serve(func(http.ResponseWriter, *http.Request) {})
	ask(t, n1, "wallet/a") // n1 has the owner and its address cached
	n0.Kill()

	n1.create("users/none")
	if resp, _ := post(t, n1, "users/none", ""); resp.StatusCode != 200 {
		t.Fatalf("a shard of n1's own: %d", resp.StatusCode)
	}
	if resp, body := post(t, n1, "wallet/a", ""); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("before the adoption: %d %q", resp.StatusCode, body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := n1.m.Load().Stats.Adoptions.Wait(ctx, 1); err != nil {
		t.Fatalf("n0 was not adopted: %v", err)
	}
	n1.serve(func(w http.ResponseWriter, q *http.Request) {
		w.Header().Set("X-Served-By", "n1")
		paths <- q.Header.Get("Shardlite-File")
	})
	// The lease scan forgot n0's entries when it found n0 dead, so the request
	// reads the manifest and claims the shard.
	resp, body := post(t, n1, "wallet/a", "")
	if resp.StatusCode != 200 || resp.Header.Get("X-Served-By") != "n1" {
		t.Fatalf("after the adoption: %d %v %q", resp.StatusCode, resp.Header, body)
	}
	if got := apptest.Query(t, <-paths, "SELECT count(*) FROM kv"); strings.TrimSpace(got) != "5" {
		t.Fatalf("%q rows on n1, want 5", got)
	}
	if owner, ok := n1.m.Load().owners.Get(shardName("wallet/a")); ok {
		t.Fatalf("the cache still names %s", owner)
	}
}
