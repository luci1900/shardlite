package node

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
)

// A killed owner: every request for its shards gets 503 with Retry-After at
// once, gRPC ones UNAVAILABLE, and the requests share one read of its lease.
func TestKilledOwnerFailsFast(t *testing.T) {
	c := newClusterWith(t, 2, func(o *CatalogOptions) { o.OwnerCheck = time.Hour })
	n0, n1 := c.nodes[0], c.nodes[1]
	n0.create("test.users/k")
	ask(t, n0, "test.users/k")
	ask(t, n1, "test.users/k") // n1 has the owner and its address cached
	leases := n1.countReads(coord.Key("n0"))
	n0.Kill()

	var g sync.WaitGroup
	for range 20 {
		g.Go(func() {
			resp, body := post(t, n1, "test.users/k", "hi")
			if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "1" || !strings.Contains(body, "does not answer") {
				t.Errorf("%d (Retry-After %q) %q", resp.StatusCode, resp.Header.Get("Retry-After"), body)
			}
		})
	}
	g.Wait()
	resp := grpcRequest(t, "http://"+n1.Public+"/svc/M", "test.users/k", nil)
	resp.Body.Close()
	if got := resp.Header.Get("Grpc-Status"); got != "14" {
		t.Fatalf("gRPC: grpc-status %q, want 14 (UNAVAILABLE)", got)
	}
	if got := leases.Load(); got != 1 {
		t.Fatalf("%d reads of the dead owner's lease for 21 requests", got)
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
	n0.create("test.users/q")
	ask(t, n0, "test.users/q")
	addr, accepted := silentPeer(t)
	reg := coord.NewRegistry(blob.WithPrefix(c.mem, n1.env.Prefix), "sleepy", coord.RegistryOptions{TTL: time.Hour})
	if _, err := reg.Acquire(context.Background(), coord.NodeInfo{Name: "sleepy", Address: addr}); err != nil {
		t.Fatal(err)
	}
	n1.believe("test.users/p", "sleepy", addr)

	type result struct {
		status int
		retry  string
	}
	paused := make(chan result, 1)
	go func() {
		resp, _ := post(t, n1, "test.users/p", strings.Repeat("x", 256<<10))
		paused <- result{resp.StatusCode, resp.Header.Get("Retry-After")}
	}()
	<-accepted
	if resp, body := post(t, n1, "test.users/q", "other"); resp.StatusCode != 200 || body != `n0 got "other"` {
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
}

// A slow app on an owner that answers is not an unreachable owner: its node
// acknowledges the forward at once, and the app takes as long as it needs.
func TestSlowAppIsNotUnreachable(t *testing.T) {
	c := newCluster(t, 2)
	n0, n1 := c.nodes[0], c.nodes[1]
	n0.create("test.users/s")
	ask(t, n0, "test.users/s")
	ask(t, n1, "test.users/s")
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
		if resp, body := post(t, n1, "test.users/s", ""); resp.StatusCode != 200 || body != "late" {
			t.Errorf("%d %q", resp.StatusCode, body)
		}
	}()
	// Past the answer timeout: the forward's own deadline is far off.
	answered := time.AfterFunc(1500*time.Millisecond, func() { close(release) })
	defer answered.Stop()
	<-done
}

// The owner checks: concurrent requests share a read, a read is used for the
// period, the answer follows the lease, and a node is warned about when it is
// first found unreachable and when it is first seen dead.
func TestOwnerChecks(t *testing.T) { synctest.Test(t, testOwnerChecks) }

func testOwnerChecks(t *testing.T) {
	now := time.Now()
	var reads atomic.Int32
	lease := &coord.NodeLease{NodeInfo: coord.NodeInfo{Name: "n1", Address: "a:1"}, Epoch: 3, Renewed: now, TTL: 10 * time.Second}
	var readErr error
	unblock := make(chan struct{})
	var logs bytes.Buffer
	c := &ownerChecks{
		get: func(context.Context, cluster.Node) (*coord.NodeLease, error) {
			reads.Add(1)
			<-unblock
			if readErr != nil {
				return nil, readErr
			}
			l := *lease
			return &l, nil
		},
		now:   func() time.Time { return now },
		every: time.Second,
		log:   slog.New(slog.NewTextHandler(&logs, nil)),
	}
	var g sync.WaitGroup
	for range 10 {
		g.Go(func() { c.check(context.Background(), "n1") })
	}
	synctest.Wait() // one reading, the others waiting for it
	close(unblock)
	g.Wait()
	if got := reads.Load(); got != 1 {
		t.Fatalf("%d reads for 10 concurrent checks", got)
	}
	c.check(context.Background(), "n1")
	if got := reads.Load(); got != 1 {
		t.Fatalf("%d reads within the period", got)
	}
	warnings := func() int { return strings.Count(logs.String(), "level=WARN") }
	if warnings() != 1 {
		t.Fatalf("logs after the first check:\n%s", logs.String())
	}

	answer := func(wantRetry time.Duration, wantAdopted bool) {
		t.Helper()
		now = now.Add(time.Second) // past the period: read again
		retry, adopted := c.check(context.Background(), "n1").answer(now)
		if retry != wantRetry || adopted != wantAdopted {
			t.Fatalf("Retry-After %v (adopted %v), want %v (%v)", retry, adopted, wantRetry, wantAdopted)
		}
	}
	answer(time.Second, false) // valid
	if warnings() != 1 {
		t.Fatalf("a check of a node still unreachable warned:\n%s", logs.String())
	}
	// Marked by an adopter (dead) though its holder's expiry is 8 s away (at the
	// next check's time, a second on).
	lease.Renewed = now.Add(-time.Second)
	lease.AdoptedBy = &coord.Incarnation{Node: "n2", Epoch: 1}
	answer(9*time.Second, false)
	if warnings() != 2 {
		t.Fatalf("no warning once the node is dead:\n%s", logs.String())
	}
	lease.AdoptedBy = nil
	lease.Renewed = now.Add(-time.Minute) // expired, nobody adopting yet
	answer(time.Second, false)
	lease.Adopted = 3
	answer(time.Second, true)
	readErr = blob.ErrNotFound
	answer(time.Second, true)
	readErr = errors.New("slow store")
	answer(time.Second, false)
	if warnings() != 2 {
		t.Fatalf("warned again for a node already dead:\n%s", logs.String())
	}

	// A forward answered: the next time the node is unreachable warns again.
	c.answered("n1")
	readErr = nil
	c.check(context.Background(), "n1")
	if warnings() != 3 {
		t.Fatalf("no warning after the node answered and went quiet again:\n%s", logs.String())
	}
}

// A killed owner with commits only in its journal: requests for its shard get
// 503 until it is adopted, and then the shard is claimed on the node the next
// request reached, which serves it with every commit.
func TestAdoptedOwnersShardIsClaimed(t *testing.T) {
	c := newClusterEnv(t, 2, map[string]string{"SHARDLITE_LEASE_TTL": "1s"}, func(o *CatalogOptions) {
		o.Leases.Every = 50 * time.Millisecond
		o.OwnerCheck = time.Millisecond // each request reads the lease again
	})
	n0, n1 := c.nodes[0], c.nodes[1]
	app := apptest.Start(t)
	paths := make(chan string, 1)
	n0.serve(func(w http.ResponseWriter, q *http.Request) { paths <- q.Header.Get("Shardlite-File") })
	n0.create("test.wallet/a")
	ask(t, n0, "test.wallet/a")
	path := <-paths
	app.Must(t, "open", "a", path)
	app.Must(t, "insert", "a", "5", "64")
	if resp, body := send(t, "POST", "http://"+n0.Control+"/_shardlite/sync", nil, ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("sync: %d %q", resp.StatusCode, body)
	}
	n0.serve(func(http.ResponseWriter, *http.Request) {})
	ask(t, n1, "test.wallet/a") // n1 has the owner and its address cached
	n0.Kill()

	n1.create("test.users/none")
	if resp, _ := post(t, n1, "test.users/none", ""); resp.StatusCode != 200 {
		t.Fatalf("a shard of n1's own: %d", resp.StatusCode)
	}
	if resp, body := post(t, n1, "test.wallet/a", ""); resp.StatusCode != http.StatusServiceUnavailable {
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
	// The lease scan forgot n0's entries when it found n0 dead. One put back by a
	// request that raced it gets 503 to try again, and goes.
	n1.m.Load().owners.Add(shardName("test.wallet/a"), "n0")
	resp, body := post(t, n1, "test.wallet/a", "")
	moved(t, resp, body)
	resp, body = post(t, n1, "test.wallet/a", "")
	if resp.StatusCode != 200 || resp.Header.Get("X-Served-By") != "n1" {
		t.Fatalf("after the adoption: %d %v %q", resp.StatusCode, resp.Header, body)
	}
	if got := apptest.Query(t, <-paths, "SELECT count(*) FROM kv"); strings.TrimSpace(got) != "5" {
		t.Fatalf("%q rows on n1, want 5", got)
	}
	if owner, ok := n1.m.Load().owners.Get(shardName("test.wallet/a")); ok {
		t.Fatalf("the cache still names %s", owner)
	}
}
