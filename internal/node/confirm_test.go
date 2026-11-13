package node

import (
	"context"
	"errors"
	"github.com/luci1900/shardlite/internal/test/apptest"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/storage/blob"
	"github.com/luci1900/shardlite/internal/storage/lease"
	"github.com/luci1900/shardlite/internal/test/blobtest"
)

// leaseProbe watches the node's lease in the store: it counts the reads of it,
// and can fail them, hold them back, or refuse the renewals (a node whose
// keeper hears of nothing). Calls with a bypass context (markLease) are not
// touched.
type leaseProbe struct {
	reads  atomic.Int64
	fail   atomic.Bool                   // reads fail
	paused atomic.Bool                   // renewals fail
	hold   atomic.Pointer[chan struct{}] // reads wait for it to close
}

type bypassKey struct{}

func probeLease(r *mrig) *leaseProbe {
	p := new(leaseProbe)
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if after || key != lease.Key("n1") || ctx.Value(bypassKey{}) != nil {
			return nil
		}
		if op == blobtest.OpGet {
			p.reads.Add(1)
			if p.fail.Load() {
				return errors.New("storage down")
			}
			if h := p.hold.Load(); h != nil {
				select {
				case <-*h:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		if op == blobtest.OpUpdate && p.paused.Load() {
			return errors.New("the renewal never reaches the store")
		}
		return nil
	})
	return p
}

// markLease has another node's adopter mark n1's lease, as a node that saw it
// go unrenewed for a TTL does.
func markLease(t *testing.T, r *mrig) {
	t.Helper()
	ctx := context.WithValue(context.Background(), bypassKey{}, true)
	reg := lease.NewRegistry(r.local, "n2", lease.RegistryOptions{TTL: time.Hour, Blob: blob.Options{Backoff: time.Millisecond}})
	l2, err := reg.Acquire(ctx, lease.NodeInfo{Name: "n2"})
	if err != nil {
		t.Fatal(err)
	}
	dead, err := reg.Get(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Adopt(ctx, *dead, l2.Incarnation()); err != nil {
		t.Fatal(err)
	}
}

// A storage answer with nothing to commit waits for a read of the node's own
// lease, and a confirmation on an idle node is one read. The response's later
// chunks, its body, ride on the headers' confirmation, and so do a write's.
func TestStorageAnswersConfirmOwnership(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	r.create("wallet/c1")
	probe := probeLease(r.mrig)
	reads := &probe.reads
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Query().Get("write") != "" {
			r.commit(q.Header.Get("Shardlite-File"))
		}
		io.WriteString(w, "done")
	})
	target := map[string]string{"Shardlite-Target": "wallet/c1"}

	// Nothing to commit: no cycle, and one read of the lease for the whole response.
	r.withoutCycle(func() {
		if resp, _ := r.req("GET", r.pub, "/r", target, ""); resp.StatusCode != 200 {
			t.Errorf("status %d", resp.StatusCode)
		}
	})
	if got := reads.Load(); got != 1 {
		t.Fatalf("%d lease reads for a read on an idle node, want 1", got)
	}
	// Another request proves for itself.
	r.withoutCycle(func() { r.req("GET", r.pub, "/r", target, "") })
	if got := reads.Load(); got != 2 {
		t.Fatalf("%d lease reads for two reads, want 2", got)
	}
	// A confirmation on an idle node is one read.
	if err := r.m.flush.confirm(context.Background(), r.m.flush.Mark()); err != nil {
		t.Fatal(err)
	}
	if got := reads.Load(); got != 3 {
		t.Fatalf("%d lease reads for one confirmation, want 1", got-2)
	}
	// A write waits for its segment, which confirms the headers, and the body
	// after it needs nothing more.
	r.withCycles(func() {
		if resp, _ := r.req("POST", r.pub, "/w?write=1", target, ""); resp.StatusCode != 200 {
			t.Errorf("status %d", resp.StatusCode)
		}
	})
	if got := reads.Load(); got != 3 {
		t.Fatalf("a write read the lease: %d reads", got-3)
	}
}

// A cycle that creates a segment confirms an answer that is waiting for the
// lease read, whichever comes first: the read is held back here.
func TestSegmentConfirmsAWaitingAnswer(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	r.create("wallet/c1", "wallet/c2")
	probe := probeLease(r.mrig)
	reads := &probe.reads
	hold := make(chan struct{})
	probe.hold.Store(&hold)
	var release atomic.Bool
	t.Cleanup(func() {
		if release.CompareAndSwap(false, true) {
			close(hold)
		}
	})
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Query().Get("write") != "" {
			r.commit(q.Header.Get("Shardlite-File"))
		}
		io.WriteString(w, "done")
	})
	done := make(chan int, 1)
	go func() {
		// The headers are the first chunk: the body would wait for the held read.
		q, _ := http.NewRequest("GET", r.pub+"/r", nil)
		q.Header.Set("Shardlite-Target", "wallet/c1")
		resp, err := r.client.Do(q)
		if err != nil {
			done <- 0
			return
		}
		resp.Body.Close()
		done <- resp.StatusCode
	}()
	apptest.Eventually(t, apptest.Timeout, "the lease read to begin", func() bool { return reads.Load() == 1 })
	// Another shard's write makes the next cycle create a segment, after the
	// read began its wait.
	go r.req("POST", r.pub, "/w?write=1", map[string]string{"Shardlite-Target": "wallet/c2"}, "")
	apptest.Eventually(t, apptest.Timeout, "the write to wait for a cycle", func() bool { return r.m.flush.waiting.Load() > 0 })
	r.cycle()
	select {
	case code := <-done:
		if code != 200 {
			t.Fatalf("status %d", code)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the segment did not confirm the answer while the lease read was held")
	}
}

// A node whose clocks saw no pause, and whose lease another node has marked, no
// longer answers on storage: the confirmation fails the node. The price of
// Shardlite-Sync: none is that it answers anyway.
func TestHiddenPauseFailsStorageReads(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	r.create("wallet/p1")
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if v := q.URL.Query().Get("sync"); v != "" {
			w.Header().Set("Shardlite-Sync", v)
		}
		io.WriteString(w, "done")
	})
	target := map[string]string{"Shardlite-Target": "wallet/p1"}
	probeLease(r.mrig).paused.Store(true)
	if resp, _ := r.req("GET", r.pub, "/r", target, ""); resp.StatusCode != 200 {
		t.Fatalf("before the mark: %d", resp.StatusCode)
	}

	markLease(t, r.mrig)
	// The keeper still believes its lease good: it has seen nothing.
	if err := r.n.Keeper.Check(); err != nil {
		t.Fatalf("the keeper noticed: %v", err)
	}
	if resp, _ := r.req("GET", r.pub, "/r?sync=none", target, ""); resp.StatusCode != 200 {
		t.Fatalf("a none read answered %d", resp.StatusCode)
	}
	if resp, _ := r.req("GET", r.pub, "/r", target, ""); resp.StatusCode == 200 {
		t.Fatal("a storage read answered with its lease marked by another node")
	}
	if err := r.n.Keeper.Check(); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("keeper: %v, want ErrLost", err)
	}
}

// A lease that can't be read fails the answer with a retry hint, never lets it
// through unconfirmed, and doesn't fail the node.
func TestUnconfirmedAnswerIsAnError(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	r.create("wallet/e1")
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if v := q.URL.Query().Get("sync"); v != "" {
			w.Header().Set("Shardlite-Sync", v)
		}
		io.WriteString(w, "done")
	})
	target := map[string]string{"Shardlite-Target": "wallet/e1"}
	fail := &probeLease(r.mrig).fail
	fail.Store(true)
	resp, body := r.req("GET", r.pub, "/r", target, "")
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("a storage read with the lease unreadable: %d %q", resp.StatusCode, body)
	}
	if err := r.n.Keeper.Check(); err != nil {
		t.Fatalf("the node failed: %v", err)
	}
	if resp, _ := r.req("GET", r.pub, "/r?sync=none", target, ""); resp.StatusCode != 200 {
		t.Fatalf("a none read answered %d", resp.StatusCode)
	}
	fail.Store(false)
	if resp, _ := r.req("GET", r.pub, "/r", target, ""); resp.StatusCode != 200 {
		t.Fatalf("after the store came back: %d", resp.StatusCode)
	}
}

// The barrier on an idle node confirms with one read of the lease, and fails the
// node once the lease is marked.
func TestBarrierConfirmsOwnership(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	probe := probeLease(r)
	reads := &probe.reads
	probe.paused.Store(true)
	ctx := context.Background()
	if err := r.m.Barrier(ctx); err != nil {
		t.Fatal(err)
	}
	if got := reads.Load(); got != 1 {
		t.Fatalf("%d lease reads for a barrier on an idle node, want 1", got)
	}
	markLease(t, r)
	if err := r.m.Barrier(ctx); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("Barrier = %v, want ErrLost", err)
	}
	if err := r.n.Keeper.Check(); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("keeper: %v, want ErrLost", err)
	}
}
