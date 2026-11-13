package lease_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/model/pos"
	"github.com/luci1900/shardlite/internal/storage/blob"
	"github.com/luci1900/shardlite/internal/storage/lease"
	"github.com/luci1900/shardlite/internal/test/blobtest"
)

func newLocal(t *testing.T) *blobtest.Hooked {
	t.Helper()
	return blobtest.WithHook(blob.NewMemory())
}

// serve serves k, as the node's tree does, and returns the func that stops it
// and waits for it to end.
func serve(k *lease.Keeper) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	var g errgroup.Group
	g.Go(func() error { return k.Serve(ctx) })
	return func() { cancel(); g.Wait() }
}

func opts() lease.RegistryOptions {
	return lease.RegistryOptions{TTL: 10 * time.Second, Blob: blob.Options{Backoff: time.Millisecond}}
}

func TestEpochsOnlyGoUp(t *testing.T) {
	t.Parallel()
	l, ctx := newLocal(t), context.Background()
	reg := lease.NewRegistry(l, "n1", opts())

	a, err := reg.Acquire(ctx, lease.NodeInfo{Name: "n1", Address: "10.0.0.1:7000"})
	if err != nil {
		t.Fatal(err)
	}
	if a.Epoch != 1 || a.Previous != nil || a.JournalFrom != 1 || a.JournalNext != 1 {
		t.Fatalf("first lease: %+v", a)
	}
	// The range moves with renewals.
	a, err = reg.Renew(ctx, a, lease.NodeInfo{Name: "n1", Address: "10.0.0.1:7000", JournalFrom: 40, JournalNext: 52})
	if err != nil {
		t.Fatal(err)
	}
	got, err := reg.Get(ctx, "n1")
	if err != nil || got.JournalFrom != 40 || got.JournalNext != 52 || got.Epoch != 1 || got.Address != "10.0.0.1:7000" {
		t.Fatalf("get: %+v %v", got, err)
	}

	// A new life takes the lease even though the old one is valid, and keeps its range.
	b, err := reg.Acquire(ctx, lease.NodeInfo{Name: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	if b.Epoch != 2 || b.Previous == nil || b.Previous.Epoch != 1 || b.JournalFrom != 40 || b.JournalNext != 52 {
		t.Fatalf("second lease: %+v prev %+v", b, b.Previous)
	}
	// The old life can no longer renew.
	if _, err := reg.Renew(ctx, a, a.NodeInfo); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("old lease renewed: %v", err)
	}

	// Releasing keeps the epoch, so the next life doesn't reuse it.
	if err := reg.Release(ctx, b); err != nil {
		t.Fatal(err)
	}
	rel, err := reg.Get(ctx, "n1")
	if err != nil || !rel.Released || rel.ValidAt(time.Now()) || rel.Epoch != 2 {
		t.Fatalf("released: %+v %v", rel, err)
	}
	c, err := reg.Acquire(ctx, lease.NodeInfo{Name: "n1"})
	if err != nil || c.Epoch != 3 {
		t.Fatalf("after release: %+v %v", c, err)
	}

	if _, err := reg.Get(ctx, "nobody"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("get of a node that never had a lease: %v", err)
	}
}

func TestRenewSurvivesALostResponse(t *testing.T) {
	t.Parallel()
	l, ctx := newLocal(t), context.Background()
	reg := lease.NewRegistry(l, "n1", opts())
	a, err := reg.Acquire(ctx, lease.NodeInfo{Name: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	// The write lands but the answer is lost, twice: blob.Store reads the write id back.
	var lost atomic.Int32
	l.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpUpdate && after && lost.Add(1) <= 2 {
			return errors.New("response lost")
		}
		return nil
	})
	b, err := reg.Renew(ctx, a, a.NodeInfo)
	if err != nil {
		t.Fatalf("renew with a lost response: %v", err)
	}
	// And the next renewal carries on from the new version.
	l.SetHook(nil)
	if _, err := reg.Renew(ctx, b, b.NodeInfo); err != nil {
		t.Fatal(err)
	}
}

func keeperOpts(info func() lease.NodeInfo) lease.KeeperOptions {
	return lease.KeeperOptions{Interval: 3 * time.Second, Margin: 2 * time.Second, Info: info}
}

func TestKeeperRenewsWhileStorageWorks(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		l, ctx := newLocal(t), context.Background()
		reg := lease.NewRegistry(l, "n1", opts())
		rec, err := reg.Acquire(ctx, lease.NodeInfo{Name: "n1"})
		if err != nil {
			t.Fatal(err)
		}
		var next atomic.Int64
		k := lease.NewKeeper(reg, rec, keeperOpts(func() lease.NodeInfo {
			return lease.NodeInfo{Name: "n1", JournalNext: pos.Seg(next.Load())}
		}))
		defer serve(k)()

		for range 20 {
			time.Sleep(3 * time.Second)
			next.Add(1)
			if err := k.Check(); err != nil {
				t.Fatalf("at %v: %v", time.Now(), err)
			}
		}
		got, err := reg.Get(ctx, "n1")
		if err != nil {
			t.Fatal(err)
		}
		if got.JournalNext < 15 || !got.ValidAt(time.Now()) {
			t.Fatalf("lease in storage: %+v", got)
		}
		if k.Lease().Renewed.Before(time.Now().Add(-4 * time.Second)) {
			t.Fatalf("lease renewed at %v, now %v", k.Lease().Renewed, time.Now())
		}
	})
}

// The holder gives up its lease Margin before others would: after the last
// renewal at T, others see it valid until T+TTL, the holder only until T+TTL-Margin.
func TestKeeperExpiresEarlyAndForGood(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		l, ctx := newLocal(t), context.Background()
		reg := lease.NewRegistry(l, "n1", opts())
		rec, err := reg.Acquire(ctx, lease.NodeInfo{Name: "n1"})
		if err != nil {
			t.Fatal(err)
		}
		k := lease.NewKeeper(reg, rec, keeperOpts(nil))
		defer serve(k)()

		time.Sleep(4 * time.Second) // renewed at 3 s
		last := k.Lease().Renewed
		if err := k.Check(); err != nil {
			t.Fatal(err)
		}

		// Storage goes away.
		var down atomic.Bool
		down.Store(true)
		l.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
			if down.Load() {
				return errors.New("storage down")
			}
			return nil
		})

		// Still good just before the holder's own deadline (last + 10 s - 2 s)...
		time.Sleep(time.Until(last.Add(8*time.Second - time.Millisecond)))
		if err := k.Check(); err != nil {
			t.Fatalf("expired %v early: %v", time.Until(last.Add(8*time.Second)), err)
		}
		// ...and expired just after, while others still consider the lease valid.
		time.Sleep(2 * time.Millisecond)
		if err := k.Check(); !errors.Is(err, lease.ErrExpired) {
			t.Fatalf("Check = %v, want ErrExpired", err)
		}
		if got, err := reg.Get(ctx, "n1"); err == nil && !got.ValidAt(last.Add(8*time.Second+time.Millisecond)) {
			t.Fatalf("others should still see the lease as valid: %+v", got)
		}
		select {
		case <-k.Lost():
		default:
			t.Fatal("Lost not closed")
		}

		// Expiry is final: the storage coming back doesn't revive the lease.
		down.Store(false)
		time.Sleep(10 * time.Second)
		if err := k.Check(); !errors.Is(err, lease.ErrExpired) {
			t.Fatalf("Check after storage returned = %v", err)
		}
	})
}

func TestKeeperSurvivesBriefTroubleAndStopsWhenReplaced(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		l, ctx := newLocal(t), context.Background()
		reg := lease.NewRegistry(l, "n1", opts())
		rec, _ := reg.Acquire(ctx, lease.NodeInfo{Name: "n1"})
		k := lease.NewKeeper(reg, rec, keeperOpts(nil))
		defer serve(k)()

		// Four seconds of failures are within the margin; renewals resume after.
		var down atomic.Bool
		l.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
			if down.Load() {
				return errors.New("storage down")
			}
			return nil
		})
		time.Sleep(time.Second)
		down.Store(true)
		time.Sleep(4 * time.Second)
		down.Store(false)
		time.Sleep(5 * time.Second)
		if err := k.Check(); err != nil {
			t.Fatalf("after brief trouble: %v", err)
		}

		// A new life of the node takes the lease: the old keeper is fenced out.
		other := lease.NewRegistry(l, "n1", opts())
		if _, err := other.Acquire(ctx, lease.NodeInfo{Name: "n1"}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Second)
		if err := k.Check(); !errors.Is(err, lease.ErrLost) {
			t.Fatalf("Check after replacement = %v, want ErrLost", err)
		}
	})
}

// A keeper with no expiry never judges its lease expired by time, however long storage
// is away, and still stops once a renewal finds the lease taken.
func TestKeeperWithNoExpiryOutlivesItsTTLUntilReplaced(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		l, ctx := newLocal(t), context.Background()
		reg := lease.NewRegistry(l, "n1", opts())
		rec, _ := reg.Acquire(ctx, lease.NodeInfo{Name: "n1"})
		o := keeperOpts(nil)
		o.NoExpiry = true
		k := lease.NewKeeper(reg, rec, o)
		defer serve(k)()

		var down atomic.Bool
		l.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
			if down.Load() {
				return errors.New("storage down")
			}
			return nil
		})
		down.Store(true)
		time.Sleep(3 * 10 * time.Second) // three TTLs without a renewal
		if err := k.Check(); err != nil {
			t.Fatalf("a keeper with no expiry expired: %v", err)
		}

		// Another life takes the lease, and the next renewal finds it taken.
		other := lease.NewRegistry(l, "n1", opts())
		down.Store(false)
		if _, err := other.Acquire(ctx, lease.NodeInfo{Name: "n1"}); err != nil {
			t.Fatal(err)
		}
		<-k.Lost()
		if err := k.Check(); !errors.Is(err, lease.ErrLost) {
			t.Fatalf("Check after replacement = %v, want ErrLost", err)
		}
	})
}

func TestKeeperReleases(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		l, ctx := newLocal(t), context.Background()
		reg := lease.NewRegistry(l, "n1", opts())
		rec, _ := reg.Acquire(ctx, lease.NodeInfo{Name: "n1"})
		k := lease.NewKeeper(reg, rec, keeperOpts(nil))
		stop := serve(k)
		time.Sleep(5 * time.Second)
		stop()
		if err := k.Release(ctx); err != nil {
			t.Fatal(err)
		}
		got, err := reg.Get(ctx, "n1")
		if err != nil || !got.Released {
			t.Fatalf("after release: %+v %v", got, err)
		}
	})
}

// Ending Serve ends the renewals and leaves the lease as it is. Release, later,
// gives it up: the node stops renewing with the rest of its services and decides
// about the lease once its shards are let go.
func TestKeeperStopsThenReleases(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		l, ctx := newLocal(t), context.Background()
		reg := lease.NewRegistry(l, "n1", opts())
		rec, _ := reg.Acquire(ctx, lease.NodeInfo{Name: "n1"})
		k := lease.NewKeeper(reg, rec, keeperOpts(nil))
		stop := serve(k)
		time.Sleep(5 * time.Second)
		stop()
		before, err := reg.Get(ctx, "n1")
		if err != nil || before.Released {
			t.Fatalf("after stop: %+v %v", before, err)
		}
		time.Sleep(5 * time.Second)
		if after, _ := reg.Get(ctx, "n1"); !after.Renewed.Equal(before.Renewed) {
			t.Fatalf("renewed after stop: %v, then %v", before.Renewed, after.Renewed)
		}
		if err := k.Release(ctx); err != nil {
			t.Fatal(err)
		}
		if got, err := reg.Get(ctx, "n1"); err != nil || !got.Released {
			t.Fatalf("after Release: %+v %v", got, err)
		}
	})
}

// A renewal that landed without the keeper hearing about it (it was stopped
// mid-request) leaves the keeper with an old ETag. Release still releases its
// own lease, and still refuses once another process has taken the node over.
func TestReleaseAfterARenewalItNeverSaw(t *testing.T) {
	t.Parallel()
	l, ctx := newLocal(t), context.Background()
	reg := lease.NewRegistry(l, "n1", opts())
	old, err := reg.Acquire(ctx, lease.NodeInfo{Name: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Renew(ctx, old, old.NodeInfo); err != nil { // lands; the caller's `old` is now stale
		t.Fatal(err)
	}
	if err := reg.Release(ctx, old); err != nil {
		t.Fatalf("release with a stale ETag of its own lease: %v", err)
	}
	if got, err := reg.Get(ctx, "n1"); err != nil || !got.Released {
		t.Fatalf("lease: %+v %v", got, err)
	}

	// A newer epoch is somebody else's.
	a, err := reg.Acquire(ctx, lease.NodeInfo{Name: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lease.NewRegistry(l, "n1", opts()).Acquire(ctx, lease.NodeInfo{Name: "n1"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Release(ctx, a); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("release of a lease that was taken over: %v, want ErrLost", err)
	}
}
