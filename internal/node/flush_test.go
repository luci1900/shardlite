package node

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/kind"
)

// longInterval makes the flush interval so long that a cycle in a test can only
// be one that output asked for, and sets the early-cycle timing.
func longInterval(coalesce, min time.Duration) func(*CatalogOptions) {
	return func(o *CatalogOptions) {
		o.Flush.Interval, o.Flush.Coalesce, o.Flush.Min = time.Hour, coalesce, min
	}
}

// journalPuts records when each journal segment is created in blob storage.
func (r *mrig) journalPuts() (times func() []time.Time) {
	var at apptest.Log[time.Time]
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && after && strings.HasPrefix(key, journal.Root) {
			at.Add(time.Now())
		}
		return nil
	})
	return at.All
}

// On the `storage` tier, a response after a write waits for one cycle that starts at
// once (after the coalescing delay), not for the interval to come round.
func TestGatedResponseDoesNotWaitForTheInterval(t *testing.T) {
	t.Parallel()
	r := newMRigWith(t, longInterval(2*time.Millisecond, 0))
	ref := r.acquire("test.wallet/f1")
	defer ref.Release()
	r.write(ref, "a")
	before := r.m.Flusher().Completed()
	start := time.Now()
	if err := r.m.Gate(context.Background(), ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("the gate took %v with a one hour flush interval", d)
	}
	if got := r.m.Flusher().Completed() - before; got != 1 {
		t.Fatalf("%d cycles ran for the gate, want 1", got)
	}
	if got := strings.Count(r.dumpBlob("test.wallet/f1"), "kv: "); got != 2 {
		t.Fatalf("%d rows durable when the gate opened", got)
	}
	r.app.Must(t, "close", "a")
}

// Gated responses that wait at the same time share cycles.
func TestConcurrentGatedResponsesShareCycles(t *testing.T) {
	t.Parallel()
	const n = 200
	r := newMRigWith(t, longInterval(20*time.Millisecond, 0))
	var refs []*Hold
	for i := range 20 {
		ref := r.acquire(fmt.Sprintf("test.wallet/c%d", i))
		defer ref.Release()
		r.write(ref, fmt.Sprint(i))
		refs = append(refs, ref)
	}
	before := r.m.Flusher().Completed()
	var g errgroup.Group
	for i := range n {
		g.Go(func() error { return r.m.Gate(context.Background(), refs[i%len(refs)], kind.SyncStorage) })
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := r.m.Flusher().Completed() - before; got < 1 || got > 4 {
		t.Fatalf("%d cycles for %d gated responses", got, n)
	}
	for i := range refs {
		r.app.Must(t, "close", fmt.Sprint(i))
	}
}

// However gated writes keep arriving, cycles start no closer than FlushMin.
func TestCyclesAreSpacedByFlushMin(t *testing.T) {
	t.Parallel()
	const min = 150 * time.Millisecond
	r := newMRigWith(t, longInterval(0, min))
	times := r.journalPuts()
	ref := r.acquire("test.wallet/f2")
	defer ref.Release()
	r.write(ref, "a")
	for range 6 {
		r.app.Must(t, "insert", "a", "2", "100")
		if err := r.m.Gate(context.Background(), ref, kind.SyncStorage); err != nil {
			t.Fatal(err)
		}
	}
	at := times()
	if len(at) < 6 {
		t.Fatalf("%d segments for 6 gated writes", len(at))
	}
	// A segment is put at the end of its cycle, so one gap between puts can be
	// short when a cycle's work runs late on a busy machine. Over all of them the
	// late puts even out: the span is the starts' span give or take the first
	// and last cycle's work, which one FlushMin of slack covers.
	if span, want := at[len(at)-1].Sub(at[0]), time.Duration(len(at)-2)*min; span < want {
		t.Fatalf("%d segments over %v, want at least %v (cycles %v apart)", len(at), span, want, min)
	}
	r.app.Must(t, "close", "a")
}

// Writes nobody waits for are flushed by the tick, and a node with nothing
// waiting runs cycles on the interval alone.
func TestNothingWaitingRunsCyclesOnTheTick(t *testing.T) {
	t.Parallel()
	const interval = 300 * time.Millisecond
	r := newMRigWith(t, func(o *CatalogOptions) { o.Flush.Interval = interval })
	times := r.journalPuts()
	ref := r.acquire("test.wallet/f3")
	defer ref.Release()
	r.write(ref, "a") // nobody waits for it
	cycles := r.m.Flusher().Completed()
	start := time.Now()
	apptest.Eventually(t, apptest.Timeout, "the tick to flush the write", func() bool { return len(times()) > 0 })
	if time.Since(start) > 3*interval {
		t.Fatalf("flushed after %v with an interval of %v", time.Since(start), interval)
	}
	// And idle: only ticks, no segments.
	puts := len(times())
	time.Sleep(4 * interval)
	if got := len(times()); got != puts {
		t.Fatalf("an idle node wrote %d segments", got-puts)
	}
	if got := r.m.Flusher().Completed() - cycles; got > 8 {
		t.Fatalf("%d cycles in %v with an interval of %v", got, 5*interval, interval)
	}
	r.app.Must(t, "close", "a")
}

// Output that passes straight through (a shard with no new frames) never waits
// for a cycle and never asks for one.
func TestOutputWithNoNewFramesTriggersNoCycle(t *testing.T) {
	t.Parallel()
	r := newMRigWith(t, longInterval(time.Millisecond, 0))
	ref := r.acquire("test.wallet/f4")
	defer ref.Release()
	r.write(ref, "a")
	if err := r.m.Gate(context.Background(), ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	// The capture can lag the gate that just returned: wait for it, or a later
	// gate rightly asks for a cycle.
	apptest.Eventually(t, apptest.Timeout, "the shard to be captured", func() bool { return !ref.a.cap.hasNewFrames() })
	before := r.m.Flusher().Completed()
	for range 20 {
		// A gate that wants a cycle waits for it, so one asked for has completed
		// by the time the loop is done.
		if err := r.m.Gate(context.Background(), ref, kind.SyncStorage); err != nil {
			t.Fatal(err)
		}
	}
	if got := r.m.Flusher().Completed() - before; got != 0 {
		t.Fatalf("%d cycles for output that had nothing to wait for", got)
	}
	r.app.Must(t, "close", "a")
}

// A waiter whose context ends while its cycle is held back leaves nothing
// asking for it: no cycle runs for nobody.
func TestCancelledWaiterTriggersNoCycle(t *testing.T) {
	t.Parallel()
	r := newMRigWith(t, longInterval(400*time.Millisecond, 0))
	ref := r.acquire("test.wallet/f5")
	defer ref.Release()
	r.write(ref, "a")
	before := r.m.Flusher().Completed()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("gate: %v, want the context's end", err)
	}
	time.Sleep(900 * time.Millisecond) // past the coalescing delay
	if got := r.m.Flusher().Completed() - before; got != 0 {
		t.Fatalf("%d cycles ran for a waiter that had gone", got)
	}
	// A waiter that comes later is served.
	if err := r.m.Gate(context.Background(), ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	r.app.Must(t, "close", "a")
}

// A waiter that is already in a cycle doesn't cancel it by giving up.
func TestWaiterGivingUpInACycleDoesNotCancelIt(t *testing.T) {
	t.Parallel()
	r := newMRigWith(t, longInterval(time.Millisecond, 0))
	var stall atomic.Bool
	release := make(chan struct{})
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after && strings.HasPrefix(key, journal.Root) && stall.Load() {
			<-release
		}
		return nil
	})
	ref := r.acquire("test.wallet/f6")
	defer ref.Release()
	r.write(ref, "a")
	stall.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.m.Gate(ctx, ref, kind.SyncStorage) }()
	apptest.Eventually(t, apptest.Timeout, "the cycle to start", func() bool { return r.m.Flusher().started.Load() > 0 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("gate: %v", err)
	}
	stall.Store(false)
	close(release)
	apptest.Eventually(t, apptest.Timeout, "the cycle to complete anyway", func() bool { return r.m.Flusher().Completed() > 0 })
	if got := strings.Count(r.dumpBlob("test.wallet/f6"), "kv: "); got != 2 {
		t.Fatalf("%d rows durable after the cycle", got)
	}
	r.app.Must(t, "close", "a")
}

// The sync barrier is released by an early cycle too.
func TestBarrierIsReleasedByAnEarlyCycle(t *testing.T) {
	t.Parallel()
	r := newMRigWith(t, longInterval(time.Millisecond, 0))
	ref := r.acquire("test.wallet/f7")
	defer ref.Release()
	r.write(ref, "a")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.m.Barrier(ctx); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(r.dumpBlob("test.wallet/f7"), "kv: "); got != 2 {
		t.Fatalf("%d rows durable after the barrier", got)
	}
	r.app.Must(t, "close", "a")
}

// A shard let go of at idle timeout doesn't wait for the tick either.
func TestDeactivationDoesNotWaitForTheInterval(t *testing.T) {
	t.Parallel()
	r := newMRigWith(t, longInterval(time.Millisecond, 0))
	ref := r.acquire("test.users/f8")
	r.write(ref, "a")
	r.app.Must(t, "close", "a")
	ref.Release()
	apptest.Eventually(t, apptest.Timeout, "deactivation", func() bool { return r.cached("test.users/f8") })
}

// A tail that passes DeltaSize gets a delta long before it is DeltaAge old.
func TestTailPassingTheSizeGetsADeltaBeforeTheAge(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	r.crash()
	r.startShards(func(o *CatalogOptions) { o.Lifecycle.DeltaAge, o.Lifecycle.DeltaSize = time.Hour, 8<<10 })
	ctx := context.Background()
	ref := r.acquire("test.wallet/d1")
	defer ref.Release()
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // several looks of the delta timer
	if m, _ := r.manifest("test.wallet/d1"); len(m.Files) != 0 {
		t.Fatalf("a delta for a small tail: %v", m.Files)
	}
	r.app.Must(t, "insert", "a", "20", "1000") // about 20 KB in one commit
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	apptest.Eventually(t, apptest.Timeout, "a delta for the large tail", func() bool {
		m, err := r.manifest("test.wallet/d1")
		return err == nil && len(m.Files) == 1
	})
	if r.m.Stats.TailBytesMax.Load() < 8<<10 {
		t.Fatalf("the highest tail held was %d bytes, want at least the size that made the delta", r.m.Stats.TailBytesMax.Load())
	}
	apptest.Eventually(t, apptest.Timeout, "the tail to be dropped", func() bool { return r.m.Stats.TailBytes.Load() < 8<<10 })
	r.app.Must(t, "close", "a")
}

// A shard that writes a little gets a delta only once its tail is DeltaAge old.
func TestSmallTailGetsADeltaAtTheAge(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	r.crash()
	const age = 600 * time.Millisecond
	r.startShards(func(o *CatalogOptions) { o.Lifecycle.DeltaAge = age })
	ctx := context.Background()
	start := time.Now() // the tail starts with the shard's first commit
	ref := r.acquire("test.wallet/d2")
	defer ref.Release()
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	for time.Since(start) < age-100*time.Millisecond {
		if m, _ := r.manifest("test.wallet/d2"); len(m.Files) != 0 {
			t.Fatalf("a delta after %v, before the age %v: %v", time.Since(start), age, m.Files)
		}
		time.Sleep(20 * time.Millisecond)
	}
	apptest.Eventually(t, apptest.Timeout, "the delta at the age", func() bool {
		m, err := r.manifest("test.wallet/d2")
		return err == nil && len(m.Files) == 1
	})
	r.app.Must(t, "close", "a")
}
