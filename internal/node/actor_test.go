package node

import (
	"context"
	"errors"
	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/harness"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// Tests of the table and the shard actor (docs/internals.md, Concurrency).

// blockCreates holds every create of a key with prefix in the bucket until the
// returned function is called: entered gets a value as each one arrives, and
// inside counts those held now.
func (r *mrig) blockCreates(prefix string) (entered chan string, inside *atomic.Int32, unblock func()) {
	entered = make(chan string, 64)
	inside = new(atomic.Int32)
	gate := make(chan struct{})
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op != blobtest.OpCreate || after || !strings.HasPrefix(key, prefix) {
			return nil
		}
		inside.Add(1)
		defer inside.Add(-1)
		entered <- key
		select {
		case <-gate:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	var once sync.Once
	return entered, inside, func() { once.Do(func() { close(gate) }) }
}

// notYet fails the test if ch delivers within a short while: for a request that
// must be waiting.
func notYet[T any](t *testing.T, what string, ch <-chan T) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s did not wait", what)
	case <-time.After(50 * time.Millisecond):
	}
}

// Two requests for a shard that isn't active share one activation: the second
// finds the first one's entry and waits for it.
func TestTwoActivationsOfOneNameShareOne(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	entered, _, unblock := r.blockCreates(shardfile.ManifestKey(shardName("users/sa1")))
	defer unblock()
	refs := make(chan *Hold, 2)
	var g errgroup.Group
	defer g.Wait()
	acquire := func() {
		g.Go(func() error {
			ref, err := r.m.Acquire(context.Background(), shardName("users/sa1"))
			if err != nil {
				return err
			}
			refs <- ref
			return nil
		})
	}
	acquire()
	<-entered // the first activation is creating the shard's manifest
	acquire()
	if e := r.m.tab.load(shardName("users/sa1")); e == nil || !e.is(entryActivating) {
		t.Fatal("no activating entry for the shard")
	}
	notYet(t, "a request for a shard being activated", refs)
	unblock()
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	a, b := <-refs, <-refs
	defer a.Release()
	defer b.Release()
	if a.a != b.a {
		t.Fatal("the two requests got different shards")
	}
	if n, c := r.m.Stats.Activations.Load(), r.m.Stats.Creates.Load(); n != 1 || c != 1 {
		t.Fatalf("%d activations and %d creates, want one of each", n, c)
	}
}

// A request that arrives while its shard is being let go of waits for the
// release, then activates the shard again: it never gets the shard that is
// going.
func TestAnActivationRacingARelease(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ctx := context.Background()
	ref := r.acquire("slow/ar1")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	want := r.liveDump(ref.Path())
	r.app.Must(t, "close", "a")
	ref.Release()
	old := ref.a

	entered, _, unblock := r.blockCreates(shardfile.ShardPrefix(shardName("slow/ar1"))) // the release's delta
	defer unblock()
	released := make(chan error, 1)
	go func() {
		ok, err := old.deactivateNow(ctx)
		if err == nil && !ok {
			err = errors.New("not released")
		}
		released <- err
	}()
	<-entered
	if !old.e.is(entryBusy) {
		t.Fatal("the entry of a shard being let go of is not busy")
	}
	got := make(chan *Hold, 1)
	go func() {
		ref, err := r.m.Acquire(ctx, shardName("slow/ar1"))
		if err != nil {
			t.Error(err)
		}
		got <- ref
	}()
	notYet(t, "a request for a shard being let go of", got)
	unblock()
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	ref = <-got
	if ref == nil {
		t.FailNow()
	}
	defer ref.Release()
	<-old.ended
	if ref.a == old {
		t.Fatal("the request got the shard that was let go of")
	}
	if n, restores := r.m.Stats.Activations.Load(), r.m.Stats.Restores.Load(); n != 2 || restores != 1 {
		t.Fatalf("%d activations, %d restores: want the shard created, then restored from storage", n, restores)
	}
	if r.liveDump(ref.Path()) != want {
		t.Fatal("the shard differs after it was activated again")
	}
}

// A request that found an entry just before it was removed looks the name up
// again, and removing an entry never removes the one that took its place.
func TestARequestRacingTheRemovalOfItsEntry(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ref := r.acquire("slow/rr1")
	r.write(ref, "a")
	r.app.Must(t, "close", "a")
	ref.Release()
	e := ref.a.e // the entry a request loaded just before the shard was let go of
	r.release("slow/rr1")
	ref = r.acquire("slow/rr1")
	defer ref.Release()
	if ref.a.e == e || r.m.Stats.Activations.Load() != 2 {
		t.Fatal("the request did not activate the shard under an entry of its own")
	}
	// The removed entry, as a request that loaded it just before sees it.
	if ok, wait, err := e.take(r.m.now()); ok || wait != nil || err != nil {
		t.Fatalf("a removed entry: ok %v, wait %v, err %v, want a look again", ok, wait != nil, err)
	}
	r.m.tab.remove(e, nil)
	r.m.tab.forget(e)
	if r.m.tab.load(shardName("slow/rr1")) != ref.a.e {
		t.Fatal("removing an old entry again removed the one that took its place")
	}
	if r.refs("slow/rr1") != 1 {
		t.Fatalf("%d references, want the request's", r.refs("slow/rr1"))
	}
	if n := len(r.m.Active()); n != 1 {
		t.Fatalf("%d shards in the table, want 1", n)
	}
}

// A request never waits for a delta: references are taken in the entry, not
// through the shard's loop, and gates wait for the flush loop alone.
func TestADeltaNeverDelaysARequest(t *testing.T) {
	t.Parallel()
	r := newMRigWith(t, func(o *ShardsetOptions) { o.Lifecycle.DeltaAge = time.Millisecond })
	ctx := context.Background()
	ref := r.acquire("wallet/dd1")
	defer ref.Release()
	entered, _, unblock := r.blockCreates(shardfile.ShardPrefix(shardName("wallet/dd1")))
	defer unblock()
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	<-entered // the shard's loop is in a delta, held in blob storage
	quick, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for range 3 {
		other, err := r.m.Acquire(quick, shardName("wallet/dd1"))
		if err != nil {
			t.Fatalf("a request waited for the delta: %v", err)
		}
		other.Release()
	}
	r.app.Must(t, "insert", "a", "1", "100")
	if err := r.m.Gate(quick, ref, kind.SyncStorage); err != nil {
		t.Fatalf("a gate waited for the delta: %v", err)
	}
	if len(entered) != 0 {
		t.Fatal("a second delta started")
	}
	unblock()
	apptest.Eventually(t, apptest.Timeout, "the delta", func() bool {
		m, err := r.manifest("wallet/dd1")
		return err == nil && len(m.Files) > 0
	})
}

// A shard is let go of once it has been idle for its kind's timeout on the
// shards' clock, counted from its last use: a request in between starts the
// count again.
func TestIdleDeactivationOnTheFakeClock(t *testing.T) {
	t.Parallel()
	clk := harness.NewFakeClock()
	r := newMRigWith(t, withClock(clk)) // users' idle timeout is 300 ms
	ref := r.acquire("users/fi1")
	r.write(ref, "a")
	if err := r.m.Gate(context.Background(), ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	r.app.Must(t, "close", "a")
	ref.Release()
	a := ref.a

	clk.Advance(200 * time.Millisecond)
	r.acquire("users/fi1").Release() // used at 200 ms: idle until 500 ms
	clk.Advance(100 * time.Millisecond)
	waitCounter(t, "the idle timer's look at 300 ms", &a.looks.idle, 1)
	if !r.m.IsActive(shardName("users/fi1")) {
		t.Fatal("let go of 100 ms after its last use")
	}
	clk.Advance(199 * time.Millisecond)
	if !r.m.IsActive(shardName("users/fi1")) || a.looks.idle.Load() != 1 {
		t.Fatalf("looked at again (%d looks) before the timeout since the last use", a.looks.idle.Load())
	}
	clk.Advance(time.Millisecond)
	waitCounter(t, "the idle timer's look at 500 ms", &a.looks.idle, 2)
	<-a.ended
	if r.m.IsActive(shardName("users/fi1")) {
		t.Fatal("not let go of after its idle timeout")
	}
	if man, err := r.manifest("users/fi1"); err != nil || man.Owner != "" {
		t.Fatalf("the manifest of a shard let go of when idle: %+v %v", man, err)
	}
}

// At most the deactivation limit of shards are let go of at once, however many
// are asked to be.
func TestTheDeactivationLimitHolds(t *testing.T) {
	t.Parallel()
	r := newMRigWith(t, func(o *ShardsetOptions) { o.limits.deactivate = 2 })
	ctx := context.Background()
	var shards []*actor
	for _, name := range []string{"slow/l1", "slow/l2", "slow/l3", "slow/l4", "slow/l5"} {
		ref := r.acquire(name)
		r.write(ref, name)
		if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
			t.Fatal(err)
		}
		r.app.Must(t, "close", name)
		ref.Release()
		shards = append(shards, ref.a)
	}
	entered, inside, unblock := r.blockCreates(shardfile.FilesPrefix) // the releases' deltas
	defer unblock()
	var g errgroup.Group
	for _, a := range shards {
		g.Go(func() error {
			ok, err := a.deactivateNow(ctx)
			if err == nil && !ok {
				err = errors.New(a.Name.String() + " not released")
			}
			return err
		})
	}
	<-entered
	<-entered
	time.Sleep(100 * time.Millisecond) // the others had time to start theirs
	if n := inside.Load(); n != 2 {
		t.Fatalf("%d deactivations at once, want the limit of 2", n)
	}
	unblock()
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	for _, a := range shards {
		if r.m.IsActive(a.Name) {
			t.Fatalf("%s is still active", a.Name)
		}
	}
}

// A delta waits for a place under the snapshot limit, and a rebase for one
// under the rebase limit too. A shard's loop waiting for one never holds up a
// request.
func TestSnapshotsWaitForAPlace(t *testing.T) {
	t.Parallel()
	for _, rebase := range []bool{false, true} {
		name := map[bool]string{false: "delta", true: "rebase"}[rebase]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := newMRigWith(t, func(o *ShardsetOptions) {
				o.Lifecycle.DeltaAge = time.Millisecond
				o.limits.snapshot, o.limits.rebase = 1, 1
				if rebase {
					o.Lifecycle.MaxFiles = 1
				}
			})
			ctx := context.Background()
			ref := r.acquire("slow/sw1")
			defer ref.Release()
			files := func() int {
				m, err := r.manifest("slow/sw1")
				if err != nil {
					return 0
				}
				return len(m.Files)
			}
			// A commit, and the delta timer's snapshot of it if it may take one.
			commit := func(settle bool) {
				r.write(ref, "a")
				if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
					t.Fatal(err)
				}
				if settle {
					apptest.Eventually(t, apptest.Timeout, "the tail to be in a delta", func() bool { n, _ := ref.a.cap.load().TailInfo(); return n == 0 })
				}
			}
			commit(true)
			sem, want := r.m.deps.sems.snapshot, files()
			if rebase {
				sem = r.m.deps.sems.rebase
				sem <- struct{}{} // the only place is taken
				// Deltas until the shard has more files than MaxFiles: then its next
				// snapshot is a rebase.
				for files() < 2 {
					commit(true)
				}
				want = 2
			} else {
				sem <- struct{}{}
				commit(false)
			}
			time.Sleep(100 * time.Millisecond) // a few looks of the delta timer
			if got := files(); got != want {
				t.Fatalf("%d files while the %s limit is taken, want %d", got, name, want)
			}
			quick, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			other, err := r.m.Acquire(quick, shardName("slow/sw1"))
			if err != nil {
				t.Fatalf("a request waited for the shard's loop: %v", err)
			}
			other.Release()
			<-sem
			if rebase {
				want = 1
			} else {
				want++
			}
			apptest.Eventually(t, apptest.Timeout, "the "+name, func() bool { return files() == want })
		})
	}
}

// shardGoroutines lists the goroutines running the shards' code or a service group's.
func shardGoroutines() []string {
	buf := make([]byte, 1<<22)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	var out []string
	for i, g := range strings.Split(string(buf), "\n\n") {
		if i == 0 {
			continue
		}
		for _, f := range []string{"node.(*actor)", "capture.(*Shard)", "node.(*msgOwner)", "node.(*wakeHints)", "node.(*messenger)", "node.(*Shardset)", "node.(*table)", "node.(*Flusher)", "service.(*Group)"} {
			if strings.Contains(g, f) {
				out = append(out, g)
				break
			}
		}
	}
	return out
}

// Once the shards are let go of, every goroutine they ran has ended: each
// shard's loop and supervisor and its wakes, even one still waiting for
// the app's answer. Not parallel: it looks at every goroutine.
func TestShardsGoroutinesAreJoinedWhenTheyAreLetGo(t *testing.T) {
	before := shardGoroutines()
	r := newSRigWith(t, quick)
	rec := &recorder{keep: true, hold: make(chan struct{})}
	defer close(rec.hold)
	r.record(rec, "")
	// A wake that takes a while to end once it is cancelled: only a join waits for it.
	hook := wakeHookFn(func(ctx context.Context, call func() (int, error)) (int, error) {
		status, err := call()
		if ctx.Err() != nil {
			time.Sleep(200 * time.Millisecond)
		}
		return status, err
	})
	r.wakeHook.Store(&hook)
	r.remind("slow/gj1", "k", time.Now().Unix())
	apptest.Eventually(t, apptest.Timeout, "the wake to reach the app", func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.got) > 0
	})
	r.acquire("users/gj2").Release() // and a shard with nothing to wake for
	r.closeInApp("slow/gj1")
	if err := r.mrig.stop(); err != nil {
		t.Fatal(err)
	}
	r.stopJournal() // its group's goroutines count too
	if left := startedSince(before, shardGoroutines()); len(left) > 0 {
		t.Fatalf("%d goroutines of the shards are left after they were let go of:\n\n%s", len(left), strings.Join(left, "\n\n"))
	}
	if n := len(r.m.Active()); n != 0 {
		t.Fatalf("%d shards still active", n)
	}
}

// deactivateNow lets go of the shard through its loop, as an idle deactivation
// does but whether or not it has been idle, and returns once its loop has ended
// and its name is free. The shard is released. It reports false if
// the shard is in use or the app has it open. A shard that was let go of
// meanwhile, or never activated, counts as let go of.
func (a *actor) deactivateNow(ctx context.Context) (bool, error) {
	type result struct {
		ok  bool
		err error
	}
	r, err := do(ctx, a.ops, a.ended, func() result {
		ok, err := a.deactivate(ctx, capture.ResumeIfHeld)
		return result{ok, err}
	})
	if errors.Is(err, errLoopEnded) {
		select {
		case <-a.ready:
		default:
			return true, nil // never activated: nothing to let go of
		}
		if a.gone {
			return true, nil
		}
		return false, errShardsStopped
	}
	if err != nil {
		return false, err
	}
	if r.ok {
		if err := a.waitEnded(ctx); err != nil {
			return false, err
		}
	}
	return r.ok, r.err
}
