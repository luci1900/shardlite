package node

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
)

// --- ids ---

// A message sent after the newest one was consumed gets a new inbox id, so a
// handler that tracks ids sees both.
func TestInboxIdsAreNeverReused(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{}
	r.record(rec, "")
	r.addMessage("users/i1", "users/i2", "/x", "first")
	eventually(t, "the first delivery", func() bool { return rec.count() == 1 })
	eventually(t, "the row to go", func() bool { return r.liveRows("users/i1") == 0 })
	r.addMessage("users/i1", "users/i2", "/x", "second")
	eventually(t, "the second delivery", func() bool { return rec.count() == 2 })
	eventually(t, "the first inbox row to go", func() bool { return r.inboxRows("users/i2") == 0 })
	got := rec.all()
	if got[0].id() != "1" || got[1].id() != "2" {
		t.Fatalf("inbox ids %q and %q", got[0].id(), got[1].id())
	}
}

func TestNeverWrittenWithAnAutoincrementSequence(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	// A message that came and went leaves a row in sqlite_sequence.
	r.sql("users/s1", `DROP TABLE kv; DROP TABLE acks; BEGIN; INSERT INTO _shardlite_outbox (target, path) VALUES ('users/x', '/x'); DELETE FROM _shardlite_outbox; COMMIT`)
	if n := r.count("users/s1", `SELECT count(*) FROM sqlite_sequence`); n != 1 {
		t.Fatalf("sqlite_sequence has %d rows", n)
	}
	r.closeInApp("users/s1")
	eventually(t, "deactivation", r.inactive("users/s1"))
	if _, err := r.rawManifest("users/s1"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("a shard with only reserved tables and a sequence was kept: %v", err)
	}
	// The app's own AUTOINCREMENT table is the app's data.
	r.sql("users/s2", `DROP TABLE kv; DROP TABLE acks; CREATE TABLE mine (id INTEGER PRIMARY KEY AUTOINCREMENT)`)
	r.closeInApp("users/s2")
	eventually(t, "deactivation", func() bool { return r.cached("users/s2") })
}

// --- wake hints and the drain ---

func (r *srig) wakes() []string {
	r.t.Helper()
	var keys []string
	for info, err := range r.n.Store.List(context.Background(), wakePrefix) {
		if err != nil {
			r.t.Fatal(err)
		}
		keys = append(keys, info.Key)
	}
	return keys
}

// shutdown is what Run does: drain, then let go of every shard and give up the lease.
func (r *srig) shutdown(appHandles ...string) {
	r.t.Helper()
	ctx := context.Background()
	r.m.Drain(ctx)
	for _, s := range appHandles {
		r.closeInApp(s)
	}
	if err := r.mrig.stop(); err != nil {
		r.t.Fatal(err)
	}
	r.stopJournal()
	if err := r.n.Keeper.Release(ctx); err != nil {
		r.t.Fatal(err)
	}
}

func (r *srig) restart(mutate func(*CatalogOptions)) {
	r.t.Helper()
	r.startNode()
	r.startShards(mutate)
	r.startServer()
}

func drainFor(d time.Duration) func(*CatalogOptions) {
	return func(o *CatalogOptions) { o.Drain, o.Wakes.Every = d, 50*time.Millisecond }
}

func TestDrainDeliversBeforeLettingGo(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, both(quick, drainFor(5*time.Second)))
	rec := &recorder{}
	r.record(rec, "")
	inner := *r.handler.Load()
	// The first receiver sends a message of its own, to a third shard on this node.
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == "/hop" && q.Header.Get("Shardlite-Target") == "users/d2" {
			r.sql("users/d2", `INSERT INTO _shardlite_outbox (target, path) VALUES ('users/d3', '/end')`)
		}
		inner(w, q)
	})
	r.addMessage("users/d1", "users/d2", "/hop", "")
	r.shutdown("users/d1", "users/d2", "users/d3")
	got := rec.all()
	if len(got) != 2 || got[0].path != "/hop" || got[1].path != "/end" {
		t.Fatalf("deliveries during the drain: %+v", got)
	}
	if keys := r.wakes(); len(keys) != 0 {
		t.Fatalf("hints after a drain that delivered everything: %v", keys)
	}
}

func TestShutdownWithAFailingReceiverLeavesAHintThatWakesTheSender(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		deleteData bool
	}{{"same machine", false}, {"new machine", true}} {
		t.Run(tc.name, func(t *testing.T) {
			r := newSRigWith(t, both(quick, drainFor(300*time.Millisecond)))
			var up atomic.Bool
			rec := &recorder{}
			r.record(rec, "")
			attempts := r.intakeDownUntil(&up)
			r.addMessage("users/w1", "users/w2", "/late", "payload")
			eventually(t, "a first attempt", func() bool { return attempts.Load() >= 1 })
			r.shutdown("users/w1")
			keys := r.wakes()
			if len(keys) != 1 || !strings.HasPrefix(keys[0], wakePrefix) || !strings.HasSuffix(keys[0], "Z/users/w1") {
				t.Fatalf("hints %v, want one for users/w1", keys)
			}
			if tc.deleteData {
				if err := os.RemoveAll(r.data); err != nil {
					t.Fatal(err)
				}
			}
			up.Store(true)
			r.restart(both(quick, drainFor(300*time.Millisecond)))
			eventually(t, "the message to be delivered after the restart, with no request for the sender", func() bool {
				return rec.count() == 1 && rec.all()[0].body == "payload"
			})
			eventually(t, "the hint to go", func() bool { return len(r.wakes()) == 0 })
			if r.liveRows("users/w1") != 0 {
				t.Fatal("the row is still live")
			}
		})
	}
}

func TestHintsThatMeanNothingAreDeleted(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	ctx := context.Background()
	now := time.Now().UTC().Format(wakeLayout)
	future := wakePrefix + "2999-01-01T00:00Z/users/future"
	for _, k := range []string{wakePrefix + now + "/users/ghost", wakePrefix + now + "/nokind/x", wakePrefix + now + "/lobby/e1", wakePrefix + now + "/users/bad id", wakePrefix + "0000-garbage", future} {
		if _, err := r.n.Store.Create(ctx, k, strings.NewReader("")); err != nil {
			t.Fatal(err)
		}
	}
	r.m.wakes.scan(ctx)
	if keys := r.wakes(); len(keys) != 1 || keys[0] != future {
		t.Fatalf("hints left: %v, want only the one in the future", keys)
	}
	for _, s := range []string{"users/ghost", "lobby/e1"} {
		if _, err := r.rawManifest(s); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("a hint created %s: %v", s, err)
		}
	}
	if len(r.m.Active()) != 0 {
		t.Fatalf("shards were activated: %v", r.m.Active())
	}
}

func TestCrashAfterTheWakeKeepsTheHint(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, both(quick, drainFor(200*time.Millisecond)))
	var up atomic.Bool
	rec := &recorder{}
	r.record(rec, "")
	attempts := r.intakeDownUntil(&up)
	r.addMessage("users/c1", "users/c2", "/c", "x")
	eventually(t, "a first attempt", func() bool { return attempts.Load() >= 1 })
	r.shutdown("users/c1")
	if len(r.wakes()) != 1 {
		t.Fatalf("hints %v", r.wakes())
	}

	// Woken, but the receiver still fails, and the node crashes.
	before := attempts.Load()
	r.restart(both(quick, drainFor(200*time.Millisecond)))
	eventually(t, "the wake to start deliveries", func() bool { return attempts.Load() > before })
	r.crash()
	r.stopJournal()
	if len(r.wakes()) != 1 {
		t.Fatalf("the hint is gone after a crash: %v", r.wakes())
	}

	// Woken again.
	up.Store(true)
	r.restart(both(quick, drainFor(200*time.Millisecond)))
	eventually(t, "delivery after the second wake", func() bool { return rec.count() == 1 })
	eventually(t, "the hint to go", func() bool { return len(r.wakes()) == 0 })
}

func TestDuplicateHintsDeliverOnce(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, both(quick, drainFor(200*time.Millisecond)))
	var up atomic.Bool
	rec := &recorder{}
	r.record(rec, "")
	attempts := r.intakeDownUntil(&up)
	r.addMessage("users/p1", "users/p2", "/once", "x")
	eventually(t, "a first attempt", func() bool { return attempts.Load() >= 1 })
	r.shutdown("users/p1")
	earlier := wakePrefix + time.Now().UTC().Add(-3*time.Minute).Format(wakeLayout) + "/users/p1"
	if _, err := r.n.Store.Create(context.Background(), earlier, strings.NewReader("")); err != nil {
		t.Fatal(err)
	}
	if len(r.wakes()) != 2 {
		t.Fatalf("hints %v", r.wakes())
	}
	up.Store(true)
	r.restart(both(quick, drainFor(200*time.Millisecond)))
	eventually(t, "the hints to go", func() bool { return len(r.wakes()) == 0 })
	eventually(t, "the inbox row to be consumed", func() bool { return r.inboxRows("users/p2") == 0 })
	r.settle("users/p2")
	if n := rec.count(); n != 1 {
		t.Fatalf("%d deliveries after two hints, want 1 (%s)", n, fmt.Sprint(rec.all()))
	}
}

// A look that can't tell whether a shard has live rows counts as live: here the
// shard is busy and its owner removed, as during a release that won't go
// through (the app has the file open), and its row is still there. The drain
// used to count that as nothing live and end with the row undelivered.
func TestDrainCountsALookThatCannotTellAsLive(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	var up atomic.Bool
	attempts := r.intakeDownUntil(&up)
	r.addMessage("users/dl1", "users/dl2", "/x", "")
	eventually(t, "a first attempt", func() bool { return attempts.Load() >= 1 })
	a := r.shard("users/dl1")
	inside, proceed := make(chan struct{}), make(chan struct{})
	held := make(chan bool, 1)
	go func() {
		ok, _ := do(context.Background(), a.ops, a.ended, func() bool {
			if !a.e.close() {
				return false
			}
			defer a.e.reopen()
			defer a.addOwner()
			if err := a.removeOwner(); err != nil {
				return false
			}
			close(inside)
			<-proceed
			return true
		})
		held <- ok
	}()
	select {
	case <-inside:
	case ok := <-held:
		t.Fatalf("could not hold the shard busy (%v)", ok)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	live := r.m.anyLiveMessages(ctx)
	cancel()
	close(proceed)
	if !<-held {
		t.Fatal("the shard was not held busy")
	}
	if !live {
		t.Fatal("the drain counted a busy shard with a live row as having nothing live")
	}
	up.Store(true)
	eventually(t, "nothing live once the row is delivered", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return !r.m.anyLiveMessages(ctx)
	})
}

// --- the wake time in the manifest ---

// deltaAge makes a shard's tail a delta once it is d old.
func deltaAge(d time.Duration) func(*CatalogOptions) {
	return func(o *CatalogOptions) { o.Lifecycle.DeltaAge = d }
}

// allInADelta is true once every commit of target is in a delta whose manifest
// has a wake time: nothing uncaptured and no tail.
func (r *srig) allInADelta(target string) func() bool {
	return func() bool {
		a := r.shard(target)
		m, err := r.manifest(target)
		return err == nil && m.Wake != 0 && !a.cap.hasNewFrames() && a.cap.load().TailBytes() == 0
	}
}

// wokeByTheDelta waits for a background delta of target to record a wake time
// with its hint, then crashes the node and starts it again with mutate. The
// shard's tail was a delta by then, so adoption doesn't wake it: the delta's
// hint does.
func (r *srig) wokeByTheDelta(target string, mutate func(*CatalogOptions)) {
	r.t.Helper()
	eventually(r.t, "a delta with a wake time", r.allInADelta(target))
	m, err := r.manifest(target)
	if err != nil {
		r.t.Fatal(err)
	}
	if keys := r.wakes(); len(keys) != 1 || keys[0] != wakeKey(time.Unix(m.Wake, 0), target) {
		r.t.Fatalf("hints %v for a wake time of %d", keys, m.Wake)
	}
	r.closeInApp(target)
	r.crash()
	r.stopJournal()
	r.restart(mutate)
	if n := r.n.Adopted.Deltas; n != 0 {
		r.t.Fatalf("adoption wrote %d deltas: the shard still had a tail", n)
	}
}

// An outbox row that keeps failing past a delta, then a crash: the delta's
// hint wakes the shard, and the row is delivered.
func TestOutboxRowFailingPastADeltaIsWokenAfterACrash(t *testing.T) {
	t.Parallel()
	opts := both(quick, deltaAge(50*time.Millisecond))
	r := newSRigWith(t, opts)
	rec := &recorder{}
	r.record(rec, "")
	var up atomic.Bool
	attempts := r.intakeDownUntil(&up)
	r.addMessage("users/bd1", "users/bd2", "/late", "x")
	eventually(t, "a first attempt", func() bool { return attempts.Load() >= 1 })
	r.wokeByTheDelta("users/bd1", opts)
	up.Store(true)
	eventually(t, "the message after the crash", func() bool { return rec.count() == 1 })
}

// The same for a due inbox row whose handler keeps failing.
func TestInboxRowFailingPastADeltaIsWokenAfterACrash(t *testing.T) {
	t.Parallel()
	opts := both(quick, deltaAge(50*time.Millisecond))
	r := newSRigWith(t, opts)
	var up atomic.Bool
	rec := &recorder{answer: func(int, *http.Request) int {
		if up.Load() {
			return http.StatusOK
		}
		return http.StatusServiceUnavailable
	}}
	r.record(rec, "")
	r.setRow("users/bi1", "k", "/due", time.Now().Unix())
	eventually(t, "a first call", func() bool { return rec.count() >= 1 })
	r.wokeByTheDelta("users/bi1", opts)
	up.Store(true)
	eventually(t, "the row to be consumed after the crash", func() bool { return r.inboxRows("users/bi1") == 0 })
}

// The same for a marked shard, which is deleted when it next goes idle.
func TestMarkedShardPastADeltaIsWokenAfterACrash(t *testing.T) {
	t.Parallel()
	opts := both(quick, deltaAge(50*time.Millisecond))
	r := newSRigWith(t, opts)
	r.sql("slow/bm1", `INSERT INTO kv (payload) VALUES (x'01')`)
	r.sql("slow/bm1", markSQL)
	r.wokeByTheDelta("slow/bm1", opts)
	eventually(t, "the shard to be woken", func() bool { return r.m.IsActive("slow/bm1") })
}

// A hint that can't be written fails the delta, and the manifest never has a
// wake time without its hint. The delta timer tries again.
func TestFailedHintWriteFailsTheDelta(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, both(quick, deltaAge(50*time.Millisecond)))
	var failing atomic.Bool
	failing.Store(true)
	var failures atomic.Int32
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if !after && failing.Load() && op == blob.OpCreate && strings.HasPrefix(key, wakePrefix) {
			failures.Add(1)
			return errors.New("storage down")
		}
		return nil
	})
	var up atomic.Bool
	attempts := r.intakeDownUntil(&up)
	r.addMessage("users/fd1", "users/fd2", "/x", "x")
	eventually(t, "a first attempt", func() bool { return attempts.Load() >= 1 })
	eventually(t, "a failed hint write", func() bool { return failures.Load() >= 1 })
	r.afterSnapshotLooks(3, "users/fd1")
	if m, err := r.manifest("users/fd1"); err != nil || m.Wake != 0 {
		t.Fatalf("manifest %+v (%v): a wake time without its hint", m, err)
	}
	failing.Store(false)
	eventually(t, "the delta with its wake time", func() bool {
		m, err := r.manifest("users/fd1")
		return err == nil && m.Wake != 0
	})
	if keys := r.wakes(); len(keys) != 1 {
		t.Fatalf("hints %v", keys)
	}
}

// A row given up on after a delta recorded it changes the wake time with no
// commit, and the deactivation records that with no delta.
func TestDeactivationWithNoTailUpdatesTheWakeTime(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, both(quick, deltaAge(50*time.Millisecond)))
	var giveUp atomic.Bool
	rec := &recorder{keep: true, answer: func(int, *http.Request) int {
		if giveUp.Load() {
			return http.StatusBadRequest
		}
		return http.StatusServiceUnavailable
	}}
	r.record(rec, "")
	r.setRow("users/sw1", "k", "/due", time.Now().Unix())
	eventually(t, "a delta with a wake time", r.allInADelta("users/sw1"))
	before, err := r.manifest("users/sw1")
	if err != nil {
		t.Fatal(err)
	}
	giveUp.Store(true)
	r.closeInApp("users/sw1")
	eventually(t, "deactivation", func() bool { return r.cached("users/sw1") })
	m, err := r.manifest("users/sw1")
	if err != nil || m.Wake != 0 || len(m.Files) != len(before.Files) {
		t.Fatalf("manifest %+v (%v), want no wake time and no new file (before %+v)", m, err, before)
	}
}

// --- the scan checks the manifest ---

// staleHint writes a due hint for target by hand.
func (r *srig) staleHint(target string) {
	r.t.Helper()
	if _, err := (hintStore{r.n.Store}).writeWake(context.Background(), target, time.Now()); err != nil {
		r.t.Fatal(err)
	}
}

// cache commits to target, has the app close it and waits until it is cached.
func (r *srig) cache(target string) {
	r.t.Helper()
	r.commitTo(target)
	r.closeInApp(target)
	eventually(r.t, "deactivation", func() bool { return r.cached(target) })
}

// A hint the manifest of a cached shard doesn't back is deleted by the scan
// without activating the shard.
func TestStaleHintForACachedShardIsDeletedWithoutAnActivation(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	r.cache("users/sc1")
	if m, err := r.manifest("users/sc1"); err != nil || m.Owner != "n1" || m.Wake != 0 {
		t.Fatalf("manifest %+v (%v)", m, err)
	}
	r.staleHint("users/sc1")
	before := r.m.Stats.Activations.Load()
	r.m.wakes.scan(context.Background())
	if n := r.m.Stats.Activations.Load(); n != before || r.m.IsActive("users/sc1") {
		t.Fatalf("the scan activated the shard (%d activations, before %d)", n, before)
	}
	if keys := r.wakes(); len(keys) != 0 {
		t.Fatalf("hints left: %v", keys)
	}
}

// The same for a shard that was evicted and has no owner.
func TestStaleHintForAnEvictedShardIsDeletedWithoutAnActivation(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	r.cache("users/se1")
	r.cache("users/se2")
	used, err := r.m.cache.idx.cachedBytes()
	if err != nil {
		t.Fatal(err)
	}
	r.m.SetCacheLimit(used + used/4)
	r.cache("users/se3")
	eventually(t, "eviction", func() bool { return !r.cached("users/se1") })
	if m, err := r.manifest("users/se1"); err != nil || m.Owner != "" {
		t.Fatalf("manifest %+v (%v)", m, err)
	}
	r.staleHint("users/se1")
	before := r.m.Stats.Activations.Load()
	r.m.wakes.scan(context.Background())
	if n := r.m.Stats.Activations.Load(); n != before || r.m.IsActive("users/se1") {
		t.Fatalf("the scan activated the shard (%d activations, before %d)", n, before)
	}
	if keys := r.wakes(); len(keys) != 0 {
		t.Fatalf("hints left: %v", keys)
	}
}

// A cached shard whose manifest has a wake time that is due is woken by its
// hint. (A shard with a due row doesn't deactivate while the row keeps failing,
// so the wake time is set by hand.)
func TestDueWakeTimeInTheManifestWakesACachedShard(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	r.cache("users/sd1")
	ctx := context.Background()
	h, err := r.n.Files.Attach(ctx, "users/sd1")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.SetWake(ctx, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	r.staleHint("users/sd1")
	before := r.m.Stats.Activations.Load()
	r.m.wakes.scan(ctx)
	if n := r.m.Stats.Activations.Load(); n != before+1 {
		t.Fatalf("%d activations, want %d", n, before+1)
	}
}

// A delta whose wake time is read while a commit that isn't captured yet has
// removed work records now, not the read's answer: the delta holds the work,
// and a crash before the commit is captured would bring it back.
func TestDeltaAheadOfAnUncapturedCommitRecordsNow(t *testing.T) {
	t.Parallel()
	// The delta timer never fires: the test runs the delta on the shard's loop.
	r := newSRigWith(t, both(manualFlush, deltaAge(time.Nanosecond), func(o *CatalogOptions) { o.Lifecycle.SnapshotEvery = time.Hour }))
	later := time.Now().Add(time.Hour).Unix()
	r.setRow("slow/ua1", "k", "/later", later)
	r.cycle() // the row is captured, in the tail
	a := r.shard("slow/ua1")
	r.sql("slow/ua1", `DELETE FROM _shardlite_inbox`) // not captured: no cycle runs
	if !a.cap.hasNewFrames() {
		t.Fatal("the delete was captured")
	}
	before := time.Now().Unix()
	if _, err := do(context.Background(), a.ops, a.ended, func() struct{} { a.snapshot(a.d.base); return struct{}{} }); err != nil {
		t.Fatal(err)
	}
	m, err := r.manifest("slow/ua1")
	if err != nil {
		t.Fatal(err)
	}
	if m.Wake == later || m.Wake < before || m.Wake > time.Now().Unix() {
		t.Fatalf("wake %d, want now (the row was due at %d)", m.Wake, later)
	}
	if keys := r.wakes(); len(keys) != 1 || keys[0] != wakeKey(time.Unix(m.Wake, 0), "slow/ua1") {
		t.Fatalf("hints %v for a wake time of %d", keys, m.Wake)
	}
}
