package node

import (
	"context"
	"errors"
	"github.com/luci1900/shardlite/internal/test/apptest"
	"github.com/luci1900/shardlite/internal/test/harness"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/node/dispatch"
	"github.com/luci1900/shardlite/internal/storage/blob"
	"github.com/luci1900/shardlite/internal/storage/shardfile"
	"github.com/luci1900/shardlite/internal/test/blobtest"
)

func TestNeverWrittenWithAnAutoincrementSequence(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	// An AUTOINCREMENT table of the app's that came and went leaves sqlite_sequence.
	r.sql("users/s1", `DROP TABLE kv; DROP TABLE acks; CREATE TABLE tmp (id INTEGER PRIMARY KEY AUTOINCREMENT); INSERT INTO tmp DEFAULT VALUES; DROP TABLE tmp`)
	if n := r.count("users/s1", `SELECT count(*) FROM sqlite_schema WHERE name = 'sqlite_sequence'`); n != 1 {
		t.Fatalf("%d sqlite_sequence tables", n)
	}
	r.closeInApp("users/s1")
	apptest.Eventually(t, apptest.Timeout, "deactivation", r.inactive("users/s1"))
	if _, err := r.rawManifest("users/s1"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("a shard with only the reserved table and a sequence was kept: %v", err)
	}
	// The app's own AUTOINCREMENT table is the app's data.
	r.sql("users/s2", `DROP TABLE kv; DROP TABLE acks; CREATE TABLE mine (id INTEGER PRIMARY KEY AUTOINCREMENT)`)
	r.closeInApp("users/s2")
	apptest.Eventually(t, apptest.Timeout, "deactivation", func() bool { return r.released("users/s2") })
}

// --- next_reminder and the reminder index ---

// withDispatch runs _dispatch, which wakes the inactive shards whose entries are
// due, with a pass every second when none is.
func withDispatch(o *ShardsetOptions) { o.Dispatch = &dispatch.Options{Every: time.Second} }

// nextReminder is the minute the manifest of target says it next needs waking, 0
// for none.
func (r *mrig) nextReminder(target string) shardfile.Minute {
	r.t.Helper()
	m, err := r.manifest(target)
	if err != nil {
		r.t.Fatal(err)
	}
	return m.NextReminder
}

// entryCount is how many entries the reminder index has for target.
func (r *mrig) entryCount(target string) int {
	r.t.Helper()
	all := allEntries(r.t, r.n.Store)
	n := 0
	for _, e := range all {
		if e.Shard.String() == target {
			n++
		}
	}
	return n
}

// shutdown is what Run does: let go of every shard and give up the lease.
func (r *srig) shutdown(appHandles ...string) {
	r.t.Helper()
	ctx := context.Background()
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

func (r *srig) restart(mutate func(*ShardsetOptions)) {
	r.t.Helper()
	r.startNode()
	r.startShardset(mutate)
	r.startServer()
}

func TestShutdownWithAFailingHandlerLeavesAnEntryThatWakesTheShard(t *testing.T) {
	t.Parallel()
	opts := both(quick, withDispatch)
	for _, tc := range []struct {
		name       string
		deleteData bool
	}{{"same machine", false}, {"new machine", true}} {
		t.Run(tc.name, func(t *testing.T) {
			r := newSRigWith(t, opts)
			var up atomic.Bool
			rec := &recorder{}
			r.record(rec, "")
			attempts := r.failUntil(&up)
			r.remind("users/w1", "k", time.Now().Unix())
			apptest.Eventually(t, apptest.Timeout, "a first attempt", func() bool { return attempts.Load() >= 1 })
			r.shutdown("users/w1")
			if r.nextReminder("users/w1") == 0 || r.entryCount("users/w1") == 0 {
				t.Fatalf("next_reminder %d and %d entries, want both", r.nextReminder("users/w1"), r.entryCount("users/w1"))
			}
			if tc.deleteData {
				if err := os.RemoveAll(r.data); err != nil {
					t.Fatal(err)
				}
			}
			up.Store(true)
			r.restart(opts)
			apptest.Eventually(t, apptest.Timeout, "the reminder to be delivered after the restart, with no request for the shard", func() bool {
				return rec.count() == 1
			})
			apptest.Eventually(t, apptest.Timeout, "the reminder to be consumed", func() bool { return r.reminderRows("users/w1") == 0 })
			r.closeInApp("users/w1") // the handler's delete opened the shard in the app again
			apptest.Eventually(t, apptest.Timeout, "the next_reminder to go", func() bool { return r.released("users/w1") && r.nextReminder("users/w1") == 0 })
		})
	}
}

// A shard that was woken and whose node crashes with nothing of it in the journal
// is woken again from its entry, which the dispatcher moved a minute on when it
// dispatched the shard.
func TestCrashAfterTheWakeKeepsTheEntry(t *testing.T) {
	t.Parallel()
	clk := harness.NewFakeClock()
	opts := both(quick, withDispatch, withClock(clk))
	r := newSRigWith(t, opts)
	var up atomic.Bool
	rec := &recorder{}
	r.record(rec, "")
	attempts := r.failUntil(&up)
	r.remind("users/c1", "k", clk.Now().Unix())
	advanceUntil(t, clk, 100*time.Millisecond, "a first attempt", func() bool { return attempts.Load() >= 1 })
	r.shutdown("users/c1")

	// Woken, but the receiver still fails, and the node crashes.
	before := attempts.Load()
	r.restart(opts)
	advanceUntil(t, clk, 100*time.Millisecond, "the wake to start deliveries", func() bool { return attempts.Load() > before })
	r.crash()
	r.stopJournal()
	if r.nextReminder("users/c1") == 0 || r.entryCount("users/c1") == 0 {
		t.Fatalf("next_reminder %d and %d entries after a crash, want both", r.nextReminder("users/c1"), r.entryCount("users/c1"))
	}

	// Woken again, once the moved entry is due.
	up.Store(true)
	r.restart(opts)
	clk.Advance(2 * time.Minute)
	advanceUntil(t, clk, 100*time.Millisecond, "delivery after the second wake", func() bool { return rec.count() == 1 })
	advanceUntil(t, clk, 100*time.Millisecond, "the reminder to be consumed", func() bool { return r.reminderRows("users/c1") == 0 })
	r.closeInApp("users/c1") // the handler's delete opened the shard in the app again
	advanceUntil(t, clk, 100*time.Millisecond, "the next_reminder to go", func() bool { return r.released("users/c1") && r.nextReminder("users/c1") == 0 })
}

// --- next_reminder at manifest writes ---

// deltaAge makes a shard's tail a delta once it is d old.
func deltaAge(d time.Duration) func(*ShardsetOptions) {
	return func(o *ShardsetOptions) { o.Lifecycle.DeltaAge = d }
}

// allInADelta is true once every commit of target is in a delta that recorded
// its next_reminder: nothing uncaptured and no tail.
func (r *srig) allInADelta(target string) func() bool {
	return func() bool {
		a := r.shard(target)
		m, err := r.manifest(target)
		return err == nil && len(m.Files) > 0 && m.NextReminder != 0 && !a.cap.hasNewFrames() && a.cap.load().TailBytes() == 0
	}
}

// wokeByTheDelta waits for a background delta of target to record its
// next_reminder, then crashes the node and starts it again with mutate. The
// shard's tail was a delta by then, so adoption doesn't create an entry for it:
// the delta's entry wakes it.
func (r *srig) wokeByTheDelta(target string, mutate func(*ShardsetOptions)) {
	r.t.Helper()
	apptest.Eventually(r.t, apptest.Timeout, "a delta with a next_reminder", r.allInADelta(target))
	m, err := r.manifest(target)
	if err != nil {
		r.t.Fatal(err)
	}
	r.closeInApp(target)
	r.crash()
	r.stopJournal()
	r.restart(mutate)
	if after, err := r.manifest(target); err != nil || after.LastPosition() != m.LastPosition() {
		r.t.Fatalf("manifest %+v (%v) after the restart, was %+v: the shard still had a tail", after, err, m)
	}
}

// A due reminder whose handler keeps failing past a delta, then a crash: the
// delta's entry wakes the shard, and the reminder is delivered.
func TestReminderFailingPastADeltaIsWokenAfterACrash(t *testing.T) {
	t.Parallel()
	opts := both(quick, withDispatch, deltaAge(50*time.Millisecond))
	r := newSRigWith(t, opts)
	var up atomic.Bool
	rec := &recorder{answer: func(int, *http.Request) int {
		if up.Load() {
			return http.StatusOK
		}
		return http.StatusServiceUnavailable
	}}
	r.record(rec, "")
	r.remind("users/bi1", "k", time.Now().Unix())
	apptest.Eventually(t, apptest.Timeout, "a first call", func() bool { return rec.count() >= 1 })
	r.wokeByTheDelta("users/bi1", opts)
	up.Store(true)
	apptest.Eventually(t, apptest.Timeout, "the reminder to be consumed after the crash", func() bool { return r.reminderRows("users/bi1") == 0 })
}

// The same for a marked shard, which is deleted when it next goes idle.
func TestMarkedShardPastADeltaIsWokenAfterACrash(t *testing.T) {
	t.Parallel()
	opts := both(quick, withDispatch, deltaAge(50*time.Millisecond))
	r := newSRigWith(t, opts)
	r.sql("slow/bm1", `INSERT INTO kv (payload) VALUES (x'01')`)
	r.sql("slow/bm1", markSQL)
	r.wokeByTheDelta("slow/bm1", opts)
	apptest.Eventually(t, apptest.Timeout, "the shard to be woken", func() bool { return r.m.IsActive(shardName("slow/bm1")) })
}

// An entry that can't be created fails the delta, so the manifest never has files
// with a reminder and no entry. The delta timer tries again.
func TestFailedEntryFailsTheDelta(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, both(quick, deltaAge(50*time.Millisecond)))
	var failing atomic.Bool
	failing.Store(true)
	var failures atomic.Int32
	r.local.SetHook(func(_ context.Context, op blobtest.Op, key string, after bool) error {
		if failing.Load() && op == blobtest.OpCreate && strings.HasPrefix(key, shardfile.ReminderPrefix) {
			failures.Add(1)
			return errors.New("storage down")
		}
		return nil
	})
	var up atomic.Bool
	attempts := r.failUntil(&up)
	r.remind("users/fd1", "k", time.Now().Unix())
	apptest.Eventually(t, apptest.Timeout, "a first attempt", func() bool { return attempts.Load() >= 1 })
	apptest.Eventually(t, apptest.Timeout, "a failed entry", func() bool { return failures.Load() >= 1 })
	r.afterSnapshotLooks(3, "users/fd1")
	if m, err := r.manifest("users/fd1"); err != nil || len(m.Files) != 0 {
		t.Fatalf("manifest %+v (%v): a delta without its entry", m, err)
	}
	failing.Store(false)
	apptest.Eventually(t, apptest.Timeout, "the delta with its entry", r.allInADelta("users/fd1"))
	if r.entryCount("users/fd1") == 0 {
		t.Fatal("no entry")
	}
}

// A reminder that goes stale after a delta recorded it changes the next
// reminder with no commit: the deactivation writes no delta, and clears the
// next_reminder alone, so the dispatcher doesn't wake the shard for nothing.
func TestDeactivationWithNoTailClearsTheNextReminder(t *testing.T) {
	t.Parallel()
	clk := harness.NewFakeClock()
	r := newSRigWith(t, both(quick, deltaAge(50*time.Millisecond), withClock(clk)))
	rec := &recorder{keep: true, answer: func(int, *http.Request) int { return http.StatusServiceUnavailable }}
	r.record(rec, "")
	r.remind("users/sw1", "k", clk.Now().Unix())
	advanceUntil(t, clk, 100*time.Millisecond, "a delta with a next_reminder", r.allInADelta("users/sw1"))
	before, err := r.manifest("users/sw1")
	if err != nil {
		t.Fatal(err)
	}
	r.closeInApp("users/sw1")
	clk.Advance(25 * time.Hour) // the reminder is stale now
	advanceUntil(t, clk, 100*time.Millisecond, "deactivation", func() bool { return r.released("users/sw1") })
	after, err := r.manifest("users/sw1")
	if err != nil || after.NextReminder != 0 || !slices.Equal(after.Files, before.Files) {
		t.Fatalf("manifest %+v (%v), want the files of %+v and no next_reminder", after, err, before)
	}
}

// A delta whose next reminder is read while a commit that isn't captured yet has
// removed work records now, not the read's answer: the delta holds the work,
// and a crash before the commit is captured would bring it back.
func TestDeltaAheadOfAnUncapturedCommitRecordsNow(t *testing.T) {
	t.Parallel()
	// The delta timer never fires: the test runs the delta on the shard's loop.
	r := newSRigWith(t, both(manualFlush, deltaAge(time.Nanosecond), func(o *ShardsetOptions) { o.Lifecycle.SnapshotEvery = time.Hour }))
	later := time.Now().Add(time.Hour).Unix()
	r.remind("slow/ua1", "k", later)
	r.cycle() // the row is captured, in the tail
	a := r.shard("slow/ua1")
	r.sql("slow/ua1", `DELETE FROM _shardlite_reminders`) // not captured: no cycle runs
	if !a.cap.hasNewFrames() {
		t.Fatal("the delete was captured")
	}
	before := time.Now().Unix()
	done := make(chan error, 1)
	go func() {
		_, err := do(context.Background(), a.ops, a.ended, func() struct{} { a.snapshot(a.d.base); return struct{}{} })
		done <- err
	}()
	if err := <-done; err != nil { // the delta waits for no flush cycle: the delete stays uncaptured
		t.Fatal(err)
	}
	m, err := r.manifest("slow/ua1")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) == 0 {
		t.Fatal("no delta")
	}
	if got := m.NextReminder; got == shardfile.MinuteOf(later) || got < shardfile.MinuteOf(before) || got > shardfile.MinuteOf(time.Now().Unix()) {
		t.Fatalf("next_reminder is minute %d, want now (the reminder was due at minute %d)", got, shardfile.MinuteOf(later))
	}
}
