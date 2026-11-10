package node

import (
	"errors"
	"fmt"
	"github.com/luci1900/shardlite/internal/apptest"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/shardfile"
)

func withClock(c Clock) func(*ShardsetOptions) {
	return func(o *ShardsetOptions) { o.Clock = c }
}

// remind sets a reminder on the shard, replacing the one with the same key.
func (r *srig) remind(target, key string, due int64) {
	r.t.Helper()
	r.sql(target, fmt.Sprintf(`INSERT OR REPLACE INTO _shardlite_reminders (key, due) VALUES ('%s', %d)`, key, due))
}

// --- the wake ---

func TestReminderWakesOnceAfterItsCommitIsDurable(t *testing.T) {
	t.Parallel()
	clk := harness.NewFakeClock()
	r := newSRigWith(t, both(quick, withClock(clk)))
	rec := &recorder{}
	r.record(rec, "wallet/rm1")
	due := clk.Now().Unix() + 2
	r.sql("wallet/rm1", fmt.Sprintf(`BEGIN; INSERT INTO kv (payload) VALUES (x'01'); INSERT INTO _shardlite_reminders (key, due) VALUES ('upgrade:barracks', %d); COMMIT;`, due))
	apptest.Eventually(t, apptest.Timeout, "the timer for the reminder", r.armed("wallet/rm1"))
	clk.Advance(time.Second)
	r.settle("wallet/rm1")
	if rec.count() != 0 {
		t.Fatal("woken early")
	}
	clk.Advance(time.Second)
	apptest.Eventually(t, apptest.Timeout, "the wake", func() bool { return rec.count() >= 1 })
	apptest.Eventually(t, apptest.Timeout, "the handler's delete", func() bool { return r.reminderRows("wallet/rm1") == 0 })
	r.settle("wallet/rm1") // a second wake would come in these cycles
	if rec.count() != 1 {
		t.Fatalf("%d wakes", rec.count())
	}
	d := rec.all()[0]
	if clk.Now().Unix() < due {
		t.Fatalf("woken %d s early", due-clk.Now().Unix())
	}
	if d.shardRows != 1 || d.method != "POST" || d.target != "wallet/rm1" || d.hdr.Get("Shardlite-Caller") != "wallet/rm1" || d.hdr.Get(hdrContext) != ctxOf("wallet/rm1", 0) {
		t.Fatalf("wake %+v", d)
	}
}

// A reminder set in the past still waits for the commit that set it.
func TestReminderInThePastWaitsForTheFlush(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	rec := &recorder{}
	r.record(rec, "wallet/rm2")
	r.sql("wallet/rm2", fmt.Sprintf(`BEGIN; INSERT INTO kv (payload) VALUES (x'01'); INSERT INTO _shardlite_reminders (key, due) VALUES ('k', %d); COMMIT;`, time.Now().Unix()-5))
	if rec.count() != 0 { // only a flush cycle reads reminders, and none runs until cyclesWhile
		t.Fatal("woken before any flush")
	}
	r.cyclesWhile("the wake", func() bool { return rec.count() >= 1 })
	if rec.all()[0].shardRows != 1 {
		t.Fatalf("the shard's commit was not in blob storage: %d rows", rec.all()[0].shardRows)
	}
}

func TestReminderOfALostCommitNeverWakes(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	rec := &recorder{}
	r.record(rec, "")
	r.remind("wallet/rm3", "k", time.Now().Unix()-5)
	r.closeInApp("wallet/rm3")
	r.crash()
	r.stopJournal()
	r.restart(manualFlush)
	ref := r.acquire("wallet/rm3")
	defer ref.Release()
	for range 10 {
		r.cycle()
	}
	if n := r.reminderRows("wallet/rm3"); n != 0 { // a reminder that is not there cannot wake
		t.Fatalf("%d reminders came back", n)
	}
	if rec.count() != 0 {
		t.Fatal("a reminder whose commit was lost woke the shard")
	}
}

// A reminder set with a sooner time while one is pending moves the wake
// earlier, and one set with a later time than the pending one doesn't move it.
func TestSoonerReminderMovesTheWakeEarlier(t *testing.T) {
	t.Parallel()
	clk := harness.NewFakeClock()
	r := newSRigWith(t, both(quick, withClock(clk)))
	rec := &recorder{}
	r.record(rec, "")
	now := clk.Now().Unix()
	r.remind("users/sm1", "late", now+100)
	apptest.Eventually(t, apptest.Timeout, "the timer", r.armed("users/sm1"))
	r.remind("users/sm1", "soon", now+10)
	r.settle("users/sm1")
	clk.Advance(20 * time.Second)
	apptest.Eventually(t, apptest.Timeout, "the wake at the sooner time", func() bool { return rec.count() == 1 })
	apptest.Eventually(t, apptest.Timeout, "the sooner reminder to be handled", func() bool { return r.reminderRows("users/sm1") == 1 })
	r.settle("users/sm1")
	if rec.count() != 1 {
		t.Fatalf("%d wakes", rec.count())
	}
}

// A wake that fails (an error status, or a 2xx that leaves a reminder due) is
// repeated after a backoff that doubles, and a success resets it.
func TestFailingWakeIsRetriedWithBackoff(t *testing.T) {
	t.Parallel()
	clk := harness.NewFakeClock()
	r := newSRigWith(t, func(o *ShardsetOptions) {
		o.Clock = clk
		o.Reminders.Backoff, o.Reminders.MaxBackoff = 10*time.Second, 40*time.Second
	})
	var mode atomic.Int32 // 0: 500, 1: 200 without handling, 2: handled
	rec := &recorder{keep: true, answer: func(int, *http.Request) int {
		if mode.Load() == 0 {
			return 500
		}
		return 200
	}}
	r.record(rec, "")
	r.remind("users/fw1", "k", clk.Now().Unix()-1)
	apptest.Eventually(t, apptest.Timeout, "the first wake", func() bool { return rec.count() == 1 })
	apptest.Eventually(t, apptest.Timeout, "the failure", func() bool { return r.m.Stats.ReminderWakeFailures.Load() == 1 })
	r.settle("users/fw1")
	clk.Advance(4 * time.Second) // under the first backoff (5 to 10 s)
	r.settle("users/fw1")
	if rec.count() != 1 {
		t.Fatalf("%d wakes inside the backoff", rec.count())
	}
	mode.Store(1)
	advanceUntil(t, clk, time.Second, "the second wake", func() bool { return rec.count() == 2 })
	apptest.Eventually(t, apptest.Timeout, "the second failure (a 2xx that left a reminder due)", func() bool { return r.m.Stats.ReminderWakeFailures.Load() == 2 })
	// The second wait is 10 to 20 s: never sooner than the first one's cap.
	r.settle("users/fw1")
	clk.Advance(9 * time.Second)
	r.settle("users/fw1")
	if rec.count() != 2 {
		t.Fatalf("%d wakes inside the second backoff", rec.count())
	}
	rec.keep = false // the handler handles the reminder now
	advanceUntil(t, clk, time.Second, "the third wake", func() bool { return rec.count() == 3 })
	apptest.Eventually(t, apptest.Timeout, "the reminder to be handled", func() bool { return r.reminderRows("users/fw1") == 0 })
	r.settle("users/fw1")
	if rec.count() != 3 || r.m.Stats.ReminderWakeFailures.Load() != 2 {
		t.Fatalf("%d wakes and %d failures", rec.count(), r.m.Stats.ReminderWakeFailures.Load())
	}
}

// A handler that keeps setting a reminder due now for itself costs one wake per
// backoff, not a loop.
func TestHandlerSettingRemindersDueNowIsWokenWithBackoff(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, func(o *ShardsetOptions) {
		o.Reminders.Backoff, o.Reminders.MaxBackoff = 200*time.Millisecond, 200*time.Millisecond
	})
	rec := &recorder{keep: true}
	r.record(rec, "")
	inner := *r.handler.Load()
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == remindersPath {
			r.sql(q.Header.Get("Shardlite-Opened"), fmt.Sprintf(`INSERT OR REPLACE INTO _shardlite_reminders (key, due) VALUES ('again', %d)`, time.Now().Unix()))
		}
		inner(w, q)
	})
	r.remind("users/hl1", "again", time.Now().Unix())
	apptest.Eventually(t, apptest.Timeout, "five wakes", func() bool { return rec.count() >= 5 })
	if f := r.m.Stats.ReminderWakeFailures.Load(); f < 4 {
		t.Fatalf("%d failures for %d wakes", f, rec.count())
	}
}

// A recurring reminder: the handler sets the next time itself in the transaction
// that does the work, and each time wakes the shard once.
func TestRecurringReminder(t *testing.T) {
	t.Parallel()
	clk := harness.NewFakeClock()
	r := newSRigWith(t, both(quick, withClock(clk)))
	rec := &recorder{keep: true}
	r.record(rec, "")
	inner := *r.handler.Load()
	var wakes atomic.Int32
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == remindersPath {
			if wakes.Add(1) < 3 {
				r.sql("users/rc1", fmt.Sprintf(`UPDATE _shardlite_reminders SET due = due + 2 WHERE due <= %d`, r.m.now().Unix()))
			} else {
				r.sql("users/rc1", fmt.Sprintf(`DELETE FROM _shardlite_reminders WHERE due <= %d`, r.m.now().Unix()))
			}
		}
		inner(w, q)
	})
	first := clk.Now().Unix() - 1
	r.remind("users/rc1", "daily", first)
	ticks := func() int { return rec.count() }
	apptest.Eventually(t, apptest.Timeout, "the first wake", func() bool { return ticks() >= 1 })
	// The times are first, first+2 and first+4, and the clock is at first+1: a
	// wake comes only when the clock reaches the reminder's new time.
	notYet := func(n int) {
		t.Helper()
		r.settle("users/rc1")
		if got := ticks(); got != n {
			t.Fatalf("%d wakes, want %d before the clock reaches the next time", got, n)
		}
	}
	notYet(1)
	clk.Advance(time.Second)
	apptest.Eventually(t, apptest.Timeout, "the second wake", func() bool { return ticks() >= 2 })
	notYet(2)
	clk.Advance(time.Second)
	notYet(2)
	clk.Advance(time.Second)
	apptest.Eventually(t, apptest.Timeout, "the third wake", func() bool { return ticks() >= 3 })
	apptest.Eventually(t, apptest.Timeout, "the last reminder to go", func() bool { return r.reminderRows("users/rc1") == 0 })
	r.settle("users/rc1")
	if got := ticks(); got != 3 {
		t.Fatalf("%d wakes, want 3", got)
	}
	if clk.Now().Unix() < first+4 {
		t.Fatal("the later times woke early")
	}
}

// The node makes no commits to a shard whose only rows are reminders.
func TestNodeMakesNoCommitsForReminders(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{keep: true}
	r.record(rec, "")
	r.sql("users/nc1", fmt.Sprintf(`INSERT INTO _shardlite_reminders (key, due) VALUES ('k', %d)`, time.Now().Unix()-1))
	apptest.Eventually(t, apptest.Timeout, "the wake", func() bool { return rec.count() >= 1 })
	sh := r.shard("users/nc1").cap.load()
	before := header(t, sh)
	pos := sh.Position()
	r.settle("users/nc1") // the node's own cycles would have committed by now
	r.cycle()
	if header(t, sh) != before || sh.Position() != pos {
		t.Fatal("the node committed to a shard with only a reminder")
	}
}

func TestReplacingAndCancellingReminders(t *testing.T) {
	t.Parallel()
	clk := harness.NewFakeClock()
	r := newSRigWith(t, both(quick, withClock(clk)))
	rec := &recorder{}
	r.record(rec, "")
	now := clk.Now().Unix()
	r.remind("users/rp1", "k", now+100)
	r.remind("users/rp1", "k", now+200) // replaces it
	r.remind("users/rp1", "gone", now+120)
	r.sql("users/rp1", `DELETE FROM _shardlite_reminders WHERE key = 'gone'`)
	r.settle("users/rp1") // the flush covering the commits, and the timer
	clk.Advance(150 * time.Second)
	r.settle("users/rp1")
	if rec.count() != 0 {
		t.Fatalf("woken: %+v", rec.all())
	}
	clk.Advance(100 * time.Second)
	apptest.Eventually(t, apptest.Timeout, "the wake", func() bool { return rec.count() == 1 })
	apptest.Eventually(t, apptest.Timeout, "the handler's delete", func() bool { return r.reminderRows("users/rp1") == 0 })
	r.settle("users/rp1")
	if rec.count() != 1 {
		t.Fatalf("%d wakes", rec.count())
	}
}

// --- waking ---

func TestReminderAnHourAwayWakesTheShard(t *testing.T) {
	t.Parallel()
	for _, tc := range []string{"after deactivation", "after a crash", "new machine"} {
		t.Run(tc, func(t *testing.T) {
			clk := harness.NewFakeClock()
			opts := both(quick, withDispatch, withClock(clk))
			r := newSRigWith(t, opts)
			rec := &recorder{}
			r.record(rec, "")
			due := clk.Now().Unix() + 3600
			r.remind("users/wh1", "k", due)
			switch tc {
			case "after deactivation":
				r.closeInApp("users/wh1")
				advanceUntil(t, clk, 100*time.Millisecond, "deactivation (a reminder an hour away does not hold the shard)", func() bool { return r.released("users/wh1") })
				if got := r.nextReminder("users/wh1"); got != shardfile.MinuteOf(due) {
					t.Fatalf("next_reminder %d, want minute %d", got, shardfile.MinuteOf(due))
				}
				clk.Advance(3601 * time.Second)
			case "after a crash":
				// The reminder's commit is in the journal, so the restart's adoption
				// records a reminder due now.
				apptest.Eventually(t, apptest.Timeout, "the reminder's commit to be durable", func() bool { return r.durableReminders("users/wh1") == 1 })
				if got := r.nextReminder("users/wh1"); got != 0 {
					t.Fatalf("next_reminder %d while the reminder is in the tail", got)
				}
				r.closeInApp("users/wh1")
				r.crash()
				r.stopJournal()
				clk.Advance(3601 * time.Second)
				r.restart(opts)
			case "new machine":
				r.shutdown("users/wh1")
				if got := r.nextReminder("users/wh1"); got != shardfile.MinuteOf(due) {
					t.Fatalf("next_reminder %d, want minute %d", got, shardfile.MinuteOf(due))
				}
				if err := removeAll(r.data); err != nil {
					t.Fatal(err)
				}
				clk.Advance(3601 * time.Second)
				r.restart(opts)
			}
			advanceUntil(t, clk, 100*time.Millisecond, "the wake", func() bool { return rec.count() >= 1 })
			// The next_reminder goes at the shard's next manifest write, its
			// deactivation, which finds no reminder.
			apptest.Eventually(t, apptest.Timeout, "the handler's delete", func() bool { return r.reminderRows("users/wh1") == 0 })
			r.closeInApp("users/wh1")
			advanceUntil(t, clk, 100*time.Millisecond, "the next_reminder to go", func() bool { return r.released("users/wh1") && r.nextReminder("users/wh1") == 0 })
		})
	}
}

func TestReminderDueNowKeepsTheShardActive(t *testing.T) {
	t.Parallel()
	clk := harness.NewFakeClock()
	// A wake counts as a use of the shard, so the backoff has to be longer than the
	// idle timeout: with a shorter one a loaded machine wakes the shard at every
	// step of the clock, the last use never goes stale and the idle timer never
	// looks at the shard.
	slow := func(o *ShardsetOptions) { o.Reminders.Backoff, o.Reminders.MaxBackoff = time.Second, 2*time.Second }
	r := newSRigWith(t, both(slow, withClock(clk)))
	var up atomic.Bool
	rec := &recorder{answer: downUntil(&up)}
	r.record(rec, "")
	r.remind("users/dn1", "k", clk.Now().Unix()-5)
	r.closeInApp("users/dn1") // idle timeout is 300 ms
	// Move the clock past the idle timeout until the idle timer has looked at the
	// shard and kept it (it sets the shard's next look when it does).
	advanceUntil(t, clk, 100*time.Millisecond, "the idle timer to keep the shard", r.lookedAt("users/dn1"))
	if !r.m.IsActive(shardName("users/dn1")) {
		t.Fatal("a shard with a reminder due was deactivated")
	}
	advanceUntil(t, clk, 50*time.Millisecond, "a second wake (the backoff is on the clock)", func() bool { return rec.count() >= 2 })
	up.Store(true)
	advanceUntil(t, clk, 50*time.Millisecond, "the reminder to be handled (the backoff is on the clock)", func() bool { return r.reminderRows("users/dn1") == 0 })
	r.closeInApp("users/dn1") // the handler's delete opened the shard in the app again
	advanceUntil(t, clk, time.Second, "deactivation after the reminder", func() bool { return r.released("users/dn1") })
}

// markSQL is what the app runs to mark a shard for deletion (the SDK's
// MarkDeleted): a row in the table the node created.
const markSQL = `INSERT OR IGNORE INTO _shardlite_delete VALUES (1)`

// A marked shard with a reminder due later is deleted at deactivation and the
// reminder goes with it.
func TestMarkedShardWithAReminderDueLaterIsDeletedWithIt(t *testing.T) {
	t.Parallel()
	clk := harness.NewFakeClock()
	r := newSRigWith(t, both(quick, withClock(clk)))
	rec := &recorder{}
	r.record(rec, "")
	r.remind("lobby/rm1", "timeout", clk.Now().Unix()+3600)
	r.sql("lobby/rm1", markSQL)
	r.closeInApp("lobby/rm1")
	advanceUntil(t, clk, time.Second, "deletion", func() bool { return r.m.Stats.Deletes.Load() == 1 && r.inactive("lobby/rm1")() })
	if _, err := r.rawManifest("lobby/rm1"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v, want none", err)
	}
	if rec.count() != 0 {
		t.Fatal("the dropped reminder woke the shard")
	}
}

// A marked shard with a reminder that is due is woken for it and kept until
// the handler has handled it, then deleted.
func TestMarkedShardWithAReminderDueIsDeletedAfterItIsHandled(t *testing.T) {
	t.Parallel()
	clk := harness.NewFakeClock()
	r := newSRigWith(t, both(quick, withClock(clk)))
	rec := &recorder{}
	r.record(rec, "")
	r.sql("lobby/rm2", markSQL)
	r.remind("lobby/rm2", "now", clk.Now().Unix())
	apptest.Eventually(t, apptest.Timeout, "the wake", func() bool { return rec.count() == 1 })
	apptest.Eventually(t, apptest.Timeout, "the handler's delete", func() bool { return r.reminderRows("lobby/rm2") == 0 })
	r.closeInApp("lobby/rm2") // the handler's delete opened the shard in the app again
	advanceUntil(t, clk, time.Second, "deletion", func() bool { return r.m.Stats.Deletes.Load() == 1 && r.inactive("lobby/rm2")() })
	if _, err := r.rawManifest("lobby/rm2"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v, want none", err)
	}
}

// Shutdown deletes a marked shard like any deactivation does, even with a
// reminder due later: the reminder goes with it.
func TestShutdownDeletesAMarkedShardWithAReminderDueLater(t *testing.T) {
	t.Parallel()
	clk := harness.NewFakeClock()
	r := newSRigWith(t, both(quick, withClock(clk)))
	r.sql("lobby/rm3", `INSERT INTO kv (payload) VALUES (x'01')`)
	r.sql("lobby/rm3", markSQL)
	r.remind("lobby/rm3", "timeout", clk.Now().Unix()+3600)
	r.shutdown("lobby/rm3")
	if _, err := r.rawManifest("lobby/rm3"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v, want none", err)
	}
	if r.m.Stats.Deletes.Load() != 1 {
		t.Fatalf("deletes %d", r.m.Stats.Deletes.Load())
	}
}

// With no reminders, shutdown deletes a marked shard.
func TestShutdownDeletesAMarkedShardWithNoReminders(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	r.sql("lobby/rm4", `INSERT INTO kv (payload) VALUES (x'01')`)
	r.sql("lobby/rm4", markSQL)
	r.shutdown("lobby/rm4")
	if _, err := r.rawManifest("lobby/rm4"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v, want none", err)
	}
}

func removeAll(p string) error { return os.RemoveAll(p) }
