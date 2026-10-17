package node

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
)

// fakeClock is the shards' Clock in tests that move time: nothing that waits
// on it (a timer, a ticker, a backoff) fires until Advance moves it past.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*fakeTimer]bool
}

type fakeTimer struct {
	at    time.Time
	every time.Duration // a ticker's period, 0 for a timer
	f     func()
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Now().Truncate(time.Minute).Add(10 * time.Second), timers: map[*fakeTimer]bool{}}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) add(d, every time.Duration, f func()) (*fakeTimer, bool) {
	c.mu.Lock()
	t := &fakeTimer{at: c.now.Add(d), every: every, f: f}
	c.timers[t] = true
	c.mu.Unlock()
	return t, d <= 0
}

func (c *fakeClock) stopper(t *fakeTimer) func() bool {
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		was := c.timers[t]
		delete(c.timers, t)
		return was
	}
}

func (c *fakeClock) NewTimer(d time.Duration) (<-chan time.Time, func() bool) {
	ch := make(chan time.Time, 1)
	t, due := c.add(d, 0, func() { ch <- c.Now() })
	if due {
		c.fire(t)
	}
	return ch, c.stopper(t)
}

func (c *fakeClock) NewTicker(d time.Duration) (<-chan time.Time, func()) {
	ch := make(chan time.Time, 1)
	t, _ := c.add(d, d, func() {
		select {
		case ch <- c.Now():
		default:
		}
	})
	stop := c.stopper(t)
	return ch, func() { stop() }
}

func (c *fakeClock) fire(t *fakeTimer) {
	c.mu.Lock()
	was := c.timers[t]
	if t.every > 0 {
		t.at = c.now.Add(t.every) // a ticker skips the periods it was behind for
	} else {
		delete(c.timers, t)
	}
	c.mu.Unlock()
	if was {
		t.f()
	}
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var due []*fakeTimer
	for t := range c.timers {
		if !t.at.After(c.now) {
			due = append(due, t)
		}
	}
	c.mu.Unlock()
	for _, t := range due {
		c.fire(t)
	}
}

func withClock(c Clock) func(*CatalogOptions) {
	return func(o *CatalogOptions) { o.Clock = c }
}

// setRow sets a keyed row in the shard's own inbox, replacing the one with the same key.
func (r *srig) setRow(target, key, path string, due int64) {
	r.t.Helper()
	r.sql(target, fmt.Sprintf(`INSERT INTO _shardlite_inbox (key, path, due) VALUES ('%s', '%s', %d) ON CONFLICT(key) DO UPDATE SET path = excluded.path, due = excluded.due`, key, path, due))
}

// --- the handler ---

func TestOwnRowFiresOnceAfterItsCommitIsDurable(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	r := newSRigWith(t, both(quick, withClock(clk)))
	rec := &recorder{}
	r.record(rec, "wallet/rm1")
	due := clk.Now().Unix() + 2
	r.sql("wallet/rm1", fmt.Sprintf(`BEGIN; INSERT INTO kv (payload) VALUES (x'01'); INSERT INTO _shardlite_inbox (key, path, body, due) VALUES ('upgrade:barracks', '/finish', 'b', %d); COMMIT;`, due))
	eventually(t, "the timer for the row", r.armed("wallet/rm1"))
	clk.Advance(time.Second)
	r.settle("wallet/rm1")
	if rec.count() != 0 {
		t.Fatal("fired early")
	}
	clk.Advance(time.Second)
	eventually(t, "the row", func() bool { return rec.count() >= 1 })
	eventually(t, "the handler's delete", func() bool { return r.inboxRows("wallet/rm1") == 0 })
	r.settle("wallet/rm1") // a second delivery would come in these cycles
	if rec.count() != 1 {
		t.Fatalf("%d calls", rec.count())
	}
	d := rec.all()[0]
	if clk.Now().Unix() < due {
		t.Fatalf("fired %d s early", due-clk.Now().Unix())
	}
	if d.senderRows != 1 || d.method != "POST" || d.path != "/finish" || d.body != "b" || d.target != "wallet/rm1" ||
		d.hdr.Get("Shardlite-Caller") != "wallet/rm1" || d.hdr.Get("Shardlite-Hops") != "0" || d.hdr.Get("Content-Type") != "application/octet-stream" ||
		d.id() != "1" || d.hdr.Get("Idempotency-Key") != "" {
		t.Fatalf("delivery %+v", d)
	}
	if r.inboxRows("wallet/rm1") != 0 {
		t.Fatal("the handler's delete left a row")
	}
}

// A row set in the past still waits for the commit that set it.
func TestOwnRowInThePastWaitsForTheFlush(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	rec := &recorder{}
	r.record(rec, "wallet/rm2")
	r.sql("wallet/rm2", fmt.Sprintf(`BEGIN; INSERT INTO kv (payload) VALUES (x'01'); INSERT INTO _shardlite_inbox (key, path, due) VALUES ('k', '/now', %d); COMMIT;`, time.Now().Unix()-5))
	if rec.count() != 0 { // only a flush cycle reads rows, and none runs until cyclesWhile
		t.Fatal("delivered before any flush")
	}
	r.cyclesWhile("the call", func() bool { return rec.count() >= 1 })
	if rec.all()[0].senderRows != 1 {
		t.Fatalf("the shard's commit was not in blob storage: %d rows", rec.all()[0].senderRows)
	}
}

func TestOwnRowOfALostCommitNeverFires(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	rec := &recorder{}
	r.record(rec, "")
	r.setRow("wallet/rm3", "k", "/never", time.Now().Unix()-5)
	r.closeInApp("wallet/rm3")
	r.crash()
	r.stopJournal()
	r.restart(manualFlush)
	ref := r.acquire("wallet/rm3")
	defer ref.Release()
	for range 10 {
		r.cycle()
	}
	if n := r.inboxRows("wallet/rm3"); n != 0 { // a row that is not there cannot fire
		t.Fatalf("%d rows came back", n)
	}
	if rec.count() != 0 {
		t.Fatal("a row whose commit was lost fired")
	}
}

// A handler that rolls back and answers 500 is called again, and so is one that
// is told to wait.
func TestHandlerIsRetried(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, quick)
	rec := &recorder{answer: func(n int, q *http.Request) int {
		switch q.URL.Path {
		case "/try":
			if n == 1 {
				return 500
			}
		case "/flaky":
			if n <= 2 {
				return 503
			}
		case "/busy":
			if n == 1 {
				return 429
			}
		}
		return 200
	}}
	r.record(rec, "")
	now := time.Now().Unix()
	r.sql("users/rr1", fmt.Sprintf(`BEGIN; INSERT INTO _shardlite_inbox (path, due) VALUES ('/try', %[1]d); INSERT INTO _shardlite_inbox (path, due) VALUES ('/flaky', %[1]d); INSERT INTO _shardlite_inbox (path, due) VALUES ('/busy', %[1]d); COMMIT;`, now-1))
	eventually(t, "the rows to be consumed", func() bool { return r.inboxRows("users/rr1") == 0 })
	r.settle("users/rr1")
	for p, want := range map[string]int{"/try": 2, "/flaky": 3, "/busy": 2} {
		if n := len(rec.onPath(p)); n != want {
			t.Errorf("%s called %d times, want %d", p, n, want)
		}
	}
}

// A recurring row: the handler sets the next time itself, and each time is
// delivered once.
func TestRecurringRow(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	r := newSRigWith(t, both(quick, withClock(clk)))
	rec := &recorder{keep: true}
	r.record(rec, "")
	inner := *r.handler.Load()
	var calls atomic.Int32
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if id := q.Header.Get("Shardlite-Inbox-Id"); q.URL.Path == "/tick" && id != "" {
			if calls.Add(1) < 3 {
				r.sql("users/rc1", `UPDATE _shardlite_inbox SET due = due + 2 WHERE id = `+id)
			} else {
				r.sql("users/rc1", `DELETE FROM _shardlite_inbox WHERE id = `+id)
			}
		}
		inner(w, q)
	})
	first := clk.Now().Unix() - 1
	r.setRow("users/rc1", "daily", "/tick", first)
	ticks := func() int { return len(rec.onPath("/tick")) }
	eventually(t, "the first call", func() bool { return ticks() >= 1 })
	// The times are first, first+2 and first+4, and the clock is at first+1: a
	// call comes only when the clock reaches the row's new time.
	notYet := func(n int) {
		t.Helper()
		r.settle("users/rc1")
		if got := ticks(); got != n {
			t.Fatalf("%d calls, want %d before the clock reaches the next time", got, n)
		}
	}
	notYet(1)
	clk.Advance(time.Second)
	eventually(t, "the second call", func() bool { return ticks() >= 2 })
	notYet(2)
	clk.Advance(time.Second)
	notYet(2)
	clk.Advance(time.Second)
	eventually(t, "the third call", func() bool { return ticks() >= 3 })
	eventually(t, "the last row to go", func() bool { return r.inboxRows("users/rc1") == 0 })
	r.settle("users/rc1")
	if got := ticks(); got != 3 {
		t.Fatalf("%d calls, want 3", got)
	}
	if clk.Now().Unix() < first+4 {
		t.Fatal("the later times fired early")
	}
	if r.inboxRows("users/rc1") != 0 {
		t.Fatal("a row is left")
	}
}

// A row the node gives up on: it logs an error, doesn't call again while the
// shard is active, lets the shard deactivate without a hint or a commit of its
// own, and calls once more after the shard is activated again.
func TestGivingUpOnARow(t *testing.T) {
	t.Parallel()
	now := time.Now().Unix()
	for _, tc := range []struct {
		name   string
		keep   bool
		answer int
		due    int64
		reason string
	}{
		{"2xx without consuming it", true, 200, now - 1, "did not delete the row"},
		{"4xx", false, 400, now - 1, "the handler answered 400"},
		{"more than a day past due", false, 503, now - 90000, "failing for more than"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs syncBuffer
			r := newSRigWith(t, both(quick, logTo(&logs)))
			rec := &recorder{keep: tc.keep, answer: func(int, *http.Request) int { return tc.answer }}
			r.record(rec, "")
			r.sql("users/gu1", fmt.Sprintf(`INSERT INTO _shardlite_inbox (key, path, due) VALUES ('stuck', '/ignored', %d)`, tc.due))
			eventually(t, "the error to be logged", func() bool { return strings.Contains(logs.String(), "giving up on an inbox row") })
			out := logs.String()
			for _, want := range []string{tc.reason, "shard=users/gu1", "key=stuck", "path=/ignored", "level=ERROR"} {
				if !strings.Contains(out, want) {
					t.Errorf("the log lacks %q:\n%s", want, out)
				}
			}
			r.settle("users/gu1")
			if n := len(rec.onPath("/ignored")); n != 1 {
				t.Fatalf("%d calls while the shard stayed active, want 1", n)
			}
			eventually(t, "the hint to go", func() bool { return len(r.wakes()) == 0 })

			// The shard can go.
			r.closeInApp("users/gu1")
			eventually(t, "deactivation", func() bool { return r.cached("users/gu1") })
			if keys := r.wakes(); len(keys) != 0 {
				t.Fatalf("hints %v for a row given up on", keys)
			}
			if n := len(rec.onPath("/ignored")); n != 1 {
				t.Fatalf("%d calls", n)
			}

			// Activated again, it is called once more.
			rec.answer, rec.keep = func(int, *http.Request) int { return 200 }, false
			ref := r.acquire("users/gu1")
			defer ref.Release()
			eventually(t, "the call after the next activation", func() bool { return len(rec.onPath("/ignored")) == 2 })
			eventually(t, "the row to be consumed", func() bool { return r.inboxRows("users/gu1") == 0 })
		})
	}
}

// The node makes no commits to a shard whose only rows are in its inbox, apart
// from intakes.
func TestNodeMakesNoCommitsForInboxRows(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	r := newSRigWith(t, both(quick, logTo(&logs)))
	rec := &recorder{keep: true}
	r.record(rec, "")
	r.sql("users/nc1", fmt.Sprintf(`INSERT INTO _shardlite_inbox (path, due) VALUES ('/ignored', %d)`, time.Now().Unix()-1))
	eventually(t, "the row to be given up", func() bool { return strings.Contains(logs.String(), "giving up on an inbox row") })
	sh := r.shard("users/nc1").cap.load()
	before := header(t, sh)
	pos := sh.Position()
	r.settle("users/nc1") // the node's own cycles would have committed by now
	r.cycle()
	if header(t, sh) != before || sh.Position() != pos {
		t.Fatal("the node committed to a shard with only an inbox row")
	}
}

func TestReplacingAndCancellingRows(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	r := newSRigWith(t, both(quick, withClock(clk)))
	rec := &recorder{}
	r.record(rec, "")
	now := clk.Now().Unix()
	r.setRow("users/rp1", "k", "/old", now+100)
	r.setRow("users/rp1", "k", "/new", now+200) // replaces it
	r.setRow("users/rp1", "gone", "/cancelled", now+120)
	r.sql("users/rp1", `DELETE FROM _shardlite_inbox WHERE key = 'gone'`)
	r.settle("users/rp1") // the flush covering the commits, and the timer
	clk.Advance(150 * time.Second)
	r.settle("users/rp1")
	if rec.count() != 0 {
		t.Fatalf("fired: %+v", rec.all())
	}
	clk.Advance(100 * time.Second)
	eventually(t, "the new row", func() bool { return rec.count() == 1 })
	eventually(t, "the handler's delete", func() bool { return r.inboxRows("users/rp1") == 0 })
	r.settle("users/rp1")
	if got := rec.all(); len(got) != 1 || got[0].path != "/new" {
		t.Fatalf("calls: %+v", got)
	}
}

// --- waking ---

func TestOwnRowAnHourAwayWakesTheShard(t *testing.T) {
	t.Parallel()
	for _, tc := range []string{"after deactivation", "after a crash", "new machine"} {
		t.Run(tc, func(t *testing.T) {
			clk := newFakeClock()
			opts := both(quick, withClock(clk), drainFor(200*time.Millisecond))
			r := newSRigWith(t, opts)
			rec := &recorder{}
			r.record(rec, "")
			due := clk.Now().Unix() + 3600
			hint := wakeKey(time.Unix(due, 0), "users/wh1")
			r.setRow("users/wh1", "k", "/ring", due)
			switch tc {
			case "after deactivation":
				r.closeInApp("users/wh1")
				advanceUntil(t, clk, 100*time.Millisecond, "deactivation (a row an hour away does not hold the shard)", func() bool { return r.cached("users/wh1") })
				if keys := r.wakes(); len(keys) != 1 || keys[0] != hint {
					t.Fatalf("hints %v, want %s", keys, hint)
				}
				clk.Advance(3601 * time.Second)
				r.m.wakes.scan(context.Background())
			case "after a crash":
				// The row's commit is in the journal, so adoption writes the hint.
				eventually(t, "the row's commit to be durable", func() bool { return r.durableInbox("users/wh1") == 1 })
				if keys := r.wakes(); len(keys) != 0 {
					t.Fatalf("hints %v while the row is in the tail", keys)
				}
				r.closeInApp("users/wh1")
				r.crash()
				r.stopJournal()
				clk.Advance(3601 * time.Second)
				r.restart(opts)
			case "new machine":
				r.shutdown("users/wh1")
				if keys := r.wakes(); len(keys) != 1 || keys[0] != hint {
					t.Fatalf("hints %v, want %s", keys, hint)
				}
				if err := removeAll(r.data); err != nil {
					t.Fatal(err)
				}
				clk.Advance(3601 * time.Second)
				r.restart(opts)
			}
			eventually(t, "the row to fire", func() bool { return len(rec.onPath("/ring")) >= 1 })
			// The next scan finds the shard with nothing left for the hint, and deletes it.
			advanceUntil(t, clk, 100*time.Millisecond, "the hint to go", func() bool { return len(r.wakes()) == 0 })
			if n := r.inboxRows("users/wh1"); n != 0 {
				t.Fatalf("%d rows left", n)
			}
		})
	}
}

func TestTwentyRowsInOneMinuteWriteOneHint(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	r := newSRigWith(t, both(quick, withClock(clk)))
	base := (clk.Now().Unix()/60)*60 + 3600
	var stmt strings.Builder
	stmt.WriteString("BEGIN;")
	for i := range 20 {
		fmt.Fprintf(&stmt, "INSERT INTO _shardlite_inbox (key, path, due) VALUES ('k%d', '/x', %d);", i, base+int64(i))
	}
	stmt.WriteString("COMMIT;")
	r.sql("users/tw1", stmt.String())
	deactivate := func() {
		t.Helper()
		r.closeInApp("users/tw1")
		advanceUntil(t, clk, 100*time.Millisecond, "deactivation", func() bool { return r.cached("users/tw1") })
	}
	deactivate()
	if n := r.calls("create", wakePrefix); n != 1 {
		t.Fatalf("%d hint writes", n)
	}
	// A later row in the same minute leaves the one hint.
	r.setRow("users/tw1", "late", "/x", base+30)
	deactivate()
	if keys := r.wakes(); len(keys) != 1 || keys[0] != wakeKey(time.Unix(base, 0), "users/tw1") {
		t.Fatalf("hints %v after another row in the same minute", keys)
	}
}

func TestInboxRowDueNowKeepsTheShardActive(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	r := newSRigWith(t, both(quick, withClock(clk)))
	var up atomic.Bool
	rec := &recorder{answer: downUntil(&up)}
	r.record(rec, "")
	r.setRow("users/dn1", "k", "/now", clk.Now().Unix()-5)
	r.closeInApp("users/dn1") // idle timeout is 300 ms
	// Move the clock past the idle timeout until the idle timer has looked at the
	// shard and kept it (it sets the shard's next look when it does).
	advanceUntil(t, clk, 100*time.Millisecond, "the idle timer to keep the shard", r.lookedAt("users/dn1"))
	if !r.m.IsActive("users/dn1") {
		t.Fatal("a shard with an inbox row due was deactivated")
	}
	advanceUntil(t, clk, 50*time.Millisecond, "a second attempt (the backoff is on the clock)", func() bool { return rec.count() >= 2 })
	up.Store(true)
	advanceUntil(t, clk, 50*time.Millisecond, "the row to be consumed (its backoff is on the clock)", func() bool { return r.inboxRows("users/dn1") == 0 })
	r.closeInApp("users/dn1") // the handler's delete opened the shard in the app again
	advanceUntil(t, clk, time.Second, "deactivation after the row", func() bool { return r.cached("users/dn1") })
}

// markSQL is what the app runs to mark a shard for deletion.
const markSQL = `CREATE TABLE _shardlite_delete (x)`

// A marked shard with an inbox row due later is deleted at deactivation and the
// row goes with it.
func TestMarkedShardWithARowDueLaterIsDeletedWithIt(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	r := newSRigWith(t, both(quick, withClock(clk)))
	rec := &recorder{}
	r.record(rec, "")
	r.setRow("lobby/rm1", "timeout", "/tick", clk.Now().Unix()+3600)
	r.sql("lobby/rm1", markSQL)
	r.closeInApp("lobby/rm1")
	advanceUntil(t, clk, time.Second, "deletion", func() bool { return r.m.Stats.Deletes.Load() == 1 && r.inactive("lobby/rm1")() })
	if _, err := r.rawManifest("lobby/rm1"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v, want none", err)
	}
	if rec.count() != 0 {
		t.Fatal("the dropped row fired")
	}
}

// A marked shard with an inbox row that is due is kept until the handler has
// consumed the row, then deleted.
func TestMarkedShardWithARowDueIsDeletedAfterItIsConsumed(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	r := newSRigWith(t, both(quick, withClock(clk)))
	rec := &recorder{}
	r.record(rec, "")
	r.sql("lobby/rm2", markSQL)
	r.setRow("lobby/rm2", "now", "/tick", clk.Now().Unix())
	eventually(t, "the row to fire", func() bool { return len(rec.onPath("/tick")) == 1 })
	eventually(t, "the handler's delete", func() bool { return r.inboxRows("lobby/rm2") == 0 })
	r.closeInApp("lobby/rm2") // the handler's delete opened the shard in the app again
	advanceUntil(t, clk, time.Second, "deletion", func() bool { return r.m.Stats.Deletes.Load() == 1 && r.inactive("lobby/rm2")() })
	if _, err := r.rawManifest("lobby/rm2"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v, want none", err)
	}
}

// Draining deletes a marked shard like it does at any deactivation, even with an
// inbox row due later: the row goes with it.
func TestDrainDeletesAMarkedShardWithARowDueLater(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	r := newSRigWith(t, both(quick, withClock(clk), drainFor(200*time.Millisecond)))
	r.sql("lobby/rm3", `INSERT INTO kv (payload) VALUES (x'01')`)
	r.sql("lobby/rm3", markSQL)
	r.setRow("lobby/rm3", "timeout", "/ring", clk.Now().Unix()+3600)
	r.shutdown("lobby/rm3")
	if _, err := r.rawManifest("lobby/rm3"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v, want none", err)
	}
	if r.m.Stats.Deletes.Load() != 1 {
		t.Fatalf("deletes %d", r.m.Stats.Deletes.Load())
	}
}

// With no rows, shutdown deletes a marked shard.
func TestShutdownDeletesAMarkedShardWithNoRows(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, both(quick, drainFor(200*time.Millisecond)))
	r.sql("lobby/rm4", `INSERT INTO kv (payload) VALUES (x'01')`)
	r.sql("lobby/rm4", markSQL)
	r.shutdown("lobby/rm4")
	if _, err := r.rawManifest("lobby/rm4"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v, want none", err)
	}
	if keys := r.wakes(); len(keys) != 0 {
		t.Fatalf("hints %v", keys)
	}
}

// A hint that can't be written at shutdown for longer than the store's own
// attempts keeps the shard active; the shutdown tries again.
func TestHintWriteFailingAtShutdownIsRetried(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	// The slow kind has an idle timeout of an hour, so the idle timer does not
	// deactivate the shard (and use up the failures) before the shutdown does.
	r := newSRigWith(t, both(quick, logTo(&logs), drainFor(200*time.Millisecond)))
	var failures atomic.Int32
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if !after && op == blob.OpCreate && strings.HasPrefix(key, wakePrefix) && failures.Add(1) <= 10 {
			return errors.New("storage down")
		}
		return nil
	})
	// A live outbox row whose receiver's node fails is what needs a hint at
	// shutdown (a row in the inbox gets its hint while the shard is active).
	var up atomic.Bool
	rec := &recorder{}
	r.record(rec, "")
	attempts := r.intakeDownUntil(&up)
	r.addMessage("slow/hf1", "users/hf2", "/later", "")
	eventually(t, "a first attempt", func() bool { return attempts.Load() >= 1 })
	defer func() {
		if t.Failed() {
			t.Log(logs.String())
		}
	}()
	r.shutdown("slow/hf1")
	if keys := r.wakes(); len(keys) != 1 || !strings.HasSuffix(keys[0], "/slow/hf1") {
		t.Fatalf("hints %v", keys)
	}
	if failures.Load() <= 4 || !strings.Contains(logs.String(), "trying again") {
		t.Fatalf("the hint write failed %d times and the shutdown did not retry (log: %v)", failures.Load(), strings.Contains(logs.String(), "trying again"))
	}
	if m, err := r.manifest("slow/hf1"); err != nil || len(m.Files) == 0 {
		t.Fatalf("manifest %+v (%v): the shard was not let go of with a delta", m, err)
	}
}

func removeAll(p string) error { return os.RemoveAll(p) }
