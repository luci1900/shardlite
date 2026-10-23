package node

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/migrate"
	"github.com/luci1900/shardlite/internal/notify"
)

// Reminders (docs/guide.md, Reminders): rows a shard sets for itself in its own
// file, with the time they are due. The node reads only the earliest due time
// and wakes the shard's app when it has come, as a Durable Objects alarm does.
// What a reminder means, and handling it, are the app's.

// reservedSchema is the reserved tables' schema.
//
//go:embed reserved.sql
var reservedSchema string

const (
	maxBackoffShift = 6
	holdWarnAfter   = time.Minute
)

// wakeFunc wakes a shard's app for its reminders: a POST to
// /_shardlite/reminders for the shard. status is the answer (0 if there was
// none), and err says why not. The server's is given when the Catalog is built.
type wakeFunc func(ctx context.Context, a *actor) (status int, err error)

// ReminderOptions configure how shards are woken for their reminders
// (docs/guide.md, Reminders).
type ReminderOptions struct {
	Backoff    time.Duration // first wait after a failed wake, 1 s if zero
	MaxBackoff time.Duration // longest wait, 1 min if zero
	MaxAge     time.Duration // a reminder due longer ago than this is stale and ignored, 24 h if zero
	NodeLimit  int           // wakes in flight on the node, 256 if zero
}

// reminders holds what the wakes of all of the node's shards share: the limit
// on wakes in flight, the backoff and the way to the app. The wakes themselves
// belong to each shard's reminders loop.
type reminders struct {
	opts  ReminderOptions
	clock Clock
	log   *slog.Logger
	stats *Stats
	wake  wakeFunc
	node  chan struct{} // the node-wide limit on wakes in flight
}

func newReminders(opts ReminderOptions, clock Clock, log *slog.Logger, stats *Stats, wake wakeFunc) *reminders {
	return &reminders{opts: opts, clock: clock, log: log, stats: stats, wake: wake, node: make(chan struct{}, opts.NodeLimit)}
}

func (rm *reminders) now() time.Time { return rm.clock.Now() }

// since is the earliest due time that isn't stale as of now.
func (rm *reminders) since(now time.Time) int64 { return now.Add(-rm.opts.MaxAge).Unix() }

// backoff is the wait after the nth failure in a row (n counts from 1):
// doubling from the base to the cap, with jitter.
func (rm *reminders) backoff(n int) time.Duration {
	d := min(rm.opts.Backoff<<min(n-1, maxBackoffShift), rm.opts.MaxBackoff)
	return d/2 + rand.N(d/2+1)
}

// call makes one wake of a's app, within the node's limit. A wake that ctx
// cut short is no failure of the app's: ok is false and the loop is ending.
func (rm *reminders) call(ctx context.Context, a *actor) (ok bool, err error) {
	select {
	case rm.node <- struct{}{}:
		defer func() { <-rm.node }()
	case <-ctx.Done():
		return false, context.Cause(ctx)
	}
	rm.stats.ReminderWakes.Add(1)
	status, err := rm.wake(ctx, a)
	if err == nil && status/100 != 2 {
		err = fmt.Errorf("the app answered %d", status)
	}
	return err == nil, err
}

// ensureTables creates the reserved table and its index through capture's
// connection, and for a system shard (sys) applies its kind's migrations and
// inserts its first reminder, "run", due at now if it has none or only a stale
// one (a cluster that was down for longer than reminders are kept must not leave
// a system shard that never runs again) (docs/internals.md, System kinds), all in
// one transaction. A system shard at a
// version newer than its kind's migrations (a newer node migrated it) is
// refused with migrate.ErrNewer, and nothing is changed. A shard that has
// everything commits nothing; otherwise the commit is captured at once, so the
// shard's final sync has nothing new to find (a shard the app never wrote to is
// still deleted as never written).
func ensureTables(ctx context.Context, sh *capture.Shard, sys *kind.Kind, now, since int64) error {
	before, herr := sh.Header()
	if err := sh.Use(ctx, func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, reservedSchema); err != nil {
			return err
		}
		if sys != nil {
			if err := migrate.Migrate(ctx, tx, sys.Migrations, migrate.Latest); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO _shardlite_reminders (key, due) VALUES ('run', ?1) ON CONFLICT (key) DO UPDATE SET due = ?1 WHERE due < ?2`, now, since); err != nil {
				return err
			}
		}
		return tx.Commit()
	}); err != nil {
		return fmt.Errorf("creating the reserved tables: %w", err)
	}
	if after, err := sh.Header(); herr != nil || err != nil || after != before {
		return sh.Capture(ctx)
	}
	return nil
}

// useDB runs fn on the shard's connection, which is capture's.
func (a *actor) useDB(ctx context.Context, fn func(*sql.DB) error) error {
	sh := a.cap.load()
	if sh == nil {
		return capture.ErrClosed
	}
	return sh.Use(ctx, fn)
}

// querier is what a *sql.DB and a *sql.Tx share.
type querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// earliest is the earliest due time of a reminder that isn't due before since
// (Unix seconds), and false if there is none.
func earliest(ctx context.Context, q querier, since int64) (due int64, ok bool, err error) {
	var d sql.NullInt64
	err = q.QueryRowContext(ctx, `SELECT MIN(due) FROM _shardlite_reminders WHERE due >= ?`, since).Scan(&d)
	return d.Int64, d.Valid, err
}

// live reports whether the shard has a reminder that is due before before (Unix
// seconds) and isn't stale. It reads through the shard's connection, so it fails
// while the shard is being let go of.
func (a *actor) live(ctx context.Context, before int64) (bool, error) {
	var live bool
	err := a.useDB(ctx, func(db *sql.DB) error {
		due, ok, err := earliest(ctx, db, a.d.rem.since(a.d.now()))
		live = ok && due < before
		return err
	})
	return live, err
}

// holds reports whether the shard has a reminder due and so must not
// deactivate. It backs off the next look (idleState.wait), and logs if a shard
// stays held for long. Only the shard's lifecycle loop calls it.
func (a *actor) holds(ctx context.Context) bool {
	live, err := a.live(ctx, a.d.now().Unix()+1)
	held, warn := a.idle.keptBy(err == nil && live, a.d.now(), a.d.opts.IdleRecheck)
	if warn > 0 {
		a.d.log.Warn("a shard has had reminders due for a long time, keeping it active", "shard", a.Name, "for", warn.Round(time.Second))
	}
	return held
}

// reminderLoop is the reminders loop of one active shard (docs/internals.md,
// Concurrency): a sequential loop that reads the earliest due time of the shard's
// reminders, wakes the shard's app when it has come and keeps the shard's
// reminder timer. It writes no index entries: while a reminder's commit is in the
// shard's tail, adoption covers it after a crash, and the delta that ends the
// tail records the shard's next_reminder with its entry. It reads when the
// flush cycle has captured new frames of the shard (a kick), when the reminder
// timer fires, at activation, and after the next completed cycle when a read
// failed. A wake goes out once what its read saw is durable by the gate's rule
// (durability).
//
// The wake runs in the loop itself, so a shard has one at a time, and stopping
// the loop cancels it. After a wake the loop reads again: a 2xx with nothing
// due is a success, anything else a failure, and the next wake waits with
// backoff. The loop keeps the backoff and nothing else: what is due is in the
// shard's file.
//
// The shard's lifecycle loop starts it once the shard is captured, and stops it
// before a release, so nothing uses the capture during the final sync.
type reminderLoop struct {
	rm   *reminders
	a    *actor                                                  // whose reminders these are (the wake takes it)
	db   func(ctx context.Context, fn func(*sql.DB) error) error // the shard's connection, capture's
	gate durability

	captured  *notify.Kick  // the flush cycle has captured new frames of the shard
	timer     reminderTimer // the loop's own
	failures  int           // wakes failed in a row
	notBefore time.Time     // no wake before this, after a failure

	// For tests: reads asked for (at activation and by each kick) and how many
	// of them the loop has finished, whether the reminder timer is set, and the
	// end of the loop.
	asked, answered atomic.Uint64
	armed           atomic.Bool
	ended           chan struct{}
}

// durability is how the loop learns that what a read saw is durable: the gate's
// rule over the shard's capture and the flush loop (readGate), a fake in the
// loop's own tests.
type durability interface {
	// mark is taken just after a read.
	mark() uint64
	// after is called with that mark. A nil channel means what the read saw is
	// durable already. Otherwise the channel is closed once a cycle that began
	// after the mark has completed, which check then confirms. It asks for no
	// cycle: the read's commits go out with the next one, as other unwaited
	// writes do.
	after(mark uint64) <-chan struct{}
	// check is nil if the cycle that after waited for made the shard's commits
	// durable, and otherwise says why not.
	check() error
	// cycleAfter is closed once a cycle that began after mark has completed: what
	// paces a read that was cut short or failed.
	cycleAfter(mark uint64) <-chan struct{}
}

// readGate is durability for a shard on the node: Gate's rule. What a read saw
// is durable at once if the shard has no new frames by the time it is marked,
// and otherwise once a cycle that began after the mark has completed with the
// shard's sync good and the lease still good.
type readGate struct {
	f   *Flusher
	cap *captureState
}

func (g readGate) mark() uint64 { return g.f.Mark() }

func (g readGate) after(mark uint64) <-chan struct{} {
	if !g.cap.hasNewFrames() {
		return nil
	}
	return g.cycleAfter(mark)
}

func (g readGate) check() error {
	if err := g.f.keeper.Check(); err != nil {
		return err
	}
	return g.cap.err()
}

func (g readGate) cycleAfter(mark uint64) <-chan struct{} { return g.f.done.Reached(mark + 1) }

// newReminderLoop makes the reminders loop of a.
func newReminderLoop(rm *reminders, a *actor, db func(context.Context, func(*sql.DB) error) error, gate durability) *reminderLoop {
	o := &reminderLoop{rm: rm, a: a, db: db, gate: gate, captured: notify.NewKick(), ended: make(chan struct{})}
	o.asked.Store(1) // the read at activation
	return o
}

func (o *reminderLoop) String() string { return "reminders " + o.a.Name.String() }

// kick is the flush cycle telling the loop it has captured new frames of the
// shard: its new reminders can be read, and are durable.
func (o *reminderLoop) kick() {
	o.asked.Add(1)
	o.captured.Kick()
}

// Serve runs the loop until ctx ends.
func (o *reminderLoop) Serve(ctx context.Context) error {
	defer func() {
		o.timer.halt()
		close(o.ended)
	}()
	for {
		n := o.asked.Load()
		again, mark, kicked := o.pass(ctx)
		if kicked {
			continue // read again at once
		}
		o.answered.Store(n)
		var pace <-chan struct{}
		if again {
			pace = o.gate.cycleAfter(mark) // so a read that keeps failing is never repeated in a tight loop
		}
		select {
		case <-ctx.Done():
			return nil
		case <-o.captured.C():
		case <-pace:
		case <-o.timer.c:
			o.timer.fired()
			o.armed.Store(false)
		}
	}
}

// pass is one turn of the loop: a read, a wait until what it saw is durable,
// then the wake if one is due and the reminder timer. again is true if the
// read failed, and mark is the read's. kicked is true if the flush cycle kicked
// the loop while it waited: that cycle may have begun before the read, so it
// doesn't count for it, but it captured the shard's new frames, so a read made
// now is likely durable at once, and the caller reads again rather than wait for
// another cycle.
func (o *reminderLoop) pass(ctx context.Context) (again bool, mark uint64, kicked bool) {
	due, ok, err := o.read(ctx)
	mark = o.gate.mark() // after the read: a cycle that begins later captures all it saw
	if err != nil {
		o.warnRead(ctx, err)
		return true, mark, false
	}
	if done := o.gate.after(mark); done != nil {
		select {
		case <-done:
		case <-o.captured.C():
			return false, mark, true
		case <-ctx.Done():
			return false, mark, false
		}
		if o.gate.check() != nil {
			return true, mark, false // the cycle didn't make it durable: read again
		}
	}
	now := o.rm.now()
	if ok && due <= now.Unix() && !now.Before(o.notBefore) {
		woke, err := o.rm.call(ctx, o.a)
		if ctx.Err() != nil {
			return false, mark, false
		}
		if due, ok, err = o.afterWake(ctx, woke, err); err != nil {
			o.warnRead(ctx, err)
			return true, mark, false
		}
		now = o.rm.now()
	}
	o.timer.arm(o.rm.clock, now, due, ok, o.notBefore)
	o.armed.Store(o.timer.armed())
	return false, mark, false
}

func (o *reminderLoop) warnRead(ctx context.Context, err error) {
	if ctx.Err() == nil && !errors.Is(err, capture.ErrClosed) {
		o.rm.log.Warn("could not read a shard's reminders", "shard", o.a.Name, "err", err)
	}
}

// afterWake reads again after a wake and decides how it went: a success needs
// the app's 2xx and nothing due afterwards. It returns the read.
func (o *reminderLoop) afterWake(ctx context.Context, woke bool, wakeErr error) (due int64, ok bool, err error) {
	due, ok, err = o.read(ctx)
	if err != nil {
		return 0, false, err
	}
	now := o.rm.now()
	stillDue := ok && due <= now.Unix()
	if woke && !stillDue {
		o.failures, o.notBefore = 0, time.Time{}
		return due, ok, nil
	}
	if woke {
		wakeErr = errors.New("reminders are still due")
	}
	o.failures++
	o.notBefore = now.Add(o.rm.backoff(o.failures))
	o.rm.stats.ReminderWakeFailures.Add(1)
	o.rm.log.Warn("waking a shard for its reminders failed, will retry", "shard", o.a.Name, "failures", o.failures, "err", wakeErr)
	return due, ok, nil
}

// reminderTimer is the shard's one timer, for the earliest reminder that is not
// due yet and the end of a backoff. When it fires the loop reads again. Only the
// loop uses it.
type reminderTimer struct {
	c    <-chan time.Time
	stop func() bool
}

// arm replaces the timer: it is set for the later of the earliest due time and
// the end of the backoff (notBefore, zero if there is none), and not at all
// without a reminder.
func (t *reminderTimer) arm(clk Clock, now time.Time, due int64, ok bool, notBefore time.Time) {
	t.halt()
	if !ok {
		return
	}
	d := max(time.Duration(due-now.Unix())*time.Second, notBefore.Sub(now), 0)
	t.c, t.stop = clk.NewTimer(d)
}

func (t *reminderTimer) fired() { t.c, t.stop = nil, nil }

func (t *reminderTimer) halt() {
	if t.stop != nil {
		t.stop()
	}
	t.c, t.stop = nil, nil
}

func (t *reminderTimer) armed() bool { return t.c != nil }

// read returns the earliest due time of the shard's reminders that aren't
// stale, through the shard's connection.
func (o *reminderLoop) read(ctx context.Context) (due int64, ok bool, err error) {
	err = o.db(ctx, func(db *sql.DB) error {
		var err error
		due, ok, err = earliest(ctx, db, o.rm.since(o.rm.now()))
		return err
	})
	return due, ok, err
}
