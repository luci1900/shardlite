package node

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/supervisor"
)

// msgOwner is the messages owner of one active shard (docs/concurrency.md, The
// tree): a loop that owns the shard's outbox and inbox bookkeeping, its inbox
// timer and its wake hints, with no lock. Everything else reaches it on its
// channel: what the deliveries learn, the lifecycle loop's questions and the
// hints the wake scan found. It reads the shard's rows itself, once the flush
// cycle has captured new frames (a kick) or when it wants a read of its own, and
// sends a row once the read that found it is durable by the gate's rule
// (durability), waiting for that in its select, never in a call. Blob storage
// for the hints runs in a child, one at a time, whose result comes back as an
// event, so a slow store never delays a read. Deliveries and the hint child run
// in errgroups inside the owner, which stopping it cancels and joins.
//
// The loop's state is in plain parts that only the loop calls (reads, rows, out,
// timer, hints), and what outlives a capture is in kept, which the shard holds
// and hands to each owner.
//
// The shard's lifecycle loop adds it to the table's supervisor, beside the
// shard, once the shard is captured, and removes it before a release, so nothing uses the capture during
// the final sync. If capture resumes, a new owner carries on with the same kept.
type msgOwner struct {
	mg *messenger
	a  *shard                                                  // whose rows these are (the transport takes it)
	db func(ctx context.Context, fn func(*sql.DB) error) error // the shard's connection, Litestream's
	k  *kept

	ev    chan func()   // requests and events, which the loop runs one at a time
	ended chan struct{} // closed once the loop takes no more events
	over  atomic.Bool   // ended is closed

	// The loop's own from here.
	ctx              context.Context
	dctx             context.Context // the deliveries'
	cancelDeliveries context.CancelFunc
	deliveries       errgroup.Group

	reads reads
	rows  rows
	out   outQueue
	timer inboxTimer
	hints wakeHints
}

// kept is what a shard's messages keep across captures while the shard stays
// active: the lifecycle loop holds it and hands it to each messages owner, which
// is the only one to use it while it runs.
type kept struct {
	givenUp   map[rowKey]bool // rows the node has given up on
	hints     map[string]bool // wake hints that exist for the shard: the ones that woke it and the ones it wrote
	hintDirty bool            // a hint write or delete failed
	quiet     bool            // deliveries stopped for the shards' shutdown: none start any more
}

func newKept() *kept { return &kept{givenUp: map[rowKey]bool{}, hints: map[string]bool{}} }

// wakeStore writes and deletes wake hints: blob storage in the node (Shards), a
// fake in the owner's own tests.
type wakeStore interface {
	writeWake(ctx context.Context, shard string, t time.Time) (string, error)
	deleteWakes(ctx context.Context, keys ...string) error
}

// durability is how the owner learns that what a read saw is durable: the
// gate's rule over the shard's capture and the flush loop (readGate), a fake in
// the owner's own tests.
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
	// paces the reads the owner wants for itself.
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

// newMsgOwner makes the owner of a's messages, with what they kept so far.
func newMsgOwner(mg *messenger, a *shard, db func(context.Context, func(*sql.DB) error) error, store wakeStore, gate durability, k *kept) *msgOwner {
	o := &msgOwner{mg: mg, a: a, db: db, k: k, ev: make(chan func()), ended: make(chan struct{})}
	o.reads = reads{gate: gate, captured: notify.NewKick(), now: true} // at activation, at once
	o.rows = rows{known: map[rowKey]bool{}, givenUp: k.givenUp, more: true}
	o.hints = wakeHints{k: k, store: store, name: a.Name, result: make(chan hintResult, 1)}
	return o
}

func (o *msgOwner) String() string { return "messages " + o.a.Name }

// Serve is the owner's loop. It runs until ctx ends, then cancels and joins its
// deliveries and its hint child.
func (o *msgOwner) Serve(ctx context.Context) error {
	o.ctx = ctx
	o.dctx, o.cancelDeliveries = context.WithCancel(ctx)
	defer func() {
		o.end()
		o.cancelDeliveries()
		o.deliveries.Wait()
		o.hints.stop()
		o.timer.halt()
		o.out.halt()
	}()
	for {
		if o.reads.now {
			o.read()
		}
		o.tend()
		o.reads.paced(o.rows.more || o.timer.due)
		select {
		case <-ctx.Done():
			return supervisor.NoRestart(nil)
		case fn := <-o.ev:
			fn()
		case <-o.reads.captured.C():
			o.reads.now = true
		case <-o.reads.pace:
			o.reads.now = true
		case <-o.reads.oldest():
			o.settleOldest()
		case <-o.timer.c:
			o.timer.fired()
		case <-o.out.c():
			o.commit()
		case r := <-o.hints.result:
			o.hints.ended(r)
		}
	}
}

// end closes ended once: when the loop stops, or when the owner couldn't be
// added because the table's supervisor had stopped.
func (o *msgOwner) end() {
	if o.over.CompareAndSwap(false, true) {
		close(o.ended)
	}
}

// send runs fn on the loop, and reports false if ctx ended or the loop has ended
// first. It is how the deliveries and the wake scan tell the owner things.
func (o *msgOwner) send(ctx context.Context, fn func()) bool {
	_, err := do(ctx, o.ev, o.ended, func() struct{} { fn(); return struct{}{} })
	return err == nil
}

// read reads the shard's rows (readNow) and keeps the read until what it saw is
// durable. A read that is durable at once settles every read before it too,
// since those saw less.
func (o *msgOwner) read() {
	r := &o.reads
	r.now, r.pace = false, nil
	snap, err := o.readNow(o.ctx)
	r.last = r.gate.mark() // after the read: a cycle that begins later captures all it saw
	if err != nil {
		if o.ctx.Err() == nil && !errors.Is(err, capture.ErrClosed) {
			o.mg.log.Warn("could not read a shard's outbox and inbox", "shard", o.a.Name, "err", err)
		}
		return
	}
	done := r.gate.after(r.last)
	if done == nil {
		for len(r.waits) > 0 {
			o.durable(r.pop().snap)
		}
		o.durable(snap)
		return
	}
	r.waits = append(r.waits, pendingRead{snap: snap, done: done})
}

// settleOldest takes the oldest pending read once its cycle has completed: its
// rows go out if the cycle made them durable, and are read again otherwise.
func (o *msgOwner) settleOldest() {
	w := o.reads.pop()
	if err := o.reads.gate.check(); err != nil {
		o.rows.forget(w.snap)
		return
	}
	o.durable(w.snap)
}

// durable is for a read whose rows are durable now: it sets the inbox timer,
// notes the earliest row for the hints and starts the deliveries.
func (o *msgOwner) durable(snap *snapshot) {
	o.timer.arm(o.mg.clock, o.mg.now(), snap)
	o.hints.noteEarliest(snap)
	o.start(snap)
}

// reads is when the owner reads its shard's rows, and the reads whose rows are
// not known to be durable yet, oldest first. Only the loop uses it, apart from
// captured, which the flush cycle kicks once it has captured new frames of the
// shard (all it does for messages).
type reads struct {
	gate     durability
	captured *notify.Kick
	now      bool            // read before the loop waits again
	last     uint64          // the mark of the last read
	pace     <-chan struct{} // closed once a cycle after the last read has completed, while the owner wants a read of its own
	waits    []pendingRead
}

// pendingRead is a read waiting for its cycle.
type pendingRead struct {
	snap *snapshot
	done <-chan struct{}
}

// paced sets pace while the owner wants a read of its own (rows left unread,
// a question, a due row): it reads at most once per completed cycle for those,
// so a table with more rows than fit is never read in a loop.
func (r *reads) paced(want bool) {
	switch {
	case !want:
		r.pace = nil
	case r.pace == nil && !r.now:
		r.pace = r.gate.cycleAfter(r.last)
	}
}

// oldest is closed once the oldest pending read's cycle has completed (nil if
// none is pending).
func (r *reads) oldest() <-chan struct{} {
	if len(r.waits) == 0 {
		return nil
	}
	return r.waits[0].done
}

func (r *reads) pop() pendingRead {
	w := r.waits[0]
	r.waits = r.waits[1:]
	return w
}

// rows is what the owner knows about the shard's rows between reads. Reads and
// the owner's own writes run one at a time on the loop, so a row that a write
// deletes, or that a read finds consumed, is gone from every later read and can
// leave known at once.
type rows struct {
	known   map[rowKey]bool // rows read and not seen gone since (being delivered, or waiting for their write)
	givenUp map[rowKey]bool // kept.givenUp
	more    bool            // rows may be waiting that the last read did not take, or none was made yet
	checks  []*check        // inbox rows waiting to be looked up by the next read
}

// check is a question to the next read: is this inbox row still in the table
// with the due time that was delivered?
type check struct {
	row rowKey
	res chan bool
}

func (r *rows) skip(k rowKey) bool { return r.known[k] || r.givenUp[k] }

// begin starts a read: it takes the questions for it. Anything that happens
// during the read and needs another sets more again.
func (r *rows) begin() []*check {
	checks := r.checks
	r.checks, r.more = nil, false
	return checks
}

// failed puts back the questions of a read that failed, for the next one.
func (r *rows) failed(checks []*check) {
	r.more = true
	r.checks = append(checks, r.checks...)
}

// read takes what a read found: the answers to its questions (a row found
// consumed leaves known) and the rows it took.
func (r *rows) read(snap *snapshot, checks []*check, present []bool, more bool) {
	for i, c := range checks {
		if !present[i] {
			delete(r.known, c.row)
		}
		c.res <- present[i] // buffered
	}
	for _, row := range snap.out {
		r.known[row.rowKey()] = true
	}
	for _, row := range snap.in {
		r.known[row.rowKey()] = true
	}
	r.more = r.more || more
}

// forget is for a read whose cycle failed: its rows are read again.
func (r *rows) forget(snap *snapshot) {
	for _, row := range snap.out {
		delete(r.known, row.rowKey())
	}
	for _, row := range snap.in {
		delete(r.known, row.rowKey())
	}
	r.more = true
}

// ask queues a question for the next read.
func (r *rows) ask(c *check) {
	r.checks = append(r.checks, c)
	r.more = true
}

// giveUp stops delivering a row for as long as the shard stays active.
func (r *rows) giveUp(k rowKey) {
	r.givenUp[k] = true
	delete(r.known, k)
	r.more = true // the earliest time and the hints change
}

// written is for outbox writes that have committed: the deleted rows leave known.
func (r *rows) written(batch []outWrite) {
	for _, w := range batch {
		if w.reason == "" {
			delete(r.known, rowKey{id: w.id})
		}
	}
}

// outQueue holds the node's changes to outbox rows until they are written, a
// moment after the first, so that more answers join the commit. The pauses are
// real time.
type outQueue struct {
	queue  []outWrite
	linger *time.Timer // the next write, nil if none is due
}

func (q *outQueue) add(w outWrite) {
	q.queue = append(q.queue, w)
	if q.linger == nil {
		q.linger = time.NewTimer(commitLinger)
	}
}

// c is ready when the queue is due to be written.
func (q *outQueue) c() <-chan time.Time {
	if q.linger == nil {
		return nil
	}
	return q.linger.C
}

// take empties the queue for a write.
func (q *outQueue) take() []outWrite {
	batch := q.queue
	q.queue, q.linger = nil, nil
	return batch
}

// retry puts back a write that failed, to be tried again after d.
func (q *outQueue) retry(batch []outWrite, d time.Duration) {
	q.queue = append(batch, q.queue...)
	if q.linger != nil {
		q.linger.Stop()
	}
	q.linger = time.NewTimer(d)
}

func (q *outQueue) halt() {
	if q.linger != nil {
		q.linger.Stop()
	}
}

// inboxTimer is the shard's one timer, for its earliest inbox row that is not
// due yet. When it fires the owner reads the due rows, once a cycle has
// completed since its last read.
type inboxTimer struct {
	due  bool // it fired: rows are due
	c    <-chan time.Time
	stop func() bool
}

// arm replaces the timer for a read that is durable now.
func (t *inboxTimer) arm(clk Clock, now time.Time, snap *snapshot) {
	t.halt()
	if !snap.hasNext {
		return
	}
	d := max(time.Duration(snap.next-now.Unix())*time.Second, 0)
	t.c, t.stop = clk.NewTimer(d)
}

func (t *inboxTimer) fired() {
	t.c, t.stop = nil, nil
	t.due = true
}

func (t *inboxTimer) halt() {
	if t.stop != nil {
		t.stop()
	}
	t.c, t.stop = nil, nil
}

func (t *inboxTimer) armed() bool { return t.c != nil }

// readNow returns the shard's live rows that the node has not read before, from
// one snapshot of the database: outbox rows, and inbox rows that are due. It also
// answers the questions about delivered inbox rows and finds the shard's
// earliest inbox row. Reads and the owner's own writes run one at a time on the
// loop, so a row that a write deletes, or that a read finds consumed, is gone
// from every later read.
func (o *msgOwner) readNow(ctx context.Context) (*snapshot, error) {
	o.timer.due = false
	checks := o.rows.begin()
	nKnown := len(o.rows.known)
	given := o.rows.givenUp
	// Full: the rows stay in the table (and more is set), but the questions and
	// the earliest inbox row still need the snapshot.
	want := max(pendingRowsLimit-nKnown, 0)
	present := make([]bool, len(checks))

	snap := &snapshot{}
	more := false
	now := o.mg.now().Unix()
	bytes := 0
	skip := o.rows.skip
	err := o.db(ctx, func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// The outbox.
		idRows, err := tx.QueryContext(ctx, `SELECT id FROM _shardlite_outbox ORDER BY id LIMIT ?`, nKnown+len(given)+want+1)
		if err != nil {
			return err
		}
		var ids []int64
		for idRows.Next() {
			var id int64
			if err := idRows.Scan(&id); err != nil {
				idRows.Close()
				return err
			}
			ids = append(ids, id)
		}
		if err := idRows.Err(); err != nil {
			return err
		}
		idRows.Close()
		var fresh []int64
		for _, id := range ids {
			if !skip(rowKey{id: id}) {
				fresh = append(fresh, id)
			}
		}
		if len(fresh) > want {
			fresh, more = fresh[:want], true
		}
		for _, id := range fresh {
			if bytes >= pendingBytesLimit {
				more = true
				break
			}
			row := &outRow{id: id}
			var ct sql.NullString
			err := tx.QueryRowContext(ctx, `SELECT target, path, content_type, body, hops, created_at FROM _shardlite_outbox WHERE id = ?`, id).
				Scan(&row.target, &row.path, &ct, &row.body, &row.hops, &row.createdAt)
			if err != nil {
				return err
			}
			row.contentType = ct.String
			bytes += len(row.body)
			snap.out = append(snap.out, row)
		}

		// The inbox: the questions, the earliest, and the rows that are due.
		for i, c := range checks {
			var one int
			err := tx.QueryRowContext(ctx, `SELECT 1 FROM _shardlite_inbox WHERE id = ? AND due = ?`, c.row.id, c.row.due).Scan(&one)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			present[i] = err == nil
		}
		earliest, ok, err := firstInbox(ctx, tx, given, ``)
		if err != nil {
			return err
		}
		snap.earliest, snap.hasEarliest = earliest, ok
		if ok && earliest <= now {
			room := max(want-len(snap.out), 0)
			dueRows, err := tx.QueryContext(ctx, `SELECT id, due FROM _shardlite_inbox WHERE due <= ? ORDER BY due, id LIMIT ?`, now, nKnown+len(given)+room+1)
			if err != nil {
				return err
			}
			var freshIn []rowKey
			for dueRows.Next() {
				var k rowKey
				k.inbox = true
				if err := dueRows.Scan(&k.id, &k.due); err != nil {
					dueRows.Close()
					return err
				}
				if !skip(k) {
					freshIn = append(freshIn, k)
				}
			}
			if err := dueRows.Err(); err != nil {
				return err
			}
			dueRows.Close()
			if len(freshIn) > room {
				freshIn, more = freshIn[:room], true
			}
			for _, k := range freshIn {
				if bytes >= pendingBytesLimit {
					more = true
					break
				}
				row := &inRow{id: k.id, due: k.due}
				var ct, key, sender sql.NullString
				err := tx.QueryRowContext(ctx, `SELECT key, sender, path, content_type, body, hops FROM _shardlite_inbox WHERE id = ? AND due = ?`, k.id, k.due).
					Scan(&key, &sender, &row.path, &ct, &row.body, &row.hops)
				if err != nil {
					return err
				}
				row.key, row.sender, row.contentType = key.String, sender.String, ct.String
				bytes += len(row.body)
				snap.in = append(snap.in, row)
			}
		}
		// The timer is for the earliest row that is not due yet.
		next, ok, err := firstInbox(ctx, tx, given, `WHERE due > ?`, now)
		if err != nil {
			return err
		}
		snap.next, snap.hasNext = next, ok
		return nil
	})
	if err != nil {
		o.rows.failed(checks)
		return nil, err
	}
	o.rows.read(snap, checks, present, more)
	return snap, nil
}

// verify asks whether an inbox row that was delivered is still there with the
// same due time, as of the owner's next read. ok is false if the deliveries
// stopped first.
func (o *msgOwner) verify(row rowKey) (present, ok bool) {
	c := &check{row: row, res: make(chan bool, 1)}
	if !o.send(o.dctx, func() { o.rows.ask(c) }) {
		return false, false
	}
	select {
	case v := <-c.res:
		return v, true
	case <-o.dctx.Done():
		return false, false
	}
}

// start moves the outbox rows of a read and delivers its inbox rows. Each row
// goes in a goroutine of its own, which ends when the row is acknowledged,
// consumed or given up on, or when the deliveries stop.
func (o *msgOwner) start(snap *snapshot) {
	if o.k.quiet {
		return
	}
	mg := o.mg
	for _, row := range snap.out {
		o.deliveries.Go(func() error { mg.deliverOut(o, row); return nil })
	}
	for _, row := range snap.in {
		o.deliveries.Go(func() error { mg.deliverInbox(o, row); return nil })
	}
}

// stopDeliveries cancels the shard's deliveries and waits for them, and no more
// start. It is for the shards' shutdown, before any shard is let go of: a
// delivery holds a reference to the shard it sends to. The caller waits for the
// deliveries, not the loop, so the owner goes on answering everything else.
func (o *msgOwner) stopDeliveries(ctx context.Context) {
	_, err := do(ctx, o.ev, o.ended, func() struct{} {
		o.k.quiet = true // so nothing is added to the group from now on
		o.cancelDeliveries()
		return struct{}{}
	})
	if err == nil {
		o.deliveries.Wait()
	}
}

// holds reports whether the shard has something to deliver now (with later, an
// inbox row not due yet counts too). It is the lifecycle loop's question before
// an idle deactivation. A shard with nothing to deliver has its hints brought in
// line, so the ones that did their job go.
func (o *msgOwner) holds(ctx context.Context, later bool) (bool, error) {
	return o.live(ctx, later, true)
}

// live is holds, without the hints unless tidy.
func (o *msgOwner) live(ctx context.Context, later, tidy bool) (bool, error) {
	type result struct {
		live bool
		err  error
	}
	r, err := do(ctx, o.ev, o.ended, func() result {
		live, err := o.liveNow(ctx, later)
		if err == nil && !live && tidy {
			o.hints.tidy = true
		}
		return result{live, err}
	})
	if err != nil {
		return false, err
	}
	return r.live, r.err
}

// liveNow says whether the shard has an outbox row not given up on, or an inbox
// row that is due and not given up on (with later, one not due yet too), as of
// one snapshot.
func (o *msgOwner) liveNow(ctx context.Context, later bool) (bool, error) {
	var live bool
	now := o.mg.now().Unix()
	given := o.k.givenUp
	err := o.db(ctx, func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if live, err = firstOutbox(ctx, tx, given); err != nil || live {
			return err
		}
		if later {
			_, live, err = firstInbox(ctx, tx, given, ``)
		} else {
			_, live, err = firstInbox(ctx, tx, given, `WHERE due <= ?`, now)
		}
		return err
	})
	return live, err
}

// write queues a change to an outbox row.
func (o *msgOwner) write(w outWrite) { o.out.add(w) }

// commit writes the node's changes to the outbox: acknowledged rows are
// deleted, several to a commit, and rows given up on get their error. The
// writes go through Shard.Use and the owner is removed before a release, so
// they never run during the final sync of a deactivation.
func (o *msgOwner) commit() {
	batch := o.out.take()
	if len(batch) == 0 {
		return
	}
	ctx := o.ctx
	err := o.db(ctx, func(db *sql.DB) error { return writeBatch(ctx, db, batch) })
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, capture.ErrClosed) {
			return // the rows stay live and are sent again after the next activation
		}
		o.mg.log.Warn("could not delete sent messages, will retry", "shard", o.a.Name, "err", err)
		o.out.retry(batch, 200*time.Millisecond)
		return
	}
	o.rows.written(batch)
	o.hints.tidy = true
}
