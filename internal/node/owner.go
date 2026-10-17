package node

import (
	"context"
	"database/sql"
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/notify"
)

// msgOwner is the messages owner of one active shard (docs/concurrency.md, The
// tree): a sequential loop that reads the shard's rows and starts their
// deliveries, and keeps the shard's inbox timer. It writes no wake hints: while
// a row's commit is in the shard's tail, adoption covers it after a crash, and
// the delta that ends the tail records the shard's wake time and writes its
// hint. It reads when the flush cycle has captured new
// frames of the shard (a kick), when the inbox timer fires, at activation, and
// after the next completed cycle when a read was cut short or failed. A read's
// rows go out once what it saw is durable by the gate's rule (durability).
//
// Deliveries run in an errgroup inside the owner, which stopping it cancels and
// joins, and record their own results through the shard's connection. What the
// owner and its deliveries share is in kept, under its own leaf lock.
//
// The shard's lifecycle loop starts it once the shard is captured, and stops it
// before a release, so nothing uses the capture during the final sync. If
// capture resumes, a new owner carries on with the same kept.
type msgOwner struct {
	mg   *messenger
	a    *shard                                                  // whose rows these are (the transport takes it)
	db   func(ctx context.Context, fn func(*sql.DB) error) error // the shard's connection, Litestream's
	gate durability
	k    *kept

	captured   *notify.Kick // the flush cycle has captured new frames of the shard
	deliveries errgroup.Group
	timer      inboxTimer // the loop's own

	// For tests: reads asked for (at activation and by each kick) and how many
	// of them the loop has finished, whether the inbox timer is set, and the end
	// of the loop.
	asked, answered atomic.Uint64
	armed           atomic.Bool
	ended           chan struct{}
}

// kept is what a shard's messages keep across owners while the shard stays
// active. The lifecycle loop holds it and hands it to each owner.
type kept struct {
	// mu guards inFlight and givenUp, which the owner's reads and its
	// deliveries change. It is a leaf.
	mu       sync.Mutex
	inFlight map[rowKey]bool // rows read whose delivery is running
	givenUp  map[rowKey]bool // rows the node has given up on for this activation
}

func newKept() *kept { return &kept{inFlight: map[rowKey]bool{}, givenUp: map[rowKey]bool{}} }

// view is what a read skips (the rows in flight and given up on), the rows
// given up on, and how many are in flight.
func (k *kept) view() (skip, given map[rowKey]bool, inFlight int) {
	k.mu.Lock()
	defer k.mu.Unlock()
	skip = maps.Clone(k.givenUp)
	maps.Copy(skip, k.inFlight)
	return skip, maps.Clone(k.givenUp), len(k.inFlight)
}

// given is a copy of the rows given up on.
func (k *kept) given() map[rowKey]bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return maps.Clone(k.givenUp)
}

// begin puts the rows of a read in flight, and forget takes them out again.
func (k *kept) begin(snap *snapshot)  { k.mark(snap, true) }
func (k *kept) forget(snap *snapshot) { k.mark(snap, false) }

func (k *kept) mark(snap *snapshot, on bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	set := func(key rowKey) {
		if on {
			k.inFlight[key] = true
		} else {
			delete(k.inFlight, key)
		}
	}
	for _, row := range snap.out {
		set(row.rowKey())
	}
	for _, row := range snap.in {
		set(row.rowKey())
	}
}

// leave is for a delivery that has ended: its row is no longer in flight.
func (k *kept) leave(key rowKey) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.inFlight, key)
}

// giveUp moves a row in flight to the rows given up on, for as long as the
// shard stays active.
func (k *kept) giveUp(key rowKey) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.inFlight, key)
	k.givenUp[key] = true
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

// newMsgOwner makes the owner of a's messages, with what they kept so far.
func newMsgOwner(mg *messenger, a *shard, db func(context.Context, func(*sql.DB) error) error, gate durability, k *kept) *msgOwner {
	o := &msgOwner{mg: mg, a: a, db: db, gate: gate, k: k, captured: notify.NewKick(), ended: make(chan struct{})}
	o.asked.Store(1) // the read at activation
	return o
}

func (o *msgOwner) String() string { return "messages " + o.a.Name }

// kick is the flush cycle telling the owner it has captured new frames of the
// shard: its new rows can be read, and are durable.
func (o *msgOwner) kick() {
	o.asked.Add(1)
	o.captured.Kick()
}

// Serve is the owner's loop. It runs until ctx ends, then cancels and joins its
// deliveries.
func (o *msgOwner) Serve(ctx context.Context) error {
	dctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		o.deliveries.Wait()
		o.timer.halt()
		close(o.ended)
	}()
	for {
		n := o.asked.Load()
		again, mark, kicked := o.pass(ctx, dctx)
		if kicked {
			continue // read again at once
		}
		o.answered.Store(n)
		var pace <-chan struct{}
		if again {
			pace = o.gate.cycleAfter(mark) // so a large table is never read in a tight loop
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
// then the deliveries of its rows and the inbox timer. again is true if the
// read was cut short or failed, and mark is the read's. kicked is true if the
// flush cycle kicked the owner while it waited: that cycle may have begun before
// the read, so it doesn't count for it, but it captured the shard's new frames,
// so a read made now is likely durable at once. The read's rows are put back,
// and the caller reads again rather than wait for another cycle.
func (o *msgOwner) pass(ctx, dctx context.Context) (again bool, mark uint64, kicked bool) {
	snap, err := o.read(ctx)
	mark = o.gate.mark() // after the read: a cycle that begins later captures all it saw
	if err != nil {
		if ctx.Err() == nil && !errors.Is(err, capture.ErrClosed) {
			o.mg.log.Warn("could not read a shard's outbox and inbox", "shard", o.a.Name, "err", err)
		}
		return true, mark, false
	}
	if done := o.gate.after(mark); done != nil {
		select {
		case <-done:
		case <-o.captured.C():
			o.k.forget(snap)
			return false, mark, true
		case <-ctx.Done():
			o.k.forget(snap)
			return false, mark, false
		}
		if o.gate.check() != nil {
			o.k.forget(snap) // the cycle didn't make them durable: read them again
			return true, mark, false
		}
	}
	o.start(dctx, snap)
	o.timer.arm(o.mg.clock, o.mg.now(), snap)
	o.armed.Store(o.timer.armed())
	return snap.more, mark, false
}

// start delivers the rows of a read, each in a goroutine of its own, which ends
// when the row is acknowledged, consumed or given up on, or when the deliveries
// stop.
func (o *msgOwner) start(ctx context.Context, snap *snapshot) {
	mg := o.mg
	for _, row := range snap.out {
		o.deliveries.Go(func() error { mg.deliverOut(ctx, o, row); return nil })
	}
	for _, row := range snap.in {
		o.deliveries.Go(func() error { mg.deliverInbox(ctx, o, row); return nil })
	}
}

// inboxTimer is the shard's one timer, for its earliest inbox row that is not
// due yet. When it fires the owner reads the rows that are due. Only the loop
// uses it.
type inboxTimer struct {
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

func (t *inboxTimer) fired() { t.c, t.stop = nil, nil }

func (t *inboxTimer) halt() {
	if t.stop != nil {
		t.stop()
	}
	t.c, t.stop = nil, nil
}

func (t *inboxTimer) armed() bool { return t.c != nil }

// read returns the shard's live rows that are not in flight or given up on,
// from one snapshot of the database: outbox rows, and inbox rows that are due,
// within the limits on rows and bytes (more is set if it left some). It also
// finds the earliest inbox row due later, for the timer. Its rows
// are in flight once it returns.
func (o *msgOwner) read(ctx context.Context) (*snapshot, error) {
	var snap *snapshot
	now := o.mg.now().Unix()
	err := o.db(ctx, func(db *sql.DB) error {
		// The ordering rule: a delivery deletes its row (or finds it consumed) and
		// leaves the rows in flight inside a call to the shard's connection, and
		// this read takes its view of them inside one too. The capture lock runs
		// the two one at a time, so the read either sees the row with its delivery
		// in flight, or doesn't see the row: a row is never started twice.
		skip, given, nInFlight := o.k.view()
		want := max(pendingRowsLimit-nInFlight, 0)
		snap = &snapshot{}
		bytes := 0

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// The outbox.
		idRows, err := tx.QueryContext(ctx, `SELECT id FROM _shardlite_outbox ORDER BY id LIMIT ?`, len(skip)+want+1)
		if err != nil {
			return err
		}
		var fresh []int64
		for idRows.Next() {
			var id int64
			if err := idRows.Scan(&id); err != nil {
				idRows.Close()
				return err
			}
			if !skip[rowKey{id: id}] {
				fresh = append(fresh, id)
			}
		}
		if err := idRows.Err(); err != nil {
			return err
		}
		idRows.Close()
		if len(fresh) > want {
			fresh, snap.more = fresh[:want], true
		}
		for _, id := range fresh {
			if bytes >= pendingBytesLimit {
				snap.more = true
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

		// The inbox rows that are due.
		room := max(want-len(snap.out), 0)
		dueRows, err := tx.QueryContext(ctx, `SELECT id, due FROM _shardlite_inbox WHERE due <= ? ORDER BY due, id LIMIT ?`, now, len(skip)+room+1)
		if err != nil {
			return err
		}
		var freshIn []rowKey
		for dueRows.Next() {
			k := rowKey{inbox: true}
			if err := dueRows.Scan(&k.id, &k.due); err != nil {
				dueRows.Close()
				return err
			}
			if !skip[k] {
				freshIn = append(freshIn, k)
			}
		}
		if err := dueRows.Err(); err != nil {
			return err
		}
		dueRows.Close()
		if len(freshIn) > room {
			freshIn, snap.more = freshIn[:room], true
		}
		for _, k := range freshIn {
			if bytes >= pendingBytesLimit {
				snap.more = true
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

		// The timer is for the earliest row that is not due yet.
		snap.next, snap.hasNext, err = firstInbox(ctx, tx, given, `WHERE due > ?`, now)
		return err
	})
	if err != nil {
		return nil, err
	}
	o.k.begin(snap) // one read at a time, so nothing started these meanwhile
	return snap, nil
}
