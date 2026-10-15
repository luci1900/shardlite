package node

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/apptest"
)

// The messages owner on its own: a SQLite database with the reserved tables in
// place of the shard's capture, a fake store for the hints and a fake transport.

// fakeWakes is a hint store that records what it is asked. With hold set, a
// write waits for hold to be closed (or for its context).
type fakeWakes struct {
	hold    chan struct{}
	started apptest.Log[string] // writes begun
	writes  apptest.Log[string] // writes done
	deletes apptest.Log[string]
}

func (f *fakeWakes) writeWake(ctx context.Context, shard string, t time.Time) (string, error) {
	key := wakeKey(t, shard)
	f.started.Add(key)
	if f.hold != nil {
		select {
		case <-f.hold:
		case <-ctx.Done():
			return key, context.Cause(ctx)
		}
	}
	f.writes.Add(key)
	return key, nil
}

func (f *fakeWakes) deleteWakes(ctx context.Context, keys ...string) error {
	for _, k := range keys {
		f.deletes.Add(k)
	}
	return nil
}

// fakeGate is durability for the owner's own tests. Every read is durable at
// once, unless hold is set: then each read waits for the test to close the
// channel it was given (in held), and fail is what check says then. The owner
// never reads for itself (cycleAfter never fires): the test reads (r.read).
type fakeGate struct {
	mu   sync.Mutex
	hold bool
	held []chan struct{}
	fail error
}

func (g *fakeGate) mark() uint64 { return 0 }

func (g *fakeGate) after(uint64) <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.hold {
		return nil
	}
	c := make(chan struct{})
	g.held = append(g.held, c)
	return c
}

func (g *fakeGate) check() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.fail
}

func (g *fakeGate) cycleAfter(uint64) <-chan struct{} { return nil }

// release ends the wait of the i-th held read, with check saying fail.
func (g *fakeGate) release(i int, fail error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.fail = fail
	close(g.held[i])
}

type orig struct {
	t     *testing.T
	db    *sql.DB
	wakes *fakeWakes
	gate  *fakeGate
	mg    *messenger
	o     *msgOwner
	stop  func() // ends the owner's Serve and waits for it
}

func newOwnerRig(t *testing.T, wakes *fakeWakes, tr *transport) *orig {
	t.Helper()
	dir := t.TempDir()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "shard.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(reservedSchema); err != nil {
		t.Fatal(err)
	}
	kindsPath := filepath.Join(dir, "kinds.json")
	if err := os.WriteFile(kindsPath, []byte(testKinds), 0o644); err != nil {
		t.Fatal(err)
	}
	kinds, err := LoadKinds(kindsPath, 0)
	if err != nil {
		t.Fatal(err)
	}
	mg := newMessenger(MessageOptions{Backoff: 20 * time.Millisecond, MaxBackoff: 50 * time.Millisecond, MaxAge: 24 * time.Hour, ReceiverLimit: 32, NodeLimit: 256},
		kinds, realClock{}, slog.New(slog.DiscardHandler), &Stats{}, tr)
	a := &shard{Name: "users/o1"}
	r := &orig{t: t, db: db, wakes: wakes, gate: &fakeGate{}, mg: mg}
	r.o = newMsgOwner(mg, a, func(_ context.Context, fn func(*sql.DB) error) error { return fn(db) }, wakes, r.gate, newKept())
	r.serve()
	return r
}

// serve runs the owner until stop.
func (r *orig) serve() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.o.Serve(ctx)
	}()
	var once sync.Once
	r.stop = func() { once.Do(func() { cancel(); <-done }) }
	r.t.Cleanup(r.stop)
}

func (r *orig) exec(stmt string) {
	r.t.Helper()
	if _, err := r.db.Exec(stmt); err != nil {
		r.t.Fatal(err)
	}
}

// read has the owner read the shard now, as a kick from the flush cycle would,
// and returns once it has.
func (r *orig) read(ctx context.Context) error {
	_, err := do(ctx, r.o.ev, r.o.ended, func() struct{} { r.o.read(); return struct{}{} })
	return err
}

// asks runs q on the owner's loop.
func (r *orig) asks(q func(o *msgOwner) bool) bool {
	v, err := do(context.Background(), r.o.ev, r.o.ended, func() bool { return q(r.o) })
	return err == nil && v
}

// within fails the test if f takes longer than d.
func within(t *testing.T, d time.Duration, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s took more than %v", what, d)
	}
}

func TestOwnerReadsWhileAHintWriteIsSlow(t *testing.T) {
	t.Parallel()
	wakes := &fakeWakes{hold: make(chan struct{})}
	r := newOwnerRig(t, wakes, &transport{maxHops: DefaultMaxHops})
	later := time.Now().Add(time.Hour).Unix()
	r.exec(`INSERT INTO _shardlite_inbox (path, due) VALUES ('/later', ` + strconv.FormatInt(later, 10) + `)`)
	ctx := context.Background()
	if err := r.read(ctx); err != nil {
		t.Fatal(err)
	}
	wakes.started.WaitLen(t, 10*time.Second, "the hint write to begin", 1)
	// The write is held in the store: reads, the flush cycle's word and the
	// lifecycle loop's question are all answered meanwhile.
	r.exec(`INSERT INTO _shardlite_inbox (path, due) VALUES ('/later2', ` + strconv.FormatInt(later+60, 10) + `)`)
	within(t, 2*time.Second, "reads during a slow hint write", func() {
		for range 5 {
			if err := r.read(ctx); err != nil {
				t.Error(err)
			}
		}
		if live, err := r.o.holds(ctx, true); err != nil || !live {
			t.Errorf("holds: %v, %v, want true", live, err)
		}
	})
	if n := wakes.writes.Len(); n != 0 {
		t.Fatalf("%d hint writes finished while the store held them", n)
	}
	if !r.asks(func(o *msgOwner) bool { return o.hints.busy && o.timer.armed() }) {
		t.Fatal("the owner is not writing a hint, or has no inbox timer")
	}
	close(wakes.hold)
	got := wakes.writes.WaitLen(t, 10*time.Second, "the hint", 1)
	if want := wakeKey(time.Unix(later, 0), "users/o1"); got[0] != want {
		t.Fatalf("hint %s, want %s", got[0], want)
	}
	apptest.Eventually(t, 10*time.Second, "the hint to be noted", func() bool {
		return r.asks(func(o *msgOwner) bool { return !o.hints.busy && o.k.hints[got[0]] })
	})
	if n := wakes.started.Len(); n != 1 {
		t.Fatalf("%d hint writes, want one for the minute of the earliest row", n)
	}
}

func TestOwnerEventsAfterItHasEndedDoNotBlock(t *testing.T) {
	t.Parallel()
	r := newOwnerRig(t, &fakeWakes{}, &transport{maxHops: DefaultMaxHops})
	r.exec(`INSERT INTO _shardlite_outbox (target, path) VALUES ('users/o2', '/x')`)
	r.stop()
	// No deadline on any of these: each has to give up because the owner ended.
	ctx := context.Background()
	within(t, 5*time.Second, "calls to an owner that has ended", func() {
		if err := r.read(ctx); !errors.Is(err, errLoopEnded) {
			t.Errorf("read: %v, want errLoopEnded", err)
		}
		r.o.reads.captured.Kick()
		if _, err := r.o.holds(ctx, true); !errors.Is(err, errLoopEnded) {
			t.Errorf("holds: %v, want errLoopEnded", err)
		}
		if _, err := r.o.live(ctx, false, false); !errors.Is(err, errLoopEnded) {
			t.Errorf("live: %v, want errLoopEnded", err)
		}
		if err := r.o.hintsBeforeRelease(ctx); !errors.Is(err, errLoopEnded) {
			t.Errorf("hints before a release: %v, want errLoopEnded", err)
		}
		r.o.woken(ctx, []string{wakeKey(time.Now(), "users/o1")})
		r.o.stopDeliveries(ctx)
		if _, ok := r.o.verify(rowKey{inbox: true, id: 1}); ok {
			t.Error("verify answered after the owner ended")
		}
		if r.o.send(ctx, func() {}) {
			t.Error("an event was taken after the owner ended")
		}
	})
}

func TestOwnerReleaseWithDeliveriesInFlight(t *testing.T) {
	t.Parallel()
	var started, ended apptest.Log[string]
	tr := &transport{maxHops: DefaultMaxHops, intake: func(ctx context.Context, _ *shard, _ string, row *outRow) (int, error) {
		started.Add(row.path)
		<-ctx.Done() // the receiver never answers
		ended.Add(row.path)
		return 0, context.Cause(ctx)
	}}
	wakes := &fakeWakes{}
	r := newOwnerRig(t, wakes, tr)
	r.exec(`INSERT INTO _shardlite_outbox (target, path) VALUES ('users/o2', '/a'), ('users/o2', '/b'), ('users/o3', '/c')`)
	ctx := context.Background()
	if err := r.read(ctx); err != nil {
		t.Fatal(err)
	}
	started.WaitLen(t, 10*time.Second, "three deliveries in flight", 3)

	// The hints for the release: one for now, since the outbox rows are live.
	before := time.Now()
	if err := r.o.hintsBeforeRelease(ctx); err != nil {
		t.Fatal(err)
	}
	got := wakes.writes.All()
	if len(got) != 1 || (got[0] != wakeKey(before, "users/o1") && got[0] != wakeKey(time.Now(), "users/o1")) {
		t.Fatalf("hints %v, want one for now", got)
	}
	// Stopping the owner cancels the deliveries and returns once they have ended.
	r.stop()
	if n := ended.Len(); n != 3 {
		t.Fatalf("%d deliveries ended by the time the owner stopped, want 3", n)
	}
	var n int
	if err := r.db.QueryRow(`SELECT count(*) FROM _shardlite_outbox`).Scan(&n); err != nil || n != 3 {
		t.Fatalf("%d outbox rows (%v), want the 3 still live", n, err)
	}
	if d := wakes.deletes.All(); len(d) != 0 {
		t.Fatalf("hints deleted after the release asked for them: %v", d)
	}

	// The release didn't go through and capture resumed: a new owner carries on
	// from what the last one knew, so it neither writes the hint again nor loses it.
	r.o = newMsgOwner(r.mg, r.o.a, r.o.db, wakes, r.gate, r.o.k)
	r.serve()
	if err := r.read(ctx); err != nil {
		t.Fatal(err)
	}
	started.WaitLen(t, 10*time.Second, "the rows to be delivered again", 6)
	if err := r.o.hintsBeforeRelease(ctx); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, k := range wakes.started.All() {
		if seen[k] {
			t.Fatalf("hint %s written again by the new owner", k)
		}
		seen[k] = true
	}
	r.stop()
	if n := ended.Len(); n != 6 {
		t.Fatalf("%d deliveries ended by the time the second owner stopped, want 6", n)
	}
}

// The owner's parts on their own, without its loop.

// A row goes out only once the read that found it is durable, reads settle in
// order, and the rows of a read whose cycle failed are read again.
func TestReadsSettleInOrder(t *testing.T) {
	t.Parallel()
	var sent apptest.Log[string]
	tr := &transport{maxHops: DefaultMaxHops, intake: func(_ context.Context, _ *shard, _ string, row *outRow) (int, error) {
		sent.Add(row.path)
		return http.StatusAccepted, nil
	}}
	r := newOwnerRig(t, &fakeWakes{}, tr)
	r.asks(func(*msgOwner) bool { return true }) // after the owner's first read, which was durable at once
	r.gate.mu.Lock()
	r.gate.hold = true
	r.gate.mu.Unlock()
	ctx := context.Background()
	r.exec(`INSERT INTO _shardlite_outbox (target, path) VALUES ('users/o2', '/a')`)
	if err := r.read(ctx); err != nil {
		t.Fatal(err)
	}
	r.exec(`INSERT INTO _shardlite_outbox (target, path) VALUES ('users/o2', '/b')`)
	if err := r.read(ctx); err != nil {
		t.Fatal(err)
	}
	pending := func() int {
		n, _ := do(ctx, r.o.ev, r.o.ended, func() int { return len(r.o.reads.waits) })
		return n
	}
	if n := pending(); n != 2 || sent.Len() != 0 {
		t.Fatalf("%d reads pending and %d rows sent before any was durable", n, sent.Len())
	}

	// The second read's cycle completing first settles nothing: the first is older.
	r.gate.release(1, nil)
	if n := pending(); n != 2 || sent.Len() != 0 {
		t.Fatalf("%d reads pending and %d rows sent with the oldest still waiting", n, sent.Len())
	}

	// The cycle failed for both (check says so): nothing goes out, and both rows
	// are read again by the next read, which is durable at once.
	r.gate.release(0, errors.New("the sync failed"))
	apptest.Eventually(t, 10*time.Second, "both reads to settle", func() bool { return pending() == 0 })
	if got := sent.All(); len(got) != 0 {
		t.Fatalf("sent %v while check says the sync failed", got)
	}
	r.gate.mu.Lock()
	r.gate.hold, r.gate.fail = false, nil
	r.gate.mu.Unlock()
	if err := r.read(ctx); err != nil {
		t.Fatal(err)
	}
	got := sent.WaitLen(t, 10*time.Second, "the rows read again", 2)
	if slices.Sort(got); !slices.Equal(got, []string{"/a", "/b"}) {
		t.Fatalf("sent %v, want /a and /b once each", got)
	}
}

func TestRowsAreKnownUntilSeenGone(t *testing.T) {
	t.Parallel()
	k := newKept()
	r := rows{known: map[rowKey]bool{}, givenUp: k.givenUp, more: true}
	out, in := &outRow{id: 1}, &inRow{id: 2, due: 10}
	asked := &check{row: in.rowKey(), res: make(chan bool, 1)}
	r.ask(asked)
	checks := r.begin()
	if len(checks) != 1 || r.more {
		t.Fatalf("begin took %d questions and left more %v", len(checks), r.more)
	}
	snap := &snapshot{out: []*outRow{out}, in: []*inRow{in}}
	r.read(snap, checks, []bool{false}, false)
	if <-asked.res {
		t.Fatal("the question got the wrong answer")
	}
	if !r.skip(out.rowKey()) || !r.skip(in.rowKey()) {
		t.Fatal("rows just read are not skipped by the next read")
	}
	r.forget(snap) // its cycle failed
	if r.skip(out.rowKey()) || !r.more {
		t.Fatal("a read whose cycle failed is not read again")
	}
	r.read(snap, nil, nil, false)
	r.written([]outWrite{{id: 1}})
	if r.skip(out.rowKey()) {
		t.Fatal("a deleted outbox row is still known")
	}
	r.giveUp(in.rowKey())
	if !r.skip(in.rowKey()) || r.known[in.rowKey()] || !k.givenUp[in.rowKey()] {
		t.Fatal("a row given up on is not skipped, or not kept")
	}
	c := &check{row: rowKey{inbox: true, id: 3}, res: make(chan bool, 1)}
	r.ask(c)
	r.failed(r.begin())
	if len(r.checks) != 1 || !r.more {
		t.Fatal("a failed read lost its question")
	}
}

func TestInboxTimerFiresOnTheClock(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	var tm inboxTimer
	tm.arm(clk, clk.Now(), &snapshot{})
	if tm.armed() {
		t.Fatal("armed with no row due later")
	}
	tm.arm(clk, clk.Now(), &snapshot{next: clk.Now().Unix() + 60, hasNext: true})
	clk.Advance(59 * time.Second)
	select {
	case <-tm.c:
		t.Fatal("fired early")
	default:
	}
	clk.Advance(time.Second)
	<-tm.c
	tm.fired()
	if !tm.due || tm.armed() {
		t.Fatal("a timer that fired doesn't say rows are due")
	}
}

func TestOutQueueWaitsForMoreAnswers(t *testing.T) {
	t.Parallel()
	var q outQueue
	if q.c() != nil {
		t.Fatal("an empty queue is due")
	}
	q.add(outWrite{id: 1})
	q.add(outWrite{id: 2})
	<-q.c()
	batch := q.take()
	if len(batch) != 2 || q.c() != nil {
		t.Fatalf("took %d writes and left the queue due %v", len(batch), q.c() != nil)
	}
	q.add(outWrite{id: 3})
	q.retry(batch, time.Millisecond)
	<-q.c()
	if batch := q.take(); len(batch) != 3 || batch[0].id != 1 || batch[2].id != 3 {
		t.Fatalf("after a retry: %v", batch)
	}
}

func TestWakeHintJobs(t *testing.T) {
	t.Parallel()
	k := newKept()
	h := wakeHints{k: k, name: "users/h1"}
	t0 := time.Now().Truncate(time.Minute)
	old, cur := wakeKey(t0, "users/h1"), wakeKey(t0.Add(2*time.Minute), "users/h1")
	k.hints[old] = true
	h.noteEarliest(&snapshot{earliest: t0.Add(2 * time.Minute).Unix(), hasEarliest: true})
	if !h.tidy || !h.read1 {
		t.Fatal("a first durable read doesn't ask for the hints to be brought in line")
	}
	job := h.reconcileJob()
	if len(job.write) != 1 || wakeKey(job.write[0], "users/h1") != cur || len(job.delete) != 1 || job.delete[0] != old {
		t.Fatalf("reconcile: write %v, delete %v, want %s written and %s deleted", job.write, job.delete, cur, old)
	}
	k.hints[cur] = true
	h.tidy = false
	h.noteEarliest(&snapshot{earliest: t0.Add(2 * time.Minute).Unix(), hasEarliest: true})
	if h.tidy {
		t.Fatal("the same earliest row asks for the hints again")
	}
	// Before a release, with a live outbox row: one for now, one for the earliest
	// row, and the old one goes.
	now := t0.Add(90 * time.Second)
	job = h.releaseJob(now, true, t0.Add(2*time.Minute).Unix(), true)
	if len(job.write) != 1 || wakeKey(job.write[0], "users/h1") != wakeKey(now, "users/h1") || len(job.delete) != 1 || job.delete[0] != old {
		t.Fatalf("release: write %v, delete %v", job.write, job.delete)
	}
}

// The messenger's limits under many senders: no receiver ever has more than its
// limit in flight, nor the node more than its own, and once every sender has
// left no receiver is kept.
func TestMessengerLimitsAndForgetsItsReceivers(t *testing.T) {
	t.Parallel()
	const perReceiver, perNode = 3, 5
	mg := newMessenger(MessageOptions{ReceiverLimit: perReceiver, NodeLimit: perNode}, nil, realClock{}, slog.New(slog.DiscardHandler), &Stats{}, &transport{maxHops: DefaultMaxHops})
	var inFlight [4]atomic.Int32
	var node, worst, worstNode atomic.Int32
	raise := func(a *atomic.Int32, v int32) {
		for {
			m := a.Load()
			if v <= m || a.CompareAndSwap(m, v) {
				break
			}
		}
	}
	var g errgroup.Group
	for i := range 400 {
		g.Go(func() error {
			target := "users/" + strconv.Itoa(i%len(inFlight))
			release, err := mg.acquire(context.Background(), target)
			if err != nil {
				return err
			}
			raise(&worst, inFlight[i%len(inFlight)].Add(1))
			raise(&worstNode, node.Add(1))
			time.Sleep(time.Duration(i%3) * time.Millisecond)
			node.Add(-1)
			inFlight[i%len(inFlight)].Add(-1)
			release()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	if worst.Load() > perReceiver || worstNode.Load() > perNode {
		t.Fatalf("%d in flight to one receiver and %d on the node, limits %d and %d", worst.Load(), worstNode.Load(), perReceiver, perNode)
	}
	mg.mu.Lock()
	for name := range mg.recv {
		t.Errorf("receiver %s kept after every sender left", name)
	}
	mg.mu.Unlock()
	// A sender that gives up while waiting takes its count with it.
	release, err := mg.acquire(context.Background(), "users/x")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	for range perReceiver - 1 {
		if _, err := mg.acquire(ctx, "users/x"); err != nil {
			t.Fatal(err) // these are never released: the receiver is full after them
		}
	}
	if _, err := mg.acquire(ctx, "users/x"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a full receiver gave a place: %v", err)
	}
	release()
	mg.mu.Lock()
	defer mg.mu.Unlock()
	if r := mg.recv["users/x"]; r == nil || r.n != perReceiver-1 {
		t.Fatal("the receiver's count is wrong after a sender gave up")
	}
}
