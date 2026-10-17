package node

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
)

// The messages owner on its own: a SQLite database with the reserved tables in
// place of the shard's capture (with a lock for the capture lock) and a fake
// transport.

// fakeGate is durability for the owner's own tests. Every read is durable at
// once, unless hold is set: then each read waits for the test to close the
// channel it was given (in held), and fail is what check says then. The owner
// never reads again for itself (cycleAfter never fires).
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

func (g *fakeGate) set(hold bool, fail error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.hold, g.fail = hold, fail
}

func (g *fakeGate) waiting() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.held)
}

// release ends the wait of the i-th held read.
func (g *fakeGate) release(i int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	close(g.held[i])
}

type orig struct {
	t    *testing.T
	db   *sql.DB
	gate *fakeGate
	mg   *messenger
	a    *shard
	k    *kept
	o    *msgOwner
	stop func() // ends the owner's Serve and waits for it

	capture sync.Mutex             // the capture lock: one call to the shard's connection at a time
	calls   atomic.Int64           // calls to the shard's connection begun
	hook    atomic.Pointer[func()] // run once, inside the next call
}

func newOwnerRig(t *testing.T, tr *transport) *orig {
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
	r := &orig{t: t, db: db, gate: &fakeGate{}, mg: mg, a: &shard{Name: "users/o1"}, k: newKept()}
	r.serve()
	return r
}

// use is the shard's connection, under the capture lock.
func (r *orig) use(_ context.Context, fn func(*sql.DB) error) error {
	r.calls.Add(1)
	r.capture.Lock()
	defer r.capture.Unlock()
	if h := r.hook.Swap(nil); h != nil {
		(*h)()
	}
	return fn(r.db)
}

// serve starts a new owner with the rig's kept, and runs it until stop.
func (r *orig) serve() {
	r.o = newMsgOwner(r.mg, r.a, r.use, r.gate, r.k)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	o := r.o
	go func() {
		defer close(done)
		o.Serve(ctx)
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

// settled is true once the owner has finished every read asked of it.
func (r *orig) settled() bool { return r.o.answered.Load() == r.o.asked.Load() }

// read has the owner read the shard, as a kick from the flush cycle would, and
// waits for the read to finish.
func (r *orig) read() {
	r.t.Helper()
	r.o.kick()
	apptest.Eventually(r.t, 10*time.Second, "the owner's read", r.settled)
}

func (r *orig) inFlight() int {
	r.k.mu.Lock()
	defer r.k.mu.Unlock()
	return len(r.k.inFlight)
}

func (r *orig) outboxRows() int {
	r.t.Helper()
	var n int
	if err := r.db.QueryRow(`SELECT count(*) FROM _shardlite_outbox`).Scan(&n); err != nil {
		r.t.Fatal(err)
	}
	return n
}

// A delivery that is acknowledged while a read runs deletes its row once the
// read is done, and the read, which saw the row in flight, doesn't start it again.
func TestOwnerNeverStartsARowTwice(t *testing.T) {
	t.Parallel()
	var sent apptest.Log[string]
	ack := make(chan struct{})
	tr := &transport{maxHops: DefaultMaxHops, intake: func(ctx context.Context, _ *shard, _ string, row *outRow) (int, error) {
		sent.Add(row.path)
		select {
		case <-ack:
			return http.StatusAccepted, nil
		case <-ctx.Done():
			return 0, context.Cause(ctx)
		}
	}}
	r := newOwnerRig(t, tr)
	apptest.Eventually(t, 10*time.Second, "the read at activation", r.settled)
	r.exec(`INSERT INTO _shardlite_outbox (target, path) VALUES ('users/o2', '/once')`)
	r.read()
	sent.WaitLen(t, 10*time.Second, "the delivery", 1)

	// The next read is held inside its call to the shard's connection.
	inRead, goOn := make(chan struct{}), make(chan struct{})
	hold := func() { close(inRead); <-goOn }
	r.hook.Store(&hold)
	r.o.kick()
	<-inRead
	before := r.calls.Load()
	close(ack) // acknowledged: the delivery wants to delete its row
	apptest.Eventually(t, 10*time.Second, "the delete to wait for the capture lock", func() bool { return r.calls.Load() > before })
	close(goOn)

	apptest.Eventually(t, 10*time.Second, "the row to be deleted", func() bool { return r.outboxRows() == 0 && r.inFlight() == 0 })
	r.read()
	r.stop()
	if got := sent.All(); len(got) != 1 {
		t.Fatalf("sent %v, want /once once", got)
	}
}

// An outbox row given up on gets its error, and is skipped for the rest of the
// activation (by later reads and by a new owner of the same activation). It is
// tried once more after the next activation.
func TestOwnerGivesUpOnAnOutboxRowForTheActivation(t *testing.T) {
	t.Parallel()
	var sent atomic.Int64
	tr := &transport{maxHops: DefaultMaxHops, intake: func(context.Context, *shard, string, *outRow) (int, error) {
		sent.Add(1)
		return http.StatusBadRequest, &answerError{body: "no such path"}
	}}
	r := newOwnerRig(t, tr)
	r.exec(`INSERT INTO _shardlite_outbox (target, path) VALUES ('users/o2', '/bad')`)
	r.read()
	apptest.Eventually(t, 10*time.Second, "the row's error", func() bool {
		var reason sql.NullString
		r.db.QueryRow(`SELECT error FROM _shardlite_outbox`).Scan(&reason)
		return strings.Contains(reason.String, "no such path")
	})
	r.read()
	r.stop()
	r.serve() // capture resumed after a release that didn't go through
	r.read()
	if n := sent.Load(); n != 1 {
		t.Fatalf("%d attempts in one activation, want 1", n)
	}

	r.stop()
	r.k = newKept() // the next activation
	r.serve()
	r.read()
	apptest.Eventually(t, 10*time.Second, "the attempt after the next activation", func() bool { return sent.Load() == 2 })
}

// An inbox row whose handler answers 2xx without consuming it is given up on:
// the delivery's own look finds it still there.
func TestOwnerGivesUpOnAnInboxRowItsHandlerKept(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	tr := &transport{maxHops: DefaultMaxHops, inbox: func(context.Context, *shard, *inRow) (int, error) {
		calls.Add(1)
		return http.StatusOK, nil
	}}
	r := newOwnerRig(t, tr)
	r.exec(`INSERT INTO _shardlite_inbox (path, due) VALUES ('/kept', 0)`)
	r.read()
	apptest.Eventually(t, 10*time.Second, "the row to be given up on", func() bool { return r.mg.stats.InboxGivenUp.Load() == 1 })
	r.k.mu.Lock()
	given, flying := len(r.k.givenUp), len(r.k.inFlight)
	r.k.mu.Unlock()
	if given != 1 || flying != 0 {
		t.Fatalf("%d rows given up on and %d in flight, want 1 and 0", given, flying)
	}
	r.read()
	r.stop()
	if n := calls.Load(); n != 1 {
		t.Fatalf("%d calls, want 1", n)
	}
}

// The rows of a read whose cycle failed don't go out, and the next read finds
// them again.
func TestOwnerReadsAgainWhenTheCycleFails(t *testing.T) {
	t.Parallel()
	var sent apptest.Log[string]
	tr := &transport{maxHops: DefaultMaxHops, intake: func(_ context.Context, _ *shard, _ string, row *outRow) (int, error) {
		sent.Add(row.path)
		return http.StatusAccepted, nil
	}}
	r := newOwnerRig(t, tr)
	apptest.Eventually(t, 10*time.Second, "the read at activation", r.settled)
	r.exec(`INSERT INTO _shardlite_outbox (target, path) VALUES ('users/o2', '/a')`)
	r.gate.set(true, errors.New("the sync failed"))
	r.o.kick()
	apptest.Eventually(t, 10*time.Second, "the read to wait for its cycle", func() bool { return r.gate.waiting() == 1 })
	r.gate.release(0)
	apptest.Eventually(t, 10*time.Second, "the read to finish", r.settled)
	if sent.Len() != 0 || r.inFlight() != 0 {
		t.Fatalf("sent %v (%d in flight) while check says the sync failed", sent.All(), r.inFlight())
	}
	r.gate.set(false, nil)
	r.read()
	if got := sent.WaitLen(t, 10*time.Second, "the row read again", 1); got[0] != "/a" {
		t.Fatalf("sent %v", got)
	}
}

// A kick while a read waits for its cycle (one that began before the read, as
// when the read races with the cycle that captures what it saw) reads again at
// once, rather than waiting for a cycle that may be long in coming.
func TestOwnerReadsAgainWhenKickedWhileWaiting(t *testing.T) {
	t.Parallel()
	var sent apptest.Log[string]
	tr := &transport{maxHops: DefaultMaxHops, intake: func(_ context.Context, _ *shard, _ string, row *outRow) (int, error) {
		sent.Add(row.path)
		return http.StatusAccepted, nil
	}}
	r := newOwnerRig(t, tr)
	apptest.Eventually(t, 10*time.Second, "the read at activation", r.settled)
	r.exec(`INSERT INTO _shardlite_outbox (target, path) VALUES ('users/o2', '/a')`)
	r.gate.set(true, nil)
	r.o.kick()
	apptest.Eventually(t, 10*time.Second, "the read to wait for its cycle", func() bool { return r.gate.waiting() == 1 })
	r.gate.set(false, nil) // the next read is durable at once; the first one's cycle never completes
	r.read()
	if got := sent.WaitLen(t, 10*time.Second, "the row", 1); got[0] != "/a" {
		t.Fatalf("sent %v", got)
	}
}

// Stopping the owner cancels its deliveries and returns once they have ended,
// with their rows still live and none left in flight.
func TestOwnerStopJoinsItsDeliveries(t *testing.T) {
	t.Parallel()
	var started, ended apptest.Log[string]
	tr := &transport{maxHops: DefaultMaxHops, intake: func(ctx context.Context, _ *shard, _ string, row *outRow) (int, error) {
		started.Add(row.path)
		<-ctx.Done() // the receiver never answers
		ended.Add(row.path)
		return 0, context.Cause(ctx)
	}}
	r := newOwnerRig(t, tr)
	r.exec(`INSERT INTO _shardlite_outbox (target, path) VALUES ('users/o2', '/a'), ('users/o2', '/b'), ('users/o3', '/c')`)
	r.read()
	started.WaitLen(t, 10*time.Second, "three deliveries in flight", 3)
	r.stop()
	if n := ended.Len(); n != 3 {
		t.Fatalf("%d deliveries ended by the time the owner stopped, want 3", n)
	}
	if n := r.outboxRows(); n != 3 || r.inFlight() != 0 {
		t.Fatalf("%d outbox rows and %d in flight, want 3 still live and none in flight", n, r.inFlight())
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
	if tm.armed() {
		t.Fatal("a timer that fired is still armed")
	}
}

// A hint that can't be written before a release keeps the shard active with a
// messages owner running, and the next idle look tries again.
func TestHintWriteFailingBeforeAReleaseKeepsTheOwner(t *testing.T) {
	t.Parallel()
	var logs syncBuffer
	r := newSRigWith(t, both(quick, logTo(&logs)))
	var failing atomic.Bool
	failing.Store(true)
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if !after && failing.Load() && op == blob.OpCreate && strings.HasPrefix(key, wakePrefix) {
			return errors.New("storage down")
		}
		return nil
	})
	due := time.Now().Add(time.Hour).Unix()
	r.setRow("users/hr1", "k", "/later", due)
	r.closeInApp("users/hr1")
	eventually(t, "a release to fail on the hint", func() bool {
		return strings.Contains(logs.String(), "could not deactivate an idle shard") && strings.Contains(logs.String(), "writing a wake hint")
	})
	if !r.m.IsActive("users/hr1") {
		t.Fatal("the shard was let go of without its hint")
	}
	o := r.shard("users/hr1").msgs.load()
	select {
	case <-o.ended:
		t.Fatal("the shard stayed active with no messages owner")
	default:
	}
	failing.Store(false)
	eventually(t, "the next idle look to let go of the shard", func() bool { return r.cached("users/hr1") })
	if keys := r.wakes(); len(keys) != 1 || keys[0] != wakeKey(time.Unix(due, 0), "users/hr1") {
		t.Fatalf("hints %v", keys)
	}
}

// At shutdown every shard's deliveries are joined before any shard is let go
// of: here a delivery held in the app, which takes a while to end once it is
// cancelled, has ended before the other shard's release writes anything.
func TestShutdownJoinsEveryDeliveryBeforeLettingGo(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, both(quick, drainFor(200*time.Millisecond)))
	rec := &recorder{keep: true, hold: make(chan struct{})}
	defer close(rec.hold)
	r.record(rec, "")
	var ended, stopping atomic.Bool
	hook := inboxHookFn(func(ctx context.Context, call func() (int, error)) (int, error) {
		status, err := call()
		if ctx.Err() != nil {
			time.Sleep(200 * time.Millisecond)
		}
		ended.Store(true)
		return status, err
	})
	r.inboxHook.Store(&hook)
	var early atomic.Int32 // writes for the other shard while the delivery was still running
	var seen atomic.Int32
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if !after && stopping.Load() && strings.Contains(key, "lj2") {
			seen.Add(1)
			if !ended.Load() {
				early.Add(1)
			}
		}
		return nil
	})
	r.sql("slow/lj2", `INSERT INTO kv (payload) VALUES (x'01')`)
	r.setRow("slow/lj1", "k", "/held", time.Now().Unix())
	eventually(t, "the delivery to reach the app", func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return len(rec.got) > 0
	})
	r.closeInApp("slow/lj1")
	r.closeInApp("slow/lj2")
	stopping.Store(true)
	if err := r.mrig.stop(); err != nil {
		t.Fatal(err)
	}
	if seen.Load() == 0 {
		t.Fatal("the other shard's release wrote nothing")
	}
	if n := early.Load(); n != 0 {
		t.Fatalf("%d writes of the other shard's release before the delivery had ended", n)
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
