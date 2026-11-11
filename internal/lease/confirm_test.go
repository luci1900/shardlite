package lease_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/lease"
)

// confirmerRig is a confirmer for n1's first life on a store the test can hook.
type confirmerRig struct {
	t      *testing.T
	b      *blobtest.Hooked
	reg    *lease.Registry
	lease  lease.Record
	keeper *lease.Keeper
	conf   *lease.Confirmer
	log    *bytes.Buffer // what the keeper and the confirmer logged, read once a confirmation has returned
}

func newConfirmerRig(t *testing.T) *confirmerRig {
	t.Helper()
	b := newLocal(t)
	reg, rec := acquire(t, b, "n1")
	var log bytes.Buffer
	k := lease.NewKeeper(reg, rec, lease.KeeperOptions{Logger: slog.New(slog.NewTextHandler(&log, nil))})
	return &confirmerRig{t: t, b: b, reg: reg, lease: rec, keeper: k, conf: lease.NewConfirmer(reg, k), log: &log}
}

// serve runs the confirmer until the test ends or stop is called.
func (r *confirmerRig) serve() (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	var g errgroup.Group
	g.Go(func() error { return r.conf.Serve(ctx) })
	stop = func() { cancel(); g.Wait() }
	r.t.Cleanup(stop)
	return stop
}

// confirm joins a round and waits for its read.
func (r *confirmerRig) confirm() error {
	r.t.Helper()
	round := r.conf.Join()
	select {
	case <-round.Done():
		return round.Err()
	case <-time.After(10 * time.Second):
		r.t.Fatal("the confirmation did not come")
		return nil
	}
}

// holdLeaseReads blocks reads of n1's lease until release, reporting each one
// that arrives on entered.
func (r *confirmerRig) holdLeaseReads() (entered chan struct{}, release func()) {
	entered, gate := make(chan struct{}, 8), make(chan struct{})
	r.b.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if !after && op == blobtest.OpGet && key == lease.Key("n1") {
			entered <- struct{}{}
			select {
			case <-gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	var once atomic.Bool
	return entered, func() {
		if once.CompareAndSwap(false, true) {
			close(gate)
		}
	}
}

func TestLeaseHolds(t *testing.T) {
	t.Parallel()
	me := lease.Incarnation{Node: "n1", Epoch: 3}
	other := lease.Incarnation{Node: "n2", Epoch: 1}
	later := lease.Incarnation{Node: "n1", Epoch: 4}
	for _, tc := range []struct {
		name  string
		lease lease.Record
		want  bool
	}{
		{"untouched", lease.Record{NodeInfo: lease.NodeInfo{Name: "n1"}, Epoch: 3}, true},
		{"expired is not taken", lease.Record{NodeInfo: lease.NodeInfo{Name: "n1"}, Epoch: 3, Renewed: time.Unix(1, 0), TTL: time.Second}, true},
		{"earlier epochs adopted", lease.Record{NodeInfo: lease.NodeInfo{Name: "n1"}, Epoch: 3, Adopted: 2}, true},
		{"its own adoption mark", lease.Record{NodeInfo: lease.NodeInfo{Name: "n1"}, Epoch: 3, AdoptedBy: &me}, true},
		{"marked by another", lease.Record{NodeInfo: lease.NodeInfo{Name: "n1"}, Epoch: 3, AdoptedBy: &other}, false},
		{"adopted up to the epoch", lease.Record{NodeInfo: lease.NodeInfo{Name: "n1"}, Epoch: 3, Adopted: 3}, false},
		{"released", lease.Record{NodeInfo: lease.NodeInfo{Name: "n1"}, Epoch: 3, Released: true}, false},
		{"a newer epoch", lease.Record{NodeInfo: lease.NodeInfo{Name: "n1"}, Epoch: 4}, false},
		{"an older epoch", lease.Record{NodeInfo: lease.NodeInfo{Name: "n1"}, Epoch: 2}, false},
	} {
		if got := tc.lease.Holds(me); got != tc.want {
			t.Errorf("%s: Holds = %v, want %v", tc.name, got, tc.want)
		}
	}
	if l := (lease.Record{NodeInfo: lease.NodeInfo{Name: "n1"}, Epoch: 4}); !l.Holds(later) {
		t.Error("the later life's own lease does not hold")
	}
}

func TestConfirmerConfirmsALeaseThatHolds(t *testing.T) {
	t.Parallel()
	r := newConfirmerRig(t)
	r.serve()
	if err := r.confirm(); err != nil {
		t.Fatal(err)
	}
	// The node's own renewal changes the ETag and nothing else that matters.
	if _, err := r.reg.Renew(context.Background(), r.lease, lease.NodeInfo{Name: "n1"}); err != nil {
		t.Fatal(err)
	}
	if err := r.confirm(); err != nil {
		t.Fatal(err)
	}
	if got := r.conf.Reads(); got != 2 {
		t.Fatalf("%d reads for two confirmations", got)
	}
	if err := r.keeper.Check(); err != nil {
		t.Fatal(err)
	}
}

// A lease taken in any way fails the keeper with ErrLost, as a lost
// renewal does, and the waiter with it.
func TestConfirmerFailsTheKeeperOnATakenLease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		take func(t *testing.T, r *confirmerRig)
	}{
		{"marked by an adopter", func(t *testing.T, r *confirmerRig) {
			reg2, l2 := acquire(t, r.b, "n2")
			if _, err := reg2.Adopt(ctx, get(t, reg2, "n1"), l2.Incarnation()); err != nil {
				t.Fatal(err)
			}
		}},
		{"a newer epoch", func(t *testing.T, r *confirmerRig) { acquire(t, r.b, "n1") }},
		{"released", func(t *testing.T, r *confirmerRig) {
			if err := r.reg.Release(ctx, get(t, r.reg, "n1")); err != nil {
				t.Fatal(err)
			}
		}},
		{"adopted up to the epoch", func(t *testing.T, r *confirmerRig) {
			reg2, l2 := acquire(t, r.b, "n2")
			marked, err := reg2.Adopt(ctx, get(t, reg2, "n1"), l2.Incarnation())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reg2.FinishAdoption(ctx, marked, marked.Epoch, 3); err != nil {
				t.Fatal(err)
			}
		}},
		{"missing", func(t *testing.T, r *confirmerRig) {
			if err := r.b.Delete(ctx, lease.Key("n1"), ""); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newConfirmerRig(t)
			r.serve()
			tc.take(t, r)
			if err := r.confirm(); !errors.Is(err, lease.ErrLost) {
				t.Fatalf("confirmation: %v, want ErrLost", err)
			}
			if err := r.keeper.Check(); !errors.Is(err, lease.ErrLost) {
				t.Fatalf("keeper: %v, want ErrLost", err)
			}
			select {
			case <-r.keeper.Lost():
			default:
				t.Fatal("the keeper's Lost is open")
			}
		})
	}
}

// A store that cannot be reached fails the waiter and nothing else, and says so
// in the log: the next confirmation reads again.
func TestConfirmerStoreErrorFailsOnlyTheWaiter(t *testing.T) {
	t.Parallel()
	r := newConfirmerRig(t)
	r.serve()
	r.b.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if !after && op == blobtest.OpGet && key == lease.Key("n1") {
			return errors.New("storage is down")
		}
		return nil
	})
	if err := r.confirm(); err == nil || !strings.Contains(err.Error(), "storage is down") || errors.Is(err, lease.ErrLost) {
		t.Fatalf("confirmation: %v, want the store's error", err)
	}
	if msg := r.log.String(); !strings.Contains(msg, "could not read the node's lease") || !strings.Contains(msg, "storage is down") {
		t.Fatalf("the failed read was not logged with its cause:\n%s", msg)
	}
	if err := r.keeper.Check(); err != nil {
		t.Fatalf("the keeper failed: %v", err)
	}
	r.b.SetHook(nil)
	if err := r.confirm(); err != nil {
		t.Fatal(err)
	}
}

// Waiters that arrive before a read begins share it. One that arrives while a
// read is in flight waits for the next, since the read in flight began before it
// did and proves nothing about what it read.
func TestConfirmerWaiterDuringAReadWaitsForTheNext(t *testing.T) {
	t.Parallel()
	r := newConfirmerRig(t)
	entered, release := r.holdLeaseReads()
	defer release()

	a1, a2 := r.conf.Join(), r.conf.Join()
	if a1 != a2 {
		t.Fatal("waiters before a read began did not share a round")
	}
	r.serve()
	<-entered // the first read is in flight
	b := r.conf.Join()
	if b == a1 {
		t.Fatal("a waiter joined a read that had begun")
	}
	release()
	for _, round := range []*lease.Round{a1, b} {
		select {
		case <-round.Done():
		case <-time.After(10 * time.Second):
			t.Fatal("a round did not finish")
		}
		if err := round.Err(); err != nil {
			t.Fatal(err)
		}
	}
	if got := r.conf.Reads(); got != 2 {
		t.Fatalf("%d reads, want 2: one for the early waiters, one for the late one", got)
	}
}

// A round whose waiters have all left has no read.
func TestConfirmerNoReadForWaitersThatLeft(t *testing.T) {
	t.Parallel()
	r := newConfirmerRig(t)
	r.conf.Join().Leave()
	stop := r.serve()
	// A round that does want its read goes after the abandoned one, so by the
	// time it is done the loop has passed the abandoned one.
	if err := r.confirm(); err != nil {
		t.Fatal(err)
	}
	stop()
	if got := r.conf.Reads(); got != 1 {
		t.Fatalf("%d reads, want 1", got)
	}
}

// Ending the service fails the round that waits and every later Join.
func TestConfirmerStopFailsWaiters(t *testing.T) {
	t.Parallel()
	r := newConfirmerRig(t)
	stop := r.serve()
	_, release := r.holdLeaseReads()
	defer release()
	round := r.conf.Join()
	stop()
	select {
	case <-round.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the round did not finish after the stop")
	}
	if err := round.Err(); !errors.Is(err, lease.ErrConfirmerStopped) {
		t.Fatalf("round: %v", err)
	}
	if err := r.confirm(); !errors.Is(err, lease.ErrConfirmerStopped) {
		t.Fatalf("a join after the stop: %v", err)
	}
	if err := r.keeper.Check(); err != nil {
		t.Fatalf("the stop failed the keeper: %v", err)
	}
}
