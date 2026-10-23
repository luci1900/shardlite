package coord_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
)

// acquire takes the lease of node on b.
func acquire(t *testing.T, b blob.Backend, node cluster.Node) (*coord.Registry, coord.NodeLease) {
	t.Helper()
	reg := coord.NewRegistry(b, node, opts())
	l, err := reg.Acquire(context.Background(), coord.NodeInfo{Name: node})
	if err != nil {
		t.Fatal(err)
	}
	return reg, l
}

func get(t *testing.T, reg *coord.Registry, node cluster.Node) coord.NodeLease {
	t.Helper()
	l, err := reg.Get(context.Background(), node)
	if err != nil {
		t.Fatal(err)
	}
	return *l
}

func TestAdoptMarksAndFinishes(t *testing.T) {
	t.Parallel()
	b, ctx := newLocal(t), context.Background()
	_, l1 := acquire(t, b, "n1")
	if l1.AdoptedBy != nil || l1.Adopted != 0 {
		t.Fatalf("a first lease has nothing to adopt: %+v", l1)
	}
	reg2, l2 := acquire(t, b, "n2")

	marked, err := reg2.Adopt(ctx, get(t, reg2, "n1"), l2.Incarnation())
	if err != nil {
		t.Fatal(err)
	}
	if got := get(t, reg2, "n1"); got.AdoptedBy == nil || *got.AdoptedBy != l2.Incarnation() || got.ValidAt(time.Now()) {
		t.Fatalf("marked lease: %+v", got)
	}
	done, err := reg2.FinishAdoption(ctx, marked, marked.Epoch, 7)
	if err != nil {
		t.Fatal(err)
	}
	if got := get(t, reg2, "n1"); got.AdoptedBy != nil || got.Adopted != 1 || got.JournalFrom != 8 || got.JournalNext != 8 || got.ETag != done.ETag {
		t.Fatalf("finished lease: %+v", got)
	}

	// The node's next life has nothing to adopt and starts its journal after the fence.
	next, err := coord.NewRegistry(b, "n1", opts()).Acquire(ctx, coord.NodeInfo{Name: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	if next.Epoch != 2 || next.AdoptedBy != nil || next.Adopted != 1 || next.JournalNext != 8 {
		t.Fatalf("next life: %+v", next)
	}
}

// A life that crashed leaves an epoch to adopt: the next life names itself as
// its adopter, and a release leaves nothing to adopt.
func TestAcquireAdoptsItsOwnPreviousLife(t *testing.T) {
	t.Parallel()
	b, ctx := newLocal(t), context.Background()
	reg, _ := acquire(t, b, "n1")
	l, err := reg.Acquire(ctx, coord.NodeInfo{Name: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	if l.AdoptedBy == nil || *l.AdoptedBy != l.Incarnation() || !l.ValidAt(time.Now()) {
		t.Fatalf("own adoption: %+v", l)
	}
	if l, err = reg.FinishAdoption(ctx, l, l.Epoch-1, 3); err != nil {
		t.Fatal(err)
	}
	if l.Adopted != 1 || l.AdoptedBy != nil || l.JournalNext != 4 || !l.ValidAt(time.Now()) {
		t.Fatalf("after its own adoption: %+v", l)
	}
	if err := reg.Release(ctx, l); err != nil {
		t.Fatal(err)
	}
	if got := get(t, reg, "n1"); !got.Released || got.Adopted != 2 {
		t.Fatalf("released: %+v", got)
	}
	l, err = reg.Acquire(ctx, coord.NodeInfo{Name: "n1"})
	if err != nil || l.Epoch != 3 || l.AdoptedBy != nil {
		t.Fatalf("after a release: %+v %v", l, err)
	}
}

// A node adopting its own journal renews its lease meanwhile, so the finish,
// made from the lease as Acquire wrote it, is written over those renewals.
func TestOwnAdoptionFinishesOverItsRenewals(t *testing.T) {
	t.Parallel()
	b, ctx := newLocal(t), context.Background()
	reg, _ := acquire(t, b, "n1")
	l, err := reg.Acquire(ctx, coord.NodeInfo{Name: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := reg.Renew(ctx, get(t, reg, "n1"), l.NodeInfo); err != nil {
			t.Fatal(err)
		}
	}
	done, err := reg.FinishAdoption(ctx, l, l.Epoch-1, 3)
	if err != nil {
		t.Fatalf("finish over the node's own renewals: %v", err)
	}
	if got := get(t, reg, "n1"); got.Epoch != 2 || got.Adopted != 1 || got.AdoptedBy != nil || got.JournalNext != 4 || got.ETag != done.ETag {
		t.Fatalf("lease: %+v", got)
	}
}

func TestAdoptSurvivesLostResponses(t *testing.T) {
	t.Parallel()
	b, ctx := newLocal(t), context.Background()
	acquire(t, b, "n1")
	reg2, l2 := acquire(t, b, "n2")
	var lost atomic.Int32
	b.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpUpdate && key == coord.Key("n1") && after && lost.Add(1)%3 != 0 {
			return errors.New("response lost")
		}
		return nil
	})
	marked, err := reg2.Adopt(ctx, get(t, reg2, "n1"), l2.Incarnation())
	if err != nil {
		t.Fatalf("adopt with lost responses: %v", err)
	}
	if _, err := reg2.FinishAdoption(ctx, marked, marked.Epoch, 1); err != nil {
		t.Fatalf("finish with lost responses: %v", err)
	}
	b.SetHook(nil)
	if got := get(t, reg2, "n1"); got.Adopted != 1 || got.AdoptedBy != nil {
		t.Fatalf("lease: %+v", got)
	}
}

func TestAdoptRefusesALeaseThatChanged(t *testing.T) {
	t.Parallel()
	b, ctx := newLocal(t), context.Background()
	reg1, l1 := acquire(t, b, "n1")
	reg2, l2 := acquire(t, b, "n2")
	reg3, l3 := acquire(t, b, "n3")

	// Two adopters read the same lease: one wins.
	read := get(t, reg2, "n1")
	if _, err := reg2.Adopt(ctx, read, l2.Incarnation()); err != nil {
		t.Fatal(err)
	}
	if _, err := reg3.Adopt(ctx, read, l3.Incarnation()); !errors.Is(err, coord.ErrNotAdoptable) {
		t.Fatalf("second adopter: %v, want ErrNotAdoptable", err)
	}

	// A lease renewed since it was read.
	read = get(t, reg2, "n3")
	if _, err := reg3.Renew(ctx, l3, l3.NodeInfo); err != nil {
		t.Fatal(err)
	}
	if _, err := reg2.Adopt(ctx, read, l2.Incarnation()); !errors.Is(err, coord.ErrNotAdoptable) {
		t.Fatalf("adopt of a renewed lease: %v, want ErrNotAdoptable", err)
	}

	// A released lease is never adopted, and the holder of a marked lease has lost it.
	if err := reg3.Release(ctx, get(t, reg3, "n3")); err != nil {
		t.Fatal(err)
	}
	if _, err := reg2.Adopt(ctx, get(t, reg2, "n3"), l2.Incarnation()); !errors.Is(err, coord.ErrNotAdoptable) {
		t.Fatalf("adopt of a released lease: %v, want ErrNotAdoptable", err)
	}
	if _, err := reg1.Renew(ctx, l1, l1.NodeInfo); !errors.Is(err, coord.ErrLeaseLost) {
		t.Fatalf("renewal of a marked lease: %v, want ErrLeaseLost", err)
	}
}

// An adopter that dies is replaced: its own finish then fails, unless the one
// that replaced it has finished already, in which case it counts as done.
func TestAdoptionTakenOver(t *testing.T) {
	t.Parallel()
	b, ctx := newLocal(t), context.Background()
	acquire(t, b, "n1")
	reg2, l2 := acquire(t, b, "n2")
	reg3, l3 := acquire(t, b, "n3")

	first, err := reg2.Adopt(ctx, get(t, reg2, "n1"), l2.Incarnation())
	if err != nil {
		t.Fatal(err)
	}
	second, err := reg3.Adopt(ctx, get(t, reg3, "n1"), l3.Incarnation())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg2.FinishAdoption(ctx, first, first.Epoch, 5); !errors.Is(err, coord.ErrNotAdoptable) {
		t.Fatalf("finish by the replaced adopter: %v, want ErrNotAdoptable", err)
	}
	if _, err := reg3.FinishAdoption(ctx, second, second.Epoch, 6); err != nil {
		t.Fatal(err)
	}
	if _, err := reg2.FinishAdoption(ctx, first, first.Epoch, 5); err != nil {
		t.Fatalf("finish once the epoch is adopted: %v", err)
	}
	if got := get(t, reg2, "n1"); got.Adopted != 1 || got.JournalNext != 7 {
		t.Fatalf("lease: %+v", got)
	}
}

func TestListShowsEveryLease(t *testing.T) {
	t.Parallel()
	b, ctx := newLocal(t), context.Background()
	reg1, l1 := acquire(t, b, "n1")
	acquire(t, b, "n2")
	if err := reg1.Release(ctx, l1); err != nil {
		t.Fatal(err)
	}
	got, err := reg1.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "n1" || got[1].Name != "n2" {
		t.Fatalf("list: %+v", got)
	}
	for _, e := range got {
		if e.ETag != get(t, reg1, e.Name).ETag || e.LastModified.IsZero() {
			t.Fatalf("listed lease: %+v", e)
		}
	}
}

// A node restarting while another adopts its journal waits until the adoption
// finishes, or until its adopter looks dead.
func TestAcquireWaitsForALiveAdopter(t *testing.T) {
	t.Parallel()
	for _, finish := range []bool{true, false} {
		synctest.Test(t, func(t *testing.T) {
			b, ctx := newLocal(t), context.Background()
			acquire(t, b, "n1")
			reg2, l2 := acquire(t, b, "n2")
			marked, err := reg2.Adopt(ctx, get(t, reg2, "n1"), l2.Incarnation())
			if err != nil {
				t.Fatal(err)
			}

			var g errgroup.Group
			var back atomic.Pointer[coord.NodeLease]
			g.Go(func() error {
				l, err := coord.NewRegistry(b, "n1", opts()).Acquire(ctx, coord.NodeInfo{Name: "n1"})
				back.Store(&l)
				return err
			})
			time.Sleep(5 * time.Second)
			synctest.Wait()
			if back.Load() != nil {
				t.Fatal("Acquire took the lease from a live adopter")
			}

			if finish {
				if _, err := reg2.FinishAdoption(ctx, marked, marked.Epoch, 9); err != nil {
					t.Fatal(err)
				}
			} else {
				time.Sleep(opts().TTL) // n2 renews nothing and looks dead
			}
			if err := g.Wait(); err != nil {
				t.Fatal(err)
			}
			l := back.Load()
			if finish && (l.Epoch != 2 || l.AdoptedBy != nil || l.JournalNext != 10) {
				t.Fatalf("after the adoption finished: %+v", l)
			}
			if !finish && (l.Epoch != 2 || l.AdoptedBy == nil || *l.AdoptedBy != l.Incarnation()) {
				t.Fatalf("after the adopter died: %+v", l)
			}
		})
	}
}

// The deadline holds by the wall clock too, for a pause the monotonic clock
// didn't count.
func TestKeeperExpiresByTheWallClock(t *testing.T) {
	t.Parallel()
	_, lease := acquire(t, newLocal(t), "n1")
	var jump atomic.Int64
	ko := keeperOpts(nil)
	ko.Now = func() time.Time { return time.Now().Round(0).Add(time.Duration(jump.Load())) }
	k := coord.NewKeeper(nil, lease, ko)
	if err := k.Check(); err != nil {
		t.Fatal(err)
	}
	jump.Store(int64(9 * time.Second)) // past TTL - margin
	if err := k.Check(); !errors.Is(err, coord.ErrLeaseExpired) {
		t.Fatalf("Check = %v, want ErrLeaseExpired", err)
	}
}
