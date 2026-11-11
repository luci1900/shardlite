package node_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/node"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// A node whose previous life another node adopted has nothing to adopt: it
// fences, starts its journal after the fence and writes no delta, and the
// shards keep what the adopter wrote.
func TestRestartAfterAnotherNodeAdoptedIt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := newRig(t)
	n1 := r.mustStart()
	s := r.create(n1, "users/1")
	r.commit(s, 3)
	dead := publishRange(t, n1)
	want := r.restoreDump(n1, "users/1")

	// n2 adopts n1's journal: mark, fence and deltas, then finish.
	reg2 := lease.NewRegistry(r.local, "n2", lease.RegistryOptions{TTL: time.Hour})
	l2, err := reg2.Acquire(ctx, lease.NodeInfo{Name: "n2"})
	if err != nil {
		t.Fatal(err)
	}
	files2 := &shardfile.Files{Blob: blob.NewStore(r.local, "n2", l2.Epoch, blob.Options{Backoff: time.Millisecond}), Node: "n2",
		Leases: reg2, Temp: tempfile.NewDir(t.TempDir())}
	marked, err := reg2.Adopt(ctx, dead, l2.Incarnation())
	if err != nil {
		t.Fatal(err)
	}
	rep, err := files2.AdoptJournal(ctx, "n1", dead.JournalFrom, dead.JournalNext, nil)
	if err != nil || rep.Deltas != 1 {
		t.Fatalf("adoption by n2: %+v %v", rep, err)
	}
	if _, err := reg2.FinishAdoption(ctx, marked, marked.Epoch, rep.Fence); err != nil {
		t.Fatal(err)
	}

	n1b := r.mustStart()
	if n1b.Adopted.Deltas != 0 || n1b.Adopted.Fence <= rep.Fence {
		t.Fatalf("restart adopted again: %+v (n2's fence %d)", n1b.Adopted, rep.Fence)
	}
	if n1b.Writer.NextSeq() != n1b.Adopted.Fence+1 {
		t.Fatalf("writer starts at %d, fence is %d", n1b.Writer.NextSeq(), n1b.Adopted.Fence)
	}
	l := n1b.Keeper.Lease()
	if l.Epoch != 2 || l.Adopted != 1 || l.AdoptedBy != nil || l.JournalNext != n1b.Adopted.Fence+1 {
		t.Fatalf("lease: %+v", l)
	}
	if got := r.restoreDump(n1b, "users/1"); got != want {
		t.Fatalf("restored after the restart:\n%s\nwant:\n%s", got, want)
	}
}

// A restart whose adoption of its own journal takes longer than the lease's TTL
// renews the lease meanwhile, so other nodes don't take it for dead and the node
// starts with a good lease.
func TestRestartRenewsItsLeaseWhileItAdopts(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	n1 := r.mustStart()
	s := r.create(n1, "users/1")
	r.commit(s, 2)
	publishRange(t, n1)

	// The adoption's delta reaches the manifest only once the lease has been
	// renewed three times, well past its TTL.
	renewed := make(chan struct{}, 16)
	var unrenewed atomic.Bool
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		switch {
		case op == blobtest.OpUpdate && key == lease.Key("n1") && after:
			select {
			case renewed <- struct{}{}:
			default:
			}
		case op == blobtest.OpUpdate && strings.HasPrefix(key, shardfile.ManifestPrefix) && !after:
			for len(renewed) > 0 {
				<-renewed // the restart's Acquire
			}
			for range 3 {
				select {
				case <-renewed:
				case <-time.After(10 * time.Second):
					unrenewed.Store(true)
					return nil
				}
			}
		}
		return nil
	})
	n2, err := node.Start(context.Background(), node.Config{
		Name: "n1", TempDir: t.TempDir(), Backend: r.local,
		Blob:   blob.Options{Backoff: time.Millisecond},
		Lease:  lease.RegistryOptions{TTL: 300 * time.Millisecond, Blob: blob.Options{Backoff: time.Millisecond}},
		Keeper: lease.KeeperOptions{Interval: 50 * time.Millisecond, Margin: 60 * time.Millisecond},
	})
	r.local.SetHook(nil)
	if err != nil {
		t.Fatal(err)
	}
	if unrenewed.Load() {
		t.Fatal("the lease was not renewed while the node adopted its journal")
	}
	if n2.Adopted.Deltas != 1 {
		t.Fatalf("adoption: %+v", n2.Adopted)
	}
	if err := n2.Keeper.Check(); err != nil {
		t.Fatalf("lease after the restart: %v", err)
	}
	if l := n2.Keeper.Lease(); l.Epoch != 2 || l.Adopted != 1 || l.AdoptedBy != nil {
		t.Fatalf("lease after the restart: %+v", l)
	}
}

// A node adopting its own journal records the adoption: the next life sees its
// previous epoch adopted, and nobody is marked as adopting it.
func TestOwnAdoptionIsRecorded(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.mustStart()
	n := r.mustStart()
	if l := n.Keeper.Lease(); l.Epoch != 2 || l.Adopted != 1 || l.AdoptedBy != nil {
		t.Fatalf("lease after its own adoption: %+v", l)
	}
}

// A restart adopts its own journal from storage alone: every commit storage
// had stays, and one it did not have (its segment was still on its way when the
// process died) is lost, with no disk copy to land it from.
func TestRestartAdoptsFromStorageAlone(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	n1 := r.mustStart()
	s := r.create(n1, "users/1")
	r.commit(s, 3) // each sync waits for its segment in storage
	publishRange(t, n1)
	want := apptest.Dump(t, s.path)

	// A fourth commit whose segment never reaches storage: the create stalls
	// until the process dies.
	var stalled atomic.Bool
	held := make(chan struct{})
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after && strings.HasPrefix(key, journal.Root) && stalled.Load() {
			select {
			case held <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	stalled.Store(true)
	r.app.Must(t, "insert", s.id, "2", "100")
	go s.sh.Sync(context.Background())
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("the fourth commit's segment never reached the store")
	}
	r.stops[n1]() // the process dies with the segment on its way
	stalled.Store(false)

	n2 := r.mustStart()
	if n2.Adopted.Deltas != 1 {
		t.Fatalf("the restart adopted %d deltas, want the one for the three commits in storage", n2.Adopted.Deltas)
	}
	if got := r.restoreDump(n2, "users/1"); got != want {
		t.Fatalf("restored after the restart:\n%s\nwant the three commits storage had:\n%s", got, want)
	}
}
