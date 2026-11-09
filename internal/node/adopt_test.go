package node

import (
	"context"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/peers"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// A node found dead and then adopted stays dead in every listing after, though
// the adopter's mark and finish are writes to its lease, and died runs once. The
// adoption is made once. A node that restarts is listed live at its first
// listing after its lease shows a new epoch, and dies again on its own.
func TestLeaseScanKeepsAnAdoptedNodeDead(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		b := blob.NewMemory()
		gone := lease.NewRegistry(b, "gone", lease.RegistryOptions{})
		if _, err := gone.Acquire(ctx, lease.NodeInfo{Name: "gone"}); err != nil {
			t.Fatal(err)
		}

		// The scan and the adopter of node "w" (epoch 1), with no tails to adopt.
		reg := lease.NewRegistry(b, "w", lease.RegistryOptions{})
		store := blob.NewStore(b, "w", 1, blob.Options{})
		self := func() lease.Incarnation { return lease.Incarnation{Node: "w", Epoch: 1} }
		log := slog.New(slog.DiscardHandler)
		a := &adopter{
			reg: reg, files: &shardfile.Files{Blob: store, Backend: b, Node: "w", Leases: reg, Temp: tempfile.NewDir(t.TempDir())}, store: store,
			self: self, nodeErr: func() error { return nil }, clock: realClock{}, log: log, adopted: &notify.Counter{},
		}
		var died notify.Counter
		s := &peers.Scan{
			Registry: reg, Self: self, Clock: realClock{}, Log: log, Scans: &notify.Counter{}, Adopt: a.adopt,
			Options: peers.ScanOptions{Every: 2 * time.Second, TTL: 10 * time.Second},
			Died: func(n cluster.Node) {
				if n == "gone" {
					died.Inc()
				}
			},
		}
		sctx, cancel := context.WithCancel(ctx)
		var g errgroup.Group
		g.Go(func() error { return s.Serve(sctx) })
		t.Cleanup(func() { cancel(); g.Wait() })

		dead := func() bool {
			if l := s.Nodes(); l != nil {
				for _, v := range l.Nodes {
					if v.Name == "gone" {
						return v.Dead
					}
				}
			}
			return false
		}
		// deadAfter waits for the scan's next listing (and any adoption it started) and says whether it shows the node dead.
		deadAfter := func() bool {
			if err := s.Scans.Wait(ctx, s.Scans.Load()+1); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			return dead()
		}

		synctest.Wait()
		if dead() {
			t.Fatal("a lease written a moment ago is dead")
		}
		if err := a.adopted.Wait(ctx, 1); err != nil {
			t.Fatal(err)
		}
		l, err := gone.Get(ctx, "gone")
		if err != nil || l.Adopted < l.Epoch {
			t.Fatalf("the lease after the adoption: %+v %v", l, err)
		}
		// Every listing from here on, a TTL and more of them, shows it dead.
		for range 15 {
			if !deadAfter() {
				t.Fatal("an adopted node is listed live")
			}
		}
		if got := died.Load(); got != 1 {
			t.Fatalf("died ran %d times for one death", got)
		}
		if got := a.adopted.Load(); got != 1 {
			t.Fatalf("%d adoptions of one death", got)
		}

		// It restarts: the new epoch is its own write.
		if _, err := gone.Acquire(ctx, lease.NodeInfo{Name: "gone"}); err != nil {
			t.Fatal(err)
		}
		if deadAfter() {
			t.Fatal("a restarted node is listed dead at the first listing after its restart")
		}
		// It never renews, so it dies again, and that is a second death.
		time.Sleep(time.Minute)
		synctest.Wait()
		if !dead() || died.Load() != 2 {
			t.Fatalf("the restarted node: dead %v, died %d times", dead(), died.Load())
		}
	})
}
