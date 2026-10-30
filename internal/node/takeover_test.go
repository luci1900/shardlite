package node

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/service"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
	"github.com/luci1900/shardlite/internal/tempfile"
)

const takeoverTTL = time.Second

// cnode is one node of a cluster on one store, with no servers and no Shards:
// its journal, optionally its keeper, and optionally a lease scan.
type cnode struct {
	*Node
	tails *TailSet
	scan  *leaseScan
}

// startCluster starts a node on b. A live node renews its lease. A dying one
// writes segments but never renews, so its lease runs out.
func startCluster(t *testing.T, b blob.Backend, name cluster.Node, live bool) *cnode {
	t.Helper()
	dir := t.TempDir()
	n, err := Start(context.Background(), Config{
		Name: name, Backend: b, Data: dir, TempDir: filepath.Join(dir, "tmp"),
		Blob:   blob.Options{Backoff: time.Millisecond},
		Lease:  coord.RegistryOptions{TTL: takeoverTTL, Blob: blob.Options{Backoff: time.Millisecond}},
		Keeper: coord.KeeperOptions{Interval: takeoverTTL / 4, Margin: takeoverTTL / 5},
		Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	c := &cnode{Node: n, tails: &TailSet{}}
	n.SetTails(c.tails)
	if live {
		t.Cleanup(service.Background(slog.Default(), n.Writer, n.Keeper, n.Confirmer))
	} else {
		t.Cleanup(service.Background(slog.Default(), n.Writer))
	}
	return c
}

// scanning serves a lease scan on c until the test ends.
func (c *cnode) scanning(t *testing.T) *cnode {
	t.Helper()
	c.scan = &leaseScan{
		reg: c.Registry, files: c.Files, store: c.Store, nodeErr: c.Err,
		self:  func() coord.Incarnation { l := c.Keeper.Lease(); return l.Incarnation() },
		clock: realClock{}, opts: LeaseScanOptions{Every: 50 * time.Millisecond, TTL: takeoverTTL},
		log: slog.New(slog.DiscardHandler), scans: &notify.Counter{}, adopted: &notify.Counter{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	var g errgroup.Group
	g.Go(func() error { return c.scan.Serve(ctx) })
	t.Cleanup(func() { cancel(); g.Wait() })
	return c
}

// capturedShard is a shard the app writes and the node captures, without the Catalog.
type capturedShard struct {
	name, id, path string
	app            *apptest.App
	sh             *capture.Shard
}

func (c *cnode) shard(t *testing.T, name string) *capturedShard {
	t.Helper()
	s := &capturedShard{name: name, id: strings.ReplaceAll(name, "/", "-"), app: apptest.Start(t)}
	s.path = filepath.Join(t.TempDir(), s.id+".db")
	s.app.Must(t, "open", s.id, s.path)
	h, created, err := c.Files.Open(context.Background(), shardName(name))
	if err != nil || !created {
		t.Fatalf("open %s: %v", name, err)
	}
	if s.sh, err = capture.New(capture.Config{Name: shardName(name), Path: s.path, Generation: h.Manifest.Generation, Writer: c.Writer, TempDir: tempfile.NewDir(t.TempDir())}); err != nil {
		t.Fatal(err)
	}
	c.tails.Track(s.sh)
	t.Cleanup(func() { s.sh.Close(context.Background()) })
	return s
}

// commit makes k commits and syncs each into the journal.
func (s *capturedShard) commit(t *testing.T, k int) error {
	t.Helper()
	for range k {
		s.app.Must(t, "insert", s.id, "2", "100")
		if err := s.sh.Sync(context.Background()); err != nil {
			return err
		}
	}
	return nil
}

// publish writes c's journal range into its lease, as a renewal would.
func (c *cnode) publish(t *testing.T) {
	t.Helper()
	if _, err := c.Registry.Renew(context.Background(), c.Keeper.Lease(), c.Info()); err != nil {
		t.Fatal(err)
	}
}

func leaseOf(t *testing.T, c *cnode, node cluster.Node) *coord.NodeLease {
	t.Helper()
	l, err := c.Registry.Get(context.Background(), node)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func restoreOn(t *testing.T, c *cnode, shard string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "r.db")
	if _, err := c.Files.Restore(context.Background(), shardName(shard), dst, shardfile.RestoreOptions{}); err != nil {
		t.Fatalf("restore %s: %v", shard, err)
	}
	return sqlitetest.Dump(t, dst)
}

// A node that dies with a tail in its journal is adopted: the tail is a delta,
// the shard can be claimed by others, the journal's non-empty segments are gone
// and the node, carrying on as a zombie, is fenced.
func TestDeadNodeIsAdopted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b := blob.NewMemory()
	dead := startCluster(t, b, "dead", false)
	s := dead.shard(t, "test.users/1")
	if err := s.commit(t, 3); err != nil {
		t.Fatal(err)
	}
	dead.publish(t)
	want := apptest.Dump(t, s.path)

	a := startCluster(t, b, "a", true).scanning(t)
	third := startCluster(t, b, "third", true)
	select {
	case <-a.scan.adopted.Reached(1):
	case <-time.After(30 * time.Second):
		t.Fatal("the dead node wasn't adopted")
	}
	if l := leaseOf(t, a, "dead"); l.Adopted != l.Epoch || l.AdoptedBy != nil {
		t.Fatalf("dead lease after its adoption: %+v", l)
	}
	if m, _, err := third.Files.GetManifest(ctx, shardName("test.users/1")); err != nil || m.Journal != "" {
		t.Fatalf("manifest after the adoption: %+v %v", m, err)
	}
	if got := restoreOn(t, third, "test.users/1"); got != want {
		t.Fatalf("restored:\n%s\nwant:\n%s", got, want)
	}
	for info, err := range a.Store.List(ctx, journal.Prefix("dead")) {
		if err != nil {
			t.Fatal(err)
		}
		if info.Size > 0 {
			t.Fatalf("a non-empty segment is left: %s", info.Key)
		}
	}

	// The zombie's next segment is refused, and what it commits now is in no restore.
	if err := s.commit(t, 1); err == nil {
		t.Fatal("a sync on the fenced zombie succeeded")
	}
	h, err := third.Files.Attach(ctx, shardName("test.users/1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Claim(ctx); err != nil {
		t.Fatalf("claim after the adoption: %v", err)
	}
	if got := restoreOn(t, third, "test.users/1"); got != want {
		t.Fatalf("restored after the zombie wrote:\n%s\nwant:\n%s", got, want)
	}
}

// An adopter that dies after marking a lease, before finishing, is replaced: a
// third node adopts both the adopter and the node it was adopting.
func TestDeadAdopterIsReplaced(t *testing.T) {
	t.Parallel()
	b := blob.NewMemory()
	x := startCluster(t, b, "x", false)
	s := x.shard(t, "test.users/x")
	if err := s.commit(t, 2); err != nil {
		t.Fatal(err)
	}
	x.publish(t)
	want := apptest.Dump(t, s.path)

	y := startCluster(t, b, "y", false) // marks x, then renews nothing
	yl := y.Keeper.Lease()
	if _, err := y.Registry.Adopt(context.Background(), *leaseOf(t, y, "x"), yl.Incarnation()); err != nil {
		t.Fatal(err)
	}

	z := startCluster(t, b, "z", true).scanning(t)
	select {
	case <-z.scan.adopted.Reached(2):
	case <-time.After(30 * time.Second):
		t.Fatalf("adoptions: %d, want both x and y", z.scan.adopted.Load())
	}
	for _, node := range []cluster.Node{"x", "y"} {
		if l := leaseOf(t, z, node); l.Adopted != l.Epoch || l.AdoptedBy != nil {
			t.Fatalf("%s after its adoption: %+v", node, l)
		}
	}
	if got := restoreOn(t, z, "test.users/x"); got != want {
		t.Fatalf("restored:\n%s\nwant:\n%s", got, want)
	}
}

// Two nodes that find the same dead node at once: one adopts it.
func TestTwoAdoptersAdoptOnce(t *testing.T) {
	t.Parallel()
	b := blob.NewMemory()
	dead := startCluster(t, b, "dead", false)
	s := dead.shard(t, "test.users/d")
	if err := s.commit(t, 2); err != nil {
		t.Fatal(err)
	}
	dead.publish(t)

	a := startCluster(t, b, "a", true).scanning(t)
	c := startCluster(t, b, "c", true).scanning(t)
	apptest.Eventually(t, 30*time.Second, "the adoption", func() bool {
		return leaseOf(t, a, "dead").Adopted == 1
	})
	// Both have listed again since: neither adopts a second time.
	sa, sc := a.scan.scans.Load(), c.scan.scans.Load()
	<-a.scan.scans.Reached(sa + 3)
	<-c.scan.scans.Reached(sc + 3)
	if n := a.scan.adopted.Load() + c.scan.adopted.Load(); n != 1 {
		t.Fatalf("%d adoptions, want 1", n)
	}
}
