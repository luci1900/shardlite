package node

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/service"
)

// Tests of the parts of the Catalog on their own, each with what it depends on
// passed in: a node on the in-memory store, and no Catalog.

// serveJournal serves n's writer and keeper, as Run's journal stage does, until
// stop is called or the test ends.
func serveJournal(t *testing.T, n *Node) (stop func()) {
	stop = service.Background(slog.Default(), n.Writer, n.Keeper, n.Confirmer)
	t.Cleanup(stop)
	return stop
}

// partsNode starts a node on the in-memory store, with the test kinds.
func partsNode(t *testing.T) (*Node, *Kinds) {
	t.Helper()
	dir := t.TempDir()
	n, err := Start(context.Background(), Config{
		Name: "n1", Backend: blob.NewMemory(), Data: dir, TempDir: filepath.Join(dir, "tmp"),
		Blob:   blob.Options{Backoff: time.Millisecond},
		Lease:  coord.RegistryOptions{TTL: time.Hour, Blob: blob.Options{Backoff: time.Millisecond}},
		Keeper: coord.KeeperOptions{Interval: 100 * time.Millisecond, Margin: time.Minute},
	})
	if err != nil {
		t.Fatal(err)
	}
	serveJournal(t, n)
	a, err := kind.ParseApp([]byte(testKinds), 0)
	if err != nil {
		t.Fatal(err)
	}
	kinds := NewKinds(0)
	kinds.Update(a)
	return n, kinds
}

// The cache evicts over its limit, but never a shard that has an entry in the
// table (active, being activated or deleted).
func TestCacheEvictionOnItsOwn(t *testing.T) {
	t.Parallel()
	n, _ := partsNode(t)
	ctx := context.Background()
	data := t.TempDir()
	tab := newTable(slog.New(slog.DiscardHandler))
	c := &cache{data: data, tab: tab, files: n.Files, node: n.Name, clock: realClock{}, log: slog.New(slog.DiscardHandler), stats: &Stats{}}
	db := testDB(t)
	if err := c.open(CacheOptions{Limit: 1, Every: time.Hour}, db); err != nil {
		t.Fatal(err)
	}
	path := c.pathFor(shardName("test.users/ce1"), "g1")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("cached"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := c.idx.put(indexRow{Name: shardName("test.users/ce1"), Path: path, Generation: "g1", State: stateCached, LastUsed: time.Now(), Bytes: 6}); err != nil {
		t.Fatal(err)
	}
	e, ok := tab.claim(shardName("test.users/ce1"))
	if !ok {
		t.Fatal("could not claim the name")
	}
	c.evictIfNeeded(ctx)
	if names, _ := c.cached(); len(names) != 1 {
		t.Fatalf("evicted a shard with an entry: cached %v", names)
	}
	tab.remove(e, nil)
	c.evictIfNeeded(ctx)
	if names, _ := c.cached(); len(names) != 0 {
		t.Fatalf("not evicted: cached %v", names)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the evicted shard's file is still there: %v", err)
	}
}

// The watchdog's period is on the Clock: it looks at the shards only when the
// clock moves.
func TestWatchdogRunsOnTheClock(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	st := &Stats{}
	tab := newTable(slog.New(slog.DiscardHandler))
	w := &watchdog{tab: tab, clock: clk, every: time.Second, alertAge: time.Minute, log: slog.New(slog.DiscardHandler), stats: st}
	ctx, cancel := context.WithCancel(context.Background())
	var g errgroup.Group
	g.Go(func() error { w.Serve(ctx); return nil })
	defer func() { cancel(); g.Wait() }()
	time.Sleep(50 * time.Millisecond)
	if n := st.Watchdog.Load(); n != 0 {
		t.Fatalf("%d passes with the clock standing still", n)
	}
	advanceUntil(t, clk, time.Second, "a pass of the watchdog", func() bool { return st.Watchdog.Load() >= 1 })
}
