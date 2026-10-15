package node

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/thejerf/suture/v4"
	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/supervisor"
)

// Tests of the parts of Shards on their own, each with what it depends on
// passed in: a node on the in-memory store, and no Shards.

// serveJournal serves n's writer and keeper, as Run's journal stage does, until
// stop is called or the test ends.
func serveJournal(t *testing.T, n *Node) (stop func()) {
	stop = supervisor.Background(slog.Default(), "journal", n.Writer, n.Keeper)
	t.Cleanup(stop)
	return stop
}

// partsNode starts a node on the in-memory store, with the test kinds.
func partsNode(t *testing.T) (*Node, *Kinds) {
	t.Helper()
	dir := t.TempDir()
	n, err := Start(context.Background(), Config{
		Name: "n1", Backend: blob.NewMemory(), TempDir: filepath.Join(dir, "tmp"),
		Blob:   blob.Options{Backoff: time.Millisecond},
		Lease:  coord.RegistryOptions{TTL: time.Hour, Blob: blob.Options{Backoff: time.Millisecond}},
		Keeper: coord.KeeperOptions{Interval: 100 * time.Millisecond, Margin: time.Minute},
	})
	if err != nil {
		t.Fatal(err)
	}
	serveJournal(t, n)
	path := filepath.Join(dir, "kinds.json")
	if err := os.WriteFile(path, []byte(testKinds), 0o644); err != nil {
		t.Fatal(err)
	}
	kinds, err := LoadKinds(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	return n, kinds
}

// The wake scan wakes the shards whose hints are due and that have a manifest,
// deletes the hints that mean nothing, and leaves alone the ones not due yet.
func TestWakeScanOnItsOwn(t *testing.T) {
	t.Parallel()
	n, kinds := partsNode(t)
	ctx := context.Background()
	if _, _, err := n.Files.Create(ctx, "users/ws1", false); err != nil {
		t.Fatal(err)
	}
	hints := hintStore{n.Store}
	now := time.Now()
	for _, h := range []struct {
		shard string
		at    time.Time
	}{{"users/ws1", now}, {"users/ghost", now}, {"nokind/x", now}, {"users/later", now.Add(time.Hour)}} {
		if _, err := hints.writeWake(ctx, h.shard, h.at); err != nil {
			t.Fatal(err)
		}
	}
	var woken apptest.Log[string]
	w := &wakeScan{store: n.Store, hints: hints, files: n.Files, keeper: n.Keeper, kinds: kinds, clock: realClock{},
		opts: WakeOptions{Every: time.Hour, ActivationLimit: 2}, log: slog.New(slog.DiscardHandler),
		acquire: func(_ context.Context, target string) (*Ref, error) {
			woken.Add(target)
			return nil, errors.New("not activated in this test")
		}}
	w.scan(ctx)
	if got := woken.All(); !slices.Equal(got, []string{"users/ws1"}) {
		t.Fatalf("woke %v, want users/ws1", got)
	}
	var left []string
	for info, err := range n.Store.List(ctx, wakePrefix) {
		if err != nil {
			t.Fatal(err)
		}
		_, shard, _ := strings.Cut(strings.TrimPrefix(info.Key, wakePrefix), "/")
		left = append(left, shard)
	}
	slices.Sort(left)
	// The woken shard's hint goes once its messages are delivered, not here.
	if !slices.Equal(left, []string{"users/later", "users/ws1"}) {
		t.Fatalf("hints left: %v", left)
	}
}

// The cache evicts over its limit, but never a shard that has an entry in the
// table (active, being activated or deleted).
func TestCacheEvictionOnItsOwn(t *testing.T) {
	t.Parallel()
	n, _ := partsNode(t)
	ctx := context.Background()
	data := t.TempDir()
	tab := newTable(suture.New("table", supervisor.Spec(slog.New(slog.DiscardHandler), time.Second)))
	c := &cache{data: data, tab: tab, files: n.Files, node: n.Name, clock: realClock{}, log: slog.New(slog.DiscardHandler), stats: &Stats{}}
	if err := c.open(CacheOptions{Limit: 1, Every: time.Hour}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.idx.close() })
	path := c.pathFor("users", "ce1", "g1")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("cached"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := c.idx.put(indexRow{Name: "users/ce1", Path: path, Generation: "g1", State: stateCached, LastUsed: time.Now(), Bytes: 6}); err != nil {
		t.Fatal(err)
	}
	e, ok := tab.claim("users/ce1")
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
	tab := newTable(suture.New("table", supervisor.Spec(slog.New(slog.DiscardHandler), time.Second)))
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
