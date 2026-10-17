package node

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// An eviction whose release may have landed without anybody hearing drops the
// shard as if it had: keeping it cached would let the next activation serve it
// with no request to blob storage, while the manifest names no journal.
func TestEvictionWithAnUnknownReleaseDropsTheShard(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dir := t.TempDir()
	b := blob.WithHook(blob.NewMemory())
	n, err := Start(ctx, Config{
		Name: "n1", Backend: b, TempDir: filepath.Join(dir, "tmp"),
		Blob:   blob.Options{Backoff: time.Millisecond},
		Lease:  coord.RegistryOptions{TTL: time.Hour, Blob: blob.Options{Backoff: time.Millisecond}},
		Keeper: coord.KeeperOptions{Interval: time.Minute, Margin: time.Minute},
	})
	if err != nil {
		t.Fatal(err)
	}
	serveJournal(t, n)
	if _, created, err := n.Files.Open(ctx, "users/u1"); err != nil || !created {
		t.Fatalf("create: %v", err)
	}
	tab := newTable(slog.New(slog.DiscardHandler))
	data := t.TempDir()
	c := &cache{data: data, tab: tab, files: n.Files, node: n.Name, clock: realClock{}, log: slog.New(slog.DiscardHandler), stats: &Stats{}}
	if err := c.open(CacheOptions{Every: time.Hour}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.idx.close() })
	path := c.pathFor("users", "u1", "g1")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("cached"), 0o644); err != nil {
		t.Fatal(err)
	}
	row := indexRow{Name: "users/u1", Path: path, Generation: "g1", State: stateCached, LastUsed: time.Now(), Bytes: 6}
	if err := c.idx.put(row); err != nil {
		t.Fatal(err)
	}

	// The release lands, and from then on nothing about the manifest can be learned.
	var landed atomic.Bool
	b.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if key != shardfile.ManifestKey("users/u1") {
			return nil
		}
		if op == blob.OpUpdate && after {
			landed.Store(true)
			return errors.New("response lost")
		}
		if landed.Load() && !after {
			return errors.New("unreachable")
		}
		return nil
	})
	evicted, err := c.evict(ctx, row)
	if err != nil || !evicted {
		t.Fatalf("evict = %v, %v, want the shard dropped", evicted, err)
	}
	if _, ok, err := c.idx.get("users/u1"); err != nil || ok {
		t.Fatalf("cache row left: %v %v", ok, err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("file left: %v", err)
	}
	b.SetHook(nil)
	if m, _, err := n.Files.GetManifest(ctx, "users/u1"); err != nil || m.Owner != "" {
		t.Fatalf("manifest: %+v %v, want the release landed", m, err)
	}
}
