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

// evicted checks that target was evicted from this node: its manifest names no
// owner and no journal, and nothing of it is left here.
func (r *mrig) evicted(target, path string) {
	r.t.Helper()
	man, err := r.manifest(target)
	if err != nil {
		r.t.Fatal(err)
	}
	if man.Owner != "" || man.Journal != "" {
		r.t.Fatalf("the manifest after an eviction: owner %q, journal %q", man.Owner, man.Journal)
	}
	if r.m.IsActive(target) || r.cached(target) {
		r.t.Fatal("the shard is still on the node")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		r.t.Fatalf("its file is still there: %v", err)
	}
}

// An active shard with sync on is evicted: every acknowledged commit is in
// blob storage, the manifest is released and nothing is left on the node. The
// next request claims and restores it.
func TestEvictAnActiveShard(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ref := r.acquire("wallet/g1")
	r.write(ref, "a")
	if err := r.m.Gate(context.Background(), ref, true); err != nil {
		t.Fatal(err)
	}
	want := r.liveDump(ref.Path())
	path := ref.Path()
	r.app.Must(t, "close", "a")
	ref.Release()

	if err := r.m.Evict(context.Background(), "wallet/g1"); err != nil {
		t.Fatal(err)
	}
	r.evicted("wallet/g1", path)
	if got := r.dumpBlob("wallet/g1"); got != want {
		t.Fatalf("after the eviction:\n%s\nwant:\n%s", got, want)
	}
	restores := r.m.Stats.Restores.Load()
	ref = r.acquire("wallet/g1")
	defer ref.Release()
	if r.m.Stats.Restores.Load() != restores+1 || r.liveDump(ref.Path()) != want {
		t.Fatal("the next request did not claim and restore the shard")
	}
}

// A cached shard is evicted without being activated again.
func TestEvictACachedShard(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ref := r.acquire("slow/g2")
	r.write(ref, "a")
	r.app.Must(t, "close", "a")
	path := ref.Path()
	ref.Release()
	if ok, err := r.shard("slow/g2").deactivateNow(context.Background()); !ok || err != nil {
		t.Fatalf("deactivate: %v %v", ok, err)
	}
	want := r.dumpBlob("slow/g2")
	activations := r.m.Stats.Activations.Load()
	if err := r.m.Evict(context.Background(), "slow/g2"); err != nil {
		t.Fatal(err)
	}
	if r.m.Stats.Activations.Load() != activations {
		t.Fatal("evicting the cached shard activated it")
	}
	r.evicted("slow/g2", path)
	if r.dumpBlob("slow/g2") != want {
		t.Fatal("evicting the shard changed it")
	}
}

// A shard in use or one the app has open is not evicted: it stays active and
// served, and its manifest still names the node. One that isn't on the node is
// refused.
func TestEvictRefusesAShardInUse(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ref := r.acquire("slow/g3")
	if err := r.m.Evict(context.Background(), "slow/g3"); !errors.Is(err, ErrBusy) {
		t.Fatalf("evicting a shard a request holds: %v", err)
	}
	r.write(ref, "a") // the app keeps its connection open
	ref.Release()
	if err := r.m.Evict(context.Background(), "slow/g3"); !errors.Is(err, ErrAppHolds) {
		t.Fatalf("evicting a shard the app holds: %v", err)
	}
	if man, err := r.manifest("slow/g3"); err != nil || man.Owner != "n1" {
		t.Fatalf("the manifest after a refusal: %+v %v", man, err)
	}
	ref = r.acquire("slow/g3")
	r.app.Must(t, "insert", "a", "3", "100")
	if err := r.m.Gate(context.Background(), ref, true); err != nil {
		t.Fatal(err)
	}
	if r.dumpBlob("slow/g3") != r.liveDump(ref.Path()) {
		t.Fatal("not captured after the refusal")
	}
	ref.Release()

	if err := r.m.Evict(context.Background(), "slow/nothere"); !errors.Is(err, ErrNotHere) {
		t.Fatalf("evicting a shard that isn't here: %v", err)
	}
}
