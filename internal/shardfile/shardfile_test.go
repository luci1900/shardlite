package shardfile_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
)

const shard = "users/42"

// write runs a few app commits and syncs them.
func write(t *testing.T, _ *harness.Env, app mustDoer, sync func() error, i int) {
	t.Helper()
	app.Must(t, "insert", "a", "2", "300")
	app.Must(t, "exec", "a", fmt.Sprintf("UPDATE kv SET payload = zeroblob(%d) WHERE id = 1", 50+i))
	if err := sync(); err != nil {
		t.Fatal(err)
	}
}

// removeLocal deletes a shard's local file, as eviction or a lost disk would.
func removeLocal(t *testing.T, path string) {
	t.Helper()
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		if err := os.RemoveAll(p); err != nil {
			t.Fatal(err)
		}
	}
}

func restoreDump(t *testing.T, e *harness.Env, name string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), name+".db")
	if _, err := e.Files.Restore(context.Background(), shard, dst, shardfile.RestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	return sqlitetest.Dump(t, dst)
}

func TestDeltaEqualsReplayingTail(t *testing.T) {
	t.Parallel()
	e := harness.New(t, 10*time.Millisecond)
	ctx := context.Background()
	app := e.StartApp(shard)
	app.Must(t, "insert", "a", "200", "1000") // about 50 pages, so a delta is clearly smaller than the base
	h, sh := e.NewShard(shard)
	for i := range 5 {
		write(t, e, app, func() error { return sh.Sync(ctx) }, i)
	}
	tail := sh.Tail()
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	if len(sh.Tail()) != 0 {
		t.Fatal("tail not dropped")
	}
	if got := h.Manifest.Files; len(got) != 1 || got[0] != fmt.Sprintf("1-%d.%d", tail[len(tail)-1].Last(), h.F.Blob.Epoch()) {
		t.Fatalf("manifest files %v", got)
	}
	// The manifest was stored.
	m, _, err := e.Files.GetManifest(ctx, shard)
	if err != nil || len(m.Files) != 1 {
		t.Fatalf("stored manifest %+v err=%v", m, err)
	}
	// Restoring from the delta alone (no journal entries beyond it) equals the app's database.
	if want, got := apptest.Dump(t, e.Path(shard)), restoreDump(t, e, "r"); want != got {
		t.Fatal("restored database differs")
	}

	// A second delta carries only the pages changed since.
	write(t, e, app, func() error { return sh.Sync(ctx) }, 9)
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	if len(h.Manifest.Files) != 2 {
		t.Fatalf("files %v", h.Manifest.Files)
	}
	first, _ := e.Store.Head(ctx, shardfile.FileKey(shard, h.Manifest.Generation, mustFile(t, h.Manifest.Files[0])))
	second, _ := e.Store.Head(ctx, shardfile.FileKey(shard, h.Manifest.Generation, mustFile(t, h.Manifest.Files[1])))
	t.Logf("base %d bytes, delta %d bytes", first.Size, second.Size)
	if second.Size >= first.Size {
		t.Fatalf("delta (%d) not smaller than base (%d)", second.Size, first.Size)
	}
	if want, got := apptest.Dump(t, e.Path(shard)), restoreDump(t, e, "r2"); want != got {
		t.Fatal("restored database differs after second delta")
	}
	sh.Close(ctx)
}

func mustFile(t *testing.T, s string) shardfile.File {
	r, err := shardfile.ParseFile(s)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestMaybeSnapshotByAge(t *testing.T) {
	t.Parallel()
	e := harness.New(t, 10*time.Millisecond)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	write(t, e, app, func() error { return sh.Sync(ctx) }, 0)
	if got, err := h.MaybeSnapshot(ctx, sh, time.Hour, shardfile.Policy{}, shardfile.Hooks{}); got != shardfile.Nothing || err != nil {
		t.Fatalf("wrote a delta too early: %v %v", got, err)
	}
	// The tail is older than a nanosecond by now.
	if got, err := h.MaybeSnapshot(ctx, sh, time.Nanosecond, shardfile.Policy{}, shardfile.Hooks{}); got != shardfile.WroteDelta || err != nil {
		t.Fatalf("no delta: %v %v", got, err)
	}
	sh.Close(ctx)
}

func TestCrashBetweenFileAndManifest(t *testing.T) {
	t.Parallel()
	e := harness.New(t, 10*time.Millisecond)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	write(t, e, app, func() error { return sh.Sync(ctx) }, 0)
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	before := h.Manifest.Files[0]
	write(t, e, app, func() error { return sh.Sync(ctx) }, 1)

	crash := errors.New("crash")
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{AfterFileWrite: func() error { return crash }}); !errors.Is(err, crash) {
		t.Fatalf("got %v", err)
	}
	// The orphan file exists, but the stored manifest is unchanged and valid.
	m, _, err := e.Files.GetManifest(ctx, shard)
	if err != nil || len(m.Files) != 1 || m.Files[0] != before {
		t.Fatalf("manifest %+v err=%v", m, err)
	}
	var keys int
	for _, err := range e.Store.List(ctx, "shards/"+shard+"/") {
		if err != nil {
			t.Fatal(err)
		}
		keys++
	}
	if keys != 2 {
		t.Fatalf("%d files, want the base and one orphan", keys)
	}
	// Restore ignores the orphan and still reaches the latest state through the journal.
	if want, got := apptest.Dump(t, e.Path(shard)), restoreDump(t, e, "r"); want != got {
		t.Fatal("restore after crash differs")
	}
	// The tail was kept, so the retry writes the same delta again and succeeds.
	if len(sh.Tail()) == 0 {
		t.Fatal("tail dropped by a failed delta")
	}
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	if len(h.Manifest.Files) != 2 {
		t.Fatalf("files %v", h.Manifest.Files)
	}
	sh.Close(ctx)
}

// A failed manifest update (storage error) behaves the same as a crash before it.
func TestManifestUpdateFails(t *testing.T) {
	t.Parallel()
	e := harness.New(t, 10*time.Millisecond)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	write(t, e, app, func() error { return sh.Sync(ctx) }, 0)
	e.Blob.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpUpdate && !after {
			return errors.New("storage down")
		}
		return nil
	})
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err == nil {
		t.Fatal("want error")
	}
	e.Blob.SetHook(nil)
	if len(h.Manifest.Files) != 0 || len(sh.Tail()) == 0 {
		t.Fatal("state changed by a failed update")
	}
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	sh.Close(ctx)
}

type mustDoer interface {
	Must(testing.TB, ...string) string
}

// After a delta empties the tail, a failed upload must not make Litestream
// start again from transaction 1 (whose local file the delta removed).
func TestUploadFailsAfterDelta(t *testing.T) {
	t.Parallel()
	e := harness.New(t, 10*time.Millisecond)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	write(t, e, app, func() error { return sh.Sync(ctx) }, 0)
	write(t, e, app, func() error { return sh.Sync(ctx) }, 1)
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	e.Blob.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && !after {
			return errors.New("storage down")
		}
		return nil
	})
	app.Must(t, "insert", "a", "1", "100")
	// The segment is retried until storage answers, so the caller gives up on its own.
	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if err := sh.Sync(short); err == nil {
		t.Fatal("want error")
	}
	e.Blob.SetHook(nil)
	if err := sh.Sync(ctx); err != nil {
		t.Fatalf("retry after a failed upload: %v", err)
	}
	// The commit is in the journal once, not once per attempt.
	es := entries(t, e, shard)
	for i := 1; i < len(es); i++ {
		if es[i].Position == es[i-1].Position {
			t.Fatalf("position %d is in the journal twice", es[i].Position)
		}
	}
	sh.Close(ctx)
}

// Any write to blob storage can succeed while its response is lost. Each write
// the shardfile layer makes must recognise its own earlier attempt afterwards.
func TestLostResponses(t *testing.T) {
	t.Parallel()
	// lose drops the response of the first successful op on a key with the given prefix.
	lose := func(e *harness.Env, op blob.Op, prefix string) *atomic.Bool {
		var fired atomic.Bool
		e.Blob.SetHook(func(ctx context.Context, o blob.Op, key string, after bool) error {
			if o == op && after && strings.HasPrefix(key, prefix) && fired.CompareAndSwap(false, true) {
				return errors.New("response lost")
			}
			return nil
		})
		return &fired
	}
	ctx := context.Background()

	t.Run("manifest create", func(t *testing.T) {
		e := harness.New(t, 10*time.Millisecond)
		fired := lose(e, blob.OpCreate, "manifests/")
		h, created, err := e.Files.Open(ctx, shard)
		if err != nil || !created || !fired.Load() {
			t.Fatalf("created=%v fired=%v err=%v", created, fired.Load(), err)
		}
		e.Blob.SetHook(nil)
		if err := h.Touch(ctx); err != nil {
			t.Fatalf("the ETag adopted after the lost response is wrong: %v", err)
		}
	})

	t.Run("manifest update", func(t *testing.T) {
		e := harness.New(t, 10*time.Millisecond)
		ctx := context.Background()
		app := e.StartApp(shard)
		h, sh := e.NewShard(shard)
		write(t, e, app, func() error { return sh.Sync(ctx) }, 0)
		fired := lose(e, blob.OpUpdate, "manifests/")
		if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil || !fired.Load() {
			t.Fatalf("fired=%v err=%v", fired.Load(), err)
		}
		e.Blob.SetHook(nil)
		if len(h.Manifest.Files) != 1 || len(sh.Tail()) != 0 {
			t.Fatalf("files %v, tail %d", h.Manifest.Files, len(sh.Tail()))
		}
		if err := h.Touch(ctx); err != nil { // the ETag kept is current
			t.Fatal(err)
		}
		sh.Close(ctx)
	})

	t.Run("conflict is not mistaken for our own write", func(t *testing.T) {
		e := harness.New(t, 10*time.Millisecond)
		h, _, _ := e.Files.Open(ctx, shard)
		other, _, _ := e.Files.Open(ctx, shard) // somebody else holding the same manifest
		if err := other.Touch(ctx); err != nil {
			t.Fatal(err)
		}
		if err := h.Touch(ctx); !errors.Is(err, blob.ErrConflict) {
			t.Fatalf("got %v, want ErrConflict", err)
		}
	})

	t.Run("ltx file create", func(t *testing.T) {
		e := harness.New(t, 10*time.Millisecond)
		app := e.StartApp(shard)
		h, sh := e.NewShard(shard)
		write(t, e, app, func() error { return sh.Sync(ctx) }, 0)
		fired := lose(e, blob.OpCreate, "shards/")
		if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil || !fired.Load() {
			t.Fatalf("fired=%v err=%v", fired.Load(), err)
		}
		e.Blob.SetHook(nil)
		sh.Close(ctx)
	})

	t.Run("a different object under the name is a conflict", func(t *testing.T) {
		e := harness.New(t, 10*time.Millisecond)
		app := e.StartApp(shard)
		h, sh := e.NewShard(shard)
		write(t, e, app, func() error { return sh.Sync(ctx) }, 0)
		tail := sh.Tail()
		key := shardfile.FileKey(shard, h.Manifest.Generation, shardfile.File{Range: shardfile.Range{Min: tail[0].Position, Max: tail[len(tail)-1].Last()}, Epoch: h.F.Blob.Epoch()})
		e.Store.Create(ctx, key, strings.NewReader("someone else's bytes"))
		if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); !errors.Is(err, blob.ErrExists) {
			t.Fatalf("got %v, want blob.ErrExists", err)
		}
		sh.Close(ctx)
	})

	t.Run("delete", func(t *testing.T) {
		e := harness.New(t, 10*time.Millisecond)
		app := e.StartApp(shard)
		h, sh := e.NewShard(shard)
		for i := range 3 {
			write(t, e, app, func() error { return sh.Sync(ctx) }, i)
			if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
				t.Fatal(err)
			}
		}
		fired := lose(e, blob.OpDelete, "shards/")
		if _, err := h.Rebase(ctx, sh, shardfile.Hooks{}); err != nil || !fired.Load() {
			t.Fatalf("fired=%v err=%v", fired.Load(), err)
		}
		e.Blob.SetHook(nil)
		n := 0
		for range e.Store.List(ctx, "shards/"+shard+"/") {
			n++
		}
		if n != 1 {
			t.Fatalf("%d files left, want 1", n)
		}
		sh.Close(ctx)
	})
}

// A manifest update that lands while every answer, and every read-back, is lost
// leaves the caller not knowing. The handle must not keep the old version: the
// next delta, rebase or release would fail with a conflict for good (a 412 at
// shutdown, with the shard kept).
func TestManifestUpdateWithAnUnknownOutcomeDoesNotLeaveAStaleHandle(t *testing.T) {
	t.Parallel()
	e := harness.New(t, 10*time.Millisecond)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	write(t, e, app, func() error { return sh.Sync(ctx) }, 0)
	var down atomic.Bool
	down.Store(true)
	e.Blob.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if down.Load() && strings.HasPrefix(key, "manifests/") {
			if op == blob.OpUpdate && after {
				return errors.New("response lost") // it happened
			}
			if op == blob.OpHead || op == blob.OpGet {
				return errors.New("storage slow") // and nothing can be read back
			}
		}
		return nil
	})
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err == nil {
		t.Fatal("want an error: the outcome was unknown")
	}
	down.Store(false)
	// Storage is back. The delta is in the manifest, whatever the first call said.
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatalf("the next delta: %v", err)
	}
	if err := h.ReleaseOwnership(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	if got := restoreDump(t, e, "unknown"); got == "" {
		t.Fatal("nothing restored")
	}
	sh.Close(ctx)
}

// The same when the caller's context ends during the update, as at shutdown: the
// write lands, and the next call (with a live context) must still work.
func TestManifestUpdateCancelledAfterLandingDoesNotLeaveAStaleHandle(t *testing.T) {
	t.Parallel()
	e := harness.New(t, 10*time.Millisecond)
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	write(t, e, app, func() error { return sh.Sync(context.Background()) }, 0)
	ctx, cancel := context.WithCancel(context.Background())
	e.Blob.SetHook(func(c context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpUpdate && after && strings.HasPrefix(key, "manifests/") {
			cancel() // the update has happened; the caller is told to stop
			return context.Canceled
		}
		if ctx.Err() != nil && (op == blob.OpHead || op == blob.OpGet) {
			return ctx.Err() // and can't look
		}
		return nil
	})
	// blob.Store reads back on a context of its own, so the landed write is
	// reported as the success it is (an error would be fine too: the next call
	// must work either way).
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	e.Blob.SetHook(nil)
	if err := h.WriteDelta(context.Background(), sh, shardfile.Hooks{}); err != nil {
		t.Fatalf("the next delta: %v", err)
	}
	if err := h.ReleaseOwnership(context.Background()); err != nil {
		t.Fatalf("release: %v", err)
	}
	sh.Close(context.Background())
}
