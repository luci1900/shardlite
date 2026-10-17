package shardfile_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/shardfile"
)

func wakeHook(w int64) shardfile.Hooks {
	return shardfile.Hooks{Wake: func(context.Context, uint64) (int64, error) { return w, nil }}
}

func storedWake(t *testing.T, e *harness.Env) (int64, []string) {
	t.Helper()
	m, _, err := e.Files.GetManifest(context.Background(), shard)
	if err != nil {
		t.Fatal(err)
	}
	return m.Wake, m.Files
}

// A delta, a rebase and a nil hook: the wake time lands in the update that adds the
// files, a nil hook keeps the previous one.
func TestDeltaAndRebaseWriteTheWakeTimeWithTheirFiles(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)

	// The hook runs after the file is written and before the manifest changes.
	// It is given the position the files reach.
	ack(t, app, sh, 1)
	var end uint64
	hooks := shardfile.Hooks{Wake: func(_ context.Context, e2 uint64) (int64, error) {
		if w, files := storedWake(t, e); w != 0 || len(files) != 0 {
			t.Errorf("manifest changed before the hook: wake %d files %v", w, files)
		}
		end = e2
		return 100, nil
	}}
	if err := h.WriteDelta(ctx, sh, hooks); err != nil {
		t.Fatal(err)
	}
	if w, files := storedWake(t, e); w != 100 || len(files) != 1 {
		t.Fatalf("after the delta: wake %d files %v", w, files)
	}
	if last := h.Manifest.LastPosition(); end != last || end != sh.Position() {
		t.Fatalf("the hook was given %d, the manifest reaches %d and the shard is at %d", end, last, sh.Position())
	}

	// A nil hook leaves it alone.
	ack(t, app, sh, 2)
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	if w, files := storedWake(t, e); w != 100 || len(files) != 2 {
		t.Fatalf("after a delta with no hook: wake %d files %v", w, files)
	}

	// The delta of MaybeSnapshot.
	ack(t, app, sh, 3)
	if got, err := h.MaybeSnapshot(ctx, sh, 0, shardfile.Policy{}, wakeHook(200)); got != shardfile.WroteDelta || err != nil {
		t.Fatalf("snapshot: %v %v", got, err)
	}
	if w, files := storedWake(t, e); w != 200 || len(files) != 3 {
		t.Fatalf("after MaybeSnapshot's delta: wake %d files %v", w, files)
	}

	// Its rebase.
	ack(t, app, sh, 4)
	if got, err := h.MaybeSnapshot(ctx, sh, 0, shardfile.Policy{MaxFiles: 2}, wakeHook(300)); got != shardfile.WroteRebase || err != nil {
		t.Fatalf("snapshot: %v %v", got, err)
	}
	if w, files := storedWake(t, e); w != 300 || len(files) != 1 {
		t.Fatalf("after MaybeSnapshot's rebase: wake %d files %v", w, files)
	}

	// Rebase itself, with a nil hook and then a hook that gives 0 (no wake).
	ack(t, app, sh, 5)
	if did, err := h.Rebase(ctx, sh, shardfile.Hooks{}); !did || err != nil {
		t.Fatalf("rebase: %v %v", did, err)
	}
	if w, _ := storedWake(t, e); w != 300 {
		t.Fatalf("a rebase with no hook changed the wake time to %d", w)
	}
	ack(t, app, sh, 6)
	if did, err := h.Rebase(ctx, sh, wakeHook(0)); !did || err != nil {
		t.Fatalf("rebase: %v %v", did, err)
	}
	if w, _ := storedWake(t, e); w != 0 {
		t.Fatalf("a hook giving 0 left the wake time at %d", w)
	}
}

// An error from the hook fails the operation and leaves the manifest, the handle
// and the tail as they were.
func TestWakeHookErrorLeavesTheManifest(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)
	ack(t, app, sh, 1)
	if err := h.WriteDelta(ctx, sh, wakeHook(100)); err != nil {
		t.Fatal(err)
	}
	etag := h.ETag

	boom := errors.New("hint write failed")
	failing := shardfile.Hooks{Wake: func(context.Context, uint64) (int64, error) { return 999, boom }}
	ack(t, app, sh, 2)
	if err := h.WriteDelta(ctx, sh, failing); !errors.Is(err, boom) {
		t.Fatalf("delta: %v", err)
	}
	ack(t, app, sh, 3)
	if _, err := h.Rebase(ctx, sh, failing); !errors.Is(err, boom) {
		t.Fatalf("rebase: %v", err)
	}
	if w, files := storedWake(t, e); w != 100 || len(files) != 1 {
		t.Fatalf("manifest changed: wake %d files %v", w, files)
	}
	if h.ETag != etag || h.Manifest.Wake != 100 || len(h.Manifest.Files) != 1 || len(sh.Tail()) == 0 {
		t.Fatal("handle or tail changed by a failed hook")
	}
	// The same tail goes through once the hook works.
	if err := h.WriteDelta(ctx, sh, wakeHook(150)); err != nil {
		t.Fatal(err)
	}
	if w, files := storedWake(t, e); w != 150 || len(files) != 2 {
		t.Fatalf("retry: wake %d files %v", w, files)
	}
}

func TestSetWake(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	e.StartApp(shard)
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)

	if err := h.SetWake(ctx, 0); err != nil {
		t.Fatal(err)
	}
	etag := h.ETag
	if err := h.SetWake(ctx, 500); err != nil {
		t.Fatal(err)
	}
	if h.ETag == etag {
		t.Fatal("a changed wake time wrote nothing")
	}
	if w, _ := storedWake(t, e); w != 500 || h.Manifest.Owner != e.Node {
		t.Fatalf("wake %d, manifest %+v", w, h.Manifest)
	}
	etag = h.ETag
	if err := h.SetWake(ctx, 500); err != nil {
		t.Fatal(err)
	}
	if h.ETag != etag {
		t.Fatal("an unchanged wake time wrote the manifest")
	}
	if m, tag, err := e.Files.GetManifest(ctx, shard); err != nil || tag != etag || m.Wake != 500 {
		t.Fatalf("stored %+v %v %v", m, tag, err)
	}
	if err := h.SetWake(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if w, _ := storedWake(t, e); w != 0 {
		t.Fatalf("wake %d after clearing", w)
	}
}

// The adopter can't read a shard's rows, so its delta sets the wake time to now in
// the update that clears the journal.
func TestAdoptersDeltaSetsTheWakeTimeToNow(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)
	if err := h.SetWake(ctx, 5); err != nil {
		t.Fatal(err)
	}
	write(t, e, app, func() error { return sh.Sync(ctx) }, 0)

	before := time.Now().Unix()
	rep, err := e.Files.AdoptJournal(ctx, e.Node, 1, e.W.NextSeq(), func(context.Context, string) error { return nil }, nil)
	if err != nil || rep.Deltas != 1 {
		t.Fatalf("adoption: %+v %v", rep, err)
	}
	after := time.Now().Unix()
	m, _, err := e.Files.GetManifest(ctx, shard)
	if err != nil {
		t.Fatal(err)
	}
	if m.Journal != "" || len(m.Files) != 1 || m.Wake < before || m.Wake > after {
		t.Fatalf("manifest %+v, want wake in [%d, %d] with the journal cleared in the same update", m, before, after)
	}
}
