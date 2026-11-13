package shardfile_test

import (
	"context"
	"errors"
	"testing"

	"github.com/luci1900/shardlite/internal/model/pos"
	"github.com/luci1900/shardlite/internal/storage/shardfile"
	"github.com/luci1900/shardlite/internal/test/harness"
)

func storedFiles(t *testing.T, e *harness.Env) []shardfile.File {
	t.Helper()
	m, _, err := e.Files.GetManifest(context.Background(), shardName(shard))
	if err != nil {
		t.Fatal(err)
	}
	return m.Files
}

// BeforeUpdate runs before the file is written and before the manifest changes,
// with the position the files reach, for a delta, MaybeSnapshot's delta and
// rebase, and a rebase.
func TestBeforeUpdateRunsBeforeTheManifestUpdate(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)

	var calls int
	var end pos.Position
	hooks := func(wantFiles int) shardfile.Hooks {
		return shardfile.Hooks{BeforeUpdate: func(_ context.Context, e2 pos.Position) (shardfile.Minute, error) {
			if files := storedFiles(t, e); len(files) != wantFiles {
				t.Errorf("manifest changed before the hook: files %v", files)
			}
			calls++
			end = e2
			return 0, nil
		}}
	}
	ack(t, app, sh, 1)
	if err := h.WriteDelta(ctx, sh, hooks(0)); err != nil {
		t.Fatal(err)
	}
	if last := h.Manifest.LastPosition(); end != last || end != sh.Position() {
		t.Fatalf("the hook was given %d, the manifest reaches %d and the shard is at %d", end, last, sh.Position())
	}
	ack(t, app, sh, 2)
	if err := h.MaybeSnapshot(ctx, sh, 0, shardfile.Policy{}, hooks(1)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if got := len(h.Manifest.Files); got != 2 {
		t.Fatalf("%d files after the snapshot's delta, want 2", got)
	}
	ack(t, app, sh, 3)
	if err := h.MaybeSnapshot(ctx, sh, 0, shardfile.Policy{MaxFiles: 1}, hooks(2)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if got := len(h.Manifest.Files); got != 1 {
		t.Fatalf("%d files after the snapshot's rebase, want 1", got)
	}
	ack(t, app, sh, 4)
	if did, err := h.Rebase(ctx, sh, hooks(1)); !did || err != nil {
		t.Fatalf("rebase: %v %v", did, err)
	}
	if calls != 4 || end != sh.Position() {
		t.Fatalf("%d calls, the last at %d (the shard is at %d)", calls, end, sh.Position())
	}
}

// An error from BeforeUpdate fails the operation and leaves the manifest, the
// handle and the tail as they were.
func TestBeforeUpdateErrorLeavesTheManifest(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)
	ack(t, app, sh, 1)
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	etag := h.ETag

	boom := errors.New("row record failed")
	failing := shardfile.Hooks{BeforeUpdate: func(context.Context, pos.Position) (shardfile.Minute, error) { return 0, boom }}
	ack(t, app, sh, 2)
	if err := h.WriteDelta(ctx, sh, failing); !errors.Is(err, boom) {
		t.Fatalf("delta: %v", err)
	}
	ack(t, app, sh, 3)
	if _, err := h.Rebase(ctx, sh, failing); !errors.Is(err, boom) {
		t.Fatalf("rebase: %v", err)
	}
	if files := storedFiles(t, e); len(files) != 1 {
		t.Fatalf("manifest changed: files %v", files)
	}
	if h.ETag != etag || len(h.Manifest.Files) != 1 || len(tailOf(t, sh).Files) == 0 {
		t.Fatal("handle or tail changed by a failed hook")
	}
	// The same tail goes through once the hook works.
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	if files := storedFiles(t, e); len(files) != 2 {
		t.Fatalf("retry: files %v", files)
	}
}
