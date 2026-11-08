package capture_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/sqlitetest"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// tailShard is a captured shard with an app that has a table, over a temporary
// directory whose hook the test can set.
func tailShard(t *testing.T) (*capture.Shard, *apptest.App, *env, *tempfile.Dir, string) {
	t.Helper()
	e := newEnv(t)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path)
	dir := tempDir(t)
	sh, err := capture.New(capture.Config{TempDir: dir, Name: shardName("users/1"), Path: path, Generation: "g1", Writer: e.w})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sh.Close(context.Background()) })
	return sh, app, e, dir, path
}

func commit(t *testing.T, sh *capture.Shard, app *apptest.App) {
	t.Helper()
	app.Must(t, "insert", "a", "1", "300")
	if err := sh.Capture(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// A shard holds one descriptor for a tail of many commits, not one for each.
func TestATailOfManyCommitsIsOneLog(t *testing.T) {
	t.Parallel()
	sh, app, _, _, _ := tailShard(t)
	for range 50 {
		commit(t, sh, app)
	}
	tail := tailOf(t, sh)
	if len(tail.Files) < 50 {
		t.Fatalf("%d files for 50 commits", len(tail.Files))
	}
	if len(tail.Logs) != 1 {
		t.Fatalf("%d logs for one run of commits, want 1", len(tail.Logs))
	}
}

// A drop that leaves commits behind cuts the tail: the next commit starts a log
// and the old one is closed once its last file is dropped, so a shard that keeps
// committing holds one or two logs, not a growing one.
func TestADropCutsTheTail(t *testing.T) {
	t.Parallel()
	sh, app, _, _, _ := tailShard(t)
	logs := func() int { return len(tailOf(t, sh).Logs) }
	for range 3 {
		commit(t, sh, app)
	}
	first := tailOf(t, sh).Files
	sh.DropTail(first[0].Last())
	if got := logs(); got != 1 {
		t.Fatalf("%d logs after a drop that left files, want the one they are in", got)
	}
	for range 2 {
		commit(t, sh, app)
	}
	if got := logs(); got != 2 {
		t.Fatalf("%d logs after commits past a drop, want 2 (the cut)", got)
	}
	// The files in the old log go: only the new log is left.
	sh.DropTail(first[2].Last())
	if got := logs(); got != 1 {
		t.Fatalf("%d logs after the old log's files were dropped, want 1", got)
	}
	for range 2 {
		commit(t, sh, app)
	}
	sh.DropTail(sh.Position())
	if got := logs(); got != 0 {
		t.Fatalf("%d logs after everything was dropped", got)
	}
	if n, _ := sh.TailInfo(); n != 0 || sh.TailBytes() != 0 {
		t.Fatalf("%d files and %d bytes in an empty tail", n, sh.TailBytes())
	}
	commit(t, sh, app) // a tail that was emptied starts again
	if got := logs(); got != 1 {
		t.Fatalf("%d logs for a tail that started again", got)
	}
}

// What Tail returns is read through descriptors of its own, so a delta that is
// still reading is not cut off when the tail is dropped under it.
func TestARetainedTailOutlivesDropTail(t *testing.T) {
	t.Parallel()
	sh, app, _, _, _ := tailShard(t)
	for range 3 {
		commit(t, sh, app)
	}
	tail := tailOf(t, sh)
	var want [][]byte
	for _, f := range tail.Files {
		b, err := f.Body.Bytes()
		if err != nil || len(b) == 0 {
			t.Fatalf("%d bytes, %v", len(b), err)
		}
		want = append(want, b)
	}
	sh.DropTail(sh.Position())
	sh.Discard()
	for i, f := range tail.Files {
		if got, err := f.Body.Bytes(); err != nil || !bytes.Equal(got, want[i]) {
			t.Fatalf("file %d after the tail was dropped: %d bytes, %v", i, len(got), err)
		}
	}
}

// TakeTail moves the tail out: the shard holds nothing, and what was taken is
// read through the original logs, which the taker closes.
func TestTakeTailMovesTheTail(t *testing.T) {
	t.Parallel()
	sh, app, _, _, _ := tailShard(t)
	for range 3 {
		commit(t, sh, app)
	}
	pos := sh.Position()
	taken := sh.TakeTail()
	defer taken.Close()
	if n, _ := sh.TailInfo(); n != 0 || sh.TailBytes() != 0 {
		t.Fatalf("the shard holds %d files and %d bytes after TakeTail", n, sh.TailBytes())
	}
	if sh.Position() != pos {
		t.Fatalf("position %d after TakeTail, was %d", sh.Position(), pos)
	}
	if len(taken.Files) < 3 || len(taken.Logs) != 1 {
		t.Fatalf("taken %d files in %d logs", len(taken.Files), len(taken.Logs))
	}
	for i, f := range taken.Files {
		if b, err := f.Body.Bytes(); err != nil || len(b) == 0 {
			t.Fatalf("file %d: %d bytes, %v", i, len(b), err)
		}
	}
	// The shard goes on with a log of its own.
	commit(t, sh, app)
	if got := len(tailOf(t, sh).Logs); got != 1 {
		t.Fatalf("%d logs after a commit past TakeTail", got)
	}
}

// A write that fails fails the sync and loses nothing: the log is broken, the
// retried sync starts a new one, and the journal replays to the app's database.
func TestAFailedAppendFailsTheSyncAndTheNextOneStartsANewLog(t *testing.T) {
	t.Parallel()
	sh, app, e, dir, path := tailShard(t)
	ctx := context.Background()
	commit(t, sh, app)
	boom := errors.New("injected: disk full")
	dir.SetHook(func(op tempfile.Op) error {
		if op == tempfile.OpAppend {
			return boom
		}
		return nil
	})
	app.Must(t, "insert", "a", "1", "300")
	if err := sh.Capture(ctx); !errors.Is(err, boom) {
		t.Fatalf("capture: %v, want the write's error", err)
	}
	dir.SetHook(nil)
	if err := sh.Sync(ctx); err != nil {
		t.Fatalf("the retried sync: %v", err)
	}
	tail := tailOf(t, sh)
	if len(tail.Logs) != 2 {
		t.Fatalf("%d logs after a broken one, want 2", len(tail.Logs))
	}
	for i := 1; i < len(tail.Files); i++ {
		if tail.Files[i].Position != tail.Files[i-1].Last()+1 {
			t.Fatalf("a gap in the tail before position %d", tail.Files[i].Position)
		}
	}
	replayed := e.replayTo(t, "users/1")
	if want, got := apptest.Dump(t, path), sqlitetest.Dump(t, replayed); want != got {
		t.Fatalf("replayed database differs from the app's (%d and %d bytes)", len(want), len(got))
	}
}

// A log that cannot be created fails the sync the same way.
func TestAFailedCreateFailsTheSync(t *testing.T) {
	t.Parallel()
	sh, app, _, dir, _ := tailShard(t)
	boom := errors.New("injected: no space")
	dir.SetHook(func(op tempfile.Op) error {
		if op == tempfile.OpCreate {
			return boom
		}
		return nil
	})
	app.Must(t, "insert", "a", "1", "300")
	if err := sh.Capture(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("capture: %v, want the create's error", err)
	}
	dir.SetHook(nil)
	commit(t, sh, app)
	if got := len(tailOf(t, sh).Logs); got != 1 {
		t.Fatalf("%d logs, want 1", got)
	}
}
