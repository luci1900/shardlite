package capture_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/segment"
)

// logFiles prints every file the shard's client has passed to the writer, as
// segment entries (one per WriteLTXFile call).
func logFiles(t *testing.T, e *env, label string) {
	t.Helper()
	entries, seqs := e.entriesFor(t, "users/1")
	for i, en := range entries {
		b, err := segment.ReadEntry(context.Background(), e.store, "n1", seqs[i], en)
		if err != nil {
			t.Fatal(err)
		}
		h, _, err := ltx.PeekHeader(bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: file %d: position %d, txid %d-%d, commit %d pages, %d bytes", label, i, en.Position, h.MinTXID, h.MaxTXID, h.Commit, len(b))
	}
}

// Which files does a new shard produce from activation to its first write?
func TestFilesFromActivationToFirstWrite(t *testing.T) {
	e := newEnv(t, 5*time.Millisecond)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "open", "a", path) // the app's own tables: its migration
	sh := newShard(t, e, path)
	ctx := context.Background()
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	logFiles(t, e, "after the first sync")
	t.Logf("litestream's tables: %s", apptest.Query(t, path, `SELECT name FROM sqlite_master WHERE name LIKE '_litestream%'`))
	app.Must(t, "insert", "a", "1", "100")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	logFiles(t, e, "after the first write")
	app.Must(t, "insert", "a", "1", "100")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	logFiles(t, e, "after the second write")
	sh.Close(ctx)
}

// A sync made after Litestream's checkpoint interval has passed since the last
// write to the database makes two files: the commits, and then the checkpoint
// it runs at the end of the sync followed by a write to _litestream_seq
// (db.go, checkpointIfNeeded and bumpLitestreamSeq). Both are uploaded by the
// same Replica.Sync.
func TestTimeBasedCheckpointMakesASecondFile(t *testing.T) {
	e := newEnv(t, 5*time.Millisecond)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "open", "a", path)
	sh, err := capture.New(capture.Config{
		Name: "users/1", Path: path, Generation: "g1", MetaRoot: e.meta, Writer: e.w,
		Tune: func(db *litestream.DB) { db.CheckpointInterval = 100 * time.Millisecond },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	app.Must(t, "insert", "a", "1", "100")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	logFiles(t, e, "first sync")
	time.Sleep(300 * time.Millisecond) // longer than the checkpoint interval since the last write
	app.Must(t, "insert", "a", "1", "100")
	before := sh.Position()
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	logFiles(t, e, "sync after the interval")
	if got := sh.Position() - before; got != 2 {
		t.Fatalf("the sync added %d commits to the shard's position, want 2 (the write and the checkpoint's bookkeeping)", got)
	}
	sh.Close(ctx)
}
