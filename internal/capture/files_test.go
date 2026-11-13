package capture_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/test/apptest"
	"github.com/luci1900/shardlite/internal/test/harness"
)

// logFiles prints every file the shard's capture has passed to the writer, as
// segment entries (one per AppendFile call).
func logFiles(t *testing.T, e *env, label string) {
	t.Helper()
	entries, seqs := e.entriesFor(t, "users/1")
	for i, en := range entries {
		b, err := harness.ReadEntry(context.Background(), e.journal, seqs[i], en)
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
	t.Parallel()
	e := newEnv(t)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path) // the app's own tables: its migration
	sh := newShard(t, e, path)
	ctx := context.Background()
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	logFiles(t, e, "after the first sync")
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
