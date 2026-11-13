package capture_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/test/apptest"
	"github.com/luci1900/shardlite/internal/test/harness"
	"github.com/luci1900/shardlite/internal/test/sqlitetest"
)

// stored builds a shard of about mb megabytes in blob storage (one commit per
// megabyte, then a delta), closes it, and returns what activating it from blob
// storage needs: the restored file's path is the harness's path for the shard.
func stored(t *testing.T, e *harness.Env, mb int) *shardfile.Restored {
	t.Helper()
	ctx := context.Background()
	app := e.StartApp(name)
	h, sh := e.NewShard(name)
	for range mb {
		app.Must(t, "insert", "a", "1", "1000000")
		if err := sh.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	app.Must(t, "close", "a")
	if err := sh.Close(ctx); err != nil {
		t.Fatal(err)
	}
	app.Exit()
	for _, p := range []string{"", "-wal", "-shm"} {
		os.Remove(e.Path(name) + p)
	}
	return restore(t, e)
}

// restore restores the shard from blob storage to its path.
func restore(t testing.TB, e *harness.Env) *shardfile.Restored {
	t.Helper()
	res, err := e.Files.Restore(context.Background(), shardName(name), e.Path(name), shardfile.RestoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// readBytes is how many bytes this process has read with read system calls (the
// page cache counts), or -1 where that can't be told.
func readBytes() int64 {
	b, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(b), "\n") {
		if v, ok := strings.CutPrefix(line, "rchar: "); ok {
			n, _ := strconv.ParseInt(v, 10, 64)
			return n
		}
	}
	return -1
}

// Activating a restored shard reads a few pages of its database, however large
// it is: no snapshot is taken of it. (Where the process's reads can't be counted
// the test checks only that no snapshot was written.)
func TestActivatingARestoredShardReadsNoPages(t *testing.T) {
	t.Parallel()
	mb := 40
	if testing.Short() {
		mb = 8
	}
	e := harness.New(t)
	res := stored(t, e, mb)
	ctx := context.Background()

	before := readBytes()
	start := time.Now()
	sh := e.Activate(res.Handle, res.Capture)
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)
	read := readBytes() - before
	defer sh.Close(ctx)

	fi, _ := os.Stat(e.Path(name))
	t.Logf("activated a %d MB shard in %v, reading %d bytes", fi.Size()>>20, took, read)
	if n, _ := sh.TailInfo(); n != 0 {
		t.Fatalf("activation put %d files in the tail", n)
	}
	if runtime.GOOS == "linux" {
		if read < 0 {
			t.Fatal("cannot read /proc/self/io")
		}
		// The file itself is mb megabytes. A few pages of it, SQLite's own reading of
		// its schema, and the files of the process's start are all well under that.
		if limit := int64(fi.Size() / 20); read > limit {
			t.Fatalf("activation read %d bytes of a %d byte shard (limit %d)", read, fi.Size(), limit)
		}
	}
}

// replayInto applies a shard's whole history onto path: the entries of each of
// its activations, in order.
func replayInto(t testing.TB, e *harness.Env, path string) {
	t.Helper()
	ap, err := shardfile.NewApplier(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var last pos.Position
	err = journal.Scan(ctx, e.Journal, 1, 0, func(seq pos.Seg, en journal.Entry) error {
		if en.Shard.String() != name || en.Last() <= last {
			return nil
		}
		b, err := harness.ReadEntry(ctx, e.Journal, seq, en)
		if err != nil {
			return err
		}
		last = en.Last()
		return ap.Apply(bytes.NewReader(b), shardfile.Range{Min: en.Position, Max: en.Last()})
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ap.Finish(); err != nil {
		t.Fatal(err)
	}
}

// Every way the shard's WAL can go after a restored activation leaves a history
// that restores to the database the app has: the read rule is a claim about
// the WAL, and these are the ways it can be tried.
func TestRestoredShardCapturePaths(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		run  func(t *testing.T, app *apptest.App, sh *capture.Shard, ctx context.Context)
	}{
		{"commits", func(t *testing.T, app *apptest.App, sh *capture.Shard, ctx context.Context) {
			for range 5 {
				app.Must(t, "insert", "a", "2", "300")
				if err := sh.Sync(ctx); err != nil {
					t.Fatal(err)
				}
			}
		}},
		{"checkpoint before the first commit", func(t *testing.T, app *apptest.App, sh *capture.Shard, ctx context.Context) {
			app.Must(t, "ckpt", "a", "TRUNCATE")
			app.Must(t, "insert", "a", "2", "300")
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			app.Must(t, "insert", "a", "2", "300")
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
		}},
		{"WAL restart straight after activation", func(t *testing.T, app *apptest.App, sh *capture.Shard, ctx context.Context) {
			app.Must(t, "ckpt", "a", "RESTART")
			app.Must(t, "insert", "a", "2", "300")
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
		}},
		{"external checkpoints between commits", func(t *testing.T, app *apptest.App, sh *capture.Shard, ctx context.Context) {
			for i := range 6 {
				app.Must(t, "insert", "a", "2", "300")
				if i%2 == 0 {
					app.Must(t, "ckpt", "a", "PASSIVE")
				}
				if err := sh.Sync(ctx); err != nil {
					t.Fatal(err)
				}
				if i == 3 {
					app.Must(t, "ckpt", "a", "TRUNCATE")
				}
			}
		}},
		{"commits that pile up unsynced, with checkpoints", func(t *testing.T, app *apptest.App, sh *capture.Shard, ctx context.Context) {
			for range 4 {
				app.Must(t, "insert", "a", "1", "300")
				app.Must(t, "ckpt", "a", "PASSIVE")
				app.Must(t, "insert", "a", "1", "300")
			}
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := harness.New(t)
			res := stored(t, e, 3)
			ctx := context.Background()
			sh := e.Activate(res.Handle, res.Capture)
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			// A short busy timeout: a TRUNCATE checkpoint waits for the node's reader.
			app := apptest.Start(t)
			app.Must(t, "create", "a", e.Path(name), "200")
			tc.run(t, app, sh, ctx)
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			// The history in the journal (before the delta) plus this activation's.
			replayed := filepath.Join(t.TempDir(), "replayed.db")
			replayInto(t, e, replayed)
			want, got := apptest.Dump(t, e.Path(name)), sqlitetest.Dump(t, replayed)
			if want != got {
				t.Fatalf("the history does not restore to the app's database (%d and %d bytes of dump)", len(want), len(got))
			}
			sh.Close(ctx)
		})
	}
}

// A shard that was restored and never written to deactivates without leaving
// anything behind: no files in the tail, the same position.
func TestRestoredShardNeverWritten(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	res := stored(t, e, 2)
	ctx := context.Background()
	sh := e.Activate(res.Handle, res.Capture)
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	released, f, err := sh.Release(ctx, nil, capture.ResumeIfHeld)
	if err != nil || !released {
		t.Fatalf("release: %v, %v", released, err)
	}
	if n, _ := sh.TailInfo(); n != 0 {
		t.Fatalf("a restored shard nobody wrote to has %d tail files", n)
	}
	if got := sh.Position(); got != res.State.Position {
		t.Fatalf("position moved from %d to %d", res.State.Position, got)
	}
	if _, ok := f.(*capture.Deleted); ok {
		t.Fatal("a shard with data in it counts as empty")
	}
}

// A shard whose release did not go through (its delta failed, or the app held the
// file) is captured again on the same file with the WAL the release left, which
// may hold commits that are not in the database file. The history stays whole
// across the two captures.
func TestReactivationFoldsTheWALItLeft(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	res := stored(t, e, 2)
	ctx := context.Background()
	sh := e.Activate(res.Handle, res.Capture)
	app := e.StartApp(name)
	for range 3 {
		app.Must(t, "insert", "a", "2", "400")
		if err := sh.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	app.Must(t, "close", "a")
	released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld)
	if err != nil || !released {
		t.Fatalf("release: %v, %v", released, err)
	}
	if fi, err := os.Stat(e.Path(name) + "-wal"); err != nil || fi.Size() == 0 {
		t.Fatalf("expected a WAL left behind: %v", err)
	}
	// The next activation continues from the position the last one reached.
	sh2 := e.Activate(res.Handle, &capture.Restored{Position: sh.Position(), Tail: sh.TakeTail(), Checksums: sh.Checksums()})
	sh.Discard()
	if err := sh2.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	app2 := e.StartApp(name)
	app2.Must(t, "insert", "a", "2", "400")
	if err := sh2.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	replayed := filepath.Join(t.TempDir(), "replayed.db")
	replayInto(t, e, replayed)
	if want, got := apptest.Dump(t, e.Path(name)), sqlitetest.Dump(t, replayed); want != got {
		t.Fatal("the history across two activations does not restore to the app's database")
	}
	sh2.Close(ctx)
}

// A commit as large as a database is appended to the tail's log like any other,
// reaches the journal intact, and leaves no name in the temporary directory at
// any point (the log never had one).
func TestLargeCommitIsOneFileInTheTailsLog(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		mb   int
	}{{"3 MB", 3}, {"6 MB", 6}} {
		t.Run(tc.name, func(t *testing.T) {
			e := harness.New(t)
			res := stored(t, e, 1)
			ctx := context.Background()
			tmp := e.Files.Temp.Path()
			inTemp := func() int {
				ents, err := os.ReadDir(tmp)
				if err != nil {
					t.Fatal(err)
				}
				return len(ents)
			}
			sh := e.Activate(res.Handle, res.Capture)
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			app := e.StartApp(name)

			// One transaction.
			app.Must(t, "insert", "a", strconv.Itoa(tc.mb), "1000000")
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			tail := tailOf(t, sh)
			if len(tail.Files) == 0 || tail.Files[0].Body.Size() < int64(tc.mb)*1_000_000 {
				t.Fatalf("tail %+v: want a first file of %d MB", tail, tc.mb)
			}
			if sh.TailBytes() < int64(tc.mb)*1_000_000 {
				t.Fatalf("the tail holds %d bytes for a %d MB commit", sh.TailBytes(), tc.mb)
			}
			if got := inTemp(); got != 0 {
				t.Fatalf("%d names in the temporary directory with a %d MB commit in the tail", got, tc.mb)
			}
			// It reaches the journal, becomes a delta and restores to the same database.
			if err := e.W.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := res.Handle.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
				t.Fatal(err)
			}
			if n, _ := sh.TailInfo(); n != 0 || sh.TailBytes() != 0 {
				t.Fatalf("the tail holds %d files and %d bytes after the delta", n, sh.TailBytes())
			}
			// Capture goes on incrementally.
			app.Must(t, "insert", "a", "1", "200")
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			tail = tailOf(t, sh)
			if len(tail.Files) == 0 || tail.Files[0].Body.Size() > 64<<10 {
				t.Fatalf("the commit after the large one made %d files, the first of %d bytes: capture snapshotted", len(tail.Files), tail.Files[0].Body.Size())
			}
			if err := res.Handle.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
				t.Fatal(err)
			}
			app.Must(t, "close", "a")
			if err := sh.Close(ctx); err != nil {
				t.Fatal(err)
			}
			sh.Discard()

			dst := filepath.Join(t.TempDir(), "restored.db")
			if _, err := e.Files.Restore(ctx, shardName(name), dst, shardfile.RestoreOptions{}); err != nil {
				t.Fatal(err)
			}
			if want, got := apptest.Dump(t, e.Path(name)), sqlitetest.Dump(t, dst); want != got {
				t.Fatalf("the restored database differs from the app's (%d and %d bytes of dump)", len(want), len(got))
			}
		})
	}
}

// A release that finds the app still has the shard open reopens capture over
// the same WAL. That works when the tail was dropped (written as a delta)
// before: capture goes on from where it had read to and doesn't look for the
// files that were dropped.
func TestRefusedReleaseAfterTheTailWasDropped(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	res := stored(t, e, 2)
	ctx := context.Background()
	sh := e.Activate(res.Handle, res.Capture)
	app := e.StartApp(name)
	for range 2 {
		app.Must(t, "insert", "a", "2", "300")
		if err := sh.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		if err := res.Handle.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
			t.Fatal(err)
		}
		if n, _ := sh.TailInfo(); n != 0 {
			t.Fatalf("%d files in the tail after a delta", n)
		}
		released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld)
		if err != nil || released {
			t.Fatalf("release with the app holding the shard: %v, %v", released, err)
		}
	}
	app.Must(t, "insert", "a", "2", "300")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "restored.db")
	if _, err := e.Files.Restore(ctx, shardName(name), dst, shardfile.RestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	if want, got := apptest.Dump(t, e.Path(name)), sqlitetest.Dump(t, dst); want != got {
		t.Fatal("the restored database differs from the app's")
	}
	sh.Close(ctx)
}

// The seed works for any page size.
func TestSeedAnyPageSize(t *testing.T) {
	t.Parallel()
	for _, size := range []int{1024, 8192, 65536} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			e := harness.New(t)
			ctx := context.Background()
			// The database is made with the page size before the app's own tables.
			db, err := sql.Open("sqlite", "file:"+e.Path(name)+"?_pragma=busy_timeout(5000)")
			if err != nil {
				t.Fatal(err)
			}
			for _, q := range []string{fmt.Sprintf("PRAGMA page_size=%d", size), "PRAGMA journal_mode=WAL", "CREATE TABLE seedtest (x)"} {
				if _, err := db.Exec(q); err != nil {
					t.Fatal(err)
				}
			}
			db.Close()
			app := e.StartApp(name)
			h, sh := e.NewShard(name)
			for range 3 {
				app.Must(t, "insert", "a", "5", "30000")
				if err := sh.Sync(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
				t.Fatal(err)
			}
			app.Must(t, "close", "a")
			sh.Close(ctx)
			app.Exit()
			for _, p := range []string{"", "-wal", "-shm"} {
				os.Remove(e.Path(name) + p)
			}
			res := restore(t, e)
			if res.State.PageSize != capture.PageSize(size) {
				t.Fatalf("page size %d, want %d", res.State.PageSize, size)
			}
			sh = e.Activate(res.Handle, res.Capture)
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			app = e.StartApp(name)
			app.Must(t, "insert", "a", "2", "30000")
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			if n, _ := sh.TailInfo(); n != 1 {
				t.Fatalf("%d tail files", n)
			}
			tail := tailOf(t, sh)
			if tail.Files[0].Body.Size() > 2*60000+8*int64(size) {
				t.Fatalf("the first commit after the seed made a %d byte file: a snapshot", tail.Files[0].Body.Size())
			}
			sh.Close(ctx)
		})
	}
}

// A restored shard has to bring its checksums: capture does not read the
// database to find them.
func TestRestoredWithoutChecksumsIsRefused(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	res := stored(t, e, 1)
	res.Capture.Checksums = nil
	sh, err := capture.New(capture.Config{Name: res.Handle.Name, Path: e.Path(name), Generation: res.Handle.Manifest.Generation, Writer: e.W, Restored: res.Capture, TempDir: e.Files.Temp})
	if err == nil {
		sh.Discard()
		t.Fatal("New accepted a restored shard without checksums")
	}
}
