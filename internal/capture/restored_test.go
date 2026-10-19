package capture_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
)

// dirBytes is the size of every file under dir.
func dirBytes(t *testing.T, dir string) (total int64, files []string) {
	t.Helper()
	filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if fi, err := d.Info(); err == nil {
			total += fi.Size()
			rel, _ := filepath.Rel(dir, p)
			files = append(files, fmt.Sprintf("%s(%d)", rel, fi.Size()))
		}
		return nil
	})
	return total, files
}

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
func restore(t *testing.T, e *harness.Env) *shardfile.Restored {
	t.Helper()
	res, err := e.Files.Restore(context.Background(), name, e.Path(name), shardfile.RestoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// metaDir is the metadata directory of the shard under test.
func metaDir(e *harness.Env, res *shardfile.Restored) string {
	return capture.MetaDir(e.Meta, name, res.Handle.Manifest.Generation)
}

// What does the metadata directory of a restored shard hold? Before stage 1z,
// Litestream's first sync wrote a snapshot the size of the database there, which
// DropTail never deleted, so it stayed for the whole activation.
func TestRestoredShardMetadataStaysSmall(t *testing.T) {
	t.Parallel()
	const mb = 20
	e := harness.New(t)
	res := stored(t, e, mb)
	fi, err := os.Stat(e.Path(name))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("restored: %d bytes, position %d", fi.Size(), res.State.Position)

	ctx := context.Background()
	sh := e.Activate(res.Handle, res.Capture)
	defer sh.Close(ctx)
	meta := metaDir(e, res)
	check := func(step string) {
		t.Helper()
		n, files := dirBytes(t, meta)
		t.Logf("%-18s metadata %6d bytes in %v", step, n, files)
		if n > 256<<10 {
			t.Fatalf("%s: the metadata directory holds %d bytes for a %d byte shard", step, n, fi.Size())
		}
	}
	check("created")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	check("first sync")
	if n, _ := sh.TailInfo(); n != 0 {
		t.Fatalf("the first sync of a restored shard put %d files in the tail", n)
	}
	app := e.StartApp(name)
	for i := range 4 {
		app.Must(t, "insert", "a", "2", "200")
		if err := sh.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		check("commit " + strconv.Itoa(i+1))
		sh.DropTail(sh.Position())
		check("  after DropTail")
		if _, files := dirBytes(t, meta); len(files) > 1 {
			t.Fatalf("after DropTail: %v, want only the newest file", files)
		}
	}
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
	if n, _ := dirBytes(t, metaDir(e, res)); n > 64<<10 {
		t.Fatalf("activation wrote %d bytes of metadata: a snapshot was taken", n)
	}
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
func replayInto(t *testing.T, e *harness.Env, path string) {
	t.Helper()
	ap, err := shardfile.NewApplier(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var last uint64
	err = segment.Scan(ctx, e.Store, e.Node, 1, 0, func(seq uint64, en segment.Entry) error {
		if en.Shard != name || en.Last() <= last {
			return nil
		}
		b, err := harness.ReadEntry(ctx, e.Store, e.Node, seq, en)
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
// that restores to the database the app has. The seeded start (see seed.go) is a
// claim about the WAL, and these are the ways it can stop being true.
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
			// A short busy timeout: a TRUNCATE checkpoint waits for Litestream's reader.
			app := apptest.Start(t)
			app.Must(t, "open", "a", e.Path(name), "200")
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
	released, f, err := sh.Release(ctx, nil)
	if err != nil || !released {
		t.Fatalf("release: %v, %v", released, err)
	}
	if n, _ := sh.TailInfo(); n != 0 {
		t.Fatalf("a restored shard nobody wrote to has %d tail files", n)
	}
	if got := sh.Position(); got != res.State.Position {
		t.Fatalf("position moved from %d to %d", res.State.Position, got)
	}
	if f.Empty {
		t.Fatal("a shard with data in it counts as empty")
	}
}

// A cached shard is activated again with the WAL its last activation left, which
// may hold commits that are not in the database file. The history stays whole
// across activations.
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
	released, _, err := sh.Release(ctx, nil)
	if err != nil || !released {
		t.Fatalf("release: %v, %v", released, err)
	}
	if fi, err := os.Stat(e.Path(name) + "-wal"); err != nil || fi.Size() == 0 {
		t.Fatalf("expected a WAL left behind: %v", err)
	}
	// The next activation continues from the position the last one reached.
	sh2 := e.Activate(res.Handle, &capture.Restored{Position: sh.Position(), Tail: sh.Tail()})
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

// The seeded start relies on parts of Litestream that are not its API. These
// tests fail if an upgrade changes any of them (the list is in seed.go and the
// stage 1z report).
func TestSeedRelianceOnLitestream(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	res := stored(t, e, 2)
	ctx := context.Background()
	var logs bytes.Buffer
	sh, err := capture.New(capture.Config{
		Name: name, Path: e.Path(name), Generation: res.Handle.Manifest.Generation, MetaRoot: e.Meta, Writer: e.W, Restored: res.Capture,
		TempDir: e.Files.TempDir,
		Logger:  slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sh.Close(ctx)
	db := sh.DB()

	// The seed is where Litestream looks for a level 0 file: one commit number,
	// min and max alike, under the level's directory in the metadata directory.
	seedPath := db.LTXPath(0, 2, 2)
	if want := filepath.Join(metaDir(e, res), "ltx", "0", "0000000000000002-0000000000000002.ltx"); seedPath != want {
		t.Fatalf("Litestream's layout changed: %s, want %s", seedPath, want)
	}
	f, err := os.Open(seedPath)
	if err != nil {
		t.Fatal(err)
	}
	dec := ltx.NewDecoder(f)
	if err := dec.Verify(); err != nil { // what DB.Pos does
		t.Fatalf("the seed does not verify: %v", err)
	}
	hdr := dec.Header()
	f.Close()
	if err := hdr.Validate(); err != nil {
		t.Fatalf("the seed's header is not valid: %v", err)
	}
	if hdr.WALOffset != 32 || hdr.WALSize == 0 || hdr.PageSize == 0 {
		t.Fatalf("unexpected seed header %+v", hdr)
	}

	// Litestream takes the seed as its position, and its first sync writes nothing.
	pos, err := db.Pos()
	if err != nil || pos.TXID != 2 {
		t.Fatalf("Litestream's position is %v, %v, want commit 2", pos, err)
	}
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got := dirList(t, metaDir(e, res)); len(got) != 1 {
		t.Fatalf("the first sync wrote %v", got)
	}
	if strings.Contains(logs.String(), "snapshotting") || strings.Contains(logs.String(), "snap=true") {
		t.Fatalf("Litestream snapshotted at its first sync:\n%s", logs.String())
	}

	// The next file takes the next commit number, and continues without a snapshot,
	// also after the app has opened the file and written to it.
	app := e.StartApp(name)
	for i := range 3 {
		app.Must(t, "insert", "a", "1", "100")
		if err := sh.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		next := db.LTXPath(0, ltx.TXID(3+i), ltx.TXID(3+i))
		fi, err := os.Stat(next)
		if err != nil {
			t.Fatalf("after commit %d: %v (files %v)", i+1, err, dirList(t, metaDir(e, res)))
		}
		if fi.Size() > 64<<10 {
			t.Fatalf("commit %d made a %d byte file: a snapshot", i+1, fi.Size())
		}
	}
	if strings.Contains(logs.String(), "snap=true") {
		t.Fatalf("Litestream snapshotted after the seed:\n%s", logs.String())
	}
	// The WAL still has the salts the seed named: the app's writes went after the
	// seed's frame, and SQLite didn't start a new WAL.
	walHdr := make([]byte, 32)
	w, err := os.Open(e.Path(name) + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	w.ReadAt(walHdr, 0)
	w.Close()
	if s1 := uint32(walHdr[16])<<24 | uint32(walHdr[17])<<16 | uint32(walHdr[18])<<8 | uint32(walHdr[19]); s1 != hdr.WALSalt1 {
		t.Fatalf("SQLite started a new WAL: salt %x, the seed names %x", s1, hdr.WALSalt1)
	}
}

// A commit larger than the spool limit is held in a temporary file in the
// temporary directory (also what the segment writer builds from it), reaches the
// journal intact, and the file is gone when the tail is dropped.
//
// Litestream keeps its newest level 0 file, which for a commit of this size is as
// large as the commit and sits in the scratch directory. Litestream checkpoints
// and writes a small file after a commit of a thousand pages, which then is the
// newest; a smaller commit is its own newest file, and is shrunk once its delta
// is written.
func TestLargeCommitSpillsAndLeavesNothingBehind(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		mb   int
	}{{"3 MB, below Litestream's checkpoint threshold", 3}, {"6 MB", 6}} {
		t.Run(tc.name, func(t *testing.T) {
			e := harness.New(t)
			res := stored(t, e, 1)
			ctx := context.Background()
			e.Files.SpoolLimit = 64 << 10
			tmp := e.Files.TempDir
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
			if got := inTemp(); got != 0 {
				t.Fatalf("%d temporary files before any commit", got)
			}
			// While the segment is uploaded there is the commit's file and the
			// segment's own, both in temporary files.
			var atUpload atomic.Int32
			e.Blob.SetHook(func(_ context.Context, op blob.Op, key string, after bool) error {
				if op == blob.OpCreate && !after && strings.HasPrefix(key, segment.Root) {
					// The most there were at any upload: a later, smaller segment (the writer
					// cuts one every few milliseconds) finds the commit's file already gone.
					n := int32(inTemp())
					for old := atUpload.Load(); n > old && !atUpload.CompareAndSwap(old, n); old = atUpload.Load() {
					}
				}
				return nil
			})

			// One transaction.
			app.Must(t, "insert", "a", strconv.Itoa(tc.mb), "1000000")
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			tail := sh.Tail()
			if len(tail) == 0 || !tail[0].Body.InFile() || tail[0].Body.Size() < int64(tc.mb)*1_000_000 {
				t.Fatalf("tail %+v: want a first file of %d MB in a temporary file", tail, tc.mb)
			}
			capture.CloseTail(tail)
			if inTemp() == 0 {
				t.Fatalf("no temporary file for a %d MB commit", tc.mb)
			}
			// The large file is in the scratch directory too, until the delta.
			if n, _ := dirBytes(t, metaDir(e, res)); n < int64(tc.mb)*1_000_000 {
				t.Fatalf("Litestream's own copy of the commit is only %d bytes", n)
			}

			// It reaches the journal, becomes a delta and restores to the same database.
			if err := e.W.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if tc.mb*1_000_000 > 4<<20 && atUpload.Load() < 2 {
				t.Fatalf("%d temporary files while a segment of over %d MB was uploaded, want the commit's and the segment's", atUpload.Load(), tc.mb)
			}
			if err := res.Handle.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
				t.Fatal(err)
			}
			if got := inTemp(); got != 0 {
				ents, _ := os.ReadDir(tmp)
				t.Fatalf("%d temporary files after the delta: %v", got, ents)
			}
			n, files := dirBytes(t, metaDir(e, res))
			t.Logf("after the delta the metadata directory holds %d bytes: %v", n, files)
			if n > 256<<10 {
				t.Fatalf("after the delta the metadata directory holds %d bytes: %v", n, files)
			}
			// Litestream goes on from what is left of it, incrementally.
			app.Must(t, "insert", "a", "1", "200")
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			tail = sh.Tail()
			if len(tail) == 0 || tail[0].Body.Size() > 64<<10 {
				t.Fatalf("the commit after the large one made %d files, the first of %d bytes: Litestream snapshotted", len(tail), tail[0].Body.Size())
			}
			capture.CloseTail(tail)
			if err := res.Handle.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
				t.Fatal(err)
			}
			app.Must(t, "close", "a")
			if err := sh.Close(ctx); err != nil {
				t.Fatal(err)
			}
			sh.Discard()

			dst := filepath.Join(t.TempDir(), "restored.db")
			if _, err := e.Files.Restore(ctx, name, dst, shardfile.RestoreOptions{}); err != nil {
				t.Fatal(err)
			}
			if want, got := apptest.Dump(t, e.Path(name)), sqlitetest.Dump(t, dst); want != got {
				t.Fatalf("the restored database differs from the app's (%d and %d bytes of dump)", len(want), len(got))
			}
			if got := inTemp(); got != 0 {
				t.Fatalf("%d temporary files after the restore", got)
			}
		})
	}
}

// A release that finds the app still has the shard open reopens capture over the
// same metadata directory. That works when the tail was dropped (written as a
// delta) before: Litestream goes on from its newest file and doesn't look for
// the ones that were deleted.
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
		released, _, err := sh.Release(ctx, nil)
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
	if _, err := e.Files.Restore(ctx, name, dst, shardfile.RestoreOptions{}); err != nil {
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
			if res.State.PageSize != uint32(size) {
				t.Fatalf("page size %d, want %d", res.State.PageSize, size)
			}
			sh = e.Activate(res.Handle, res.Capture)
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			if got, _ := dirBytes(t, metaDir(e, res)); got > int64(size)*3 {
				t.Fatalf("the metadata directory holds %d bytes after the first sync", got)
			}
			app = e.StartApp(name)
			app.Must(t, "insert", "a", "2", "30000")
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			if n, _ := sh.TailInfo(); n != 1 {
				t.Fatalf("%d tail files", n)
			}
			tail := sh.Tail()
			defer capture.CloseTail(tail)
			if tail[0].Body.Size() > 2*60000+8*int64(size) {
				t.Fatalf("the first commit after the seed made a %d byte file: a snapshot", tail[0].Body.Size())
			}
			sh.Close(ctx)
		})
	}
}
