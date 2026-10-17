package capture_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/litestream"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
)

const name = "users/7"

func entriesOf(t *testing.T, e *harness.Env, shard string) (out []segment.Entry) {
	t.Helper()
	err := segment.Scan(context.Background(), e.Store, e.Node, 1, 0, func(_ uint64, en segment.Entry) error {
		if en.Shard == shard {
			out = append(out, en)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func l0Files(t *testing.T, sh *capture.Shard) []string {
	t.Helper()
	ents, err := os.ReadDir(sh.DB().LTXLevelDir(0))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	return out
}

// 6.1: without the Store, does Litestream delete its local level 0 files?
func TestL0RetentionWithoutStore(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	h, sh := e.NewShard(name)
	for range 30 {
		app.Must(t, "insert", "a", "1", "100")
		if err := sh.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	n := len(l0Files(t, sh))
	t.Logf("default L0Retention=%v; after 30 commits there are %d local level 0 files", litestream.DefaultL0Retention, n)
	if n < 30 {
		t.Fatalf("only %d local level 0 files after 30 commits", n)
	}
	sh.Sync(ctx) // nothing but a Store's compaction deletes level 0 files, and there is none
	if m := len(l0Files(t, sh)); m < n {
		t.Fatalf("Litestream deleted local level 0 files by itself (%d -> %d)", n, m)
	}
	// They pile up until shardlite drops them with the tail.
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	if m := len(l0Files(t, sh)); m != 1 {
		t.Fatalf("%d local level 0 files after the delta, want 1 (the newest)", m)
	}
	// The shard still syncs after the trim.
	app.Must(t, "insert", "a", "1", "100")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	sh.Close(ctx)
}

// ageDatabase sets the last write time of the shard's database file (which
// Litestream measures its checkpoint interval from, and which commits to a
// WAL-mode database do not touch) to long ago: the interval has passed.
func ageDatabase(t *testing.T, path string) {
	t.Helper()
	old := time.Now().Add(-10 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
}

// 6.2: how many commits does Litestream make by itself on an idle shard? Each
// sync here comes after the checkpoint interval has passed since the database
// file was last written (the worst case for an idle shard: whatever the interval
// allows is allowed every time), and the shard has to settle.
func TestIdleShardCommits(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		interval time.Duration
		syncs    int
	}{
		{"checkpoint interval 300ms", 300 * time.Millisecond, 40},
		{"default checkpoint interval (1m)", 0, 40},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := harness.New(t)
			ctx := context.Background()
			app := e.StartApp(name)
			h, created, _ := e.Files.Open(ctx, name)
			_ = h
			_ = created
			sh, err := capture.New(capture.Config{Name: name, Path: e.Path(name), Generation: h.Manifest.Generation, MetaRoot: e.Meta, Writer: e.W,
				Tune: func(db *litestream.DB) {
					if tc.interval > 0 {
						db.CheckpointInterval = tc.interval
					}
				}})
			if err != nil {
				t.Fatal(err)
			}
			for range 10 {
				app.Must(t, "insert", "a", "5", "200")
				if err := sh.Sync(ctx); err != nil {
					t.Fatal(err)
				}
			}
			lastApp := len(entriesOf(t, e, name))
			var lastChange int
			prev := lastApp
			for i := 1; i <= tc.syncs; i++ {
				ageDatabase(t, e.Path(name))
				if err := sh.Sync(ctx); err != nil {
					t.Fatal(err)
				}
				if n := len(entriesOf(t, e, name)); n != prev {
					prev, lastChange = n, i
				}
			}
			extra := prev - lastApp
			t.Logf("idle for %d syncs: %d commits by Litestream after the last app write (last one at sync %d)", tc.syncs, extra, lastChange)
			if extra > 0 && tc.interval > 0 && lastChange > tc.syncs/2 {
				t.Fatalf("still committing at sync %d of %d: does not settle", lastChange, tc.syncs)
			}
			sh.Close(ctx)
		})
	}
}

// 6.3: Litestream never checkpoints frames it hasn't captured. Random bursts
// of writes, syncs at random points, and checkpoints triggered both by size
// (default thresholds) and by time (short interval). Replaying what was
// captured must give the app's database.
func TestCheckpointsNeverLoseFrames(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		tune func(*litestream.DB)
		rows int // rows of 1 KB per burst
	}{
		{"default thresholds, big bursts", nil, 1400}, // a burst is over 1,000 pages (MinCheckpointPageN)
		{"short checkpoint interval", func(db *litestream.DB) { db.CheckpointInterval = 100 * time.Millisecond }, 20},
		{"tiny thresholds", func(db *litestream.DB) { db.MinCheckpointPageN = 10; db.TruncatePageN = 40 }, 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := harness.New(t)
			ctx := context.Background()
			app := e.StartApp(name)
			h, _, err := e.Files.Open(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			sh, err := capture.New(capture.Config{Name: name, Path: e.Path(name), Generation: h.Manifest.Generation, MetaRoot: e.Meta, Writer: e.W, Tune: tc.tune})
			if err != nil {
				t.Fatal(err)
			}
			for burst := range 12 {
				for range 3 {
					app.Must(t, "insert", "a", fmt.Sprint(tc.rows/3), "1000")
					if burst%2 == 0 {
						ageDatabase(t, e.Path(name)) // 120 ms pass between these writes
					}
				}
				if burst%3 != 2 { // sometimes let several bursts pile up unsynced
					if err := sh.Sync(ctx); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			// Replay everything captured.
			replayed := filepath.Join(t.TempDir(), "replayed.db")
			ap, _ := shardfile.NewApplier(replayed)
			var full int
			err = segment.Scan(ctx, e.Store, e.Node, 1, 0, func(seq uint64, en segment.Entry) error {
				if en.Shard != name {
					return nil
				}
				b, err := segment.ReadEntry(ctx, e.Store, e.Node, seq, en)
				if err != nil {
					return err
				}
				if pages, commit := pageCount(t, b); pages*2 > commit && commit > 20 {
					full++
				}
				return ap.Apply(bytesReader(b), shardfile.Range{Min: en.Position, Max: en.Last()})
			})
			if err != nil {
				t.Fatal(err)
			}
			ap.Finish()
			if want, got := apptest.Dump(t, e.Path(name)), sqlitetest.Dump(t, replayed); want != got {
				t.Fatal("captured history does not reproduce the app's database: frames were lost")
			}
			t.Logf("%d entries, %d of them hold over half the database (checkpoint or WAL-reset snapshots)", len(entriesOf(t, e, name)), full)
			sh.Close(ctx)
		})
	}
}

// The -shm check, in the order docs/internals.md gives: the node closes everything it
// has on the shard, then asks, then reopens capture if the app still has the
// shard open.
func TestReleaseChecksShm(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	h, _, _ := e.Files.Open(ctx, name)
	sh, err := capture.New(capture.Config{Name: name, Path: e.Path(name), Generation: h.Manifest.Generation, MetaRoot: e.Meta, Writer: e.W})
	if err != nil {
		t.Fatal(err)
	}
	app.Must(t, "insert", "a", "5", "100")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	obs := apptest.Start(t) // a third process, to see who holds the shard

	// The app has the shard open: the node doesn't let go, and capture carries on where it was.
	if released, _, err := sh.Release(ctx); err != nil || released {
		t.Fatalf("app open: released=%v err=%v", released, err)
	}
	pos := sh.Position()
	app.Must(t, "insert", "a", "5", "100")
	if err := sh.Sync(ctx); err != nil {
		t.Fatalf("sync after a refused release: %v", err)
	}
	if sh.Position() <= pos {
		t.Fatal("commits after a refused release were not captured")
	}
	// Doing it again and again changes nothing.
	for range 5 {
		if released, _, err := sh.Release(ctx); err != nil || released {
			t.Fatalf("app open: released=%v err=%v", released, err)
		}
	}
	if v := obs.Must(t, "probe", "a", e.Path(name)); !strings.HasPrefix(v, "held") {
		t.Fatalf("observer sees %s while app and node have the shard open", v)
	}

	// The app closes its connection: now the node can let go.
	app.Must(t, "close", "a")
	if released, _, err := sh.Release(ctx); err != nil || !released {
		t.Fatalf("app closed: released=%v err=%v", released, err)
	}
	if _, err := os.Stat(capture.MetaDir(e.Meta, name, h.Manifest.Generation)); !os.IsNotExist(err) {
		t.Fatalf("metadata directory left behind: %v", err)
	}
	if err := sh.Sync(ctx); err == nil {
		t.Fatal("sync of a released shard should fail")
	}
	if v := obs.Must(t, "probe", "a", e.Path(name)); v != "free" {
		t.Fatalf("observer sees %s after release", v)
	}
}

// Once the node has closed everything on a shard, checking it never drops the
// app's locks: the check's descriptor has none of the node's own to drop.
func TestCheckAfterClosingKeepsAppLocks(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	h, _, _ := e.Files.Open(ctx, name)
	sh, err := capture.New(capture.Config{Name: name, Path: e.Path(name), Generation: h.Manifest.Generation, MetaRoot: e.Meta, Writer: e.W})
	if err != nil {
		t.Fatal(err)
	}
	app.Must(t, "insert", "a", "5", "100")
	sh.Sync(ctx)
	if err := sh.Close(ctx); err != nil { // the node has nothing open on the shard now
		t.Fatal(err)
	}
	obs := apptest.Start(t)
	for i := range 20 {
		if held, err := capture.ProbeOnce(e.Path(name)); err != nil || !held {
			t.Fatalf("check %d: held=%v err=%v", i, held, err)
		}
		if v := obs.Must(t, "probe", "a", e.Path(name)); !strings.HasPrefix(v, "held") {
			t.Fatalf("check %d dropped the app's lock: observer sees %s", i, v)
		}
	}
	app.Must(t, "close", "a")
	if held, err := capture.ProbeOnce(e.Path(name)); err != nil || held {
		t.Fatalf("app closed: held=%v err=%v", held, err)
	}
}

// Why the order matters: a throwaway descriptor on the -shm file, opened and
// closed while the node still has a connection, drops the node's own lock
// (fcntl locks belong to the process), so another process would no longer
// see the node's connection.
func TestThrowawayProbeDropsLocks(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	h, _, _ := e.Files.Open(ctx, name)
	sh, err := capture.New(capture.Config{Name: name, Path: e.Path(name), Generation: h.Manifest.Generation, MetaRoot: e.Meta, Writer: e.W})
	if err != nil {
		t.Fatal(err)
	}
	app.Must(t, "insert", "a", "5", "100")
	sh.Sync(ctx)
	if v := app.Must(t, "probe", "a", e.Path(name)); !strings.HasPrefix(v, "held") {
		t.Fatalf("setup: %s", v)
	}
	f, err := os.Open(e.Path(name) + "-shm")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if v := app.Must(t, "probe", "a", e.Path(name)); v != "free" {
		t.Fatalf("after the node opened and closed a descriptor on -shm, the app still sees the node's connection (%s): the premise of the ordering rule no longer holds", v)
	}
	sh.Close(ctx)
}

// 6.5: what Litestream creates or writes in the app's directory and database.
func TestLitestreamFootprint(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	app.Must(t, "insert", "a", "5", "100")
	before := tables(t, e.Path(name))
	h, _, _ := e.Files.Open(ctx, name)
	sh, err := capture.New(capture.Config{Name: name, Path: e.Path(name), Generation: h.Manifest.Generation, MetaRoot: e.Meta, Writer: e.W})
	if err != nil {
		t.Fatal(err)
	}
	app.Must(t, "insert", "a", "5", "100")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("tables before capture: %v", before)
	t.Logf("tables after capture:  %v", tables(t, e.Path(name)))
	t.Logf("_litestream_seq rows: %s", rowsOf(t, e.Path(name), "_litestream_seq"))
	t.Logf("_litestream_lock rows: %s", rowsOf(t, e.Path(name), "_litestream_lock"))
	var files []string
	filepath.WalkDir(e.Dir, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			rel, _ := filepath.Rel(e.Dir, p)
			if strings.Contains(rel, "litestream") {
				rel = filepath.Dir(rel) + "/*"
			}
			files = append(files, rel)
		}
		return nil
	})
	sort.Strings(files)
	t.Logf("files next to the shard: %v", dedupe(files))
	t.Logf("files in the metadata root: %v", dirList(t, e.Meta))
	sh.Close(ctx)
	t.Logf("after Close: next to the shard %v, metadata root %v", dirList(t, e.Dir), dirList(t, e.Meta))
	if left := dirList(t, e.Meta); len(left) != 0 {
		t.Fatalf("metadata left after Close: %v", left)
	}
}

// A commit the app makes after Litestream's final sync, closing the file before
// the -shm check, is seen from the wal-index header: the node doesn't let go,
// and capture picks the commit up.
func TestReleaseSeesACommitAfterTheFinalSync(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	h, _, _ := e.Files.Open(ctx, name)
	late := false
	sh, err := capture.New(capture.Config{Name: name, Path: e.Path(name), Generation: h.Manifest.Generation, MetaRoot: e.Meta, Writer: e.W,
		Durable: func(ctx context.Context) error {
			if late {
				// Between Litestream closing and the -shm check.
				late = false
				app.Must(t, "open", "a", e.Path(name))
				app.Must(t, "insert", "a", "5", "100")
				app.Must(t, "close", "a")
			}
			return e.W.Flush(ctx)
		}})
	if err != nil {
		t.Fatal(err)
	}
	app.Must(t, "insert", "a", "5", "100")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	app.Must(t, "close", "a")
	pos := sh.Position()

	late = true
	if released, _, err := sh.Release(ctx); err != nil || released {
		t.Fatalf("commit after the final sync: released=%v err=%v", released, err)
	}
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if sh.Position() <= pos {
		t.Fatal("the late commit was not captured after capture resumed")
	}
	// With nothing new, the node lets go.
	if released, _, err := sh.Release(ctx); err != nil || !released {
		t.Fatalf("nothing new: released=%v err=%v", released, err)
	}
}

// A release whose wait for durability fails reopens capture itself and says
// so (Open), so the node carries on with it rather than starting a second
// capture of the same file.
func TestReleaseWhoseWaitFailsReopensCapture(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	h, _, _ := e.Files.Open(ctx, name)
	errStorage := errors.New("storage down")
	fail := true
	sh, err := capture.New(capture.Config{Name: name, Path: e.Path(name), Generation: h.Manifest.Generation, MetaRoot: e.Meta, Writer: e.W,
		Durable: func(ctx context.Context) error {
			if fail {
				fail = false
				return errStorage
			}
			return e.W.Flush(ctx)
		}})
	if err != nil {
		t.Fatal(err)
	}
	app.Must(t, "insert", "a", "5", "100")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	app.Must(t, "close", "a")
	if released, _, err := sh.Release(ctx); !errors.Is(err, errStorage) || released {
		t.Fatalf("release whose wait failed: released=%v err=%v", released, err)
	}
	if !sh.Open() {
		t.Fatal("capture was not reopened")
	}
	pos := sh.Position()
	app.Must(t, "open", "a", e.Path(name))
	app.Must(t, "insert", "a", "5", "100")
	app.Must(t, "close", "a")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if sh.Position() <= pos {
		t.Fatal("a commit after the failed release was not captured")
	}
	if released, _, err := sh.Release(ctx); err != nil || !released {
		t.Fatalf("second release: released=%v err=%v", released, err)
	}
	if sh.Open() {
		t.Fatal("open after it was released")
	}
}

// Litestream retries its final sync for 30 seconds by default. The client only
// fails for good, so closing a shard whose writer has been fenced must fail at
// once rather than hold the shard's loop.
func TestCloseWithAFencedWriterDoesNotRetry(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	h, _, _ := e.Files.Open(ctx, name)
	sh := e.Activate(h, nil)
	app.Must(t, "insert", "a", "5", "100")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// An adopter fences the journal, and the writer finds out at its next flush.
	adopter := blob.NewStore(e.Blob, "n2", 1, blob.Options{})
	if _, err := segment.Fence(ctx, adopter, e.Node, 1); err != nil {
		t.Fatal(err)
	}
	app.Must(t, "insert", "a", "6", "100")
	if err := sh.Sync(ctx); !errors.Is(err, segment.ErrFenced) {
		t.Fatalf("sync after the fence: %v, want ErrFenced", err)
	}
	// The final sync has a commit to queue, and the writer refuses it.
	app.Must(t, "insert", "a", "7", "100")
	done := make(chan error, 1)
	go func() { done <- sh.Close(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, segment.ErrFenced) {
			t.Fatalf("close: %v, want ErrFenced", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("close is still retrying its final sync")
	}
}
