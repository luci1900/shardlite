package capture_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/service"
	shardpkg "github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// tempDir is a new temporary directory for a shard's tail.
func tempDir(t testing.TB) *tempfile.Dir { return tempfile.NewDir(t.TempDir()) }

// tailOf is the shard's tail, closed when the test ends.
func tailOf(t testing.TB, sh *capture.Shard) capture.TailFiles {
	t.Helper()
	tail, err := sh.Tail()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tail.Close)
	return tail
}

// shardName is a shard's name in the tests that need the parsed type.
func shardName(s string) shardpkg.Name { return shardpkg.MustParse(s) }

type env struct {
	dir     string
	meta    string
	journal *journal.Store
	w       *journal.Writer
}

// newEnv is a shard's storage with a journal writer, which writes only when
// flushed (Sync flushes it, as the node's flush cycle does).
func newEnv(t *testing.T) *env {
	t.Helper()
	local := blobtest.WithHook(blob.NewMemory())
	js := journal.NewStore(journal.NewBlob(local, "n1"), "n1", 1, journal.Options{Backoff: time.Millisecond})
	w := journal.NewWriter(js, journal.WriterOptions{})
	serveWriter(t, w)
	return &env{dir: t.TempDir(), meta: t.TempDir(), journal: js, w: w}
}

// serveWriter serves w, as the node's tree does, until the test ends.
func serveWriter(t *testing.T, w *journal.Writer) {
	t.Cleanup(service.Background(slog.Default(), w))
}

// entriesFor returns every segment entry of a shard, in journal order.
func (e *env) entriesFor(t *testing.T, shard string) (out []journal.Entry, seqs []pos.Seg) {
	t.Helper()
	err := journal.Scan(context.Background(), e.journal, 1, 0, func(seq pos.Seg, en journal.Entry) error {
		if en.Shard.String() == shard {
			out, seqs = append(out, en), append(seqs, seq)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out, seqs
}

// replay applies every entry of a shard onto a new file.
func (e *env) replay(t *testing.T, shard, path string) shardfile.State {
	t.Helper()
	ap, err := shardfile.NewApplier(path)
	if err != nil {
		t.Fatal(err)
	}
	entries, seqs := e.entriesFor(t, shard)
	for i, en := range entries {
		b, err := harness.ReadEntry(context.Background(), e.journal, seqs[i], en)
		if err != nil {
			t.Fatal(err)
		}
		if err := ap.Apply(bytes.NewReader(b), shardfile.Range{Min: en.Position, Max: en.Last()}); err != nil {
			t.Fatalf("entry %d (%d-%d): %v", i, en.Position, en.Last(), err)
		}
	}
	st, err := ap.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestCommitsAppearAsEntries(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path)

	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName("users/1"), Path: path, Generation: "g1", Writer: e.w})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := range 20 {
		app.Must(t, "insert", "a", "3", "200")
		if err := sh.Sync(ctx); err != nil {
			t.Fatalf("sync %d: %v", i, err)
		}
	}
	entries, _ := e.entriesFor(t, "users/1")
	if len(entries) < 20 {
		t.Fatalf("got %d entries for 20 commits", len(entries))
	}
	var last pos.Position
	for _, en := range entries {
		if en.Position != last+1 || en.Count == 0 {
			t.Fatalf("gap or overlap: entry %d-%d after %d", en.Position, en.Last(), last)
		}
		last = en.Last()
	}
	if err := sh.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestReplayEqualsApp(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path)
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName("users/1"), Path: path, Generation: "g1", Writer: e.w})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	app.Must(t, "insert", "a", "50", "1000")
	app.Must(t, "exec", "a", "CREATE TABLE t2 (a, b)")
	for i := range 10 {
		app.Must(t, "exec", "a", fmt.Sprintf("INSERT INTO t2 VALUES (%d, 'x%d')", i, i))
		app.Must(t, "exec", "a", fmt.Sprintf("UPDATE kv SET payload = zeroblob(%d) WHERE id = %d", 100*i, i+1))
		if i%3 == 0 {
			app.Must(t, "exec", "a", "DELETE FROM kv WHERE id % 7 = 0")
		}
		if err := sh.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	replayed := filepath.Join(e.dir, "replayed.db")
	e.replay(t, "users/1", replayed)
	want, got := apptest.Dump(t, path), sqlitetest.Dump(t, replayed)
	if want != got {
		t.Fatalf("replayed database differs from the app's\nwant %d bytes, got %d bytes", len(want), len(got))
	}
	if len(want) < 1000 {
		t.Fatalf("dump is suspiciously small: %d", len(want))
	}
	sh.Close(ctx)
}

func TestManyShardsOneSegmentPerFlush(t *testing.T) {
	t.Parallel()
	const shards, rounds = 100, 5
	e := newEnv(t)
	ctx := context.Background()

	apps := make([]*apptest.App, 4)
	for i := range apps {
		apps[i] = apptest.Start(t)
	}
	caps := make([]*capture.Shard, shards)
	for i := range shards {
		path := filepath.Join(e.dir, fmt.Sprintf("s%d.db", i))
		apps[i%len(apps)].Must(t, "create", fmt.Sprint(i), path)
		sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName(fmt.Sprintf("users/%d", i)), Path: path, Generation: "g", Writer: e.w})
		if err != nil {
			t.Fatal(err)
		}
		caps[i] = sh
	}
	// Like the flush cycle: start every shard's sync, wait for all of them to
	// queue their files, then flush once.
	syncAll := func() {
		var g errgroup.Group
		for _, sh := range caps {
			g.Go(func() error { return sh.Capture(ctx) })
		}
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
		if err := e.w.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	syncAll() // every shard's first sync is its snapshot
	before := e.w.Flushes()
	for range rounds {
		for i := range shards {
			apps[i%len(apps)].Must(t, "insert", fmt.Sprint(i), "1", "100")
		}
		syncAll()
	}
	flushes := e.w.Flushes() - before
	t.Logf("%d shards x %d rounds: %d segments", shards, rounds, flushes)
	// One segment per round, never one per shard.
	if flushes != rounds {
		t.Fatalf("%d segments for %d rounds", flushes, rounds)
	}
	var entries int
	journal.Scan(ctx, e.journal, 1, 0, func(pos.Seg, journal.Entry) error { entries++; return nil })
	if perSeg := float64(entries) / float64(e.w.Flushes()); perSeg < 3 {
		t.Fatalf("%.1f entries per segment: shards are not sharing segments", perSeg)
	}
	for _, sh := range caps {
		sh.Close(ctx)
	}
}

// replayTo applies a shard's entries onto a new file in a temp dir.
func (e *env) replayTo(t *testing.T, shard string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replayed.db")
	e.replay(t, shard, path)
	return path
}

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

func tables(t *testing.T, path string) []string {
	t.Helper()
	out := apptest.Query(t, path, `SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`)
	if out == "" {
		return nil
	}
	return strings.Split(out, "\n")
}

func rowsOf(t *testing.T, path, table string) string {
	t.Helper()
	return strings.ReplaceAll(apptest.Query(t, path, `SELECT * FROM `+table), "\n", ", ")
}

func dedupe(a []string) []string {
	var out []string
	for _, s := range a {
		if len(out) == 0 || out[len(out)-1] != s {
			out = append(out, s)
		}
	}
	return out
}

func dirList(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			rel, _ := filepath.Rel(dir, p)
			out = append(out, rel)
		}
		return nil
	})
	return out
}

// pageCount returns how many pages an LTX file holds and the database size in pages.
func pageCount(t *testing.T, b []byte) (pages, commit int) {
	t.Helper()
	dec := ltx.NewDecoder(bytes.NewReader(b))
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, dec.Header().PageSize)
	for {
		var ph ltx.PageHeader
		if err := dec.DecodePage(&ph, buf); err != nil {
			break
		}
		pages++
	}
	return pages, int(dec.Header().Commit)
}

// A caller that stops waiting doesn't cut the sync short: the sync runs to
// the end on its own, so the commit reaches the journal once and a later
// Sync finds nothing left to do.
func TestSyncOutlivesItsCaller(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	local := blobtest.WithHook(blob.NewMemory())
	e.journal = journal.NewStore(journal.NewBlob(local, "n1"), "n1", 1, journal.Options{Backoff: time.Millisecond})
	e.w = journal.NewWriter(e.journal, journal.WriterOptions{Retry: time.Millisecond})
	serveWriter(t, e.w)

	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path)
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName("users/1"), Path: path, Generation: "g1", Writer: e.w})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	app.Must(t, "insert", "a", "3", "200")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	up := make(chan struct{})
	local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && !after {
			select { // storage hangs until released
			case <-up:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
		return nil
	})
	app.Must(t, "insert", "a", "3", "200")
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := sh.Sync(short); err == nil {
		t.Fatal("sync returned while storage was hanging")
	}
	if sh.TailFrom() == 0 {
		t.Fatal("a file in the writer's queue is not counted in the shard's tail range")
	}
	// The file is in the tail as soon as it is queued.
	if len(tailOf(t, sh).Files) == 0 {
		t.Fatal("a queued file is not in the tail")
	}
	close(up)

	// Nothing asks again, yet the commit arrives: the writer never drops a queued file.
	apptest.Eventually(t, 5*time.Second, "the commit to reach the journal after the caller left", func() bool {
		entries, _ := e.entriesFor(t, "users/1")
		return len(entries) == len(tailOf(t, sh).Files)
	})
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	entries, _ := e.entriesFor(t, "users/1")
	var last pos.Position
	for _, en := range entries {
		if en.Position != last+1 {
			t.Fatalf("gap or repeat: entry %d-%d after %d", en.Position, en.Last(), last)
		}
		last = en.Last()
	}
	if got := len(tailOf(t, sh).Files); got != len(entries) {
		t.Fatalf("tail has %d files, journal %d entries", got, len(entries))
	}
	sh.Close(ctx)
}

// The tail's segment bound is what the lease records as the oldest segment a
// restart must read.
func TestTailFromFollowsTheTail(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path)
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName("users/1"), Path: path, Generation: "g1", Writer: e.w})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if sh.TailFrom() != 0 {
		t.Fatal("a shard with nothing captured has a tail")
	}
	var first pos.Seg
	for i := range 6 {
		app.Must(t, "insert", "a", "2", "100")
		if err := sh.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		apptest.Eventually(t, 5*time.Second, "the commit to reach the journal", func() bool {
			entries, _ := e.entriesFor(t, "users/1")
			return len(entries) == len(tailOf(t, sh).Files)
		})
		if i == 0 {
			_, seqs := e.entriesFor(t, "users/1")
			first = seqs[0]
		}
	}
	_, seqs := e.entriesFor(t, "users/1")
	got := sh.TailFrom()
	if got == 0 || got > first || got < 1 {
		t.Fatalf("TailFrom = %d, first entry is in segment %d", got, first)
	}
	if got < first-1 {
		t.Fatalf("TailFrom = %d is far below the first entry's segment %d (of %d..%d)", got, first, seqs[0], seqs[len(seqs)-1])
	}
	sh.DropTail(sh.Position())
	if sh.TailFrom() != 0 {
		t.Fatalf("TailFrom = %d after the tail was dropped", sh.TailFrom())
	}
	sh.Close(ctx)
}

// Many commits between two syncs make one LTX file that keeps only the latest
// version of each page: rewriting the same pages 300 times costs the pages once,
// not 300 times (merging commits per flush window is a possible later step).
func TestCommitsInOneWindowKeepOnlyTheLatestPages(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path)
	app.Must(t, "exec", "a", "CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB)")
	for i := range 40 {
		app.Must(t, "exec", "a", fmt.Sprintf("INSERT INTO t (id, v) VALUES (%d, randomblob(1000))", i))
	}
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName("users/1"), Path: path, Generation: "g1", Writer: e.w})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	defer sh.Close(ctx)
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	before, _ := e.entriesFor(t, "users/1")

	const commits = 300
	for range commits {
		app.Must(t, "exec", "a", "UPDATE t SET v = randomblob(1000)") // every page of the table, each time
	}
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	entries, seqs := e.entriesFor(t, "users/1")
	// The window may be cut into more than one file, but never one per commit.
	var totalPages, totalBytes, dbPages int
	for i := len(before); i < len(entries); i++ {
		b, err := harness.ReadEntry(ctx, e.journal, seqs[i], entries[i])
		if err != nil {
			t.Fatal(err)
		}
		pages, commit := pageCount(t, b)
		t.Logf("file %d: commits %d-%d, %d pages, %d bytes, database of %d pages", i-len(before)+1, entries[i].Position, entries[i].Last(), pages, len(b), commit)
		totalPages += pages
		totalBytes += len(b)
		dbPages = commit
		if pages > commit {
			t.Fatalf("%d pages in a file for a database of %d: older versions are kept", pages, commit)
		}
	}
	files := len(entries) - len(before)
	t.Logf("%d commits in one window: %d LTX files, %d pages and %d bytes in all, for a database of %d pages", commits, files, totalPages, totalBytes, dbPages)
	if files > 5 || totalPages > files*dbPages {
		t.Fatalf("%d files and %d pages for %d commits of the same %d pages", files, totalPages, commits, dbPages)
	}
}

// A panic under the shard's lock ends the activation, so a caller that recovers
// it (as net/http does for a handler) can't go on to use the shard in whatever
// state the panic left.
func TestPanicUnderTheLockEndsTheActivation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	path := filepath.Join(e.dir, "users-1.db")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	sh := newShard(t, e, path)
	defer sh.Abandon()
	ctx := context.Background()
	func() {
		defer func() {
			if r := recover(); r != "boom" {
				t.Fatalf("recovered %v, want the panic", r)
			}
		}()
		sh.Use(ctx, func(*sql.DB) error { panic("boom") })
	}()
	if sh.Open() {
		t.Fatal("the shard is still open after a panic under its lock")
	}
	if err := sh.Use(ctx, func(*sql.DB) error { return nil }); !errors.Is(err, capture.ErrClosed) {
		t.Fatalf("use after the panic: %v, want ErrClosed", err)
	}
}

// UseAt reports the captured position fn saw when nothing was committed while
// it ran (whether or not the commits before it were captured yet), and 0 when
// something was.
func TestUseAtReportsTheExactPosition(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	path := filepath.Join(e.dir, "users-1.db")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	sh := newShard(t, e, path)
	defer sh.Abandon()
	ctx := context.Background()
	if err := sh.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	app, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(wal)")
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	if _, err := app.Exec(`CREATE TABLE t (x)`); err != nil { // not captured yet
		t.Fatal(err)
	}
	var n int
	at, err := sh.UseAt(ctx, func(db *sql.DB) error {
		return db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE name = 't'`).Scan(&n)
	})
	if err != nil || n != 1 || at == 0 || at != sh.Position() {
		t.Fatalf("idle: at %d (position %d), saw the table %d times, err %v", at, sh.Position(), n, err)
	}
	at, err = sh.UseAt(ctx, func(*sql.DB) error {
		_, err := app.Exec(`INSERT INTO t VALUES (1)`) // committed while fn runs
		return err
	})
	if err != nil || at != 0 {
		t.Fatalf("with a commit during fn: at %d, err %v", at, err)
	}
}

// Release's read sees the final state: it reports the final position, or 0
// when something was committed after the read (which the close then captured).
func TestReleaseReadsTheFinalState(t *testing.T) {
	t.Parallel()
	for _, commitDuring := range []bool{false, true} {
		e := newEnv(t)
		path := filepath.Join(e.dir, "users-1.db")
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		sh := newShard(t, e, path)
		ctx := context.Background()
		if err := sh.Capture(ctx); err != nil {
			t.Fatal(err)
		}
		app, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(wal)")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := app.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}
		released, f, err := sh.Release(ctx, func(db *sql.DB) error {
			if commitDuring {
				if _, err := app.Exec(`INSERT INTO t VALUES (1)`); err != nil {
					return err
				}
			}
			var n int
			return db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema WHERE name = 't'`).Scan(&n)
		}, capture.ResumeIfHeld)
		app.Close()
		if err != nil || !released {
			t.Fatalf("commit during the read %v: released %v, err %v", commitDuring, released, err)
		}
		switch r := f.(type) {
		case *capture.Kept:
			if commitDuring {
				t.Fatalf("commit during the read %v: kept at %d", commitDuring, r.At)
			}
			if want := sh.Position(); r.At != want {
				t.Fatalf("commit during the read %v: at %d, final position %d", commitDuring, r.At, want)
			}
		case *capture.Uncertain:
			if !commitDuring {
				t.Fatalf("commit during the read %v: uncertain", commitDuring)
			}
		case *capture.Deleted:
			t.Fatalf("commit during the read %v: deleted", commitDuring)
		}
	}
}
