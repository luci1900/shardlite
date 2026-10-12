package capture_test

import (
	"bytes"
	"context"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
)

type env struct {
	dir   string
	meta  string
	store *blob.Store
	w     *segment.Writer
}

func newEnv(t *testing.T, interval time.Duration) *env {
	t.Helper()
	local, err := blob.NewLocal(filepath.Join(t.TempDir(), "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	store := blob.NewStore(local, "n1", 1, blob.Options{Backoff: time.Millisecond})
	w := segment.NewWriter(store, "n1", segment.Options{Interval: interval})
	t.Cleanup(w.Close)
	return &env{dir: t.TempDir(), meta: t.TempDir(), store: store, w: w}
}

// entriesFor returns every segment entry of a shard, in journal order.
func (e *env) entriesFor(t *testing.T, shard string) (out []segment.Entry, seqs []uint64) {
	t.Helper()
	err := segment.Scan(context.Background(), e.store, "n1", 1, func(seq uint64, en segment.Entry) error {
		if en.Shard == shard {
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
		b, err := segment.ReadEntry(context.Background(), e.store, "n1", seqs[i], en)
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
	e := newEnv(t, 20*time.Millisecond)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "open", "a", path)

	sh, err := capture.New(capture.Config{Name: "users/1", Path: path, Generation: "g1", MetaRoot: e.meta, Writer: e.w})
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
	var last uint64
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
	e := newEnv(t, 20*time.Millisecond)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "open", "a", path)
	sh, err := capture.New(capture.Config{Name: "users/1", Path: path, Generation: "g1", MetaRoot: e.meta, Writer: e.w})
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
	const shards, rounds = 100, 5
	e := newEnv(t, time.Hour) // flushed by hand, as the node's flush cycle does
	ctx := context.Background()

	apps := make([]*apptest.App, 4)
	for i := range apps {
		apps[i] = apptest.Start(t)
	}
	caps := make([]*capture.Shard, shards)
	for i := range shards {
		path := filepath.Join(e.dir, fmt.Sprintf("s%d.db", i))
		apps[i%len(apps)].Must(t, "open", fmt.Sprint(i), path)
		sh, err := capture.New(capture.Config{Name: fmt.Sprintf("users/%d", i), Path: path, Generation: "g", MetaRoot: e.meta, Writer: e.w})
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
	segment.Scan(ctx, e.store, "n1", 1, func(uint64, segment.Entry) error { entries++; return nil })
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

// A caller that stops waiting doesn't cut the sync short: Litestream's sync
// runs to the end on its own, so the commit reaches the journal once and a later
// Sync finds nothing left to do.
func TestSyncOutlivesItsCaller(t *testing.T) {
	e := newEnv(t, 5*time.Millisecond)
	local, err := blob.NewLocal(filepath.Join(t.TempDir(), "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	e.store = blob.NewStore(local, "n1", 1, blob.Options{Backoff: time.Millisecond})
	e.w = segment.NewWriter(e.store, "n1", segment.Options{Interval: 5 * time.Millisecond, Retry: time.Millisecond})
	t.Cleanup(e.w.Close)

	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "open", "a", path)
	sh, err := capture.New(capture.Config{Name: "users/1", Path: path, Generation: "g1", MetaRoot: e.meta, Writer: e.w})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	app.Must(t, "insert", "a", "3", "200")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	up := make(chan struct{})
	local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if op == blob.OpCreate && !after {
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
	if len(sh.Tail()) == 0 {
		t.Fatal("a queued file is not in the tail")
	}
	close(up)

	// Nothing asks again, yet the commit arrives: the writer never drops a queued file.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if entries, _ := e.entriesFor(t, "users/1"); len(entries) == len(sh.Tail()) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the commit never reached the journal after the caller left")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	entries, _ := e.entriesFor(t, "users/1")
	var last uint64
	for _, en := range entries {
		if en.Position != last+1 {
			t.Fatalf("gap or repeat: entry %d-%d after %d", en.Position, en.Last(), last)
		}
		last = en.Last()
	}
	if got := len(sh.Tail()); got != len(entries) {
		t.Fatalf("tail has %d files, journal %d entries", got, len(entries))
	}
	sh.Close(ctx)
}

// The tail's segment bound is what the lease records as the oldest segment a
// restart must read.
func TestTailFromFollowsTheTail(t *testing.T) {
	e := newEnv(t, 5*time.Millisecond)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "open", "a", path)
	sh, err := capture.New(capture.Config{Name: "users/1", Path: path, Generation: "g1", MetaRoot: e.meta, Writer: e.w})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if sh.TailFrom() != 0 {
		t.Fatal("a shard with nothing captured has a tail")
	}
	var first uint64
	for i := range 6 {
		app.Must(t, "insert", "a", "2", "100")
		if err := sh.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
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
