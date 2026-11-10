package shardfile_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/sqlitetest"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// ack commits one row to the acks table and captures it: one commit, so one
// position of the shard.
func ack(t *testing.T, app *apptest.App, sh *capture.Shard, n int) {
	t.Helper()
	app.Must(t, "ack", "a", itoa(n))
	if err := sh.Capture(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

func acksOf(t *testing.T, path string) []int {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT n FROM acks ORDER BY n`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		out = append(out, n)
	}
	return out
}

func upTo(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i + 1
	}
	return out
}

// A shard collects one delta per snapshot however old its base is (there is no
// age for a base, only the file limit), and rebases once the limit is passed.
func TestOneDeltaPerSnapshotUntilThereAreTooManyFiles(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)
	policy := shardfile.Policy{MaxFiles: 5}

	snapshot := func(n int, want shardfile.Snapshotted) {
		t.Helper()
		ack(t, app, sh, n)
		if got, err := h.MaybeSnapshot(ctx, sh, 0, policy, shardfile.Hooks{}); err != nil || got != want {
			t.Fatalf("snapshot %d: wrote %v (want %v), err=%v", n, got, want, err)
		}
	}
	for i := 1; i <= 6; i++ {
		snapshot(i, shardfile.WroteDelta) // (six files: the rebase is for the one after the limit)
	}
	if got := len(h.Manifest.Files); got != 6 {
		t.Fatalf("%d files after six snapshots, want 6: %v", got, h.Manifest.Files)
	}
	snapshot(7, shardfile.WroteRebase)
	if got := h.Manifest.Files; len(got) != 1 {
		t.Fatalf("files %v after the rebase, want one", got)
	}
	snapshot(8, shardfile.WroteDelta)
	snapshot(9, shardfile.WroteDelta)
	if got := len(h.Manifest.Files); got != 3 {
		t.Fatalf("%d files, want the base and two deltas: %v", got, h.Manifest.Files)
	}
	if got := acksOf(t, restoreTo(t, e, "r")); !slices.Equal(got, upTo(9)) {
		t.Fatalf("restored acks %v", got)
	}
	// Nothing superseded is left behind.
	if n := countFiles(t, e); n != 3 {
		t.Fatalf("%d objects, want 3", n)
	}
}

// The tail passing the size makes a delta due before it is old; a small, young
// tail makes none.
func TestMaybeSnapshotBySize(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)
	ack(t, app, sh, 1)
	size := sh.TailBytes()
	if size == 0 {
		t.Fatal("the tail has no bytes")
	}
	if got, err := h.MaybeSnapshot(ctx, sh, time.Hour, shardfile.Policy{MaxBytes: size + 1}, shardfile.Hooks{}); err != nil || got != shardfile.Nothing {
		t.Fatalf("a tail under the size and the age: wrote %v, err=%v", got, err)
	}
	if got, err := h.MaybeSnapshot(ctx, sh, time.Hour, shardfile.Policy{MaxBytes: size}, shardfile.Hooks{}); err != nil || got != shardfile.WroteDelta {
		t.Fatalf("a tail at the size: wrote %v, err=%v", got, err)
	}
	if sh.TailBytes() != 0 {
		t.Fatalf("the tail still holds %d bytes after its delta", sh.TailBytes())
	}
}

// With many snapshots within the interval, the shard rebases once it has more
// than MaxFiles files.
func TestRebaseWhenThereAreManyFiles(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)
	policy := shardfile.Policy{MaxFiles: 8}

	var rebases []int
	for i := 1; i <= 20; i++ {
		ack(t, app, sh, i)
		got, err := h.MaybeSnapshot(ctx, sh, 0, policy, shardfile.Hooks{})
		if err != nil {
			t.Fatal(err)
		}
		if got == shardfile.WroteRebase {
			rebases = append(rebases, i)
			if len(h.Manifest.Files) != 1 {
				t.Fatalf("round %d: files %v after a rebase", i, h.Manifest.Files)
			}
		}
		if len(h.Manifest.Files) > 9 {
			t.Fatalf("round %d: %d files", i, len(h.Manifest.Files))
		}
	}
	// Eight deltas make 8 files, the ninth snapshot adds a ninth (more than 8), and the tenth rebases.
	if !slices.Equal(rebases, []int{10, 19}) {
		t.Fatalf("rebases at rounds %v, want [10 19]", rebases)
	}
	if got := acksOf(t, restoreTo(t, e, "r")); !slices.Equal(got, upTo(20)) {
		t.Fatalf("restored acks %v", got)
	}
}

// A shard with more files than the limit and nothing new to snapshot is still
// rebased to one file, which is what merging did.
func TestRebaseWithoutATail(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)
	for i := 1; i <= 4; i++ {
		ack(t, app, sh, i)
		if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
			t.Fatal(err)
		}
	}
	policy := shardfile.Policy{MaxFiles: 3}
	if got, err := h.MaybeSnapshot(ctx, sh, time.Hour, policy, shardfile.Hooks{}); err != nil || got != shardfile.WroteRebase {
		t.Fatalf("wrote %v, err=%v", got, err)
	}
	if len(h.Manifest.Files) != 1 {
		t.Fatalf("files %v", h.Manifest.Files)
	}
	// And a shard of one file has nothing to rebase.
	if got, err := h.MaybeSnapshot(ctx, sh, time.Hour, policy, shardfile.Hooks{}); err != nil || got != shardfile.Nothing {
		t.Fatalf("wrote %v, err=%v", got, err)
	}
}

// The app writes (and checkpoints) while the shard is rebased. The base is a
// consistent database at its position: exactly the commits up to it, none
// after. The tail applies on top.
func TestRebaseOfAnActiveShardWhileTheAppWrites(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(shard)
	app.Must(t, "insert", "a", "20000", "1000") // about 20 MB, so reading the snapshot takes a while
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	ack(t, app, sh, 1)
	offset := sh.Position() - 1 // the position of commit n is offset+n
	for i := 2; i <= 4; i++ {
		ack(t, app, sh, i)
		if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
			t.Fatal(err)
		}
	}
	if len(h.Manifest.Files) != 3 {
		t.Fatalf("files %v", h.Manifest.Files)
	}

	// Each commit is captured by its own sync, so each is one position.
	var last atomic.Int64
	last.Store(4)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var werr atomic.Pointer[error]
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			n := int(last.Load()) + 1
			steps := [][]string{{"ack", "a", itoa(n)}}
			if n%3 == 0 {
				steps = append(steps, []string{"ckpt", "a", "PASSIVE"})
			}
			for _, s := range steps {
				if _, err := app.Do(s...); err != nil {
					werr.Store(&err)
					return
				}
			}
			if err := sh.Capture(ctx); err != nil {
				werr.Store(&err)
				return
			}
			last.Store(int64(n))
			time.Sleep(time.Millisecond) // the app's pace, so the rebase runs while the writer writes
		}
	}()
	apptest.Eventually(t, 10*time.Second, "the writer to get going", func() bool { return last.Load() >= 8 })
	before := last.Load()
	did, err := h.Rebase(ctx, sh, shardfile.Hooks{})
	after := last.Load()
	close(stop)
	wg.Wait()
	if p := werr.Load(); p != nil {
		t.Fatalf("writer: %v", *p)
	}
	if err != nil || !did {
		t.Fatalf("rebase: did=%v err=%v", did, err)
	}
	t.Logf("%d commits during the rebase (writer at %d before, %d after)", after-before, before, after)
	if after == before {
		t.Log("warning: the app made no commit while the rebase ran")
	}

	if len(h.Manifest.Files) != 1 {
		t.Fatalf("files %v", h.Manifest.Files)
	}
	pos := h.Manifest.LastPosition()
	k := int(pos - offset)
	if k < int(before) || k > int(after)+1 {
		t.Fatalf("the base is at position %d (commit %d), but the writer was at %d before and %d after", pos, k, before, after)
	}

	// The base alone is the database at its position.
	base := filepath.Join(t.TempDir(), "base.db")
	res, err := e.Files.Restore(ctx, shardName(shard), base, shardfile.RestoreOptions{NoJournal: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.State.Position != pos {
		t.Fatalf("restored to position %d, the base is at %d", res.State.Position, pos)
	}
	if got := acksOf(t, base); !slices.Equal(got, upTo(k)) {
		t.Fatalf("the base holds acks %v, want 1..%d", got, k)
	}
	if got := sqlitetest.Dump(t, base); got == "" {
		t.Fatal("empty dump")
	}

	// The tail (the commits after the base) applies on top: restore with the
	// journal, then after a delta of the tail.
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if tail := tailOf(t, sh).Files; len(tail) > 0 && tail[0].Position != pos+1 {
		t.Fatalf("the tail starts at position %d, the base ends at %d", tail[0].Position, pos)
	}
	want := apptest.Dump(t, e.Path(shard))
	if got := restoreDump(t, e, "full"); got != want {
		t.Fatal("base plus journal tail differs from the app's database")
	}
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	if got := restoreDump(t, e, "delta"); got != want {
		t.Fatal("base plus delta differs from the app's database")
	}
}

// A crash between writing the base and updating the manifest leaves the old
// files in use and the new base unreferenced.
func TestCrashBetweenBaseAndManifest(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)
	for i := 1; i <= 3; i++ {
		ack(t, app, sh, i)
		if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
			t.Fatal(err)
		}
	}
	files := slices.Clone(h.Manifest.Files)
	ack(t, app, sh, 4)
	if err := sh.Sync(ctx); err != nil { // commit 4 is in the journal
		t.Fatal(err)
	}

	crash := errors.New("crash")
	if _, err := h.Rebase(ctx, sh, shardfile.Hooks{AfterFileWrite: func() error { return crash }}); !errors.Is(err, crash) {
		t.Fatalf("got %v", err)
	}
	m, _, err := e.Files.GetManifest(ctx, shardName(shard))
	if err != nil || !slices.Equal(m.Files, files) {
		t.Fatalf("stored manifest %+v err=%v, want files %v", m, err, files)
	}
	// The three deltas and the unlisted base are there.
	if n := countFiles(t, e); n != 4 {
		t.Fatalf("%d objects, want 3 files and the unreferenced base", n)
	}
	// The old files are in use: a restore gives the database as it was (the journal has commit 4).
	if got := acksOf(t, restoreTo(t, e, "r")); !slices.Equal(got, upTo(4)) {
		t.Fatalf("restored acks %v", got)
	}
	if len(tailOf(t, sh).Files) == 0 {
		t.Fatal("the tail was dropped though the manifest does not cover it")
	}
	// A later rebase works, whatever the unlisted base.
	if did, err := h.Rebase(ctx, sh, shardfile.Hooks{}); err != nil || !did {
		t.Fatalf("second rebase: did=%v err=%v", did, err)
	}
	if got := acksOf(t, restoreTo(t, e, "r2")); !slices.Equal(got, upTo(4)) {
		t.Fatalf("restored acks %v after the second rebase", got)
	}
}

func restoreTo(t *testing.T, e *harness.Env, name string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), name+".db")
	if _, err := e.Files.Restore(context.Background(), shardName(shard), dst, shardfile.RestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	return dst
}

func countFiles(t *testing.T, e *harness.Env) int {
	t.Helper()
	n := 0
	for _, err := range e.Store.List(context.Background(), shardfile.ShardPrefix(shardName(shard))) {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	return n
}

// Deltas, bases and the files of a restore are built and downloaded in logs of
// their own, which are closed once the operation is done, whether it worked or
// not. The logs have no names, so what is checked is the count of descriptors the
// directory has open.
func TestDeltasBasesAndRestoresCloseTheirLogs(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	temp := e.Files.Temp
	app := e.StartApp(shard)
	h, sh := e.NewShard(shard)
	defer sh.Close(ctx)

	// The shard's tail holds a log while it has files, and the journal writer a dup
	// of it until the segment is written, so what is checked for is the count
	// with the tail discarded and the writer flushed.
	var creates, gets atomic.Int32
	temp.SetHook(func(op tempfile.Op) error {
		if op == tempfile.OpCreate {
			creates.Add(1)
		}
		return nil
	})
	e.Blob.SetHook(func(_ context.Context, op blobtest.Op, key string, after bool) error {
		if !after && op == blobtest.OpGet && strings.HasPrefix(key, shardfile.FilesPrefix) {
			gets.Add(1)
		}
		return nil
	})
	for i := 1; i <= 3; i++ {
		ack(t, app, sh, i)
		if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
			t.Fatal(err)
		}
	}
	ack(t, app, sh, 4)
	if _, err := h.Rebase(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	ack(t, app, sh, 5)
	if err := h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		t.Fatal(err)
	}
	// Three deltas, a base and a delta are built in a log each, besides the
	// tail's own logs.
	if got := creates.Load(); got < 5 {
		t.Fatalf("%d logs created for three deltas, a base and a delta", got)
	}
	sh.Discard()
	leftOpen := func() int64 {
		t.Helper()
		if err := e.W.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		return temp.Open()
	}
	if n := leftOpen(); n != 0 {
		t.Fatalf("%d descriptors left open after the deltas and the rebase", n)
	}
	gets.Store(0)
	if got := acksOf(t, restoreTo(t, e, "r")); !slices.Equal(got, upTo(5)) {
		t.Fatalf("restored acks %v", got)
	}
	if gets.Load() != 2 {
		t.Fatalf("%d downloads, want the base and the delta", gets.Load())
	}
	if n := leftOpen(); n != 0 {
		t.Fatalf("%d descriptors left open after the restore", n)
	}

	// A failed restore cleans up too. The shard's last file is the delta, which
	// the restore has to download.
	lastFile := h.Manifest.Files[len(h.Manifest.Files)-1]
	e.Blob.SetHook(func(_ context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpGet && strings.HasSuffix(key, ".ltx") && strings.Contains(key, lastFile.String()) {
			return errors.New("injected")
		}
		return nil
	})
	if _, err := e.Files.Restore(ctx, shardName(shard), filepath.Join(t.TempDir(), "x.db"), shardfile.RestoreOptions{}); err == nil {
		t.Fatal("restore succeeded")
	}
	if n := leftOpen(); n != 0 {
		t.Fatalf("%d descriptors left open after a failed restore", n)
	}
}
