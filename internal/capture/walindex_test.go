package capture_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/tempfile"
)

func shmIdent(t *testing.T, path string) (ino uint64, size int64) {
	t.Helper()
	fi, err := os.Stat(path + "-shm")
	if err != nil {
		t.Fatal(err)
	}
	return fi.Sys().(*syscall.Stat_t).Ino, fi.Size()
}

func newShard(t *testing.T, e *env, path string) *capture.Shard {
	t.Helper()
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName("users/1"), Path: path, Generation: "g1", Writer: e.w})
	if err != nil {
		t.Fatal(err)
	}
	return sh
}

// Before any connection has opened the database, the mapped page is beyond
// the end of the file: reading it must be an error, not a crash.
func TestWALIndexBeforeOpenIsAnError(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "users-1.db")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	w, err := capture.MapWALIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Read(); !errors.Is(err, capture.ErrFault) {
		t.Fatalf("read of an empty -shm: %v", err)
	}
}

// A missing -shm is made with the database file's permissions, as SQLite would
// make it, whatever the node's umask: SQLite reuses the file, so an app running
// as another user in the database's group would otherwise find it read-only.
func TestWALIndexCreatesShmWithTheDatabaseMode(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "users-1.db")
	if err := os.WriteFile(path, nil, 0o660); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o660); err != nil { // the umask may have narrowed it
		t.Fatal(err)
	}
	w, err := capture.MapWALIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	fi, err := os.Stat(path + "-shm")
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o660 {
		t.Fatalf("-shm mode %v, want %v", fi.Mode().Perm(), os.FileMode(0o660))
	}
}

// The mapping made before capture opens the shard stays valid: the -shm
// file is truncated once, when the first connection (capture's) opens it,
// and never again however often the app connects and disconnects.
func TestWALIndexSurvivesAppChurn(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path)
	app.Must(t, "close", "a") // the app is gone before the node maps
	w, err := capture.MapWALIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	sh := newShard(t, e, path)
	defer sh.Close(context.Background())
	ctx := context.Background()
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	ino, _ := shmIdent(t, path)
	last, err := w.Read()
	if err != nil {
		t.Fatal(err)
	}
	for i := range 200 {
		app.Must(t, "create", "a", path)
		app.Must(t, "ack", "a", itoa(i))
		h, err := w.Read()
		if err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		if h == last {
			t.Fatalf("round %d: commit not seen", i)
		}
		last = h
		if i%2 == 0 {
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
		}
		app.Must(t, "close", "a")
		h, err = w.Read()
		if err != nil || h != last {
			t.Fatalf("round %d after close: %v changed=%v", i, err, h != last)
		}
		if got, size := shmIdent(t, path); got != ino || size < 32768 {
			t.Fatalf("round %d: -shm replaced or shrunk (inode %d -> %d, size %d)", i, ino, got, size)
		}
	}
}

func itoa(i int) string { return string(appendInt(nil, i)) }

func appendInt(b []byte, i int) []byte {
	if i >= 10 {
		b = appendInt(b, i/10)
	}
	return append(b, byte('0'+i%10))
}

// Reads in a tight loop while capture syncs, restarts the WAL and the app
// connects and disconnects: none may fault once capture has opened the
// shard (SIGBUS would mean SQLite shortened -shm under the mapping).
func TestWALIndexNeverFaultsWhileCaptured(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path)
	app.Must(t, "close", "a")
	w, err := capture.MapWALIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	sh := newShard(t, e, path)
	defer sh.Close(context.Background())
	ctx := context.Background()
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan error, 1)
	reads := 0
	go func() {
		for {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			if _, err := w.Read(); err != nil {
				done <- err
				return
			}
			reads++
		}
	}()
	for i := range 40 {
		app.Must(t, "create", "a", path)
		app.Must(t, "insert", "a", "5", "2000")
		if i%3 == 0 {
			app.Must(t, "ckpt", "a", "PASSIVE")
		}
		if err := sh.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		app.Must(t, "close", "a")
		if err := sh.Sync(ctx); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	t.Logf("%d reads", reads)
}

// Release closes capture's connections. With the app holding the shard the
// -shm stays and capture reopens; the mapping, made again, is valid.
func TestWALIndexAcrossHeldRelease(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path)
	w, err := capture.MapWALIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	sh := newShard(t, e, path)
	ctx := context.Background()
	app.Must(t, "ack", "a", "1")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	h0, err := w.Read()
	if err != nil {
		t.Fatal(err)
	}
	w.Close()
	released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld)
	if err != nil || released {
		t.Fatalf("release: %v %v", released, err)
	}
	if w, err = capture.MapWALIndex(path); err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if h, err := w.Read(); err != nil || h != h0 {
		t.Fatalf("remapped header: %v changed=%v", err, h != h0)
	}
	app.Must(t, "ack", "a", "2")
	if h, err := w.Read(); err != nil || h == h0 {
		t.Fatalf("commit after remap not seen: %v", err)
	}
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	app.Must(t, "close", "a")
	if released, _, err = sh.Release(ctx, nil, capture.ResumeIfHeld); err != nil || !released {
		t.Fatalf("final release: %v %v", released, err)
	}
}

// A release refused because the app holds the shard leaves capture holding
// its read transaction again, so the app closing the file afterwards can't
// checkpoint and reset the WAL behind it: the next sync and release capture
// nothing new, where the node would otherwise have to write the whole
// database again.
func TestHeldReleaseKeepsTheWALForTheNextRelease(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path)
	sh := newShard(t, e, path)
	ctx := context.Background()
	app.Must(t, "insert", "a", "20", "1000")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld); err != nil || released {
		t.Fatalf("release with the app holding the shard: %v %v", released, err)
	}
	before, _ := e.entriesFor(t, "users/1")
	app.Must(t, "close", "a")
	if err := sh.Sync(ctx); err != nil { // the next flush cycle, or the messages owner's first read
		t.Fatal(err)
	}
	if released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld); err != nil || !released {
		t.Fatalf("release once the app closed it: %v %v", released, err)
	}
	if after, _ := e.entriesFor(t, "users/1"); len(after) != len(before) {
		t.Fatalf("the shard's sync and release after the app closed it captured %d files, want none", len(after)-len(before))
	}
}

// Every commit changes the header, including two within one clock tick and
// across WAL restarts (the node restarts the WAL every few frames here, so
// the log restarts many times and its salts change).
func TestWALIndexSeesEveryCommit(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path)
	w, err := capture.MapWALIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	sh, err := capture.New(capture.Config{
		TempDir: tempDir(t),
		Name:    shardName("users/1"), Path: path, Generation: "g1", Writer: e.w,
		RestartFrames: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sh.Close(context.Background())
	ctx := context.Background()
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	last := header(t, sh)
	salts := map[[8]byte]bool{}
	for i := range 400 {
		app.Must(t, "ack", "a", itoa(i))
		h := header(t, sh)
		if h == last {
			t.Fatalf("commit %d not seen", i)
		}
		last = h
		var salt [8]byte
		copy(salt[:], h[32:40])
		salts[salt] = true
		// Syncs only some of the time, so there are runs of commits between them.
		if i%7 == 0 {
			if err := sh.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			// The node's own WAL restarts change the header too (an extra
			// sync for the node, never a missed one).
			last = header(t, sh)
		}
	}
	if len(salts) < 3 {
		t.Fatalf("the WAL restarted %d times: the test is not testing restarts", len(salts)-1)
	}
	t.Logf("%d WAL restarts", len(salts)-1)
}

// header reads the shard's wal-index header.
func header(t *testing.T, sh *capture.Shard) capture.Header {
	t.Helper()
	h, err := sh.Header()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// appendHook calls before at the start of every AppendFile.
type appendHook struct {
	*journal.Writer
	before func()
}

func (w appendHook) AppendFile(name shard.Name, generation shard.Gen, position pos.Position, ltxFile tempfile.File) error {
	w.before()
	return w.Writer.AppendFile(name, generation, position, ltxFile)
}

// A header read while a sync is held up (here as the sync queues its file in
// the writer) returns at once with the header: the flush cycle and the gates
// don't wait behind the sync. What others read without the shard's loop answers
// too, and the file being queued is in TailFrom already.
func TestHeaderDuringASlowSyncAnswers(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var armed atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path)
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName("users/1"), Path: path, Generation: "g1", Writer: appendHook{e.w, func() {
		if armed.CompareAndSwap(true, false) {
			close(entered)
			<-release
		}
	}}})
	if err != nil {
		t.Fatal(err)
	}
	defer sh.Close(context.Background())
	ctx := context.Background()
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	sh.DropTail(sh.Position())
	if sh.TailFrom() != 0 {
		t.Fatal("a tail after it was dropped")
	}
	before := header(t, sh)
	pos := sh.Position()

	app.Must(t, "insert", "a", "1", "100")
	armed.Store(true)
	synced := make(chan error, 1)
	go func() { synced <- sh.Capture(ctx) }()
	waitFor(t, entered, "the sync to queue its file")
	start := time.Now()
	if h, err := sh.Header(); err != nil || h == before {
		t.Fatalf("header during a sync: %v (unchanged: %v), want the commit's header", err, h == before)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("the header took %v during a sync", took)
	}
	if sh.TailFrom() == 0 {
		t.Fatal("a file being queued is not counted in TailFrom")
	}
	if sh.Position() != pos {
		t.Fatalf("position %d before the file is queued, want %d", sh.Position(), pos)
	}
	close(release)
	if err := <-synced; err != nil {
		t.Fatal(err)
	}
	if sh.Position() <= pos {
		t.Fatal("the commit was not captured")
	}
	if header(t, sh) == before {
		t.Fatal("the commit is not seen in the header once the sync is done")
	}
}

// Once the shard is closed its mapping is gone, and the header says so.
func TestHeaderAfterCloseIsClosed(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path)
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName("users/1"), Path: path, Generation: "g1", Writer: e.w})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sh.Header(); err != nil {
		t.Fatal(err)
	}
	if err := sh.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := sh.Header(); !errors.Is(err, capture.ErrClosed) {
		t.Fatalf("header after close: %v, want ErrClosed", err)
	}
}

// A release told to go ahead lets go of a shard the app still holds, and says the file was open.
func TestReleaseGoesAheadOverAHeldFile(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path)
	sh := newShard(t, e, path)
	ctx := context.Background()
	app.Must(t, "ack", "a", "1")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	released, _, err := sh.Release(ctx, nil, capture.GoAheadIfHeld)
	if err != nil || !released {
		t.Fatalf("release over a held file: %v %v", released, err)
	}
	if !sh.OpenAtRelease() {
		t.Fatal("the release did not say the file was open")
	}
}

// A file nobody holds is released as not open, going ahead or not.
func TestReleaseGoingAheadReportsNothingOpenWhenTheFileIsClosed(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := apptest.Start(t)
	path := filepath.Join(e.dir, "users-1.db")
	app.Must(t, "create", "a", path)
	sh := newShard(t, e, path)
	ctx := context.Background()
	app.Must(t, "ack", "a", "1")
	app.Must(t, "close", "a")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	released, _, err := sh.Release(ctx, nil, capture.GoAheadIfHeld)
	if err != nil || !released {
		t.Fatalf("release of a closed file: %v %v", released, err)
	}
	if sh.OpenAtRelease() {
		t.Fatal("a file nobody had open was reported as open")
	}
}
