package capture_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/model/pos"
	"github.com/luci1900/shardlite/internal/storage/journal"
	"github.com/luci1900/shardlite/internal/storage/shardfile"
	"github.com/luci1900/shardlite/internal/test/apptest"
	"github.com/luci1900/shardlite/internal/test/harness"
	"github.com/luci1900/shardlite/internal/test/sqlitetest"
)

const name = "users/7"

func entriesOf(t *testing.T, e *harness.Env, shard string) (out []journal.Entry) {
	t.Helper()
	err := journal.Scan(context.Background(), e.Journal, 1, 0, func(_ pos.Seg, en journal.Entry) error {
		if en.Shard.String() == shard {
			out = append(out, en)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// 6.3: no frame is checkpointed away or wiped by a WAL restart before it is
// captured. Bursts of writes, syncs at random points, the app's checkpoints in
// every mode between them (all of which find the node's read transaction in the
// way), an app that leaves checkpointing to its automatic ones or turns them
// off entirely, and the node's own restart of the WAL at several sizes.
// Replaying what was captured must give the app's database.
func TestCheckpointsNeverLoseFrames(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		restart capture.Frame // frames before the node restarts the WAL (0 for the default)
		rows    int           // rows of 1 KB per burst
		manual  bool          // the app checkpoints by hand between commits; off, its automatic ones are turned off too
	}{
		{"default threshold, big bursts", 0, 1400, true}, // a burst is over the default 1,000 frames
		{"small threshold", 20, 20, true},
		{"tiny threshold", 3, 30, true},
		{"automatic checkpoints off", 0, 1400, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := harness.New(t)
			ctx := context.Background()
			app := e.StartApp(name)
			if tc.manual {
				// The checkpoints find the read transaction in the way by
				// design, so the app's busy timeout would spend five seconds
				// on every one of them. A short one keeps the attempts quick:
				// what is under test is that a checkpoint that cannot restart
				// the WAL loses nothing, not how long the wait takes.
				app.Must(t, "exec", "a", "PRAGMA busy_timeout=100")
			} else {
				app.Must(t, "exec", "a", "PRAGMA wal_autocheckpoint=0")
			}
			h, _, err := e.Files.Open(ctx, shardName(name))
			if err != nil {
				t.Fatal(err)
			}
			sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName(name), Path: e.Path(name), Generation: h.Manifest.Generation, Writer: e.W, RestartFrames: tc.restart})
			if err != nil {
				t.Fatal(err)
			}
			for burst := range 12 {
				for range 3 {
					app.Must(t, "insert", "a", fmt.Sprint(tc.rows/3), "1000")
					if tc.manual {
						app.Must(t, "ckpt", "a", []string{"PASSIVE", "RESTART", "TRUNCATE"}[burst%3])
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
			err = journal.Scan(ctx, e.Journal, 1, 0, func(seq pos.Seg, en journal.Entry) error {
				if en.Shard.String() != name {
					return nil
				}
				b, err := harness.ReadEntry(ctx, e.Journal, seq, en)
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

// A byte flipped in a frame past the last commit captured fails the sync and
// captures nothing past the last good commit.
func TestCorruptFrameFailsTheSync(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	_, sh := e.NewShard(name)
	app.Must(t, "insert", "a", "5", "100")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	pos := sh.Position()
	before := len(entriesOf(t, e, name))
	app.Must(t, "insert", "a", "10", "100") // committed past the sync, not captured
	// Flip a byte in the last frame's page: the last frame in the file is the
	// last commit's, the writer being idle.
	f, err := os.OpenFile(e.Path(name)+"-wal", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, fi.Size()-1); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b, fi.Size()-1); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := sh.Sync(ctx); err == nil {
		t.Fatal("the sync of a corrupt WAL succeeded")
	}
	if sh.Position() != pos {
		t.Fatalf("position moved from %d to %d past a corrupt frame", pos, sh.Position())
	}
	if got := len(entriesOf(t, e, name)); got != before {
		t.Fatalf("%d entries captured past a corrupt frame, want %d", got-before, before)
	}
	if err := sh.Close(ctx); err == nil {
		t.Fatal("the final sync of a corrupt WAL succeeded")
	}
}

// A WAL restart between two syncs: the first sync, over the restart threshold,
// checkpoints everything under the write lock, and the app's next write
// restarts the WAL (new salts, frames from the start again). The second sync
// reads the new WAL from its first frame, the positions carry on and the
// replay matches.
func TestSyncAfterARestartReadsTheWALFromItsFirstFrame(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	h, _, _ := e.Files.Open(ctx, shardName(name))
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName(name), Path: e.Path(name), Generation: h.Manifest.Generation, Writer: e.W, RestartFrames: 20})
	if err != nil {
		t.Fatal(err)
	}
	for range 30 { // enough frames to pass the threshold, one commit each
		app.Must(t, "insert", "a", "1", "300")
	}
	old := header(t, sh)
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	pos := sh.Position()
	app.Must(t, "insert", "a", "10", "300") // the write after the checkpoint restarts the WAL
	restarted := header(t, sh)
	if string(old[32:40]) == string(restarted[32:40]) {
		t.Fatal("the app's write after the sync's checkpoint did not restart the WAL: the test is not testing a restart between two syncs")
	}
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if sh.Position() <= pos {
		t.Fatal("the second sync captured nothing from the restarted WAL")
	}
	replayed := filepath.Join(t.TempDir(), "replayed.db")
	replayInto(t, e, replayed)
	if want, got := apptest.Dump(t, e.Path(name)), sqlitetest.Dump(t, replayed); want != got {
		t.Fatal("the history across a WAL restart does not restore to the app's database")
	}
	sh.Close(ctx)
}

// An app that commits as fast as it can, with the node checking the WAL on
// every sync (a locked restart tries a short busy timeout and is tried again
// at the next sync): the WAL stays within a small multiple of its restart
// threshold. The longest wait of one of the app's writes for the node is
// recorded, and the replay matches the app's database.
func TestUnthrottledWriterKeepsTheWALBounded(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	h, _, _ := e.Files.Open(ctx, shardName(name))
	const restartFrames = 400
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName(name), Path: e.Path(name), Generation: h.Manifest.Generation, Writer: e.W, RestartFrames: restartFrames})
	if err != nil {
		t.Fatal(err)
	}
	const writes = 3000
	type report struct {
		err     error
		longest time.Duration
	}
	done := make(chan report, 1)
	go func() {
		var r report
		for range writes {
			start := time.Now()
			if _, err := app.Do("insert", "a", "1", "100"); err != nil {
				r.err = err
				break
			}
			if d := time.Since(start); d > r.longest {
				r.longest = d
			}
		}
		done <- r
	}()
	pageSize, err := strconv.Atoi(apptest.Query(t, e.Path(name), `PRAGMA page_size`))
	if err != nil {
		t.Fatal(err)
	}
	threshold := int64(restartFrames) * int64(pageSize)
	var peak int64
	var r report
	for stopped := false; !stopped; {
		select {
		case r = <-done:
			stopped = true
		default:
		}
		if err := sh.Capture(ctx); err != nil {
			t.Fatal(err)
		}
		if fi, err := os.Stat(e.Path(name) + "-wal"); err == nil && fi.Size() > peak {
			peak = fi.Size()
		}
	}
	if r.err != nil {
		t.Fatal(r.err)
	}
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("an unthrottled writer, %d writes: peak WAL of %d bytes (%.1fx the restart threshold of %d frames), the longest write waited %v", writes, peak, float64(peak)/float64(threshold), restartFrames, r.longest)
	// The bound catches the design the spike ruled out (a timer-checked
	// restart, whose WAL ran to gigabytes), not load: a loaded machine can
	// hold the node's restarts back for a while, which only grows the WAL,
	// so the multiple is loose and the number above says what happened.
	if peak > 8*threshold {
		t.Fatalf("the WAL reached %d bytes, %.1fx the restart threshold of %d frames", peak, float64(peak)/float64(threshold), restartFrames)
	}
	replayed := filepath.Join(t.TempDir(), "replayed.db")
	replayInto(t, e, replayed)
	if want, got := apptest.Dump(t, e.Path(name)), sqlitetest.Dump(t, replayed); want != got {
		t.Fatal("the history of an unthrottled writer does not restore to the app's database")
	}
	sh.Close(ctx)
}

// The -shm check, in the order docs/internals.md gives: the node closes everything it
// has on the shard, then asks, then reopens capture if the app still has the
// shard open.
func TestReleaseChecksShm(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	h, _, _ := e.Files.Open(ctx, shardName(name))
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName(name), Path: e.Path(name), Generation: h.Manifest.Generation, Writer: e.W})
	if err != nil {
		t.Fatal(err)
	}
	app.Must(t, "insert", "a", "5", "100")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	obs := apptest.Start(t) // a third process, to see who holds the shard

	// The app has the shard open: the node doesn't let go, and capture carries on where it was.
	if released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld); err != nil || released {
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
		if released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld); err != nil || released {
			t.Fatalf("app open: released=%v err=%v", released, err)
		}
	}
	if v := obs.Must(t, "probe", "a", e.Path(name)); !strings.HasPrefix(v, "held") {
		t.Fatalf("observer sees %s while app and node have the shard open", v)
	}

	// The app closes its connection: now the node can let go.
	app.Must(t, "close", "a")
	if released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld); err != nil || !released {
		t.Fatalf("app closed: released=%v err=%v", released, err)
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
	h, _, _ := e.Files.Open(ctx, shardName(name))
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName(name), Path: e.Path(name), Generation: h.Manifest.Generation, Writer: e.W})
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
		if held, err := capture.WALSidecarsOf(e.Path(name)).Held(); err != nil || !held {
			t.Fatalf("check %d: held=%v err=%v", i, held, err)
		}
		if v := obs.Must(t, "probe", "a", e.Path(name)); !strings.HasPrefix(v, "held") {
			t.Fatalf("check %d dropped the app's lock: observer sees %s", i, v)
		}
	}
	app.Must(t, "close", "a")
	if held, err := capture.WALSidecarsOf(e.Path(name)).Held(); err != nil || held {
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
	h, _, _ := e.Files.Open(ctx, shardName(name))
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName(name), Path: e.Path(name), Generation: h.Manifest.Generation, Writer: e.W})
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

// A commit the app makes after the final sync, closing the file before
// the -shm check, is seen from the wal-index header: the node doesn't let go,
// and capture picks the commit up.
func TestReleaseSeesACommitAfterTheFinalSync(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	h, _, _ := e.Files.Open(ctx, shardName(name))
	late := false
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName(name), Path: e.Path(name), Generation: h.Manifest.Generation, Writer: e.W,
		Durable: func(ctx context.Context) error {
			if late {
				// Between the connections closing and the -shm check.
				late = false
				app.Must(t, "create", "a", e.Path(name))
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
	at := sh.Position()

	late = true
	if released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld); err != nil || released {
		t.Fatalf("commit after the final sync: released=%v err=%v", released, err)
	}
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if sh.Position() <= at {
		t.Fatal("the late commit was not captured after capture resumed")
	}
	// The late commit landed while the node held nothing, so the resumed
	// release could not trust the WAL: it wrote the whole database as one
	// file, which is the one entry past the position the release stopped at.
	var en journal.Entry
	var seq pos.Seg
	if err := journal.Scan(ctx, e.Journal, 1, 0, func(s pos.Seg, en2 journal.Entry) error {
		if en2.Shard.String() == name && en2.Position > at {
			en, seq = en2, s
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seq == 0 {
		t.Fatal("the late commit was not captured as a file of its own")
	}
	b, err := harness.ReadEntry(ctx, e.Journal, seq, en)
	if err != nil {
		t.Fatal(err)
	}
	if pages, commit := pageCount(t, b); pages != commit {
		t.Fatalf("the file after the refused release holds %d of the database's %d pages: the whole database was not written", pages, commit)
	}
	replayed := filepath.Join(t.TempDir(), "replayed.db")
	replayInto(t, e, replayed)
	if want, got := apptest.Dump(t, e.Path(name)), sqlitetest.Dump(t, replayed); want != got {
		t.Fatal("the history after a commit the final sync missed does not restore to the app's database")
	}
	// With nothing new, the node lets go.
	if released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld); err != nil || !released {
		t.Fatalf("nothing new: released=%v err=%v", released, err)
	}
}

// walSalts is the salt pair a wal-index header carries (the WAL's own, which
// change when SQLite restarts the WAL).
func walSalts(h capture.Header) [8]byte {
	var s [8]byte
	copy(s[:], h[32:40])
	return s
}

// walFrames is the wal-index header's mxFrame: how many frames of its WAL are
// committed.
func walFrames(h capture.Header) uint32 { return binary.NativeEndian.Uint32(h[16:20]) }

// walFileInfo reads a WAL file's own header: its salts and how many frames
// the FILE holds, stale ones included (a restarted WAL is rewritten from its
// first frame in place, so the file keeps its high-water size).
func walFileInfo(t *testing.T, path string) (salts [8]byte, fileFrames int64) {
	t.Helper()
	b, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < 32 {
		t.Fatalf("%s-wal is %d bytes, shorter than a WAL header", path, len(b))
	}
	copy(salts[:], b[16:24])
	frameSize := int64(24 + binary.BigEndian.Uint32(b[8:12]))
	fileFrames = int64(len(b)-32) / frameSize
	return salts, fileFrames
}

// The release window with a storm in it, the #1540 shape: while capture holds
// nothing (the connections are closed, the -shm check still to come) the app's
// automatic checkpoints, set small here, restart the WAL several times, and
// none of the generations grows as far as the frames capture had read.
// journal_size_limit is left unset, so the file keeps its high-water size and
// stale frames sit past the new end, which is what made a position-and-size
// check pass wrongly in the production report. Nothing here may be decided
// from sizes: the resumed release proves the WAL from its salts, the proof
// fails, and the whole database is written as one file counted as one commit.
// The replayed history is the app's database.
func TestReleaseResumesAcrossARestartStorm(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	h, _, _ := e.Files.Open(ctx, shardName(name))
	storm := false
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName(name), Path: e.Path(name), Generation: h.Manifest.Generation, Writer: e.W,
		Durable: func(ctx context.Context) error {
			if storm {
				// Between the connections closing and the -shm check: the app
				// holds the file alone. Twenty small commits against an
				// automatic checkpoint of four pages restart the WAL every
				// few of them, several times over.
				storm = false
				app.Must(t, "exec", "a", "PRAGMA wal_autocheckpoint=4")
				for range 20 {
					app.Must(t, "insert", "a", "1", "100")
				}
			}
			return e.W.Flush(ctx)
		}})
	if err != nil {
		t.Fatal(err)
	}
	// One commit of many pages, so the WAL capture reads to is far longer
	// than any generation the storm leaves behind.
	app.Must(t, "insert", "a", "5", "20000")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	at := sh.Position()
	before := header(t, sh)
	beforeFrames := walFrames(before)
	storm = true
	if released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld); err != nil || released {
		t.Fatalf("release in a restart storm: released=%v err=%v", released, err)
	}
	// The resumed sync queued its file after the release's own wait for
	// durability, so the writer is flushed here before the journal is read.
	if err := e.W.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	after := header(t, sh)
	afterSalts, afterFileFrames := walFileInfo(t, e.Path(name))
	if walSalts(before) == walSalts(after) || walSalts(after) != afterSalts {
		t.Fatal("the WAL of the wal-index and the WAL file do not agree, or was not restarted in the window")
	}
	if afterFrames := walFrames(after); afterFrames >= beforeFrames {
		t.Fatalf("the storm's last generation holds %d frames, not fewer than the %d capture had read", afterFrames, beforeFrames)
	}
	if afterFileFrames <= int64(walFrames(after)) {
		t.Fatalf("the WAL file holds %d frames for %d live ones: it lost its high-water size, so the test is not testing the size trap", afterFileFrames, walFrames(after))
	}
	// The resumed sync wrote the whole database: one file past the position
	// the release stopped at, every page of it, counted as one commit.
	var en journal.Entry
	var seq pos.Seg
	if err := journal.Scan(ctx, e.Journal, 1, 0, func(s pos.Seg, en2 journal.Entry) error {
		if en2.Shard.String() == name && en2.Position > at {
			if en.Position != 0 {
				t.Fatalf("two files past %d (%d and %d), want one whole-database file", at, en.Position, en2.Position)
			}
			en, seq = en2, s
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seq == 0 {
		t.Fatal("the storm's commits were not captured as a file of their own")
	}
	if en.Position != at+1 || en.Count != 1 {
		t.Fatalf("the file past %d is %d+%d, want %d+1", at, en.Position, en.Count, at+1)
	}
	b, err := harness.ReadEntry(ctx, e.Journal, seq, en)
	if err != nil {
		t.Fatal(err)
	}
	if pages, commit := pageCount(t, b); pages != commit {
		t.Fatalf("the file after the storm holds %d of the database's %d pages: the whole database was not written", pages, commit)
	}
	if got := sh.Position(); got != at+1 {
		t.Fatalf("position %d after the storm, want %d advanced by the one commit alone", got, at+1)
	}
	replayed := filepath.Join(t.TempDir(), "replayed.db")
	replayInto(t, e, replayed)
	if want, got := apptest.Dump(t, e.Path(name)), sqlitetest.Dump(t, replayed); want != got {
		t.Fatal("the history across the restart storm does not restore to the app's database")
	}
	// With the app closed and nothing new, the node lets go.
	app.Must(t, "close", "a")
	if released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld); err != nil || !released {
		t.Fatalf("release after the storm: released=%v err=%v", released, err)
	}
}

// The truncate in the window: between the final sync and the resumed one the
// app runs wal_checkpoint(TRUNCATE), which empties the WAL and starts a new
// one, then commits pages the new WAL never holds (the truncate copied the
// earlier commits into the database file, and the new WAL never sees them).
// The salts say the WAL is not the one capture was reading, so the proof
// fails and the whole database is written as one file, those pages carried
// from the database file. The replay equals the app's database.
func TestReleaseResumesAcrossATruncate(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	h, _, _ := e.Files.Open(ctx, shardName(name))
	truncate := false
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName(name), Path: e.Path(name), Generation: h.Manifest.Generation, Writer: e.W,
		Durable: func(ctx context.Context) error {
			if truncate {
				truncate = false
				// busy first in the reply: 0 says the truncate ran and emptied
				// the WAL rather than finding the file busy.
				if reply := app.Must(t, "ckpt", "a", "TRUNCATE"); !strings.HasPrefix(reply, "0,") {
					t.Fatalf("wal_checkpoint(TRUNCATE) answered %s, want busy 0", reply)
				}
				for range 3 {
					app.Must(t, "insert", "a", "1", "100")
				}
			}
			return e.W.Flush(ctx)
		}})
	if err != nil {
		t.Fatal(err)
	}
	app.Must(t, "insert", "a", "5", "20000")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	at := sh.Position()
	before := header(t, sh)
	_, beforeFileFrames := walFileInfo(t, e.Path(name))
	truncate = true
	if released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld); err != nil || released {
		t.Fatalf("release across the truncate: released=%v err=%v", released, err)
	}
	// The resumed sync queued its file after the release's own wait for
	// durability, so the writer is flushed here before the journal is read.
	if err := e.W.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	after := header(t, sh)
	afterSalts, afterFileFrames := walFileInfo(t, e.Path(name))
	if walSalts(before) == walSalts(after) || walSalts(after) != afterSalts {
		t.Fatal("the WAL was not truncated and restarted in the window")
	}
	// The truncate cut the file down to the new commits' frames, far fewer
	// than it held before: whatever reads the WAL by its file size reads a
	// shorter WAL than the one capture had read to.
	if afterFileFrames >= beforeFileFrames || afterFileFrames > 6 {
		t.Fatalf("the WAL file holds %d frames after the truncate, want the new commits' few (was %d)", afterFileFrames, beforeFileFrames)
	}
	// The whole database as one file, counted as one commit: one entry past
	// the position the release stopped at.
	var en journal.Entry
	var seq pos.Seg
	if err := journal.Scan(ctx, e.Journal, 1, 0, func(s pos.Seg, en2 journal.Entry) error {
		if en2.Shard.String() == name && en2.Position > at {
			if en.Position != 0 {
				t.Fatalf("two files past %d (%d and %d), want one whole-database file", at, en.Position, en2.Position)
			}
			en, seq = en2, s
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seq == 0 {
		t.Fatal("the commits past the truncate were not captured as a file of their own")
	}
	if en.Position != at+1 || en.Count != 1 {
		t.Fatalf("the file past %d is %d+%d, want %d+1", at, en.Position, en.Count, at+1)
	}
	b, err := harness.ReadEntry(ctx, e.Journal, seq, en)
	if err != nil {
		t.Fatal(err)
	}
	pages, commit := pageCount(t, b)
	if pages != commit {
		t.Fatalf("the file after the truncate holds %d of the database's %d pages: the whole database was not written", pages, commit)
	}
	// The new WAL never held most of those pages (its few frames are the
	// window's commits), so the file carried them from the database file.
	if int64(commit) <= afterFileFrames {
		t.Fatalf("the database is %d pages and the new WAL %d frames: the truncate did not leave pages for the database file to carry", commit, afterFileFrames)
	}
	if got := sh.Position(); got != at+1 {
		t.Fatalf("position %d after the truncate, want %d advanced by the one commit alone", got, at+1)
	}
	replayed := filepath.Join(t.TempDir(), "replayed.db")
	replayInto(t, e, replayed)
	if want, got := apptest.Dump(t, e.Path(name)), sqlitetest.Dump(t, replayed); want != got {
		t.Fatal("the history across the truncate does not restore to the app's database")
	}
	app.Must(t, "close", "a")
	if released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld); err != nil || !released {
		t.Fatalf("release after the truncate: released=%v err=%v", released, err)
	}
}

// The same-WAL resume, the good branch beside the storm and the truncate:
// the app keeps the shard open through the window, so the checkpoint a last
// connection closing runs is not the node's to trigger (the app's connection
// is not the last), and with the app's automatic checkpoints off its commits
// stay in the same WAL, salts and all. The resumed release's proof holds
// (the same salts, at least the frames capture had read), so the sync
// captures only the new frames: one small file, the position advanced by
// them alone, not a whole database.
func TestReleaseResumesFromTheSameWAL(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	h, _, _ := e.Files.Open(ctx, shardName(name))
	commit := false
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName(name), Path: e.Path(name), Generation: h.Manifest.Generation, Writer: e.W,
		Durable: func(ctx context.Context) error {
			if commit {
				commit = false
				// Nothing in the window may restart the WAL: the automatic
				// checkpoints off, three small commits.
				app.Must(t, "exec", "a", "PRAGMA wal_autocheckpoint=0")
				for range 3 {
					app.Must(t, "insert", "a", "1", "100")
				}
			}
			return e.W.Flush(ctx)
		}})
	if err != nil {
		t.Fatal(err)
	}
	app.Must(t, "insert", "a", "5", "20000")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	at := sh.Position()
	before := header(t, sh)
	commit = true
	if released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld); err != nil || released {
		t.Fatalf("release with the app committing in the window: released=%v err=%v", released, err)
	}
	// The resumed sync queued its file after the release's own wait for
	// durability, so the writer is flushed here before the journal is read.
	if err := e.W.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	after := header(t, sh)
	if walSalts(before) != walSalts(after) {
		t.Fatal("the WAL did not survive the window: the commits did not stay in the same one")
	}
	if got, want := walFrames(after), walFrames(before)+3; got != want {
		t.Fatalf("the WAL grew to %d frames in the window, want %d", got, want)
	}
	// One delta of the new commits: one file past the release's position,
	// three commits, and pages fewer than the database holds (not a whole
	// database write, which is the other branch).
	var en journal.Entry
	var seq pos.Seg
	if err := journal.Scan(ctx, e.Journal, 1, 0, func(s pos.Seg, en2 journal.Entry) error {
		if en2.Shard.String() == name && en2.Position > at {
			if en.Position != 0 {
				t.Fatalf("two files past %d (%d and %d), want one", at, en.Position, en2.Position)
			}
			en, seq = en2, s
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seq == 0 {
		t.Fatal("the window's commits were not captured as a file of their own")
	}
	if en.Position != at+1 || en.Count != 3 {
		t.Fatalf("the file past %d is %d+%d, want %d+3", at, en.Position, en.Count, at+1)
	}
	b, err := harness.ReadEntry(ctx, e.Journal, seq, en)
	if err != nil {
		t.Fatal(err)
	}
	if pages, commitPages := pageCount(t, b); pages >= commitPages {
		t.Fatalf("the resumed file holds %d pages of a %d page database: the whole database was written where only the new frames were wanted", pages, commitPages)
	}
	if got := sh.Position(); got != at+3 {
		t.Fatalf("position %d after the resume, want %d advanced by the three commits alone", got, at+3)
	}
	replayed := filepath.Join(t.TempDir(), "replayed.db")
	replayInto(t, e, replayed)
	if want, got := apptest.Dump(t, e.Path(name)), sqlitetest.Dump(t, replayed); want != got {
		t.Fatal("the history across the same-WAL resume does not restore to the app's database")
	}
	app.Must(t, "close", "a")
	if released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld); err != nil || !released {
		t.Fatalf("release after the resume: released=%v err=%v", released, err)
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
	h, _, _ := e.Files.Open(ctx, shardName(name))
	errStorage := errors.New("storage down")
	fail := true
	sh, err := capture.New(capture.Config{TempDir: tempDir(t), Name: shardName(name), Path: e.Path(name), Generation: h.Manifest.Generation, Writer: e.W,
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
	if released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld); !errors.Is(err, errStorage) || released {
		t.Fatalf("release whose wait failed: released=%v err=%v", released, err)
	}
	if !sh.Open() {
		t.Fatal("capture was not reopened")
	}
	pos := sh.Position()
	app.Must(t, "create", "a", e.Path(name))
	app.Must(t, "insert", "a", "5", "100")
	app.Must(t, "close", "a")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if sh.Position() <= pos {
		t.Fatal("a commit after the failed release was not captured")
	}
	if released, _, err := sh.Release(ctx, nil, capture.ResumeIfHeld); err != nil || !released {
		t.Fatalf("second release: released=%v err=%v", released, err)
	}
	if sh.Open() {
		t.Fatal("open after it was released")
	}
}

// The writer refuses a fenced journal for good, so closing a shard whose
// writer has been fenced must fail at once rather than hold the shard's loop.
func TestCloseWithAFencedWriterDoesNotRetry(t *testing.T) {
	t.Parallel()
	e := harness.New(t)
	ctx := context.Background()
	app := e.StartApp(name)
	h, _, _ := e.Files.Open(ctx, shardName(name))
	sh := e.Activate(h, nil)
	app.Must(t, "insert", "a", "5", "100")
	if err := sh.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// An adopter fences the journal, and the writer finds out at its next flush.
	adopter := e.Adopter("n2", e.Node)
	if _, err := journal.Fence(ctx, adopter, 1); err != nil {
		t.Fatal(err)
	}
	app.Must(t, "insert", "a", "6", "100")
	if err := sh.Sync(ctx); !errors.Is(err, journal.ErrFenced) {
		t.Fatalf("sync after the fence: %v, want ErrFenced", err)
	}
	// The final sync has a commit to queue, and the writer refuses it.
	app.Must(t, "insert", "a", "7", "100")
	done := make(chan error, 1)
	go func() { done <- sh.Close(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, journal.ErrFenced) {
			t.Fatalf("close: %v, want ErrFenced", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("close is still retrying its final sync")
	}
}
