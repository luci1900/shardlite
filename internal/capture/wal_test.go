package capture

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
)

// openWAL opens a database in WAL mode with the given page size (0 for
// SQLite's default) and commits stmts one at a time, each as its own
// transaction. The connection stays open for the test: closing the last one
// would checkpoint the WAL into the database and delete it. While it is open
// and idle the WAL holds exactly the committed frames.
func openWAL(t *testing.T, pageSize int, stmts ...string) (string, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "t.db")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	if pageSize != 0 {
		if _, err := db.Exec(fmt.Sprintf("PRAGMA page_size=%d", pageSize)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	return path, db
}

// readAll reads every frame of the WAL at path from frame 1, checking salts and
// checksums, and returns what each frame says.
func readAll(t *testing.T, path string) (walHeader, []walFrame) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	return readAllFrom(t, f)
}

// readAllFrom reads every frame of the WAL f from frame 1. The number of
// frames comes from the file's size, so it only reads them all while the
// writer is idle.
func readAllFrom(t *testing.T, f *os.File) (walHeader, []walFrame) {
	t.Helper()
	h, err := readWALHeader(f)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	frames := int((fi.Size() - walHeaderSize) / h.frameSize())
	buf := make([]byte, h.frameSize())
	ck := h.cksum
	out := make([]walFrame, 0, frames)
	for n := 1; n <= frames; n++ {
		fr, err := h.readFrame(f, Frame(n), &ck, buf)
		if err != nil {
			t.Fatalf("read frame %d of %d: %v", n, frames, err)
		}
		out = append(out, fr)
	}
	return h, out
}

// SQLite writes the checksums in its own byte order, which on the machines
// these tests run on is the little-endian one. flipChecksumOrder returns b
// with every checksum recomputed in the other order, and the magic's low bit
// turned with it: nothing else moves, the words are only read the other way
// round.
func flipChecksumOrder(t *testing.T, b []byte) []byte {
	t.Helper()
	out := slices.Clone(b)
	var bo binary.ByteOrder
	if magic := binary.BigEndian.Uint32(out[0:4]); magic&1 == 0 {
		binary.BigEndian.PutUint32(out[0:4], magic|1)
		bo = binary.BigEndian
	} else {
		binary.BigEndian.PutUint32(out[0:4], magic&^1)
		bo = binary.LittleEndian
	}
	s0, s1 := walChecksum(bo, 0, 0, out[:24])
	binary.BigEndian.PutUint32(out[24:28], s0)
	binary.BigEndian.PutUint32(out[28:32], s1)
	pageSize := binary.BigEndian.Uint32(out[8:12])
	frameSize := walFrameHeaderSize + int(pageSize)
	for off := walHeaderSize; off+frameSize <= len(out); off += frameSize {
		fh, page := out[off:off+walFrameHeaderSize], out[off+walFrameHeaderSize:off+frameSize]
		s0, s1 = walChecksum(bo, s0, s1, fh[:8])
		s0, s1 = walChecksum(bo, s0, s1, page)
		binary.BigEndian.PutUint32(fh[16:20], s0)
		binary.BigEndian.PutUint32(fh[20:24], s1)
	}
	return out
}

// The reader reads a WAL SQLite wrote at every page size: the header's page
// size, and every frame with its checksums and commit markers.
func TestWALReaderEveryPageSize(t *testing.T) {
	for _, size := range []int{512, 1024, 2048, 4096, 8192, 16384, 32768, 65536} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			path, _ := openWAL(t, size,
				`CREATE TABLE t (x)`,
				`INSERT INTO t VALUES (1)`,
				`INSERT INTO t VALUES (2)`,
				`INSERT INTO t VALUES (3)`,
			)
			h, frames := readAll(t, path+"-wal")
			if h.pageSize != PageSize(size) {
				t.Fatalf("page size %d, want %d", h.pageSize, size)
			}
			var commits uint32
			var lastCommit Pages
			for _, fr := range frames {
				if fr.commit != 0 {
					commits, lastCommit = commits+1, fr.commit
				}
			}
			if commits != 4 {
				t.Fatalf("%d commits for four statements", commits)
			}
			if lastCommit == 0 {
				t.Fatal("the last commit frame says the database is empty")
			}
		})
	}
}

// The reader reads both checksum byte orders: the one SQLite writes, and the
// other one, which the test writes by recomputing every checksum the other
// way round.
func TestWALReaderBothChecksumOrders(t *testing.T) {
	path, _ := openWAL(t, 0,
		`CREATE TABLE t (x)`,
		`INSERT INTO t VALUES (1)`,
		`INSERT INTO t VALUES (2)`,
	)
	native, frames := readAll(t, path+"-wal")
	orig, err := os.ReadFile(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(flipChecksumOrder(t, orig)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	flipped, err := os.Open(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer flipped.Close()
	h, err := readWALHeader(flipped)
	if err != nil {
		t.Fatal(err)
	}
	if h.order == native.order {
		t.Fatal("the flipped WAL has the same checksum order as the native one")
	}
	if h.salt != native.salt || h.pageSize != native.pageSize {
		t.Fatalf("the flip moved something besides the checksums: %v vs %v", h, native)
	}
	buf := make([]byte, h.frameSize())
	ck := h.cksum
	for n, want := range frames {
		got, err := h.readFrame(flipped, Frame(n+1), &ck, buf)
		if err != nil {
			t.Fatalf("read frame %d: %v", n+1, err)
		}
		if got != want {
			t.Fatalf("frame %d: %v in the flipped WAL, %v in the native one", n+1, got, want)
		}
	}
}

// Frames read from an offset, with the running checksum picked up where the
// last read left it, match frames read from the start: a cursor stepped part
// way and continued reads the same window as one that read straight through.
func TestWALCursorFramesFromAnOffset(t *testing.T) {
	path, _ := openWAL(t, 0,
		`CREATE TABLE t (x, y)`,
		`INSERT INTO t VALUES (1, 1)`,
		`INSERT INTO t VALUES (2, 2)`,
		`UPDATE t SET y = 3`,
		`UPDATE t SET x = 4`,
		`INSERT INTO t VALUES (5, 5)`,
	)
	f, err := os.Open(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h, err := readWALHeader(f)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	frames := Frame((fi.Size() - walHeaderSize) / h.frameSize())
	if frames < 5 {
		t.Fatalf("%d frames: the test wants more to step to", frames)
	}
	straight, err := walCursorAt(f)
	if err != nil {
		t.Fatal(err)
	}
	want, err := straight.readTo(f, frames)
	if err != nil {
		t.Fatalf("read straight through: %v", err)
	}
	if want.commits == 0 {
		t.Fatal("the window holds no commits")
	}
	// Steps part way must be commit boundaries: readTo is only ever called to
	// the wal-index's mxFrame, which one of the app's commits set.
	steps := []Frame{0}
	_, fs := readAllFrom(t, f)
	for n, fr := range fs {
		if fr.commit != 0 && Frame(n+1) < frames {
			steps = append(steps, Frame(n+1))
		}
	}
	for _, step := range steps {
		stepped, err := walCursorAt(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := stepped.readTo(f, step); err != nil {
			t.Fatalf("read to frame %d: %v", step, err)
		}
		got, err := stepped.readTo(f, frames)
		if err != nil {
			t.Fatalf("read on to frame %d after %d: %v", frames, step, err)
		}
		// What the frames after the step hold, derived from their headers:
		// their commits, the last one's database size, and the newest frame
		// of each page among them.
		var wantCommits uint64
		for _, fr := range fs[step:] {
			if fr.commit != 0 {
				wantCommits++
			}
		}
		wantPf, err := straight.newestFrames(f, step)
		if err != nil {
			t.Fatal(err)
		}
		if got.commits != wantCommits || got.commit != fs[frames-1].commit || !slices.Equal(got.pf, wantPf) {
			t.Fatalf("stepping through frame %d: %d commits to commit %d, want %d to %d; %d pages against %d", step, got.commits, got.commit, wantCommits, fs[frames-1].commit, len(got.pf), len(wantPf))
		}
		// And the cursor ends where one that read straight through does, with
		// the same running checksum.
		if stepped.frames != straight.frames || stepped.ck != straight.ck {
			t.Fatalf("stepping through frame %d leaves the cursor at frame %d (checksum %x), want frame %d (checksum %x)", step, stepped.frames, stepped.ck, straight.frames, straight.ck)
		}
	}
	// A cursor of another WAL (its salts differ) is never continued: the same
	// call over the other WAL and its wal-index starts a fresh cursor, and a
	// wal-index naming one WAL rejects any other.
	other, _ := openWAL(t, 0, `CREATE TABLE t (x)`, `INSERT INTO t VALUES (1)`)
	of := mustOpen(t, other+"-wal")
	oh, err := readWALHeader(of)
	if err != nil {
		t.Fatal(err)
	}
	info := Info{Salt: oh.salt, MxFrame: 2}
	c, err := walAt(of, info, h.pageSize, &straight)
	if err != nil {
		t.Fatal(err)
	}
	if c.frames != 0 || c.ck != oh.cksum {
		t.Fatal("a cursor of another WAL was continued")
	}
	// A wal-index naming one WAL rejects any other.
	if _, err := walAt(f, info, h.pageSize, &straight); err == nil {
		t.Fatal("a wal-index naming one WAL accepted another")
	}
}

// SQLite writes the WAL's header and the wal-index's header in turn when it
// restarts the WAL, so a read that straddles the restart sees the two name
// different WALs. The pair is read again until it agrees, and the frames of the
// WAL the restart replaced are not read again: the read rule means they were
// captured before the restart could happen.
func TestWALReadRetriesAcrossARestart(t *testing.T) {
	path, _ := openWAL(t, 0,
		`CREATE TABLE t (x)`,
		`INSERT INTO t VALUES (1)`,
		`INSERT INTO t VALUES (2)`,
	)
	h, frames := readAll(t, path+"-wal")
	if len(frames) < 2 {
		t.Fatalf("%d frames: the test wants a WAL with frames to read", len(frames))
	}
	// The commits the frames hold.
	var commits uint64
	for _, fr := range frames {
		if fr.commit != 0 {
			commits++
		}
	}
	f, err := os.Open(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// The wal-index names the WAL, and the WAL on disk is the one it names,
	// until the app restarts the WAL between the two reads.
	restarted := false
	readInfo := func() (Info, error) {
		return Info{Salt: h.salt, MxFrame: Frame(len(frames))}, nil
	}
	readFrames := func(info Info) (walCursor, window, error) {
		if !restarted {
			// The app restarted the WAL between the wal-index read and this
			// one: the WAL on disk now names a different WAL.
			restarted = true
			return walCursor{}, window{}, fmt.Errorf("%w: the WAL has salts %x, the wal-index %x", errRestarted, [2]uint32{h.salt[0] + 1, h.salt[1]}, info.Salt)
		}
		c, err := walCursorAt(f)
		if err != nil {
			return walCursor{}, window{}, err
		}
		w, err := c.readTo(f, info.MxFrame)
		if err != nil {
			return walCursor{}, window{}, err
		}
		return c, w, nil
	}
	c, info, w, err := walRead(readInfo, readFrames)
	if err != nil {
		t.Fatalf("read across the restart: %v", err)
	}
	if !restarted {
		t.Fatal("the restart was not seen")
	}
	if info.Salt != h.salt {
		t.Fatalf("the wal-index read names salts %x, want %x", info.Salt, h.salt)
	}
	if c.h.salt != h.salt {
		t.Fatalf("the cursor names salts %x, want %x", c.h.salt, h.salt)
	}
	if w.commits != commits {
		t.Fatalf("the window holds %d commits, want %d", w.commits, commits)
	}
}

// A read that keeps seeing a restart gives up rather than retrying for ever.
func TestWALReadGivesUpAfterManyRestarts(t *testing.T) {
	reads := 0
	readInfo := func() (Info, error) {
		reads++
		return Info{Salt: [2]uint32{1, 2}, MxFrame: 1}, nil
	}
	readFrames := func(Info) (walCursor, window, error) {
		return walCursor{}, window{}, fmt.Errorf("%w: the WAL has salts %x, the wal-index %x", errRestarted, [2]uint32{3, 4}, [2]uint32{1, 2})
	}
	_, _, _, err := walRead(readInfo, readFrames)
	if !errors.Is(err, errRestarted) {
		t.Fatalf("the read gave up with %v, want the restart", err)
	}
	if reads != walRestartTries+1 {
		t.Fatalf("the pair was read %d times, want %d", reads, walRestartTries+1)
	}
}

// An error that is not a restart is not retried: the read gives up at once.
func TestWALReadDoesNotRetryOtherErrors(t *testing.T) {
	reads := 0
	readInfo := func() (Info, error) {
		reads++
		return Info{MxFrame: 1}, nil
	}
	readFrames := func(Info) (walCursor, window, error) {
		return walCursor{}, window{}, errors.New("the WAL is corrupt")
	}
	if _, _, _, err := walRead(readInfo, readFrames); err == nil {
		t.Fatal("the read succeeded")
	}
	if reads != 1 {
		t.Fatalf("the pair was read %d times, want 1", reads)
	}
}

// A torn last frame ends the read at the last good commit: every frame before
// it reads, and the torn one fails (it is only half there, or its checksum
// doesn't match what was read).
func TestWALReaderTornLastFrame(t *testing.T) {
	path, _ := openWAL(t, 0,
		`CREATE TABLE t (x)`,
		`INSERT INTO t VALUES (1)`,
		`INSERT INTO t VALUES (2)`,
	)
	h, frames := readAll(t, path+"-wal")
	n := Frame(len(frames))
	if n < 3 {
		t.Fatalf("%d frames: the test wants a frame before the last to check", n)
	}
	last := func(b []byte) int { return len(b) - walFrameHeaderSize - int(h.pageSize) }
	for _, tc := range []struct {
		name string
		mut  func(b []byte) []byte
	}{
		{"truncated", func(b []byte) []byte { return b[:len(b)-10] }},
		{"a flipped byte in the page", func(b []byte) []byte {
			b[len(b)-1] ^= 0xff
			return b
		}},
		{"a flipped byte in the checksum", func(b []byte) []byte {
			b[last(b)+16] ^= 0xff
			return b
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orig, err := os.ReadFile(path + "-wal")
			if err != nil {
				t.Fatal(err)
			}
			w := filepath.Join(t.TempDir(), "wal")
			if err := os.WriteFile(w, tc.mut(orig), 0o644); err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(w)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			hh, err := readWALHeader(f)
			if err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, hh.frameSize())
			ck := hh.cksum
			good := Frame(0)
			for m := Frame(1); m <= n; m++ {
				if _, err := hh.readFrame(f, m, &ck, buf); err != nil {
					break // the read ends at the last good frame
				}
				good = m
			}
			if good != n-1 {
				t.Fatalf("the read ended at frame %d, want the last good one %d", good, n-1)
			}
		})
	}
}

// A frame left over from before a WAL restart carries the old salts, and ends
// the read at the last good commit. SQLite restarts the WAL in place (new
// salts, frames from the start again) once a checkpoint has copied everything,
// so the file still holds the old frames beyond the new end.
func TestWALReaderEndsAtOldSalts(t *testing.T) {
	path, db := openWAL(t, 0,
		`CREATE TABLE t (x)`,
		`INSERT INTO t VALUES (1)`,
		`INSERT INTO t VALUES (2)`,
	)
	before, frames := readAll(t, path+"-wal")
	var busy, log, done int
	if err := db.QueryRow(`PRAGMA wal_checkpoint(PASSIVE)`).Scan(&busy, &log, &done); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (3)`); err != nil {
		t.Fatal(err)
	}
	// The restart has to have happened, or the test is not testing one.
	h, err := readWALHeader(mustOpen(t, path+"-wal"))
	if err != nil {
		t.Fatal(err)
	}
	if h.salt == before.salt {
		t.Fatal("the WAL still has the salts it had before the checkpoint and the write")
	}
	f, err := os.Open(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, h.frameSize())
	ck := h.cksum
	good := Frame(0)
	for n := Frame(1); n <= Frame(len(frames))+1; n++ {
		if _, err := h.readFrame(f, n, &ck, buf); err != nil {
			break // at the first frame the old WAL left behind
		}
		good = n
	}
	if good != 1 {
		t.Fatalf("the read ended at frame %d, want the one frame the restarted WAL holds", good)
	}
}

// The map gives each page's newest frame after one frame and at or below
// another: a cursor stepped to each of the commit boundaries it can sit at
// (the wal-index's mxFrame is always one), asked for the frames after each of
// several points, matches a reference built from the frame headers.
func TestWALCursorNewestFrames(t *testing.T) {
	path, _ := openWAL(t, 0,
		`CREATE TABLE t (x)`,
		`INSERT INTO t VALUES (1)`,
		`UPDATE t SET x = 2`,
		`UPDATE t SET x = 3`,
		`UPDATE t SET x = 4`,
		`UPDATE t SET x = 5`,
	)
	_, fs := readAll(t, path+"-wal")
	f, err := os.Open(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	n := Frame(len(fs))
	if n < 4 {
		t.Fatalf("%d frames: the test wants boundaries to pick", n)
	}
	// The boundaries a cursor can sit at: every frame that ends a commit.
	var bounds []Frame
	for m, fr := range fs {
		if fr.commit != 0 {
			bounds = append(bounds, Frame(m+1))
		}
	}
	// The reference: the newest frame of each page among frames lo+1..hi.
	newest := func(lo, hi Frame) map[Pgno]Frame {
		out := map[Pgno]Frame{}
		for m := lo + 1; m <= hi; m++ {
			out[fs[m-1].pgno] = m
		}
		return out
	}
	for _, hi := range bounds {
		c, err := walCursorAt(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.readTo(f, hi); err != nil {
			t.Fatal(err)
		}
		for _, lo := range []Frame{0, 1, hi / 2, hi - 1} {
			pf, err := c.newestFrames(f, lo)
			if err != nil {
				t.Fatal(err)
			}
			ref := newest(lo, hi)
			if len(pf) != len(ref) {
				t.Fatalf("after frame %d to %d: %d pages, want %d", lo, hi, len(pf), len(ref))
			}
			for p, want := range ref {
				if got := pf.frame(p); got != want {
					t.Fatalf("after frame %d to %d: page %d is at frame %d, want %d", lo, hi, p, got, want)
				}
			}
			// Pages whose newest frame is below the window are not in the map.
			for p := range newest(0, n) {
				if _, ok := ref[p]; !ok && pf.frame(p) != 0 {
					t.Fatalf("after frame %d to %d: page %d has frame %d, want none", lo, hi, p, pf.frame(p))
				}
			}
		}
	}
}

// mustOpen opens a file or fails the test.
func mustOpen(t *testing.T, path string) *os.File {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}
