package capture

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"
)

// SQLite's WAL format (https://sqlite.org/fileformat2.html#walformat): a 32-byte
// header, then frames of a 24-byte header followed by a page. All the integers
// in the file are big-endian. The checksums are computed over words read in the
// byte order the magic number names, and run on from one frame to the next.
const (
	walHeaderSize      = 32
	walFrameHeaderSize = 24
	walMagicLittle     = 0x377f0682 // the checksums' words are little-endian
	walMagicBig        = 0x377f0683 // the low bit says they are big-endian
	walVersion         = 3007000
)

// walChecksum is SQLite's WAL checksum of b (whose length is a multiple of 8),
// continuing from (s0, s1).
func walChecksum(bo binary.ByteOrder, s0, s1 uint32, b []byte) (uint32, uint32) {
	for i := 0; i+8 <= len(b); i += 8 {
		s0 += bo.Uint32(b[i:]) + s1
		s1 += bo.Uint32(b[i+4:]) + s0
	}
	return s0, s1
}

// walHeader is the 32-byte header of a WAL file.
type walHeader struct {
	pageSize PageSize
	salt     [2]uint32
	order    binary.ByteOrder // of the checksum words
	cksum    [2]uint32        // the header's checksum, which the first frame's runs on from
}

// readWALHeader reads and checks the header of the WAL file f.
func readWALHeader(f io.ReaderAt) (walHeader, error) {
	var b [walHeaderSize]byte
	if _, err := f.ReadAt(b[:], 0); err != nil {
		return walHeader{}, fmt.Errorf("capture: read the WAL header: %w", err)
	}
	var h walHeader
	switch magic := binary.BigEndian.Uint32(b[0:]); magic {
	case walMagicLittle:
		h.order = binary.LittleEndian
	case walMagicBig:
		h.order = binary.BigEndian
	default:
		return walHeader{}, fmt.Errorf("capture: the WAL has magic %#x", magic)
	}
	if v := binary.BigEndian.Uint32(b[4:]); v != walVersion {
		return walHeader{}, fmt.Errorf("capture: the WAL has version %d", v)
	}
	h.pageSize = PageSize(binary.BigEndian.Uint32(b[8:]))
	if h.pageSize < 512 || h.pageSize > 65536 || h.pageSize&(h.pageSize-1) != 0 {
		return walHeader{}, fmt.Errorf("capture: the WAL has page size %d", h.pageSize)
	}
	h.salt = [2]uint32{binary.BigEndian.Uint32(b[16:]), binary.BigEndian.Uint32(b[20:])}
	h.cksum[0], h.cksum[1] = walChecksum(h.order, 0, 0, b[:24])
	if h.cksum[0] != binary.BigEndian.Uint32(b[24:]) || h.cksum[1] != binary.BigEndian.Uint32(b[28:]) {
		return walHeader{}, errors.New("capture: the WAL header's checksum is wrong")
	}
	return h, nil
}

// frameSize is the size of one frame in the file.
func (h walHeader) frameSize() int64 { return walFrameHeaderSize + int64(h.pageSize) }

// frameOffset is where frame n (counted from 1) starts.
func (h walHeader) frameOffset(n Frame) int64 {
	return walHeaderSize + int64(n-1)*h.frameSize()
}

// walFrame is what a frame says besides its page.
type walFrame struct {
	pgno   Pgno
	commit Pages // the database's size in pages after this frame if it ends a commit, otherwise 0
}

// readFrame reads frame n, checks its salts and its checksum (which runs on from
// ck, the checksum of frame n-1 or of the header) and leaves the frame's
// checksum in ck. buf (frameSize long) is where the frame is put.
func (h walHeader) readFrame(f io.ReaderAt, n Frame, ck *[2]uint32, buf []byte) (walFrame, error) {
	if len(buf) != int(h.frameSize()) {
		panic("capture: readFrame buffer of the wrong size")
	}
	if _, err := f.ReadAt(buf, h.frameOffset(n)); err != nil {
		return walFrame{}, fmt.Errorf("capture: read WAL frame %d: %w", n, err)
	}
	if binary.BigEndian.Uint32(buf[8:]) != h.salt[0] || binary.BigEndian.Uint32(buf[12:]) != h.salt[1] {
		return walFrame{}, fmt.Errorf("%w: frame %d has salts %x, the WAL %x", errRestarted, n, binary.BigEndian.Uint32(buf[8:]), h.salt)
	}
	s0, s1 := walChecksum(h.order, ck[0], ck[1], buf[:8])
	s0, s1 = walChecksum(h.order, s0, s1, buf[walFrameHeaderSize:])
	if s0 != binary.BigEndian.Uint32(buf[16:]) || s1 != binary.BigEndian.Uint32(buf[20:]) {
		return walFrame{}, fmt.Errorf("capture: WAL frame %d has a wrong checksum", n)
	}
	ck[0], ck[1] = s0, s1
	return walFrame{pgno: Pgno(binary.BigEndian.Uint32(buf[0:])), commit: Pages(binary.BigEndian.Uint32(buf[4:]))}, nil
}

// readFrameHeader reads only frame n's header, unchecked: for a map of which
// frame holds a page, of frames already read and checked.
func (h walHeader) readFrameHeader(f io.ReaderAt, n Frame) (walFrame, error) {
	var b [walFrameHeaderSize]byte
	if _, err := f.ReadAt(b[:], h.frameOffset(n)); err != nil {
		return walFrame{}, fmt.Errorf("capture: read WAL frame %d: %w", n, err)
	}
	return walFrame{pgno: Pgno(binary.BigEndian.Uint32(b[0:])), commit: Pages(binary.BigEndian.Uint32(b[4:]))}, nil
}

// walCursor is where capture has read a WAL to: which WAL (the header, whose
// salts say which), how many of its frames, and the running checksum after
// them. Every frame it has read is verified, so a cursor is also the proof
// that everything up to where it is is good. The salts change when SQLite
// restarts the WAL, and a cursor of the old one is useless: a fresh one
// starts at no frames of the new.
type walCursor struct {
	h      walHeader
	frames Frame     // frames of this WAL that have been read and verified
	ck     [2]uint32 // the running checksum after them
}

// window is what reading a range of a WAL's frames found: how many commits
// ended among them, the database's size in pages after the last of those
// commits, and the newest frame of each page in the range.
type window struct {
	commits uint64
	commit  Pages // 0 if the range held no commit
	pf      pageFrames
}

// walCursorAt returns a fresh cursor for the WAL f: its header read and
// verified, no frames read, the running checksum at the header's. A cursor
// made any other way can start the chain of checksums wrong.
func walCursorAt(f io.ReaderAt) (walCursor, error) {
	h, err := readWALHeader(f)
	if err != nil {
		return walCursor{}, err
	}
	return walCursor{h: h, ck: h.cksum}, nil
}

// walAt returns a cursor for the WAL f as the wal-index describes it: was, if
// that is the same WAL (the same salts) with at least the frames the
// wal-index says, or a fresh one at no frames of the WAL on disk, which must
// match the wal-index or the two are not the same database.
//
// The wal-index and the WAL's own header are separate files that SQLite
// updates in turn when it restarts the WAL, so a read that straddles the
// restart sees them name different WALs. That is errRestarted, and the caller
// reads the pair again.
func walAt(f io.ReaderAt, info Info, pageSize PageSize, was *walCursor) (walCursor, error) {
	c, err := walCursorAt(f)
	if err != nil {
		return walCursor{}, err
	}
	if c.h.pageSize != pageSize {
		return walCursor{}, fmt.Errorf("capture: the WAL has page size %d, the database %d", c.h.pageSize, pageSize)
	}
	if c.h.salt != info.Salt {
		return walCursor{}, fmt.Errorf("%w: the WAL has salts %x, the wal-index %x", errRestarted, c.h.salt, info.Salt)
	}
	if was != nil && was.h.salt == c.h.salt {
		if info.MxFrame < was.frames {
			return walCursor{}, fmt.Errorf("capture: the WAL has %d frames, fewer than the %d read", info.MxFrame, was.frames)
		}
		c.frames, c.ck = was.frames, was.ck
	}
	return c, nil
}

// errRestarted says the WAL and the wal-index name different WALs, which is
// what a read that straddles SQLite's restart of the WAL sees: the two files
// are updated in turn, so the pair agrees again once the restart has settled.
var errRestarted = errors.New("capture: the WAL was restarted")

// walRestartTries is how many times a read that straddles a restart of the
// WAL is tried again. SQLite writes the two headers a few microseconds apart,
// so one more read settles it; the rest are for a machine so loaded that the
// app is descheduled between them.
const walRestartTries = 20

// walRestartWait is how long a read waits before trying again. It grows, so a
// loaded machine settles, and stops at two milliseconds, so a read that keeps
// failing does not hold the shard's lock for long.
func walRestartWait(try int) time.Duration {
	d := time.Duration(try+1) * 100 * time.Microsecond
	if d > 2*time.Millisecond {
		d = 2 * time.Millisecond
	}
	return d
}

// walRead reads the wal-index and the WAL it names, and returns a cursor for
// the WAL with the wal-index's reading of it and the window the frames hold.
//
// The wal-index and the WAL are separate files that SQLite updates in turn
// when it restarts the WAL, so a read that straddles the restart sees them
// name different WALs, or the frames of one where the other's are. The pair is
// read again then, until the two agree.
//
// Reading across a restart is safe: the WAL only restarts once every frame of
// it is captured and in the database file (the read rule), so the frames it
// held are in the tail already.
//
// readInfo reads the wal-index, and readFrames reads the WAL it names.
func walRead(readInfo func() (Info, error), readFrames func(Info) (walCursor, window, error)) (walCursor, Info, window, error) {
	for try := 0; ; try++ {
		info, err := readInfo()
		if err != nil {
			return walCursor{}, Info{}, window{}, err
		}
		c, w, err := readFrames(info)
		if err == nil {
			return c, info, w, nil
		}
		if !errors.Is(err, errRestarted) || try >= walRestartTries {
			return walCursor{}, Info{}, window{}, err
		}
		time.Sleep(walRestartWait(try))
	}
}

// readTo reads frames up to to, verifying every one's salts and checksums,
// and leaves the cursor there. to is the wal-index's mxFrame, a commit
// boundary, so the last frame read must end a commit. Nothing is read when
// the cursor is already there. The pages it reads go no further than the
// checksums (writeLTX reads the pages it writes again, from the file).
func (c *walCursor) readTo(f io.ReaderAt, to Frame) (w window, err error) {
	buf := make([]byte, c.h.frameSize())
	ck := c.ck
	var all []pageRef
	var endsCommit bool
	for n := c.frames + 1; n <= to; n++ {
		fr, err := c.h.readFrame(f, n, &ck, buf)
		if err != nil {
			return w, err
		}
		all = append(all, refOf(fr.pgno, n))
		endsCommit = fr.commit != 0
		if endsCommit {
			w.commits++
			w.commit = fr.commit
		}
	}
	if c.frames < to && !endsCommit {
		return w, fmt.Errorf("capture: frame %d of the WAL does not end a commit", to)
	}
	c.frames, c.ck = to, ck
	w.pf = newestOf(all)
	return w, nil
}

// pageRef is one entry of pageFrames: a page number and a frame packed as
// pgno<<32|frame, so a list of them sorts by page and then by frame. A bare
// uint64 is not a page and frame; the accessors take them apart.
type pageRef uint64

func (r pageRef) pgno() Pgno   { return Pgno(r >> 32) }
func (r pageRef) frame() Frame { return Frame(r) }

// pageFrames says which frame holds the newest version of each page, for some
// range of frames. It is a sorted list of page refs, one entry per page.
type pageFrames []pageRef

// refOf packs a page number and the frame that holds its newest version.
func refOf(pgno Pgno, frame Frame) pageRef { return pageRef(pgno)<<32 | pageRef(frame) }

// newestOf reduces page refs, one per frame in order, to the newest frame of
// each page. It reuses the slice.
func newestOf(all []pageRef) pageFrames {
	slices.Sort(all)
	pf := all[:0]
	for i, e := range all {
		if i+1 == len(all) || all[i+1].pgno() != e.pgno() {
			pf = append(pf, e)
		}
	}
	return pf
}

// page reads the page that frame n of the WAL holds into buf, which is one
// page long. The frame has been read and checked already.
func (h walHeader) page(f io.ReaderAt, n Frame, buf []byte) error {
	if _, err := f.ReadAt(buf, h.frameOffset(n)+walFrameHeaderSize); err != nil {
		return fmt.Errorf("capture: read frame %d: %w", n, err)
	}
	return nil
}

// newestFrames reads the headers of the cursor's frames after lo (lo itself
// excluded), which have been read and checked already, and returns the newest
// frame of each page among them. A snapshot's use: lo is the last frame the
// database file holds.
func (c *walCursor) newestFrames(f io.ReaderAt, lo Frame) (pageFrames, error) {
	all := make([]pageRef, 0, max(int(c.frames)-int(lo), 0))
	for n := lo + 1; n <= c.frames; n++ {
		fr, err := c.h.readFrameHeader(f, n)
		if err != nil {
			return nil, err
		}
		all = append(all, refOf(fr.pgno, n))
	}
	return newestOf(all), nil
}

// frame returns the newest frame of page pgno, or 0 if the frames don't have it.
func (pf pageFrames) frame(pgno Pgno) Frame {
	i, ok := slices.BinarySearchFunc(pf, pgno, func(e pageRef, p Pgno) int {
		return int(e.pgno()) - int(p)
	})
	if !ok {
		return 0
	}
	return pf[i].frame()
}
