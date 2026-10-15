package capture

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
)

// shrinkAbove is how large Litestream's newest level 0 file may be, once its
// commits are in a delta, before DropTail shrinks it. A single commit that
// rewrites the database (an import, a VACUUM) makes a file as large as the
// database, and Litestream keeps its newest file to find its place by. It is in
// the scratch directory, which is memory.
const shrinkAbove = 1 << 20

// shrinkNewest replaces Litestream's newest level 0 file, commit txid, with one
// that says the same to Litestream and holds at most one page.
//
// What Litestream asks of the newest file (db.go: Pos, verify, snapshotWALEndOffset)
// is its header (the commit number, the size of the database, where in the WAL it
// ended and the WAL's salts) and, when it ended more than a frame past the WAL's
// header, that the page of the WAL's last synced frame is in it with the same
// bytes (lastPageMatch), which tells it the WAL has not been overwritten. So the
// file keeps its header and that page, and nothing else. The page is taken from
// the file itself and not from the WAL, so a WAL that was overwritten still
// fails the match. Pos verifies a file whole, so the new one is a complete,
// valid file.
//
// A file that can't be shrunk this way is left alone: a snapshot (it must hold
// every page) or a file whose WAL is gone.
func shrinkNewest(db *litestream.DB, txid uint64) error {
	name := db.LTXPath(0, ltx.TXID(txid), ltx.TXID(txid))
	fi, err := os.Stat(name)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if fi.Size() <= shrinkAbove {
		return nil
	}
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := ltx.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	if err := dec.DecodeHeader(); err != nil {
		return err
	}
	hdr := dec.Header()
	if hdr.IsSnapshot() {
		return nil
	}

	// The page Litestream will look for: the one in the WAL's last synced frame, if
	// that frame is of the WAL the file describes.
	var want uint32
	if end := hdr.WALOffset + hdr.WALSize; hdr.WALSize > 0 {
		frameSize := int64(hdr.PageSize) + litestream.WALFrameHeaderSize
		if prev := end - frameSize; prev > litestream.WALHeaderSize {
			var fh [litestream.WALFrameHeaderSize]byte
			if wal, err := os.Open(db.WALPath()); err == nil {
				_, rerr := wal.ReadAt(fh[:], prev)
				wal.Close()
				if rerr == nil && binary.BigEndian.Uint32(fh[8:]) == hdr.WALSalt1 && binary.BigEndian.Uint32(fh[12:]) == hdr.WALSalt2 {
					want = binary.BigEndian.Uint32(fh[0:])
				}
			}
		}
	}
	var page []byte
	buf := make([]byte, hdr.PageSize)
	for {
		var ph ltx.PageHeader
		if err := dec.DecodePage(&ph, buf); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		if want != 0 && ph.Pgno == want && page == nil {
			page = append([]byte(nil), buf...)
		}
	}
	if err := dec.Close(); err != nil { // the whole file's checksum, before it is replaced
		return err
	}

	return writeLTXFile(name, hdr, func(enc *ltx.Encoder) error {
		if page == nil {
			return nil
		}
		return enc.EncodePage(ltx.PageHeader{Pgno: want}, page)
	})
}
