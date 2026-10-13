package capture

import (
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
)

// seedTXID is the Litestream commit number of the file that seeds a restored
// shard. It is above 1 because an LTX file starting at 1 is a snapshot and must
// hold every page. Litestream's own first file in the activation is the next
// number.
const seedTXID ltx.TXID = 2

// A WAL file starts with a 32-byte header (https://sqlite.org/fileformat2.html#walformat).
const (
	walMagicBigEndian = 0x377f0683 // the low bit says the checksums are big-endian
	walVersion        = 3007000
)

// seed makes Litestream continue a restored shard from where it is, instead of
// taking a snapshot of it at its first sync.
//
// Left alone, Litestream's first sync of a database it has no position for
// writes every page into a level 0 file in its metadata directory (tmpfs, so
// memory) and keeps it as its newest file, which is the size of the database for
// every activation of a restored shard. It continues from a position when the
// newest level 0 file says the WAL was synced up to a point and the WAL still
// has the salts and the end the file names (db.go, verify). The node therefore
// gives it that file before it opens the shard, describing the state exactly as
// it is: all of the database is in the database file, and none of the WAL has
// been captured. The file has no pages.
//
// SQLite throws away a WAL that holds only a header and starts a new one with
// salts of its own at the first write, and Litestream would find the salts
// changed and snapshot. So the WAL the node writes holds one committed frame as
// well: a copy of page 1, byte for byte what the database file has, which
// changes nothing. The seed file says that frame has been synced (offset 32,
// size one frame), so the next sync starts after it.
//
// What it relies on in Litestream (TestSeedRelianceOnLitestream and the capture
// path tests in restored_test.go fail if an upgrade changes any of it, and the
// stage 1z report lists them):
//   - the layout of the metadata directory: DB.LTXPath(0, txid, txid), named by
//     one commit number (verify opens the file at the position's TXID twice
//     over, as min and max);
//   - DB.Pos reads the newest level 0 file (the highest maximum commit number)
//     and verifies the whole file;
//   - verify reads that file's header: WAL offset plus size is where the next
//     sync reads from, its salts must be the WAL's, and an offset of exactly the
//     WAL header's size (32) with matching salts continues without a snapshot;
//   - Litestream does not write to the WAL when a header is already there
//     (ensureWALExists), so no frame is made that the seed has not described;
//   - the next file takes the next commit number, so the client starts counting
//     at seedTXID;
//   - the replica starts after the highest commit number the storage client
//     lists at level 0 (Replica.calcPos), so the client lists the seed as
//     uploaded (client.LTXFiles) and Litestream doesn't look for earlier files;
//   - Litestream opens its connection at the first sync after Open (db.init),
//     which is when it reads the seed, and removes ".tmp" files in the metadata
//     directory when it opens, so the seed is renamed into place before that.
//
// What the node relies on in SQLite: a WAL with a valid header and a valid
// committed frame is used as it is, and the next frame is written after it with
// the same salts, until a checkpoint restarts the WAL.
//
// Before it, the database is made to hold everything itself: a WAL that a cached
// or resumed shard left behind may hold commits that are not in the database
// file, so a connection of the node's checkpoints and removes it. Nobody else
// may have the shard open (the caller has just checked), and the node has
// nothing open on it that closing a descriptor could drop locks of.
func seed(dbPath string, db *litestream.DB, at func(point string)) error {
	if err := foldWAL(dbPath); err != nil {
		return err
	}
	at("seed-folded")
	pageSize, pages, err := dbGeometry(dbPath)
	if err != nil {
		return err
	}
	salt1, salt2, err := writeStartWAL(dbPath, pageSize, pages)
	if err != nil {
		return err
	}
	at("seed-wal")
	if err := writeSeedFile(db, pageSize, pages, salt1, salt2); err != nil {
		return err
	}
	at("seed-file")
	return nil
}

// foldWAL leaves the database file complete on its own and no WAL or -shm
// beside it. A database that never had either needs nothing.
func foldWAL(dbPath string) error {
	_, werr := os.Stat(dbPath + "-wal")
	_, serr := os.Stat(dbPath + "-shm")
	if errors.Is(werr, fs.ErrNotExist) && errors.Is(serr, fs.ErrNotExist) {
		return nil
	}
	sdb, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return err
	}
	sdb.SetMaxOpenConns(1)
	var busy, logFrames, done int
	err = sdb.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &done)
	if cerr := sdb.Close(); err == nil { // the last connection closing removes the WAL
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("capture: checkpoint before capture: %w", err)
	} else if busy != 0 {
		return errors.New("capture: checkpoint before capture was blocked: another process has the shard open")
	}
	for _, p := range []string{dbPath + "-wal", dbPath + "-shm"} {
		if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

// dbGeometry reads the page size and the length in pages of the database file.
func dbGeometry(dbPath string) (pageSize, pages uint32, err error) {
	f, err := os.Open(dbPath)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	var hdr [100]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return 0, 0, fmt.Errorf("capture: read the database header: %w", err)
	}
	if string(hdr[:16]) != "SQLite format 3\x00" {
		return 0, 0, errors.New("capture: not a SQLite database")
	}
	pageSize = uint32(binary.BigEndian.Uint16(hdr[16:18]))
	if pageSize == 1 {
		pageSize = 65536
	}
	if !ltx.IsValidPageSize(pageSize) {
		return 0, 0, fmt.Errorf("capture: invalid page size %d", pageSize)
	}
	if hdr[18] != 2 || hdr[19] != 2 {
		return 0, 0, errors.New("capture: the database is not in WAL mode")
	}
	fi, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	pages = uint32(fi.Size() / int64(pageSize))
	if pages == 0 {
		return 0, 0, errors.New("capture: the database is empty")
	}
	return pageSize, pages, nil
}

// writeStartWAL creates a WAL with salts of its own and one committed frame,
// a copy of the database's first page.
func writeStartWAL(dbPath string, pageSize, pages uint32) (salt1, salt2 uint32, err error) {
	page := make([]byte, pageSize)
	df, err := os.Open(dbPath)
	if err != nil {
		return 0, 0, err
	}
	_, err = df.ReadAt(page, 0)
	df.Close()
	if err != nil {
		return 0, 0, fmt.Errorf("capture: read the first page: %w", err)
	}

	var salts [8]byte
	if _, err := rand.Read(salts[:]); err != nil {
		return 0, 0, err
	}
	salt1, salt2 = binary.BigEndian.Uint32(salts[:4]), binary.BigEndian.Uint32(salts[4:])
	// Checksums run on through the file: the header's, then each frame's from the
	// first 8 bytes of its header and its page.
	bo := binary.BigEndian
	var hdr [litestream.WALHeaderSize]byte
	bo.PutUint32(hdr[0:], walMagicBigEndian)
	bo.PutUint32(hdr[4:], walVersion)
	bo.PutUint32(hdr[8:], pageSize)
	bo.PutUint32(hdr[12:], 0) // checkpoint sequence
	bo.PutUint32(hdr[16:], salt1)
	bo.PutUint32(hdr[20:], salt2)
	c1, c2 := litestream.WALChecksum(bo, 0, 0, hdr[:24])
	bo.PutUint32(hdr[24:], c1)
	bo.PutUint32(hdr[28:], c2)

	frame := make([]byte, litestream.WALFrameHeaderSize+int(pageSize))
	bo.PutUint32(frame[0:], 1)     // page number
	bo.PutUint32(frame[4:], pages) // size of the database after the commit: it commits
	bo.PutUint32(frame[8:], salt1)
	bo.PutUint32(frame[12:], salt2)
	copy(frame[litestream.WALFrameHeaderSize:], page)
	c1, c2 = litestream.WALChecksum(bo, c1, c2, frame[:8])
	c1, c2 = litestream.WALChecksum(bo, c1, c2, page)
	bo.PutUint32(frame[16:], c1)
	bo.PutUint32(frame[20:], c2)

	fi, err := os.Stat(dbPath)
	if err != nil {
		return 0, 0, err
	}
	f, err := os.OpenFile(dbPath+"-wal", os.O_CREATE|os.O_EXCL|os.O_WRONLY, fi.Mode().Perm())
	if err != nil {
		return 0, 0, err
	}
	// The WAL belongs to whoever owns the database, as SQLite would have made it.
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		_ = f.Chown(int(st.Uid), int(st.Gid))
	}
	// Not synced: a node that dies now restores the shard again or discards these
	// files (their size and time differ from what the cache index recorded).
	if _, err := f.Write(append(hdr[:], frame...)); err != nil {
		f.Close()
		return 0, 0, err
	}
	return salt1, salt2, f.Close()
}

// writeSeedFile writes the level 0 file for commit seedTXID: the database as
// it is (pages long), the WAL synced up to the end of its one frame and no pages.
func writeSeedFile(db *litestream.DB, pageSize, pages uint32, salt1, salt2 uint32) error {
	name := db.LTXPath(0, seedTXID, seedTXID)
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		return err
	}
	tmp := name + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	enc, err := ltx.NewEncoder(f)
	if err != nil {
		f.Close()
		return err
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum, // as Litestream's own files
		PageSize:  pageSize,
		Commit:    pages,
		MinTXID:   seedTXID,
		MaxTXID:   seedTXID,
		Timestamp: time.Now().UnixMilli(),
		WALOffset: litestream.WALHeaderSize,
		WALSize:   int64(litestream.WALFrameHeaderSize) + int64(pageSize),
		WALSalt1:  salt1,
		WALSalt2:  salt2,
	}); err != nil {
		f.Close()
		return err
	}
	if err := enc.Close(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, name)
}
