package capture

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"
)

// HeaderSize is the size of one copy of SQLite's wal-index header. The header
// is at the start of the -shm file, twice in a row ("WAL-index format" in
// SQLite's file format documentation). It holds iVersion, iChange (a counter
// SQLite bumps on every commit), isInit, szPage, mxFrame, nPage, the frame
// checksum, the WAL salts and a checksum of the header itself.
const HeaderSize = 48

// Header is a copy of the wal-index header, comparable with ==.
type Header [HeaderSize]byte

// WALIndex is a read-only shared mapping of the first page of a database's
// -shm file. SQLite updates the header in that memory on every commit before
// the commit returns, so reading it says exactly whether the database changed,
// without a system call.
//
// The mapping is made with a descriptor that is closed at once: fcntl locks
// belong to the process and closing any descriptor on a file drops them all
// (see walsidecar.go), so the descriptor must be closed at a time when this process
// holds no locks on the shard. That is before capture opens it. The mapping
// outlives the descriptor.
//
// SQLite truncates -shm to zero and regrows it when a connection opens the
// file while no other connection has it open. A mapped page beyond the end of
// the file faults with SIGBUS, so Read must only be called while some
// connection holds the database open (capture's, while the shard is
// captured), and it turns a fault into an error anyway.
type WALIndex struct {
	mem []byte
}

// MapWALIndex maps the wal-index file of dbPath, creating it if it is missing
// (the database file must exist). It must be called when this process has no
// other descriptor open on the file (and before capture opens the shard).
func MapWALIndex(dbPath string) (*WALIndex, error) {
	f, err := WALSidecarsOf(dbPath).openOrCreate()
	if err != nil {
		return nil, err
	}
	defer f.Close()
	// Mapping needs the file to be at least that long to read, but it is not
	// needed to map: a mapping beyond the end is fine until it is read.
	mem, err := syscall.Mmap(int(f.Fd()), 0, os.Getpagesize(), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &WALIndex{mem: mem}, nil
}

// ErrFault is returned by Read when reading the mapping faulted.
var ErrFault = errors.New("capture: fault reading the wal-index")

// Read copies the header. A read that races a commit may be torn, which only
// makes a caller that compares headers see a change it didn't need to.
func (w *WALIndex) Read() (h Header, err error) {
	if w == nil || w.mem == nil {
		return h, errors.New("capture: wal-index is not mapped")
	}
	// A fault in the mapping (the file got shorter) would kill the process
	// otherwise. This makes it a panic in this goroutine only.
	old := debug.SetPanicOnFault(true)
	defer func() {
		debug.SetPanicOnFault(old)
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrFault, r)
		}
	}()
	copy(h[:], w.mem[:HeaderSize])
	return h, nil
}

// Info is what capture reads of the wal-index to find the frames it hasn't.
type Info struct {
	MxFrame   Frame     // frames in the WAL that are committed
	Salt      [2]uint32 // the WAL's salts: they change when the WAL restarts
	NBackfill Frame     // frames copied into the database file
}

// ReadInfo reads the header and the checkpoint info after it, in the byte order of the
// machine (the wal-index is not portable). SQLite keeps two copies of the header and writes them
// one after the other, so a read that finds them different raced a commit and tries
// again. A writer that never pauses can outlast busy retries (Gosched never waits
// for another process), so the later tries wait a little for one of its gaps: the
// round trip of its next command is far longer than they sleep.
func (w *WALIndex) ReadInfo() (info Info, err error) {
	if w == nil || w.mem == nil {
		return info, errors.New("capture: wal-index is not mapped")
	}
	old := debug.SetPanicOnFault(true)
	defer func() {
		debug.SetPanicOnFault(old)
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrFault, r)
		}
	}()
	var b [2 * HeaderSize]byte
	for try := 0; ; try++ {
		copy(b[:], w.mem[:2*HeaderSize])
		if bytes.Equal(b[:HeaderSize], b[HeaderSize:]) {
			break
		}
		if try >= 200 {
			return info, errors.New("capture: wal-index header is changing")
		}
		if try < 100 {
			runtime.Gosched()
		} else {
			time.Sleep(time.Duration(try-99) * 50 * time.Microsecond)
		}
	}
	if b[12] == 0 {
		return info, errors.New("capture: wal-index not initialised")
	}
	bo := binary.NativeEndian
	info.MxFrame = Frame(bo.Uint32(b[16:]))
	// The salts are copied from the WAL header as they are, so they read as it does.
	info.Salt = [2]uint32{binary.BigEndian.Uint32(b[32:]), binary.BigEndian.Uint32(b[36:])}
	// WalCkptInfo follows the two copies of the header: nBackfill is its first field.
	info.NBackfill = Frame(bo.Uint32(w.mem[2*HeaderSize : 2*HeaderSize+4]))
	return info, nil
}

// Close unmaps. Reads after Close fail.
func (w *WALIndex) Close() error {
	if w == nil || w.mem == nil {
		return nil
	}
	mem := w.mem
	w.mem = nil
	return syscall.Munmap(mem)
}
