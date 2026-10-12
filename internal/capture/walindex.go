package capture

import (
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"syscall"
)

// HeaderSize is the size of one copy of SQLite's wal-index header. The header
// is at the start of the -shm file, twice in a row ("WAL-index format" in
// SQLite's file format documentation). It holds iVersion, iChange (a counter
// SQLite bumps on every commit), isInit, szPage, mxFrame, nPage, the frame
// checksum, the WAL salts and a checksum of the header itself.
const HeaderSize = 48

// Header is a copy of the wal-index header, comparable with ==.
type Header [HeaderSize]byte

// WalIndex is a read-only shared mapping of the first page of a database's
// -shm file. SQLite updates the header in that memory on every commit before
// the commit returns, so reading it says exactly whether the database changed,
// without a system call.
//
// The mapping is made with a descriptor that is closed at once: fcntl locks
// belong to the process and closing any descriptor on a file drops them all
// (see shm.go), so the descriptor must be closed at a time when this process
// holds no locks on the shard. That is before Litestream opens it. The mapping
// outlives the descriptor.
//
// SQLite truncates -shm to zero and regrows it when a connection opens the
// file while no other connection has it open. A mapped page beyond the end of
// the file faults with SIGBUS, so Read must only be called while some
// connection holds the database open (Litestream's, while the shard is
// captured), and it turns a fault into an error anyway.
type WalIndex struct {
	mem []byte
}

// MapWalIndex maps the -shm file of dbPath, creating it if it is missing. It
// must be called when this process has no other descriptor open on the file
// (and before Litestream opens the shard).
func MapWalIndex(dbPath string) (*WalIndex, error) {
	f, err := os.OpenFile(dbPath+"-shm", os.O_RDWR|os.O_CREATE, 0o644)
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
	return &WalIndex{mem: mem}, nil
}

// ErrFault is returned by Read when reading the mapping faulted.
var ErrFault = errors.New("capture: fault reading the wal-index")

// Read copies the header. A read that races a commit may be torn, which only
// makes a caller that compares headers see a change it didn't need to.
func (w *WalIndex) Read() (h Header, err error) {
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

// Close unmaps. Reads after Close fail.
func (w *WalIndex) Close() error {
	if w == nil || w.mem == nil {
		return nil
	}
	mem := w.mem
	w.mem = nil
	return syscall.Munmap(mem)
}
