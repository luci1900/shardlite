// Package tempfile holds the bytes that don't fit in a request: unnamed
// temporary files in the node's data directory. A Log is appended to by one
// owner and read at offsets by anyone, and a File names a range of one. Nothing
// is reference counted: a handoff that must outlive its maker takes a Dup, a
// descriptor of its own that the taker closes.
package tempfile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// bufferSize is how much an Appender holds before it writes: a small commit is
// one write, a large one several, and nothing commit-sized is in memory.
const bufferSize = 256 << 10

// Op is what a Dir's hook is asked about.
type Op string

const (
	OpCreate Op = "create" // a log is created
	OpAppend Op = "append" // an appender writes its buffer to the log
	OpDup    Op = "dup"    // a log is duplicated
)

// Dir is the directory the temporary files go in.
type Dir struct {
	path string
	hook atomic.Pointer[func(Op) error]
	open atomic.Int64
}

// NewDir makes a Dir for path, which is created when the first log is.
func NewDir(path string) *Dir { return &Dir{path: path} }

// Path is the directory.
func (d *Dir) Path() string { return d.path }

// SetHook makes fn answer for every create, append and dup from now on: an error
// fails the operation. It is for the fault tests. A nil fn removes the hook.
func (d *Dir) SetHook(fn func(Op) error) {
	if fn == nil {
		d.hook.Store(nil)
		return
	}
	d.hook.Store(&fn)
}

// Open is how many descriptors of the directory's logs are open: the logs
// created and the dups made, less those closed. A test that has finished with
// every log expects none, which is how a log that was never closed shows (it has
// no name for a listing to find).
func (d *Dir) Open() int64 { return d.open.Load() }

func (d *Dir) fail(op Op) error {
	if h := d.hook.Load(); h != nil {
		return (*h)(op)
	}
	return nil
}

// ErrBroken is what an append to a log returns once an earlier write to it
// failed: the log's end is not known any more, and its owner starts another.
var ErrBroken = errors.New("tempfile: the log is broken")

// ErrReadOnly is what an append to a dup returns.
var ErrReadOnly = errors.New("tempfile: the log is a dup, which is read only")

// Log is an unnamed temporary file. It is appended to by its owner, one append
// at a time under the owner's lock, and read at offsets by anyone with the Log
// or a Dup of it, at any time: a range, once appended, doesn't change.
type Log struct {
	dir  *Dir
	f    *os.File
	dup  bool
	end  int64 // the end of what is appended; the owner's lock guards it
	open bool  // an Appender is in use; the owner's lock guards it
	bad  error // set once an append failed; the owner's lock guards it
}

// Create makes a log. On Linux it is a file that never had a name (O_TMPFILE),
// and where that is not supported a file whose name is removed at once.
func (d *Dir) Create() (*Log, error) {
	if d == nil {
		return nil, errors.New("tempfile: no directory set")
	}
	if err := d.fail(OpCreate); err != nil {
		return nil, err
	}
	if d.path == "" {
		return nil, errors.New("tempfile: no directory set")
	}
	if err := os.MkdirAll(d.path, 0o755); err != nil {
		return nil, err
	}
	f, err := createUnnamed(d.path)
	if err != nil {
		return nil, fmt.Errorf("tempfile: %w", err)
	}
	d.open.Add(1)
	return &Log{dir: d, f: f}, nil
}

// Close closes the log. The space is freed when the last descriptor of it is
// closed (the log's and its dups'). A second Close returns an error.
func (l *Log) Close() error {
	if l == nil {
		return nil
	}
	err := l.f.Close()
	if err == nil {
		l.dir.open.Add(-1)
	}
	return err
}

// Dup returns a descriptor of the same file, which the caller closes and which
// can only be read. It is an error, not a descriptor of some other file, to dup
// a log that is closed.
func (l *Log) Dup() (*Log, error) {
	if err := l.dir.fail(OpDup); err != nil {
		return nil, err
	}
	rc, err := l.f.SyscallConn()
	if err != nil {
		return nil, err
	}
	var fd int
	var ferr error
	if err := rc.Control(func(old uintptr) {
		fd, ferr = unix.FcntlInt(old, unix.F_DUPFD_CLOEXEC, 0)
	}); err != nil {
		return nil, err
	}
	if ferr != nil {
		return nil, fmt.Errorf("tempfile: dup: %w", ferr)
	}
	l.dir.open.Add(1)
	return &Log{dir: l.dir, f: os.NewFile(uintptr(fd), l.f.Name()), dup: true, end: l.end}, nil
}

// Size is how much has been appended. The owner calls it under its lock, or
// after it has stopped appending.
func (l *Log) Size() int64 { return l.end }

// File is everything appended so far.
func (l *Log) File() File { return File{log: l, size: l.end} }

// Broken is the error that broke the log, or nil. The owner calls it under its
// lock.
func (l *Log) Broken() error { return l.bad }

// File is a range of a log: bytes that are complete and don't change. It owns
// nothing, and is read through its log, which has to be open.
type File struct {
	log  *Log
	off  int64
	size int64
}

// Size is the length in bytes.
func (f File) Size() int64 { return f.size }

// Log is the log the range is in.
func (f File) Log() *Log { return f.log }

// In is the same range through another descriptor of the log (a dup of it).
func (f File) In(l *Log) File { f.log = l; return f }

// ReadAt reads at off within the range, as io.ReaderAt says.
func (f File) ReadAt(p []byte, off int64) (int, error) {
	return f.Reader().ReadAt(p, off)
}

// Reader returns a new reader at the start of the range. Each call gives an
// independent one.
func (f File) Reader() *io.SectionReader {
	return io.NewSectionReader(f.log.f, f.off, f.size)
}

// Bytes reads the whole range into memory (for tests and small things).
func (f File) Bytes() ([]byte, error) { return io.ReadAll(f.Reader()) }

var bufPool = sync.Pool{New: func() any { b := make([]byte, 0, bufferSize); return &b }}

// Appender collects one append to a log. It buffers, writes at the log's end
// when the buffer is full and at Finish, and latches the first error, so a
// failed write cannot be finished. It is single-use: Finish or Abort ends it.
// An Appender is not safe for concurrent use.
type Appender struct {
	l       *Log
	start   int64 // where the append begins
	written int64 // how much has gone to the file
	buf     *[]byte
	err     error
	done    bool
}

// Append begins an append to the log. Only one is open at a time: the owner
// holds its lock from here to Finish or Abort.
func (l *Log) Append() (*Appender, error) {
	switch {
	case l.dup:
		return nil, ErrReadOnly
	case l.bad != nil:
		return nil, fmt.Errorf("%w: %w", ErrBroken, l.bad)
	case l.open:
		return nil, errors.New("tempfile: an append is open already")
	}
	l.open = true
	return &Appender{l: l, start: l.end, buf: bufPool.Get().(*[]byte)}, nil
}

var errAppenderDone = errors.New("tempfile: the append is done")

// Write implements io.Writer.
func (a *Appender) Write(p []byte) (int, error) {
	if a.done {
		return 0, errAppenderDone
	}
	if a.err != nil {
		return 0, a.err
	}
	n := len(p)
	for len(p) > 0 {
		room := bufferSize - len(*a.buf)
		if room == 0 {
			if err := a.flush(); err != nil {
				return 0, err
			}
			continue
		}
		m := min(room, len(p))
		*a.buf = append(*a.buf, p[:m]...)
		p = p[m:]
	}
	return n, nil
}

func (a *Appender) flush() error {
	if len(*a.buf) == 0 {
		return nil
	}
	err := a.l.dir.fail(OpAppend)
	if err == nil {
		var n int
		n, err = a.l.f.WriteAt(*a.buf, a.start+a.written)
		a.written += int64(n)
	}
	if err != nil {
		a.err = err
		a.l.bad = err // what reached the file is unknown
		return err
	}
	*a.buf = (*a.buf)[:0]
	return nil
}

// Written is how many bytes have been written, to the buffer or the file.
func (a *Appender) Written() int64 { return a.written + int64(len(*a.buf)) }

// Finish ends the append and returns its range, which is complete. It returns
// an error if the append was ended already or a write failed.
func (a *Appender) Finish() (File, error) {
	if a.done {
		return File{}, errAppenderDone
	}
	if a.err == nil {
		a.flush()
	}
	if a.err != nil {
		err := a.err
		a.Abort()
		return File{}, err
	}
	a.end()
	a.l.end = a.start + a.written
	return File{log: a.l, off: a.start, size: a.written}, nil
}

// Abort drops what was appended: the file is cut back to where the append
// began, so the next one reuses the space. It is a no-op after Finish or Abort.
// A log that cannot be cut back is broken.
func (a *Appender) Abort() {
	if a.done {
		return
	}
	a.end()
	if a.written > 0 && a.l.bad == nil {
		if err := a.l.f.Truncate(a.start); err != nil {
			a.l.bad = err
		}
	}
}

// end finishes with the appender.
func (a *Appender) end() {
	a.done = true
	a.l.open = false
	*a.buf = (*a.buf)[:0]
	bufPool.Put(a.buf)
	a.buf = nil
}

// Build writes a file with fill into a log of its own and returns the log and
// the file's range in it. The caller closes the log. A failed fill leaves
// nothing behind.
func (d *Dir) Build(fill func(io.Writer) error) (*Log, File, error) {
	l, err := d.Create()
	if err != nil {
		return nil, File{}, err
	}
	a, err := l.Append()
	if err != nil {
		l.Close()
		return nil, File{}, err
	}
	if err := fill(a); err != nil {
		a.Abort()
		l.Close()
		return nil, File{}, err
	}
	f, err := a.Finish()
	if err != nil {
		l.Close()
		return nil, File{}, err
	}
	return l, f, nil
}
