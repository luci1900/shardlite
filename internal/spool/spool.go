// Package spool holds bytes that are usually small and sometimes as large as a
// database: in memory up to a limit, and in a temporary file in the data
// directory beyond it.
package spool

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
)

// DefaultLimit is how large something gets before it is kept in a temporary
// file instead of in memory. Most commits and deltas are a few pages, where a
// file costs more than the bytes do. Anything near this size is about to cost
// real I/O anyway, and a node holding many at once stays well under a gigabyte.
const DefaultLimit = 4 << 20

// File is bytes that are complete and don't change: in memory, or in a
// temporary file that is removed when the last reference is closed. Any number
// of readers can read it at once, provided each holds a reference to it for as
// long as it reads.
type File struct {
	mem  []byte
	f    *os.File
	size int64
	refs atomic.Int32
}

// Bytes wraps b, which must not be changed afterwards (for tests).
func Bytes(b []byte) *File {
	f := &File{mem: b, size: int64(len(b))}
	f.refs.Store(1)
	return f
}

// Size is the length in bytes.
func (f *File) Size() int64 { return f.size }

// InFile reports whether the bytes are in a temporary file.
func (f *File) InFile() bool { return f.f != nil }

// Reader returns a new reader at the start. Each call gives an independent one.
// The caller must hold a reference to f for as long as it reads. Reader panics
// after the last reference has been closed.
func (f *File) Reader() *io.SectionReader {
	f.check()
	if f.f != nil {
		return io.NewSectionReader(f.f, 0, f.size)
	}
	return io.NewSectionReader(bytes.NewReader(f.mem), 0, f.size)
}

// Bytes reads everything into memory and returns a copy. For tests and small
// things. Bytes panics after the last reference has been closed.
func (f *File) Bytes() ([]byte, error) {
	f.check()
	return io.ReadAll(f.Reader())
}

// check panics once the last reference has been closed.
func (f *File) check() {
	if f.refs.Load() <= 0 {
		panic("spool: read after the last close")
	}
}

// Retain adds a reference and returns f. Every reference must be closed.
// Retain panics if the last reference has already been closed: a File cannot
// be resurrected once it is closed.
func (f *File) Retain() *File {
	for {
		n := f.refs.Load()
		if n <= 0 {
			panic("spool: retain after the last close")
		}
		if f.refs.CompareAndSwap(n, n+1) {
			return f
		}
	}
}

// Close gives up one reference. The last one removes the temporary file.
// Close panics if called more often than the File was retained; it is not
// idempotent.
func (f *File) Close() error {
	if f == nil {
		return nil
	}
	n := f.refs.Add(-1)
	if n > 0 {
		return nil
	}
	if n < 0 {
		panic("spool: closed more often than retained")
	}
	if f.f == nil {
		return nil
	}
	name := f.f.Name()
	err := f.f.Close()
	if rerr := os.Remove(name); err == nil && !errors.Is(rerr, os.ErrNotExist) {
		err = rerr
	}
	return err
}

// Writer collects what is written to it, in memory up to a limit and in a
// temporary file in Dir beyond it. A limit of zero or less is always a file.
// It is single-use: Finish or Abort ends it. Write and Finish after that return
// an error; Abort after that is a no-op. A Writer is not safe for concurrent use.
type Writer struct {
	dir      string
	limit    int
	mem      bytes.Buffer
	f        *os.File
	w        *bufio.Writer
	n        int64
	done     bool
	writeErr error
}

// errWriterDone is what Write and Finish return after Finish or Abort.
var errWriterDone = errors.New("spool: the writer is done")

// NewWriter makes a Writer. dir is where a temporary file goes, and it is an
// error to write more than limit bytes without one.
func NewWriter(dir string, limit int) *Writer {
	return &Writer{dir: dir, limit: limit}
}

// Write implements io.Writer. It returns the first error a write encountered
// from then on, so a failed write cannot be finished into a partial File.
func (w *Writer) Write(p []byte) (int, error) {
	if w.done {
		return 0, errWriterDone
	}
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	if w.f == nil && (w.limit <= 0 || w.mem.Len()+len(p) > w.limit) {
		if err := w.spill(); err != nil {
			w.writeErr = err
			return 0, err
		}
	}
	var (
		n   int
		err error
	)
	if w.f != nil {
		n, err = w.w.Write(p)
	} else {
		n, err = w.mem.Write(p)
	}
	w.n += int64(n)
	if err != nil {
		w.writeErr = err
	}
	return n, err
}

// spill moves what is held to a new temporary file.
func (w *Writer) spill() error {
	if w.dir == "" {
		return fmt.Errorf("spool: %d bytes are more than the limit of %d and there is no temporary directory", w.n, w.limit)
	}
	if err := os.MkdirAll(w.dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(w.dir, "spool-*")
	if err != nil {
		return err
	}
	w.f, w.w = f, bufio.NewWriterSize(f, 256<<10)
	if _, err := w.w.Write(w.mem.Bytes()); err != nil {
		return err
	}
	w.mem = bytes.Buffer{}
	return nil
}

// Written is how many bytes have been written.
func (w *Writer) Written() int64 { return w.n }

// InFile reports whether what is written has spilled to a file.
func (w *Writer) InFile() bool { return w.f != nil }

// Finish ends the writing and returns what was written, with one reference.
// It returns an error if the Writer was ended already or a Write failed.
func (w *Writer) Finish() (*File, error) {
	if w.done {
		return nil, errWriterDone
	}
	w.done = true
	if w.writeErr != nil {
		w.abort()
		return nil, w.writeErr
	}
	out := &File{size: w.n}
	out.refs.Store(1)
	if w.f == nil {
		out.mem = w.mem.Bytes()
		return out, nil
	}
	if err := w.w.Flush(); err != nil {
		w.abort()
		return nil, err
	}
	out.f = w.f
	w.f = nil
	w.w = nil
	return out, nil
}

// Abort drops what was written, removing the temporary file if there is one.
// It is a no-op after Finish or Abort.
func (w *Writer) Abort() {
	if w.done {
		return
	}
	w.done = true
	w.abort()
}

// abort drops what was written and is safe to call after done is set.
func (w *Writer) abort() {
	if w.f != nil {
		name := w.f.Name()
		_ = w.f.Close()
		_ = os.Remove(name)
		w.f = nil
	}
	w.w = nil
	w.mem = bytes.Buffer{}
}

// Collect reads r to the end into a File.
func Collect(dir string, limit int, r io.Reader) (*File, error) {
	w := NewWriter(dir, limit)
	if _, err := io.Copy(w, r); err != nil {
		w.Abort()
		return nil, err
	}
	return w.Finish()
}
