// Package spool holds bytes that are usually small and sometimes as large as a
// database: in memory up to a limit, and in a temporary file in the data
// directory beyond it. It is never put in the node's scratch directory, which is
// tmpfs and so memory too.
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
// of readers can read it at once.
type File struct {
	mem  []byte
	f    *os.File
	size int64
	refs atomic.Int32
}

// Bytes wraps b, which must not be changed afterwards.
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
func (f *File) Reader() *io.SectionReader {
	if f.f != nil {
		return io.NewSectionReader(f.f, 0, f.size)
	}
	return io.NewSectionReader(bytes.NewReader(f.mem), 0, f.size)
}

// Bytes reads everything into memory. For tests and small things.
func (f *File) Bytes() ([]byte, error) {
	if f.f == nil {
		return f.mem, nil
	}
	return io.ReadAll(f.Reader())
}

// Retain adds a reference and returns f. Every reference must be closed.
func (f *File) Retain() *File {
	f.refs.Add(1)
	return f
}

// Close gives up one reference. The last one removes the temporary file.
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
type Writer struct {
	dir   string
	limit int
	mem   bytes.Buffer
	f     *os.File
	w     *bufio.Writer
	n     int64
}

// NewWriter makes a Writer. dir is where a temporary file goes, and it is an
// error to write more than limit bytes without one.
func NewWriter(dir string, limit int) *Writer {
	return &Writer{dir: dir, limit: limit}
}

// Write implements io.Writer.
func (w *Writer) Write(p []byte) (int, error) {
	if w.f == nil && (w.limit <= 0 || w.mem.Len()+len(p) > w.limit) {
		if err := w.spill(); err != nil {
			return 0, err
		}
	}
	w.n += int64(len(p))
	if w.f != nil {
		return w.w.Write(p)
	}
	return w.mem.Write(p)
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
// The Writer can't be used afterwards, not even to Abort.
func (w *Writer) Finish() (*File, error) {
	out := &File{size: w.n}
	out.refs.Store(1)
	if w.f == nil {
		out.mem = w.mem.Bytes()
		return out, nil
	}
	if err := w.w.Flush(); err != nil {
		w.Abort()
		return nil, err
	}
	out.f = w.f
	w.f = nil
	return out, nil
}

// Abort drops what was written, removing the temporary file if there is one.
func (w *Writer) Abort() {
	if w.f != nil {
		name := w.f.Name()
		w.f.Close()
		os.Remove(name)
		w.f = nil
	}
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
