package journal

import (
	"fmt"
	"io"
)

// filePiece is one LTX file inside a segment's body.
type filePiece struct {
	r    *io.SectionReader
	size int64
}

// segmentBody is a segment's bytes as a reader over its pieces: the LTX files the
// capture queued, then the footer that names them, then the trailer. It is not
// a copy of them, so a segment is written without being copied
// anywhere, and its bytes are never held whole in memory.
//
// The pieces don't change once the body is made, so a retried create reads
// it again from the start.
type segmentBody struct {
	files  []filePiece
	footer []byte
	tail   []byte
	size   int64
}

// Size is the length of the segment.
func (b *segmentBody) Size() int64 { return b.size }

// ReadAt reads the bytes at off, as io.ReaderAt says: it fills p or it says
// why it couldn't.
func (b *segmentBody) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("journal: a negative offset")
	}
	if off >= b.size {
		return 0, io.EOF
	}
	n := 0
	for _, f := range b.files {
		if off >= f.size {
			off -= f.size
			continue
		}
		m, err := f.r.ReadAt(p[n:], off)
		n += m
		if err != nil && err != io.EOF {
			return n, err
		}
		off = 0
		if n == len(p) {
			return n, nil
		}
	}
	for _, s := range [][]byte{b.footer, b.tail} {
		if off >= int64(len(s)) {
			off -= int64(len(s))
			continue
		}
		m := copy(p[n:], s[off:])
		n += m
		off = 0
		if n == len(p) {
			return n, nil
		}
	}
	return n, io.EOF
}
