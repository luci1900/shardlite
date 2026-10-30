package segment

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// filePiece is one LTX file inside a segment's body.
type filePiece struct {
	r    *io.SectionReader
	size int64
}

// body is a segment's bytes as a reader over its pieces: the LTX files the
// capture queued, then the footer that names them, then the trailer. It is not
// a copy of them, so a segment is written to two stores without being copied
// anywhere, and its bytes are never held whole in memory.
//
// The pieces don't change once the body is made, so any number of readers may
// read it at once: the record's uploader and the local store's uploader read
// the same body.
type body struct {
	files  []filePiece
	footer []byte
	tail   []byte
	size   int64
}

// Size is the length of the segment.
func (b *body) Size() int64 { return b.size }

// ReadAt reads the bytes at off, as io.ReaderAt says: it fills p or it says
// why it couldn't.
func (b *body) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("segment: a negative offset")
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

// batchQueue is an unbounded queue of batches: the record's uploader hands a
// batch to the local store's and the peer's uploaders and goes on, so a slow
// disk or peer never holds the record's writes. It is unbounded because the
// alternative (a bounded queue) would make the record wait for the slow side
// once it filled.
type batchQueue struct {
	mu     sync.Mutex
	list   []*batch
	closed bool
	wake   chan struct{}
}

func newBatchQueue() *batchQueue { return &batchQueue{wake: make(chan struct{}, 1)} }

// push adds a batch. It never blocks.
func (q *batchQueue) push(b *batch) {
	q.mu.Lock()
	q.list = append(q.list, b)
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// pop takes the next batch, waiting for one. It returns nil when the queue is
// closed or ctx ends.
func (q *batchQueue) pop(ctx context.Context) *batch {
	for {
		q.mu.Lock()
		if len(q.list) > 0 {
			b := q.list[0]
			q.list = q.list[1:]
			q.mu.Unlock()
			return b
		}
		closed := q.closed
		q.mu.Unlock()
		if closed {
			return nil
		}
		select {
		case <-q.wake:
		case <-ctx.Done():
			return nil
		}
	}
}

// drain takes every batch waiting and returns them, so a hand-off can finish
// them without holding the queue.
func (q *batchQueue) drain() []*batch {
	q.mu.Lock()
	defer q.mu.Unlock()
	held := q.list
	q.list = nil
	return held
}

// close empties the queue and wakes the waiter.
func (q *batchQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.list = nil
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}
