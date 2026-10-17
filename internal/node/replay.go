package node

import (
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
)

// replayLimit is how much of a forwarded request's body the node keeps so that
// it can send it again (docs/internals.md, Routing and forwarding). It is fixed,
// as Linkerd's is: no operator could tell what to set it to.
const replayLimit = 64 << 10

var errNotKept = errors.New("the body is past what was kept to send it again")

// replayBody holds the bytes of a request body as they are read, up to a limit,
// the way a proxy keeps a body it may have to retry (Linkerd and Envoy do the
// same). Nothing is read ahead: the body streams to the owner as it arrives, and
// the bytes the owner has been sent are the ones kept. Each attempt reads
// through a view of its own, which starts at the beginning, takes the kept bytes
// first and then reads on from the body.
//
// An attempt that has been given up on may still be reading when the next one
// starts, so views share the body: one read of it at a time, with everything it
// returns kept for whoever comes next. The kept bytes have a lock of their own so
// that a view can send them while another is blocked reading on: a stream whose
// client waits for a reply before sending more would otherwise wait for its
// deadline, since the reply needs the bytes the retry can't send.
type replayBody struct {
	src   io.Reader
	limit int

	read sync.Mutex // held for the length of a read of src, which may block
	mu   sync.Mutex // guards the rest
	buf  []byte     // the first len(buf) bytes of the body
	pos  int64      // how much of the body has been read
	err  error      // what ended the body
}

func newReplayBody(src io.Reader, limit int) *replayBody { return &replayBody{src: src, limit: limit} }

// replayable reports whether a view can still read the whole body: every byte
// read so far is kept.
func (b *replayBody) replayable() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pos == int64(len(b.buf))
}

// view is a reader of the body from its first byte.
func (b *replayBody) view() io.ReadCloser { return &replayView{b: b} }

type replayView struct {
	b      *replayBody
	off    int64
	closed atomic.Bool
}

// Close ends this view only: the body belongs to the request.
func (v *replayView) Close() error { v.closed.Store(true); return nil }

func (v *replayView) Read(p []byte) (int, error) {
	if v.closed.Load() {
		return 0, http.ErrBodyReadAfterClose
	}
	b := v.b
	if n, ok := v.kept(p); ok {
		return n, nil
	}
	b.read.Lock()
	defer b.read.Unlock()
	if n, ok := v.kept(p); ok { // another view read it while this one waited
		return n, nil
	}
	b.mu.Lock()
	switch {
	case v.off != b.pos:
		b.mu.Unlock()
		return 0, errNotKept
	case b.err != nil:
		err := b.err
		b.mu.Unlock()
		return 0, err
	}
	b.mu.Unlock()
	n, err := b.src.Read(p)
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > 0 {
		if b.pos == int64(len(b.buf)) && len(b.buf)+n <= b.limit {
			b.buf = append(b.buf, p[:n]...)
		}
		b.pos += int64(n)
		v.off += int64(n)
	}
	if err != nil {
		b.err = err
	}
	return n, err
}

// kept reads from the bytes kept, if the view is behind them.
func (v *replayView) kept(p []byte) (int, bool) {
	v.b.mu.Lock()
	defer v.b.mu.Unlock()
	if v.off >= int64(len(v.b.buf)) {
		return 0, false
	}
	n := copy(p, v.b.buf[v.off:])
	v.off += int64(n)
	return n, true
}
