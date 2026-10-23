package node

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/luci1900/shardlite/internal/kind"
)

// gate holds back each chunk of a shard's output until the shard's earlier
// commits are durable. A response is gated on the callee that produced it, and
// what a calling shard sends (a request body, an upgraded connection's writes)
// on the caller.
type gate struct {
	m    *Catalog
	hold *Hold
	sync kind.Sync
	ctx  context.Context
	resp *http.Response // the response being gated, nil for a caller's gate
	// calls are the request's gates in progress: once the request has ended, a
	// gate fails without looking at the shard, whose hold is released.
	calls *inFlight
}

// errRequestEnded is what a gate returns once its request has ended.
var errRequestEnded = errors.New("the request has ended")

// wait gates a chunk that was just read (n bytes, or the end of the body, whose
// trailers are output too).
func (g *gate) wait(n int, err error) error {
	if n > 0 || errors.Is(err, io.EOF) {
		if !g.calls.enter() {
			return errRequestEnded
		}
		defer g.calls.leave()
		return g.m.Gate(g.ctx, g.hold, g.sync)
	}
	return nil
}

func (g *gate) read(n int, err error) (int, error) {
	if gerr := g.wait(n, err); gerr != nil {
		return 0, gerr
	}
	if errors.Is(err, io.EOF) && g.resp != nil {
		stripShardliteHeaders(g.resp.Trailer)
	}
	return n, err
}

// gatedBody is a body whose chunks are gated: a response's on the callee that
// produced it, and a call's request body on the caller. Each chunk, and the end
// of the body with its trailers, gets through only once what the shard
// committed before it is durable.
type gatedBody struct {
	io.ReadCloser
	g *gate
}

func (b gatedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	return b.g.read(n, err)
}

// gatedConn is an upgraded connection (a WebSocket) to the app, or to the
// owner's node for a forward: what the app sends is gated on its shard (g, nil
// on a forward, which the owner gates). What the client sends is not (a call
// from a shard is never upgraded). Each read and write counts as one of the
// request's calls in progress, so once the request has ended none is and none
// starts: the proxy's second copier outlives its handler only to return.
type gatedConn struct {
	io.ReadWriteCloser
	g     *gate
	calls *inFlight
}

func (c gatedConn) Read(p []byte) (int, error) {
	if !c.calls.enter() {
		return 0, errRequestEnded
	}
	defer c.calls.leave()
	n, err := c.ReadWriteCloser.Read(p)
	if c.g == nil {
		return n, err
	}
	return c.g.read(n, err)
}

func (c gatedConn) Write(p []byte) (int, error) {
	if !c.calls.enter() {
		return 0, errRequestEnded
	}
	defer c.calls.leave()
	return c.ReadWriteCloser.Write(p)
}
