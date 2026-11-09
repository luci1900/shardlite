package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/luci1900/shardlite/internal/kind"
)

// gate holds back each chunk of a shard's output until the shard's earlier
// commits are durable. A response is gated on the callee that produced it, and
// what a calling shard sends (a request body, an upgraded connection's writes)
// on the caller.
type gate struct {
	m    *Shardset
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

// gateCaller holds a call until the calling shard's earlier commits are
// durable, in the store the caller's tier names (or the one the call says with
// Shardlite-Sync), and returns the gate for the rest of the call. The gate
// keeps the caller active: the hold is released when the call ends. It is nil,
// with nothing held, when the call isn't gated: the caller isn't active on this
// node (it committed nothing here to wait for).
func (s *Server) gateCaller(r *http.Request, info *reqInfo) (*gate, error) {
	hold := s.m.tryHold(info.callerName)
	if hold == nil {
		return nil, nil
	}
	sync, err := syncSetting(hold, r.Header.Get(hdrSync))
	if err != nil {
		hold.Release()
		return nil, err
	}
	g := &gate{m: s.m, hold: hold, sync: sync, ctx: r.Context(), calls: &info.gates}
	if err := g.wait(1, nil); err != nil { // the request line and headers
		hold.Release()
		return nil, err
	}
	return g, nil
}

// syncSetting applies a Shardlite-Sync override ("none" or "storage") to the
// kind's tier. An unknown name is refused with a 400: copying the kind's tier
// could quietly give less than what the app said, and the guide's status table
// already fail a malformed Shardlite-* header.
func syncSetting(hold *Hold, override string) (kind.Sync, error) {
	if override != "" {
		s, err := kind.ParseSync(override)
		if err != nil {
			return 0, &statusError{http.StatusBadRequest, reasonBadRequest, fmt.Sprintf("bad Shardlite-Sync: %v", err)}
		}
		return s, nil
	}
	return hold.Sync(), nil
}

// modifyResponse is where the app's output meets the gate.
func (s *Server) modifyResponse(resp *http.Response) error {
	info, _ := resp.Request.Context().Value(ctxKey{}).(*reqInfo)
	override := resp.Header.Get(hdrSync)
	stripShardliteHeaders(resp.Header)
	if info == nil || info.hold == nil {
		return nil // shardless: a plain reverse proxy
	}
	sync, err := syncSetting(info.hold, override)
	if err != nil {
		return err
	}
	ctx := resp.Request.Context()
	// The status line and headers are the first chunk.
	if err := s.m.Gate(ctx, info.hold, sync); err != nil {
		return err
	}
	g := &gate{m: s.m, hold: info.hold, sync: sync, ctx: ctx, resp: resp, calls: &info.gates}
	if rwc, ok := resp.Body.(io.ReadWriteCloser); ok && resp.StatusCode == http.StatusSwitchingProtocols {
		resp.Body = gatedConn{rwc, g, &info.gates}
	} else {
		resp.Body = gatedBody{resp.Body, g}
	}
	return nil
}
