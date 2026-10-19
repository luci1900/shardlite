package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"strings"
	"time"
)

// errMoved is a forward's answer from a node that found the shard isn't its own.
var errMoved = errors.New("the shard has moved")

// forward sends a request for a shard another node owns to that node's peer
// listener, which serves it as this node would have (docs/internals.md, Routing
// and forwarding). The caller's gate, if any, is on the request's body already.
// A forward is never forwarded again: a node that finds a forward's shard isn't
// its own answers 421, naming the owner it found if it found one. The node that
// forwarded it corrects its owner cache (the owner named, or none) and answers
// 503 with Retry-After: 0, since the client's next attempt finds the shard: the
// node retries nothing itself.
func (s *Server) forward(w http.ResponseWriter, r *http.Request, info *reqInfo) {
	if info.forwarded {
		w.Header().Set(hdrOwner, info.owner)
		writeError(w, r, true, http.StatusMisdirectedRequest, fmt.Sprintf("%s is owned by %s", info.target, info.owner))
		return
	}
	address, err := s.m.addrs.address(r.Context(), info.owner)
	if err != nil {
		s.fail(w, r, info.own, fmt.Errorf("forwarding to %s, the owner of %s: %w", info.owner, info.target, err))
		return
	}
	info.peer = address
	// The owner's node acknowledges the forward with 102 as soon as it arrives.
	// One that hasn't within the answer timeout is unreachable from here, and the
	// forward is cancelled.
	ctx, cancel := context.WithCancelCause(r.Context())
	defer cancel(nil)
	info.answer.Store(answerAwaited)
	info.unreachable = false
	late := time.AfterFunc(s.opts.AnswerTimeout, func() {
		if info.answer.CompareAndSwap(answerAwaited, answerLate) {
			cancel(errNoAnswer)
		}
	})
	defer late.Stop()
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{Got1xxResponse: func(int, textproto.MIMEHeader) error {
		info.answer.CompareAndSwap(answerAwaited, answerHeard)
		return nil
	}})
	// The owner's 1xx are its own (it drops its app's), so none reaches the client.
	s.forwarder.ServeHTTP(noInformational{w}, r.WithContext(context.WithValue(ctx, ctxKey{}, info)))
	switch {
	case info.unreachable:
		s.unreachable(w, r, info)
	case info.moved:
		s.m.owners.Remove(info.target)
		if info.movedTo != "" && info.movedTo != s.m.n.Name {
			s.m.owners.Add(info.target, info.movedTo)
		}
		s.answer(w, r, info.own, &unavailable{fmt.Sprintf("%s is not owned by %s: try again", info.target, info.owner), 0})
	}
}

// forwardError is proxyError for a forward. A forward that failed before the
// owner's node answered anything (a refused or reset connection, a dial that
// timed out or no answer within the answer timeout) never reached the owner's
// app, and forward answers it from the owner's lease. The client going away is
// answered as for any request.
func (s *Server) forwardError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, errMoved) {
		return // forward reports it, and forwardOrRetry answers
	}
	info, _ := r.Context().Value(ctxKey{}).(*reqInfo)
	if cause := context.Cause(r.Context()); info != nil && info.answer.Load() != answerHeard && (cause == nil || cause == errNoAnswer) {
		info.unreachable = true
		return
	}
	s.proxyError(w, r, err)
}

// forwardedResponse is the owner's answer to a forward. It passes through as it
// is (the owner gated it), except a 421 from the owner's node. What a calling
// shard sends over an upgraded connection is gated here, on the caller.
func (s *Server) forwardedResponse(resp *http.Response) error {
	info, _ := resp.Request.Context().Value(ctxKey{}).(*reqInfo)
	info.answer.Store(answerHeard)
	s.m.checks.answered(info.owner)
	// Only a node sets Shardlite-Owner (an owner strips the app's), though it may
	// be empty: the node that answered didn't find the owner.
	if owner, ok := resp.Header[hdrOwner]; ok && resp.StatusCode == http.StatusMisdirectedRequest {
		info.moved, info.movedTo = true, strings.Join(owner, "")
		return fmt.Errorf("%w: %s is not owned by %s but by %q", errMoved, info.target, info.owner, info.movedTo)
	}
	if rwc, ok := resp.Body.(io.ReadWriteCloser); ok && resp.StatusCode == http.StatusSwitchingProtocols {
		resp.Body = gatedConn{rwc, nil, &info.gates}
		return nil
	}
	return nil
}
