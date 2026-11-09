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

	"github.com/luci1900/shardlite/internal/cluster"
)

// errMoved is a forward's answer from a node that found the shard isn't its own.
var errMoved = errors.New("the shard has moved")

// errNoAnswer is the cause a forward is cancelled with when the owner's node
// hasn't acknowledged it within the answer timeout.
var errNoAnswer = errors.New("the owner's node did not answer")

// What a forward has heard from the owner's node (reqInfo.answer). A node
// acknowledges every forward with 102 Processing as soon as it arrives, so the
// forwarding node can tell a node that doesn't answer from a slow app.
type answerState int32

const (
	answerAwaited answerState = iota // nothing yet
	answerHeard                      // the owner's 102, or the response's headers
	answerLate                       // nothing within the answer timeout: the forward was cancelled
)

// unavailable is a 503 the node answers with, and the Retry-After it gives.
type unavailable struct {
	reason reason
	msg    string
	retry  time.Duration
}

func (e *unavailable) Error() string { return e.msg }

// forward sends a request for a shard another node owns to that node's peer
// listener, which serves it as this node would have (docs/internals.md, Routing
// and forwarding). The caller's gate, if any, is on the request's body already.
// A forward is never forwarded again: a node that finds a forward's shard isn't
// its own answers 421, naming the owner it found if it found one. The node that
// forwarded it corrects its owner cache (the owner named, or none) and answers
// 503 with Retry-After: 0, since the client's next attempt finds the shard: the
// node retries nothing itself. A forward that gets no answer from the owner's
// node is answered 503 with Retry-After: 1. The owner cache keeps its entry
// while the lease scan counts the owner live, and drops it once the scan counts
// the owner dead: the scan empties the entries that name a node when it first
// finds it dead, and this catches one added after that. The attempt after that
// reads the manifest.
func (s *Server) forward(w http.ResponseWriter, r *http.Request, info *reqInfo) {
	if info.forwarded {
		w.Header().Set(hdrOwner, string(info.owner))
		writeError(w, r, true, http.StatusMisdirectedRequest, reasonNotOwner, fmt.Sprintf("%s is owned by %s", info.name, info.owner))
		return
	}
	address, err := s.m.addrs.Address(r.Context(), info.owner)
	if err != nil {
		s.fail(w, r, info.own, fmt.Errorf("forwarding to %s, the owner of %s: %w", info.owner, info.name, err))
		return
	}
	info.peer = address
	// The owner's node acknowledges the forward with 102 as soon as it arrives.
	// One that hasn't within the answer timeout is unreachable from here, and the
	// forward is cancelled.
	ctx, cancel := context.WithCancelCause(r.Context())
	defer cancel(nil)
	info.answer.Store(int32(answerAwaited))
	info.unreachable = false
	late := time.AfterFunc(s.opts.AnswerTimeout, func() {
		if info.answer.CompareAndSwap(int32(answerAwaited), int32(answerLate)) {
			cancel(errNoAnswer)
		}
	})
	defer late.Stop()
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{Got1xxResponse: func(_ int, h textproto.MIMEHeader) error {
		info.answer.CompareAndSwap(int32(answerAwaited), int32(answerHeard))
		s.m.placer.Answered(info.owner, http.Header(h))
		return nil
	}})
	// The owner's 1xx are its own (it drops its app's), so none reaches the client.
	s.forwarder.ServeHTTP(noInformational{w}, r.WithContext(context.WithValue(ctx, ctxKey{}, info)))
	switch {
	case info.unreachable:
		s.m.Stats.Unanswered.Inc()
		s.m.placer.Unanswered(info.owner)
		if !s.m.isLive(info.owner) {
			s.m.owners.Remove(info.name)
		}
		s.answer(w, r, info.own, &unavailable{reasonOwnerUnreachable, fmt.Sprintf("%s, the owner of %s, does not answer", info.owner, info.name), time.Second})
	case info.moved:
		s.m.owners.Remove(info.name)
		if info.movedTo != "" && info.movedTo != s.m.n.Name {
			s.m.owners.Add(info.name, info.movedTo)
		}
		s.answer(w, r, info.own, &unavailable{reasonMoved, fmt.Sprintf("%s is not owned by %s: try again", info.name, info.owner), 0})
	}
}

// forwardError is proxyError for a forward. A forward that failed before the
// owner's node answered anything (a refused or reset connection, a dial that
// timed out or no answer within the answer timeout) never reached the owner's
// app, and forward answers it with a 503. The client going away is answered as
// for any request.
func (s *Server) forwardError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, errMoved) {
		return // forwardedResponse reports it, and forward answers
	}
	info, _ := r.Context().Value(ctxKey{}).(*reqInfo)
	if cause := context.Cause(r.Context()); info != nil && info.answer.Load() != int32(answerHeard) && (cause == nil || cause == errNoAnswer) {
		info.unreachable = true
		return
	}
	s.upstreamError(w, r, err, reasonOwnerFailed, reasonOwnerUnreachable)
}

// forwardedResponse is the owner's answer to a forward. It passes through as it
// is (the owner gated it), except for its Shardlite-Placement, which is taken
// out, and a 421 from the owner's node. Its Shardlite-Error goes through, so the
// reason names the cause where it happened. What a calling shard sends over an
// upgraded connection is gated here, on the caller.
func (s *Server) forwardedResponse(resp *http.Response) error {
	info, _ := resp.Request.Context().Value(ctxKey{}).(*reqInfo)
	info.answer.Store(int32(answerHeard))
	s.m.placer.Answered(info.owner, resp.Header) // and takes the placement out, which the client never sees
	// Only a node sets Shardlite-Owner (an owner strips the app's), though it may
	// be empty: the node that answered didn't find the owner.
	if owner, ok := resp.Header[hdrOwner]; ok && resp.StatusCode == http.StatusMisdirectedRequest {
		info.moved, info.movedTo = true, cluster.Node(strings.Join(owner, ""))
		return fmt.Errorf("%w: %s is not owned by %s but by %q", errMoved, info.name, info.owner, info.movedTo)
	}
	if rwc, ok := resp.Body.(io.ReadWriteCloser); ok && resp.StatusCode == http.StatusSwitchingProtocols {
		resp.Body = gatedConn{rwc, nil, &info.gates}
		return nil
	}
	return nil
}
