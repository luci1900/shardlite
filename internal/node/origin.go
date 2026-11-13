package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/luci1900/shardlite/internal/model/cluster"
)

// origin says who asked for a request: the public handler (a client) and the
// control handler (a call by the app) set it from the request. The node's own
// requests say what they are. A wake of an app for a shard's reminders comes
// from the shard's own file, so it isn't gated on the caller. A wake is for
// the node that owns the shard and is never forwarded. A request of the node's
// own (the dispatcher's activation of any shard, which has no calling shard) is a
// call like the app's, except that it may use paths under /_shardlite/, which
// only the node sends, and keeps that when it is forwarded.
//
// A request another node forwarded keeps the kind it had there (a client's, a
// call or the node's) and is marked forwarded: the node it came from has gated
// its caller and counted its hops.
type origin struct {
	kind      originKind
	forwarded bool
	// from is the node a forward came through, as it named itself in Shardlite-Node: empty if the forward carried no good name.
	from cluster.Node
	// avoid is whether a forward carried Shardlite-Placement: avoid, which a draining node's forwards do. They are claims it placed here.
	avoid bool
}

type originKind int

const (
	fromClient originKind = iota // the public handler
	fromCall                     // the control handler
	fromWake                     // the node, waking the app for a shard's reminders
	fromNode                     // the node's own request (Server.call)
)

// call is true for a request with a caller behind it, which names its target in
// Shardlite-Target, has its hops read, may use the options of a call
// (Shardlite-If-Active and Shardlite-If-Exists) and may create a shard:
// everything but a client's request.
func (o origin) call() bool { return !o.client() }

// client is true for a request held to a client's limits: one from a client.
func (o origin) client() bool { return o.kind == fromClient }

// remindersPath is the path of the wake. The listeners refuse every path under
// /_shardlite/, so a request for it reaches the app only from the node's own
// routing, here.
const remindersPath = apiPrefix + "reminders"

// wakeApp wakes a shard's app for its reminders: a POST with no body to
// /_shardlite/reminders for the shard itself, through the same routing as a call
// over the control API, in process with no socket and with the reminder timeout
// on its context.
func (s *Server) wakeApp(ctx context.Context, a *actor) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, reminderTimeout)
	defer cancel()
	req := (&http.Request{Method: http.MethodPost, URL: &url.URL{Path: remindersPath}, Host: "localhost", Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1, Header: http.Header{}, Body: http.NoBody}).WithContext(ctx)
	req.Header.Set(hdrTarget, a.Name.String())
	req.Header.Set(hdrCaller, a.Name.String())
	w := &discardWriter{header: http.Header{}}
	status, err := s.serveInternal(w, req, origin{kind: fromWake})
	if err != nil || status/100 == 2 {
		return status, err
	}
	return status, fmt.Errorf("the app answered %d: %s", status, bytes.TrimSpace(w.head))
}

// call sends a request of the node's own through the node's routing, in process
// or forwarded to the shard's owner, and returns the whole answer: activations
// answer small. The request is for any shard (Shardlite-Target).
func (s *Server) call(req *http.Request) (*http.Response, error) {
	w := &bufferWriter{header: http.Header{}}
	status, err := s.serveInternal(w, req, origin{kind: fromNode})
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: status, Header: w.header, Body: io.NopCloser(&w.body), Request: req}, nil
}

// internalWriter is the response writer of an in-process request, which keeps
// the status.
type internalWriter interface {
	http.ResponseWriter
	code() int
}

// serveInternal runs a request through the node's routing in process into w and
// returns the status. status is 0 if there was none, and err says why not.
func (s *Server) serveInternal(w internalWriter, req *http.Request, o origin) (status int, err error) {
	ctx := req.Context()
	defer func() {
		// The reverse proxy aborts the handler when the receiver's response breaks off.
		if r := recover(); r != nil {
			if r == http.ErrAbortHandler {
				status, err = 0, errors.New("the receiver's response was cut off")
				return
			}
			panic(r)
		}
	}()
	s.route(w, req, o)
	if w.code() == 0 {
		if ctx.Err() != nil {
			return 0, context.Cause(ctx)
		}
		return http.StatusOK, nil
	}
	return w.code(), nil
}

// discardWriter is the response writer of an in-process request: it keeps the
// status and the start of the body (for the log) and drops the rest.
type discardWriter struct {
	header http.Header
	status int
	head   []byte
}

func (w *discardWriter) Header() http.Header { return w.header }

func (w *discardWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
}

func (w *discardWriter) Write(p []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	if room := 256 - len(w.head); room > 0 {
		w.head = append(w.head, p[:min(room, len(p))]...)
	}
	return len(p), nil
}

func (w *discardWriter) Flush() {}

func (w *discardWriter) code() int { return w.status }

// bufferWriter keeps a node request's whole response.
type bufferWriter struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (w *bufferWriter) Header() http.Header { return w.header }

func (w *bufferWriter) WriteHeader(code int) {
	if w.status == 0 && code >= 200 { // a forward's 102 is the owner's, not the answer
		w.status = code
	}
}

func (w *bufferWriter) code() int { return w.status }

func (w *bufferWriter) Flush() {}

func (w *bufferWriter) Write(p []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return w.body.Write(p)
}
