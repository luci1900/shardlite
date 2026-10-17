package node

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// origin says who asked for a request: the public handler (a client) and the
// control handler (a call by the app) set it from the request. The node's own
// requests, an intake or the delivery of an inbox row, say what they are. Such a
// request comes from a row that is durable already, so it isn't gated on the
// caller, and a gRPC failure is read from its status rather than turned into a
// gRPC response.
//
// A request another node forwarded keeps the kind it had there (a client's or a
// call) and is marked forwarded: the node it came from has gated its caller and
// counted its hops.
type origin struct {
	kind      originKind
	forwarded bool
	inboxID   int64 // the row a delivery is for
	hops      int   // the row's own hops, which the handler sees
}

type originKind int

const (
	fromClient   originKind = iota // the public handler
	fromCall                       // the control handler
	fromIntake                     // the node, delivering an outbox row to the target's inbox
	fromDelivery                   // the node, calling the handler for an inbox row
)

func (o origin) control() bool  { return o.kind != fromClient }
func (o origin) internal() bool { return o.kind >= fromIntake }

// transport is the node's way of reaching its shards in process, for the
// messenger: the server's routing, with the message timeout and the hop limit.
func (s *Server) transport() *transport {
	return &transport{intake: s.sendIntake, inbox: s.sendInbox, maxHops: s.opts.MaxHops}
}

// sendIntake sends an outbox row to the node of target as the call it will
// become, from its sender, through the same routing as a call over the control
// API (activation, forwarding, deadlines and the target checks): the node's own
// handler is called in process, with no socket. Between nodes the marker will be
// the header Shardlite-Inbox: true; here it is the origin.
func (s *Server) sendIntake(ctx context.Context, sender *shard, target string, row *outRow) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, s.opts.MessageTimeout)
	defer cancel()
	req := s.internalRequest(ctx, row.path, row.contentType, row.body)
	req.Header.Set(hdrTarget, target)
	req.Header.Set(hdrCaller, sender.Name)
	req.Header.Set(hdrHops, strconv.Itoa(row.hops))
	w, status, err := s.serveInternal(req, origin{kind: fromIntake})
	if err != nil || status/100 == 2 {
		return status, err
	}
	return status, &answerError{status: status, body: string(bytes.TrimSpace(w.head))}
}

// answerError is a receiver's answer that isn't a 2xx: its status and the start
// of its body.
type answerError struct {
	status int
	body   string
}

func (e *answerError) Error() string {
	return fmt.Sprintf("the receiver's node answered %d: %s", e.status, e.body)
}

// sendInbox calls a shard's handler for one of its inbox rows: a POST to the
// row's path on the shard itself.
func (s *Server) sendInbox(ctx context.Context, a *shard, row *inRow) (int, error) {
	if _, err := url.ParseRequestURI(row.path); err != nil {
		return http.StatusBadRequest, fmt.Errorf("bad path %q: %w", row.path, err)
	}
	ctx, cancel := context.WithTimeout(ctx, s.opts.MessageTimeout)
	defer cancel()
	req := s.internalRequest(ctx, row.path, row.contentType, row.body)
	req.Header.Set(hdrTarget, a.Name)
	req.Header.Set(hdrCaller, cmp.Or(row.sender, a.Name))
	w, status, err := s.serveInternal(req, origin{kind: fromDelivery, inboxID: row.id, hops: row.hops})
	if err != nil || status/100 == 2 {
		return status, err
	}
	return status, fmt.Errorf("the handler answered %d: %s", status, bytes.TrimSpace(w.head))
}

// defaultContentType is what a message without a content type of its own is sent as.
const defaultContentType = "application/octet-stream"

// internalRequest builds a POST for the node's own routing, with the message
// timeout as the deadline the app is told, and contentType (or the default).
func (s *Server) internalRequest(ctx context.Context, path, contentType string, body []byte) *http.Request {
	u, _ := url.ParseRequestURI(path)
	if u == nil {
		u = &url.URL{Path: path}
	}
	req := (&http.Request{Method: http.MethodPost, URL: u, Host: "localhost", Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1, Header: http.Header{}}).WithContext(ctx)
	if len(body) > 0 {
		req.Body, req.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
	} else {
		req.Body = http.NoBody
	}
	req.Header.Set(hdrDeadline, strconv.FormatInt(s.opts.MessageTimeout.Milliseconds(), 10))
	req.Header.Set("Content-Type", cmp.Or(contentType, defaultContentType))
	return req
}

// serveInternal runs a request through the node's routing in process and returns
// the answer. status is 0 if there was none, and err says why not.
func (s *Server) serveInternal(req *http.Request, o origin) (w *discardWriter, status int, err error) {
	ctx := req.Context()
	w = &discardWriter{header: http.Header{}}
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
	if w.status == 0 {
		if ctx.Err() != nil {
			return w, 0, context.Cause(ctx)
		}
		w.status = http.StatusOK
	}
	if w.status/100 == 2 && isGRPC(req) {
		// A gRPC receiver answers 200 and says how it went in a status.
		v := w.header.Get("Grpc-Status")
		if v == "" {
			v = w.header.Get(http.TrailerPrefix + "Grpc-Status")
		}
		if code, err := strconv.Atoi(v); err == nil && code != 0 {
			w.status = http.StatusBadRequest
			switch code {
			case 4, 8, 10, 13, 14: // deadline exceeded, resource exhausted, aborted, internal, unavailable
				w.status = http.StatusServiceUnavailable
			}
			return w, w.status, fmt.Errorf("the handler answered with grpc-status %d: %s", code, w.header.Get("Grpc-Message"))
		}
	}
	return w, w.status, nil
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
