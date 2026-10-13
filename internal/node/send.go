package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// internalCtxKey marks the context of a request the node makes to itself: an
// intake or the delivery of an inbox row. Such a request comes from a row that
// is durable already, so it isn't gated on the caller, and a gRPC failure is
// read from its status rather than turned into a gRPC response.
type internalCtxKey struct{}

// deliveryCtxKey carries what the node tells a handler about the inbox row it is
// called for.
type deliveryCtxKey struct{}

type deliveryInfo struct {
	id   int64
	hops int
}

// sendIntake sends an outbox row to the node of target as the call it will
// become, from its sender, through the same routing as a call over the control
// API (activation, forwarding, deadlines and the target checks): the node's own
// handler is called in process, with no socket. Between nodes the marker will be
// the header Shardlite-Inbox: true; here it is a context value.
func (s *Server) sendIntake(ctx context.Context, sender *ActiveShard, target string, row *outRow) (int, error) {
	ctx, cancel := context.WithTimeout(context.WithValue(context.WithValue(ctx, internalCtxKey{}, true), intakeCtxKey{}, true), s.opts.MessageTimeout)
	defer cancel()
	req := s.internalRequest(ctx, row.path, row.body)
	req.Header.Set(hdrTarget, target)
	req.Header.Set(hdrCaller, sender.Name)
	req.Header.Set(hdrHops, strconv.Itoa(row.hops))
	ct := row.contentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	req.Header.Set("Content-Type", ct)
	w, status, err := s.serveInternal(req)
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
func (s *Server) sendInbox(ctx context.Context, a *ActiveShard, row *inRow) (int, error) {
	if _, err := url.ParseRequestURI(row.path); err != nil {
		return http.StatusBadRequest, fmt.Errorf("bad path %q: %w", row.path, err)
	}
	ctx, cancel := context.WithTimeout(context.WithValue(context.WithValue(ctx, internalCtxKey{}, true), deliveryCtxKey{}, &deliveryInfo{id: row.id, hops: row.hops}), s.opts.MessageTimeout)
	defer cancel()
	req := s.internalRequest(ctx, row.path, row.body)
	req.Header.Set(hdrTarget, a.Name)
	caller := row.sender
	if caller == "" {
		caller = a.Name
	}
	req.Header.Set(hdrCaller, caller)
	ct := row.contentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	req.Header.Set("Content-Type", ct)
	w, status, err := s.serveInternal(req)
	if err != nil || status/100 == 2 {
		return status, err
	}
	return status, fmt.Errorf("the handler answered %d: %s", status, bytes.TrimSpace(w.head))
}

// internalRequest builds a POST for the node's own routing, with the message
// timeout as the deadline the app is told.
func (s *Server) internalRequest(ctx context.Context, path string, body []byte) *http.Request {
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
	return req
}

// serveInternal runs a request through the node's routing in process and returns
// the answer. status is 0 if there was none, and err says why not.
func (s *Server) serveInternal(req *http.Request) (w *discardWriter, status int, err error) {
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
	s.route(w, req, true)
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
