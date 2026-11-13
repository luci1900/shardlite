package blob_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/smithy-go"

	"github.com/luci1900/shardlite/internal/storage/blob"
)

// roundTripFunc is an http.RoundTripper for tests.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// wrote tells the S3 backend the request was sent in full, as net/http does.
func wrote(r *http.Request) {
	if tr := httptrace.ContextClientTrace(r.Context()); tr != nil && tr.WroteRequest != nil {
		tr.WroteRequest(httptrace.WroteRequestInfo{})
	}
}

// s3Error is a response as S3 sends it for an error.
func s3Error(status int, code string) *http.Response {
	body := ""
	if code != "" {
		body = fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>injected</Message><RequestId>1</RequestId></Error>`, code)
	}
	return &http.Response{
		StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)), ProtoMajor: 1, ProtoMinor: 1,
		Header: http.Header{"Content-Type": {"application/xml"}, "X-Amz-Request-Id": {"1"}}, Body: io.NopCloser(strings.NewReader(body)),
	}
}

// s3Object is a successful response carrying an ETag and a write id.
func s3Object(etag, write string) *http.Response {
	h := http.Header{"Content-Length": {"1"}}
	if etag != "" {
		h.Set("ETag", etag)
	}
	if write != "" {
		h.Set("X-Amz-Meta-Shardlite-Write", write)
	}
	return &http.Response{
		StatusCode: 200, Status: "200 OK", ProtoMajor: 1, ProtoMinor: 1,
		Header: h, Body: io.NopCloser(strings.NewReader("")),
	}
}

// fakeS3 is an S3 backend whose every request goes to rt.
func fakeS3(t *testing.T, rt http.RoundTripper) *blob.S3 {
	t.Helper()
	b, err := blob.NewS3(context.Background(), blob.S3Options{Bucket: "b", Endpoint: "http://s3.invalid", HTTPClient: &http.Client{Transport: rt}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The responses S3 and compatible stores give map onto the outcomes (docs/internals.md,
// Coordination), with no store behind them.
func TestS3StatusMapping(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	type call func(b *blob.S3) error
	create := func(b *blob.S3) error {
		_, err := b.Create(ctx, "k", strings.NewReader("v"), blob.WriteID{Node: "n", Epoch: 1, N: 1})
		return err
	}
	update := func(b *blob.S3) error {
		_, err := b.Update(ctx, "k", strings.NewReader("v"), "etag", blob.WriteID{Node: "n", Epoch: 1, N: 1})
		return err
	}
	get := func(b *blob.S3) error { _, _, err := b.Get(ctx, "k"); return err }
	head := func(b *blob.S3) error { _, err := b.Head(ctx, "k"); return err }
	del := func(b *blob.S3) error { return b.Delete(ctx, "k", "") }

	for _, tc := range []struct {
		name      string
		do        call
		status    int
		code      string
		want      error // nil: success or a plain error, see plain
		plain     bool  // a plain error: none of the outcomes, and not retried
		transient bool
	}{
		{"create over an object", create, 412, "PreconditionFailed", blob.ErrExists, false, false},
		{"update of a changed object", update, 412, "PreconditionFailed", blob.ErrConflict, false, false},
		{"update of a missing object", update, 404, "NoSuchKey", blob.ErrNotFound, false, false},
		{"create racing another write", create, 409, "ConditionalRequestConflict", blob.ErrNotApplied, false, true},
		{"update racing another write", update, 409, "ConditionalRequestConflict", blob.ErrNotApplied, false, true},
		{"create, too many requests", create, 429, "", blob.ErrNotApplied, false, true},
		{"update, slow down", update, 503, "SlowDown", blob.ErrNotApplied, false, true},
		{"create, service unavailable", create, 503, "ServiceUnavailable", blob.ErrUnknown, false, true},
		{"update, 503 with no code", update, 503, "", blob.ErrUnknown, false, true},
		{"delete, service unavailable", del, 503, "ServiceUnavailable", blob.ErrUnknown, false, true},
		{"create, internal error", create, 500, "InternalError", blob.ErrUnknown, false, true},
		{"update, bad gateway", update, 502, "", blob.ErrUnknown, false, true},
		{"create, request timeout", create, 400, "RequestTimeout", blob.ErrUnknown, false, true},
		{"create, access denied", create, 403, "AccessDenied", nil, true, false},
		{"update, bad request", update, 400, "InvalidArgument", nil, true, false},
		{"create, no such bucket", create, 404, "NoSuchBucket", nil, true, false},
		{"get of a missing object", get, 404, "NoSuchKey", blob.ErrNotFound, false, false},
		{"head of a missing object", head, 404, "", blob.ErrNotFound, false, false},
		{"get, internal error", get, 500, "InternalError", blob.ErrUnknown, false, true},
		{"delete of a missing object", del, 404, "NoSuchKey", nil, false, false},
		{"delete, slow down", del, 503, "SlowDown", blob.ErrNotApplied, false, true},
		{"delete, access denied", del, 403, "AccessDenied", nil, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := fakeS3(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
				wrote(r)
				return s3Error(tc.status, tc.code), nil
			}))
			err := tc.do(b)
			switch {
			case tc.want != nil:
				if !errors.Is(err, tc.want) {
					t.Fatalf("got %v, want %v", err, tc.want)
				}
			case tc.plain:
				if err == nil || blob.Transient(err) || errors.Is(err, blob.ErrExists) || errors.Is(err, blob.ErrConflict) || errors.Is(err, blob.ErrNotFound) {
					t.Fatalf("got %v, want a plain error", err)
				}
			default:
				if err != nil {
					t.Fatalf("got %v, want success", err)
				}
			}
			if blob.Transient(err) != tc.transient {
				t.Fatalf("Transient(%v) = %v, want %v", err, blob.Transient(err), tc.transient)
			}
			// The SDK's error stays in the chain, and brings no outcome of its own.
			for _, o := range []error{blob.ErrExists, blob.ErrConflict, blob.ErrNotFound, blob.ErrNotApplied, blob.ErrUnknown} {
				if errors.Is(err, o) != (o == tc.want) {
					t.Fatalf("errors.Is(%v, %v) = %v", err, o, !(o == tc.want))
				}
			}
			if ae, ok := errors.AsType[smithy.APIError](err); tc.code != "" && err != nil && (!ok || ae.ErrorCode() != tc.code) {
				t.Fatalf("%v has no smithy.APIError with code %q in its chain", err, tc.code)
			}
		})
	}
}

// A request that never got to the store is not applied; one that was sent and
// got no answer may have been.
func TestS3TransportFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	var attempts atomic.Int32
	b := fakeS3(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts.Add(1)
		return nil, refused // nothing written
	}))
	write := func() error { _, err := b.Create(ctx, "k", strings.NewReader("v"), blob.WriteID{}); return err }
	if err := write(); !errors.Is(err, blob.ErrNotApplied) || errors.Is(err, blob.ErrUnknown) {
		t.Fatalf("connection refused: %v, want ErrNotApplied", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("the SDK retried %d times: blob.Store owns retries", attempts.Load())
	}
	if _, _, err := b.Get(ctx, "k"); !errors.Is(err, blob.ErrNotApplied) {
		t.Fatalf("get with the connection refused: %v", err)
	}

	for name, cause := range map[string]error{
		"connection reset":   &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)},
		"unexpected EOF":     io.ErrUnexpectedEOF,
		"a timeout":          &timeoutError{},
		"the server hung up": io.EOF,
	} {
		b := fakeS3(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
			wrote(r) // the whole request went out
			return nil, cause
		}))
		for op, f := range map[string]func() error{
			"create": func() error { _, err := b.Create(ctx, "k", strings.NewReader("v"), blob.WriteID{}); return err },
			"update": func() error { _, err := b.Update(ctx, "k", strings.NewReader("v"), "e", blob.WriteID{}); return err },
			"delete": func() error { return b.Delete(ctx, "k", "") },
		} {
			if err := f(); !errors.Is(err, blob.ErrUnknown) || errors.Is(err, blob.ErrNotApplied) {
				t.Errorf("%s, %s after the request was sent: %v, want ErrUnknown", op, name, err)
			}
		}
	}
	// A cancelled write may have happened, and says so, keeping the context's error findable.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	b = fakeS3(t, roundTripFunc(func(r *http.Request) (*http.Response, error) { return nil, r.Context().Err() }))
	if _, err := b.Create(cctx, "k", strings.NewReader("v"), blob.WriteID{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled create: %v", err)
	}
}

// A timeout of the transport's own (ResponseHeaderTimeout) matches
// context.DeadlineExceeded, but it is not the caller's deadline: a read that hangs
// past it is retried, and a write that was fully sent is unknown. The caller's own
// context ending is returned as it is, and is not retried.
func TestS3TransportTimeouts(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		// Never answers. (A request with an unread body doesn't notice its client leave.)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer srv.Close()
	defer close(release)
	tr := &http.Transport{ResponseHeaderTimeout: 20 * time.Millisecond}
	defer tr.CloseIdleConnections()
	b, err := blob.NewS3(context.Background(), blob.S3Options{Bucket: "b", Endpoint: srv.URL, HTTPClient: &http.Client{Transport: tr}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if _, err := b.Head(ctx, "k"); !errors.Is(err, blob.ErrNotApplied) || errors.Is(err, blob.ErrUnknown) {
		t.Fatalf("head past the header timeout: %v, want ErrNotApplied", err)
	}
	if _, _, err := b.Get(ctx, "k"); !errors.Is(err, blob.ErrNotApplied) {
		t.Fatalf("get past the header timeout: %v, want ErrNotApplied", err)
	}
	requests.Store(0)
	s := blob.NewStore(b, "n1", 1, blob.Options{Attempts: 3, Backoff: time.Millisecond})
	if _, err := s.Head(ctx, "k"); !errors.Is(err, blob.ErrNotApplied) {
		t.Fatalf("store head: %v", err)
	}
	if requests.Load() != 3 {
		t.Fatalf("%d requests, want one per attempt", requests.Load())
	}
	if _, err := b.Create(ctx, "k", strings.NewReader("v"), blob.WriteID{}); !errors.Is(err, blob.ErrUnknown) || errors.Is(err, blob.ErrNotApplied) {
		t.Fatalf("create past the header timeout: %v, want ErrUnknown", err)
	}

	// The caller's own deadline or cancellation is theirs, and not retried.
	dctx, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	<-dctx.Done()
	cctx, cancelNow := context.WithCancel(ctx)
	cancelNow()
	for ctx, want := range map[context.Context]error{dctx: context.DeadlineExceeded, cctx: context.Canceled} {
		requests.Store(0)
		if _, err := s.Head(ctx, "k"); !errors.Is(err, want) || blob.Transient(err) {
			t.Fatalf("head with the caller's context ended: %v, want %v and not retryable", err, want)
		}
		if requests.Load() > 1 {
			t.Fatalf("%d requests after the caller gave up", requests.Load())
		}
	}
}

// timeoutError stands in for the error net/http produces on a timeout.
type timeoutError struct{}

func (*timeoutError) Error() string   { return "i/o timeout" }
func (*timeoutError) Timeout() bool   { return true }
func (*timeoutError) Temporary() bool { return true }

// A write id goes out in the object's metadata as plain ASCII, and Head reads
// it back; a value that doesn't decode is no id. ETags come back without the
// quotes S3 puts around them.
func TestS3WriteIDEncoding(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var meta atomic.Pointer[string]
	b := fakeS3(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.Method {
		case http.MethodPut:
			v := r.Header.Get("X-Amz-Meta-Shardlite-Write")
			meta.Store(&v)
			return s3Object(`"put"`, ""), nil
		case http.MethodHead:
			write := ""
			if v := meta.Load(); v != nil {
				write = *v
			}
			return s3Object(`W/"abc"`, write), nil
		}
		return s3Error(404, "NoSuchKey"), nil
	}))
	for _, id := range []blob.WriteID{{Node: "n1", Epoch: 1, N: 1}, {Node: "a:b c/é.d%", Epoch: 3, N: 9}, {Node: "a.b.c", Epoch: 2, N: 7}, {Node: "", Epoch: 0, N: 5}} {
		if _, err := b.Create(ctx, "k", strings.NewReader("v"), id); err != nil {
			t.Fatal(err)
		}
		v := *meta.Load()
		for _, c := range []byte(v) {
			if c < 0x20 || c > 0x7e {
				t.Fatalf("%q is not plain ASCII", v)
			}
		}
		info, err := b.Head(ctx, "k")
		if err != nil || info.WriteID != id {
			t.Fatalf("%+v -> %q -> %+v (%v)", id, v, info.WriteID, err)
		}
		if info.ETag != "abc" {
			t.Fatalf("etag %q", info.ETag)
		}
	}
	// A metadata value that doesn't decode is no id, not a failure.
	bad := "garbage"
	meta.Store(&bad)
	if info, err := b.Head(ctx, "k"); err != nil || !info.WriteID.IsZero() {
		t.Fatalf("garbage metadata: %+v %v", info, err)
	}
}

// faults sits in front of the emulator's transport.
type faults struct {
	inner http.RoundTripper
	// dropPUT, if it returns true for a PUT, lets the request reach the store and then
	// loses the response.
	dropPUT func(r *http.Request) bool
	puts    atomic.Int32
	delay   time.Duration // before returning any response
}

func (f *faults) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := f.inner.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	if r.Method == http.MethodPut {
		f.puts.Add(1)
		if f.dropPUT != nil && f.dropPUT(r) {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			return nil, &net.OpError{Op: "read", Net: "tcp", Err: os.NewSyscallError("read", syscall.ECONNRESET)}
		}
	}
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-r.Context().Done():
			resp.Body.Close()
			return nil, r.Context().Err()
		}
	}
	return resp, nil
}

// A real store sends a time with every response, which is within a second of the
// process's, since the emulator runs beside it, so the check is quiet.
func TestS3SamplesAgainstTheEmulator(t *testing.T) {
	t.Parallel()
	endpoint := os.Getenv("SHARDLITE_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("SHARDLITE_TEST_S3_ENDPOINT is not set (docker/compose.s3.yaml runs an emulator)")
	}
	var l logged
	b := s3At(t, endpoint, func(o *blob.S3Options) { o.Logger = l.log() })
	for range 2 * blob.ClockRun {
		b.Head(context.Background(), "missing") // an error's response carries a time too
	}
	if got := l.lines(); len(got) != 0 {
		t.Fatalf("the emulator's responses logged %q", got)
	}
}

// With the emulator behind a fault-injecting transport: a response lost after
// the PUT reached the store is ErrUnknown, and Store recognises its own write by
// its write id; a refused connection is ErrNotApplied.
func TestS3LostResponsesAgainstTheEmulator(t *testing.T) {
	t.Parallel()
	endpoint := os.Getenv("SHARDLITE_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("SHARDLITE_TEST_S3_ENDPOINT is not set (docker/compose.s3.yaml runs an emulator)")
	}
	ctx := context.Background()
	f := &faults{inner: http.DefaultTransport}
	b := s3At(t, endpoint, func(o *blob.S3Options) { o.HTTPClient = &http.Client{Transport: f} })

	// The backend alone: the object is there, the caller was told "unknown".
	var dropped atomic.Bool
	f.dropPUT = func(*http.Request) bool { return dropped.CompareAndSwap(false, true) }
	id := blob.WriteID{Node: "n1", Epoch: 1, N: 1}
	if _, err := b.Create(ctx, "lost", strings.NewReader("v"), id); !errors.Is(err, blob.ErrUnknown) {
		t.Fatalf("a response lost after the PUT: %v, want ErrUnknown", err)
	}
	if info, err := b.Head(ctx, "lost"); err != nil || info.WriteID != id {
		t.Fatalf("the object the lost response was for: %+v %v", info, err)
	}

	// Through the Store: retried, and the "exists" that answers is recognised as its own.
	f.puts.Store(0)
	dropped.Store(false)
	s := blob.NewStore(b, "n1", 1, blob.Options{Backoff: time.Millisecond})
	etag, err := s.Create(ctx, "lost2", strings.NewReader("v"))
	if err != nil {
		t.Fatalf("Store.Create after a lost response: %v", err)
	}
	if info, err := s.Head(ctx, "lost2"); err != nil || info.ETag != etag || info.WriteID.Node != "n1" {
		t.Fatalf("head %+v %v (create gave %q)", info, err, etag)
	}
	if f.puts.Load() != 2 {
		t.Fatalf("%d PUTs, want the first and one retry", f.puts.Load())
	}
	// ...and the same for an update.
	dropped.Store(false)
	etag2, err := s.Update(ctx, "lost2", strings.NewReader("w"), etag)
	if err != nil {
		t.Fatalf("Store.Update after a lost response: %v", err)
	}
	if info, _ := s.Head(ctx, "lost2"); info.ETag != etag2 {
		t.Fatalf("the ETag Update returned is stale: %q vs %q", etag2, info.ETag)
	}
	// Somebody else's write is still a conflict after a lost response.
	other := blob.NewStore(b, "n2", 1, blob.Options{Backoff: time.Millisecond})
	dropped.Store(true) // no more drops
	if _, err := other.Update(ctx, "lost2", strings.NewReader("theirs"), etag2); err != nil {
		t.Fatal(err)
	}
	dropped.Store(false)
	if _, err := s.Update(ctx, "lost2", strings.NewReader("mine"), etag2); !errors.Is(err, blob.ErrConflict) {
		t.Fatalf("update from a stale ETag: %v, want ErrConflict", err)
	}

	// A timeout after the request was sent is unknown too: the client gives up on
	// a response the store has already produced.
	slow := &faults{inner: http.DefaultTransport, delay: 400 * time.Millisecond}
	b2 := s3At(t, endpoint, func(o *blob.S3Options) { o.HTTPClient = &http.Client{Transport: slow, Timeout: 150 * time.Millisecond} })
	if _, err := b2.Create(ctx, "slowwrite", strings.NewReader("v"), blob.WriteID{}); !errors.Is(err, blob.ErrUnknown) {
		t.Fatalf("a timeout after sending: %v, want ErrUnknown", err)
	}

	// The connection refused: nothing was sent.
	down := s3At(t, "http://127.0.0.1:1", nil)
	if _, err := down.Create(ctx, "k", strings.NewReader("v"), blob.WriteID{}); !errors.Is(err, blob.ErrNotApplied) {
		t.Fatalf("connection refused: %v, want ErrNotApplied", err)
	}
	if _, err := blob.NewStore(down, "n1", 1, blob.Options{Backoff: time.Millisecond}).Create(ctx, "k", strings.NewReader("v")); !errors.Is(err, blob.ErrNotApplied) {
		t.Fatalf("Store.Create with the store down: %v", err)
	}
}
