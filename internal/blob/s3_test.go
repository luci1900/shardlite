package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
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

// fakeS3 is an S3 backend whose every request goes to rt.
func fakeS3(t *testing.T, rt http.RoundTripper) *S3 {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "k")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "s")
	t.Setenv("AWS_REGION", "us-east-1")
	b, err := NewS3(context.Background(), S3Options{Bucket: "b", Endpoint: "http://s3.invalid", HTTPClient: &http.Client{Transport: rt}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The responses S3 and compatible stores give map onto the outcomes (design.md,
// Coordination), with no store behind them.
func TestS3StatusMapping(t *testing.T) {
	ctx := context.Background()
	type call func(b *S3) error
	create := func(b *S3) error {
		_, err := b.Create(ctx, "k", strings.NewReader("v"), WriteID{Node: "n", Epoch: 1, N: 1})
		return err
	}
	update := func(b *S3) error {
		_, err := b.Update(ctx, "k", strings.NewReader("v"), "etag", WriteID{Node: "n", Epoch: 1, N: 1})
		return err
	}
	get := func(b *S3) error { _, _, err := b.Get(ctx, "k"); return err }
	head := func(b *S3) error { _, err := b.Head(ctx, "k"); return err }
	del := func(b *S3) error { return b.Delete(ctx, "k", "") }

	for _, tc := range []struct {
		name      string
		do        call
		status    int
		code      string
		want      error // nil: success or a plain error, see plain
		plain     bool  // a plain error: none of the outcomes, and not retried
		transient bool
	}{
		{"create over an object", create, 412, "PreconditionFailed", ErrExists, false, false},
		{"update of a changed object", update, 412, "PreconditionFailed", ErrConflict, false, false},
		{"update of a missing object", update, 404, "NoSuchKey", ErrNotFound, false, false},
		{"create racing another write", create, 409, "ConditionalRequestConflict", ErrNotApplied, false, true},
		{"update racing another write", update, 409, "ConditionalRequestConflict", ErrNotApplied, false, true},
		{"create, too many requests", create, 429, "", ErrNotApplied, false, true},
		{"update, slow down", update, 503, "SlowDown", ErrNotApplied, false, true},
		{"create, service unavailable", create, 503, "ServiceUnavailable", ErrNotApplied, false, true},
		{"create, internal error", create, 500, "InternalError", ErrUnknown, false, true},
		{"update, bad gateway", update, 502, "", ErrUnknown, false, true},
		{"create, request timeout", create, 400, "RequestTimeout", ErrUnknown, false, true},
		{"create, access denied", create, 403, "AccessDenied", nil, true, false},
		{"update, bad request", update, 400, "InvalidArgument", nil, true, false},
		{"create, no such bucket", create, 404, "NoSuchBucket", nil, true, false},
		{"get of a missing object", get, 404, "NoSuchKey", ErrNotFound, false, false},
		{"head of a missing object", head, 404, "", ErrNotFound, false, false},
		{"get, internal error", get, 500, "InternalError", ErrUnknown, false, true},
		{"delete of a missing object", del, 404, "NoSuchKey", nil, false, false},
		{"delete, slow down", del, 503, "SlowDown", ErrNotApplied, false, true},
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
				if err == nil || Transient(err) || errors.Is(err, ErrExists) || errors.Is(err, ErrConflict) || errors.Is(err, ErrNotFound) {
					t.Fatalf("got %v, want a plain error", err)
				}
			default:
				if err != nil {
					t.Fatalf("got %v, want success", err)
				}
			}
			if Transient(err) != tc.transient {
				t.Fatalf("Transient(%v) = %v, want %v", err, Transient(err), tc.transient)
			}
		})
	}
}

// A request that never got to the store is not applied; one that was sent and
// got no answer may have been.
func TestS3TransportFailures(t *testing.T) {
	ctx := context.Background()
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
	var attempts atomic.Int32
	b := fakeS3(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts.Add(1)
		return nil, refused // nothing written
	}))
	write := func() error { _, err := b.Create(ctx, "k", strings.NewReader("v"), WriteID{}); return err }
	if err := write(); !errors.Is(err, ErrNotApplied) || errors.Is(err, ErrUnknown) {
		t.Fatalf("connection refused: %v, want ErrNotApplied", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("the SDK retried %d times: blob.Store owns retries", attempts.Load())
	}
	if _, _, err := b.Get(ctx, "k"); !errors.Is(err, ErrNotApplied) {
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
			"create": func() error { _, err := b.Create(ctx, "k", strings.NewReader("v"), WriteID{}); return err },
			"update": func() error { _, err := b.Update(ctx, "k", strings.NewReader("v"), "e", WriteID{}); return err },
			"delete": func() error { return b.Delete(ctx, "k", "") },
		} {
			if err := f(); !errors.Is(err, ErrUnknown) || errors.Is(err, ErrNotApplied) {
				t.Errorf("%s, %s after the request was sent: %v, want ErrUnknown", op, name, err)
			}
		}
	}
	// A cancelled write may have happened, and says so, keeping the context's error findable.
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	b = fakeS3(t, roundTripFunc(func(r *http.Request) (*http.Response, error) { return nil, r.Context().Err() }))
	if _, err := b.Create(cctx, "k", strings.NewReader("v"), WriteID{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled create: %v", err)
	}
}

// timeoutError stands in for the error net/http produces on a timeout.
type timeoutError struct{}

func (*timeoutError) Error() string   { return "i/o timeout" }
func (*timeoutError) Timeout() bool   { return true }
func (*timeoutError) Temporary() bool { return true }

func TestS3WriteIDEncoding(t *testing.T) {
	for _, id := range []WriteID{{Node: "n1", Epoch: 1, N: 1}, {Node: "a:b c/é.d%", Epoch: 3, N: 9}, {Node: "a.b.c", Epoch: 2, N: 7}, {Node: "", Epoch: 0, N: 5}} {
		v := encodeWriteID(id)
		for _, c := range []byte(v) {
			if c < 0x20 || c > 0x7e {
				t.Fatalf("%q is not plain ASCII", v)
			}
		}
		if got := decodeWriteID(v); got != id {
			t.Fatalf("%+v -> %q -> %+v", id, v, got)
		}
	}
	if decodeWriteID("garbage") != (WriteID{}) {
		t.Fatal("garbage decoded to an id")
	}
	if got := s3ETag(strPtr(`"abc"`)); got != "abc" {
		t.Fatalf("etag %q", got)
	}
	if got := s3ETag(strPtr(`W/"abc"`)); got != "abc" {
		t.Fatalf("weak etag %q", got)
	}
}

func strPtr(s string) *string { return &s }

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

// With the emulator behind a fault-injecting transport: a response lost after
// the PUT reached the store is ErrUnknown, and Store recognises its own write by
// its write id; a refused connection is ErrNotApplied.
func TestS3LostResponsesAgainstTheEmulator(t *testing.T) {
	endpoint := os.Getenv("SHARDLITE_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("SHARDLITE_TEST_S3_ENDPOINT is not set (docker/compose.s3.yaml runs an emulator)")
	}
	ctx := context.Background()
	f := &faults{inner: http.DefaultTransport}
	b := s3At(t, endpoint, func(o *S3Options) { o.HTTPClient = &http.Client{Transport: f} })

	// The backend alone: the object is there, the caller was told "unknown".
	var dropped atomic.Bool
	f.dropPUT = func(*http.Request) bool { return dropped.CompareAndSwap(false, true) }
	id := WriteID{Node: "n1", Epoch: 1, N: 1}
	if _, err := b.Create(ctx, "lost", strings.NewReader("v"), id); !errors.Is(err, ErrUnknown) {
		t.Fatalf("a response lost after the PUT: %v, want ErrUnknown", err)
	}
	if info, err := b.Head(ctx, "lost"); err != nil || info.WriteID != id {
		t.Fatalf("the object the lost response was for: %+v %v", info, err)
	}

	// Through the Store: retried, and the "exists" that answers is recognised as its own.
	f.puts.Store(0)
	dropped.Store(false)
	s := NewStore(b, "n1", 1, Options{Backoff: time.Millisecond})
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
	other := NewStore(b, "n2", 1, Options{Backoff: time.Millisecond})
	dropped.Store(true) // no more drops
	if _, err := other.Update(ctx, "lost2", strings.NewReader("theirs"), etag2); err != nil {
		t.Fatal(err)
	}
	dropped.Store(false)
	if _, err := s.Update(ctx, "lost2", strings.NewReader("mine"), etag2); !errors.Is(err, ErrConflict) {
		t.Fatalf("update from a stale ETag: %v, want ErrConflict", err)
	}

	// A timeout after the request was sent is unknown too: the client gives up on
	// a response the store has already produced.
	slow := &faults{inner: http.DefaultTransport, delay: 400 * time.Millisecond}
	b2 := s3At(t, endpoint, func(o *S3Options) { o.HTTPClient = &http.Client{Transport: slow, Timeout: 150 * time.Millisecond} })
	if _, err := b2.Create(ctx, "slowwrite", strings.NewReader("v"), WriteID{}); !errors.Is(err, ErrUnknown) {
		t.Fatalf("a timeout after sending: %v, want ErrUnknown", err)
	}

	// The connection refused: nothing was sent.
	down := s3At(t, "http://127.0.0.1:1", nil)
	if _, err := down.Create(ctx, "k", strings.NewReader("v"), WriteID{}); !errors.Is(err, ErrNotApplied) {
		t.Fatalf("connection refused: %v, want ErrNotApplied", err)
	}
	if _, err := NewStore(down, "n1", 1, Options{Backoff: time.Millisecond}).Create(ctx, "k", strings.NewReader("v")); !errors.Is(err, ErrNotApplied) {
		t.Fatalf("Store.Create with the store down: %v", err)
	}
}
