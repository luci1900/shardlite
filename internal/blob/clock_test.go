package blob_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
)

// sampled collects what a backend hands its clock sink.
type sampled struct {
	mu      sync.Mutex
	samples []blob.ClockSample
	errs    []error
}

func (s *sampled) sink(c blob.ClockSample, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.errs = append(s.errs, err)
		return
	}
	s.samples = append(s.samples, c)
}

func (s *sampled) counts() (samples, errs int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.samples), len(s.errs)
}

// withDate is a response carrying a Date header as given (none if empty).
func withDate(r *http.Response, date string) *http.Response {
	if date != "" {
		r.Header.Set("Date", date)
	}
	return r
}

func sampledS3(t *testing.T, rt http.RoundTripper) (*blob.S3, *sampled) {
	t.Helper()
	var got sampled
	b, err := blob.NewS3(context.Background(), blob.S3Options{Bucket: "b", Endpoint: "http://s3.invalid", HTTPClient: &http.Client{Transport: rt}, ClockSink: got.sink})
	if err != nil {
		t.Fatal(err)
	}
	return b, &got
}

// Every response gives a sample: the header's time, and a window that holds the
// exchange. An error's response carries a time as a good one does.
func TestS3SamplesEveryResponse(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const date = "Tue, 15 Nov 1994 08:12:31 GMT"
	want := time.Date(1994, 11, 15, 8, 12, 31, 0, time.UTC)
	for name, resp := range map[string]func() *http.Response{
		"success": func() *http.Response { return s3Object(`"e"`, "") },
		"error":   func() *http.Response { return s3Error(http.StatusNotFound, "NoSuchKey") },
		"refused": func() *http.Response { return s3Error(http.StatusForbidden, "AccessDenied") },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b, got := sampledS3(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
				wrote(r)
				return withDate(resp(), date), nil
			}))
			before := time.Now()
			b.Head(ctx, "k")
			after := time.Now()
			if n, errs := got.counts(); n != 1 || errs != 0 {
				t.Fatalf("%d samples and %d failures, want one sample", n, errs)
			}
			s := got.samples[0]
			if !s.Store.Equal(want) {
				t.Fatalf("store time %v, want %v", s.Store, want)
			}
			if s.Start.Before(before) || s.End.Before(s.Start) || s.End.After(after) {
				t.Fatalf("window [%v, %v] is not inside the call [%v, %v]", s.Start, s.End, before, after)
			}
		})
	}
}

// Each kind of call samples, the listing and the writes included.
func TestS3SamplesEveryCall(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b, got := sampledS3(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		wrote(r)
		return withDate(s3Object(`"e"`, ""), "Tue, 15 Nov 1994 08:12:31 GMT"), nil
	}))
	id := blob.WriteID{Node: "n", Epoch: 1, N: 1}
	b.Create(ctx, "k", strings.NewReader("v"), id)
	b.Update(ctx, "k", strings.NewReader("v"), "e", id)
	b.Get(ctx, "k")
	b.GetRange(ctx, "k", 0, 1)
	b.Head(ctx, "k")
	b.Delete(ctx, "k", "")
	for range b.List(ctx, "", "") {
	}
	if n, errs := got.counts(); n < 7 || errs != 0 {
		t.Fatalf("%d samples and %d failures from seven calls", n, errs)
	}
}

// A response without a usable time is reported as a failure, not dropped, and a
// request that got no response has nothing to report.
func TestS3SampleFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, date := range map[string]string{"omitted": "", "malformed": "yesterday-ish"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b, got := sampledS3(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
				wrote(r)
				return withDate(s3Object(`"e"`, ""), date), nil
			}))
			if _, err := b.Head(ctx, "k"); err != nil {
				t.Fatalf("a response without a usable Date failed the call: %v", err)
			}
			if n, errs := got.counts(); n != 0 || errs != 1 {
				t.Fatalf("%d samples and %d failures, want one failure", n, errs)
			}
		})
	}
	t.Run("no response", func(t *testing.T) {
		t.Parallel()
		b, got := sampledS3(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("connection refused")
		}))
		if _, err := b.Head(ctx, "k"); err == nil {
			t.Fatal("a refused connection succeeded")
		}
		if n, errs := got.counts(); n != 0 || errs != 0 {
			t.Fatalf("%d samples and %d failures from a request with no response", n, errs)
		}
	})
}

// Without a sink the backend takes no samples and the calls work as before.
func TestS3NoSinkNoSamples(t *testing.T) {
	t.Parallel()
	b := fakeS3(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		wrote(r)
		return s3Object(`"e"`, ""), nil
	}))
	if _, err := b.Head(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
}

// The memory backend's store time is its own clock, and the window is the
// process's.
func TestMemorySamples(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	var got sampled
	m := blob.NewMemory(blob.WithClock(func() time.Time { return store }), blob.WithClockSink(got.sink))
	before := time.Now()
	id := blob.WriteID{Node: "n", Epoch: 1, N: 1}
	e, _ := m.Create(ctx, "k", strings.NewReader("v"), id)
	m.Update(ctx, "k", strings.NewReader("w"), e, id)
	m.Get(ctx, "k")
	m.GetRange(ctx, "k", 0, 1)
	m.Head(ctx, "k")
	for range m.List(ctx, "", "") {
	}
	m.Delete(ctx, "k", "")
	after := time.Now()
	if n, errs := got.counts(); n != 7 || errs != 0 {
		t.Fatalf("%d samples and %d failures from seven calls", n, errs)
	}
	for _, s := range got.samples {
		if !s.Store.Equal(store) || s.Start.Before(before) || s.End.Before(s.Start) || s.End.After(after) {
			t.Fatalf("sample %+v outside the calls [%v, %v]", s, before, after)
		}
	}
	// A failed call is sampled too: the store answered.
	if _, err := m.Head(ctx, "gone"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatal(err)
	}
	if n, _ := got.counts(); n != 8 {
		t.Fatalf("%d samples after a failed call, want 8", n)
	}
}
