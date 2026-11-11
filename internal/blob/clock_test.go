package blob_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
)

// logged is a handler that keeps the messages it is given, with their levels.
type logged struct {
	mu   sync.Mutex
	msgs []string
}

func (l *logged) Enabled(context.Context, slog.Level) bool { return true }
func (l *logged) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *logged) WithGroup(string) slog.Handler            { return l }
func (l *logged) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.msgs = append(l.msgs, r.Level.String()+" "+r.Message)
	return nil
}

func (l *logged) log() *slog.Logger { return slog.New(l) }

// lines are the messages so far.
func (l *logged) lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.msgs)
}

// count is how many messages contain s.
func (l *logged) count(s string) int {
	n := 0
	for _, m := range l.lines() {
		if strings.Contains(m, s) {
			n++
		}
	}
	return n
}

const (
	warnsOff    = "WARN the node's clock is more than a second from the store's"
	backWithin  = "INFO the node's clock is within a second of the store's again"
	warnsBlind  = "WARN the store's responses carry no usable time"
	seesTimeNow = "INFO the store's responses carry a time again"
)

// sampleOff is a sample of a store whose clock is ahead of the node's by off, as
// a store sends it, in whole seconds.
func sampleOff(off, round time.Duration) blob.ClockSample {
	start := time.Now()
	mid := start.Add(round / 2)
	return blob.ClockSample{Store: mid.Add(off).Truncate(time.Second), Start: start, End: start.Add(round)}
}

func feed(c *blob.ClockCheck, n int, off, round time.Duration) {
	for range n {
		c.Observe(sampleOff(off, round), nil)
	}
}

// A run of samples over a second off warns once, and one back within it says so,
// whichever way the clock is off. Noise within the second is quiet.
func TestClockWarnsOnARunOverASecond(t *testing.T) {
	t.Parallel()
	for name, off := range map[string]time.Duration{"store ahead": 3 * time.Second, "store behind": -3 * time.Second} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var l logged
			c := blob.NewClockCheck(l.log())
			feed(c, blob.ClockRun, 0, 10*time.Millisecond)
			feed(c, blob.ClockRun, 400*time.Millisecond, 10*time.Millisecond)
			if got := l.lines(); len(got) != 0 {
				t.Fatalf("noise within a second was logged: %q", got)
			}
			feed(c, blob.ClockRun-1, off, 10*time.Millisecond)
			if got := l.lines(); len(got) != 0 {
				t.Fatalf("a run one short of %d warned: %q", blob.ClockRun, got)
			}
			feed(c, 1, off, 10*time.Millisecond)
			if l.count(warnsOff) != 1 {
				t.Fatalf("the run did not warn once: %q", l.lines())
			}
			feed(c, 3*blob.ClockRun, off, 10*time.Millisecond)
			if len(l.lines()) != 1 {
				t.Fatalf("a long run warned again: %q", l.lines())
			}
			feed(c, 1, 0, 10*time.Millisecond)
			if l.count(backWithin) != 1 || len(l.lines()) != 2 {
				t.Fatalf("one sample back within a second did not recover once: %q", l.lines())
			}
			feed(c, blob.ClockRun, 0, 10*time.Millisecond)
			if len(l.lines()) != 2 {
				t.Fatalf("a quiet clock logged: %q", l.lines())
			}
			// And it can warn again.
			feed(c, blob.ClockRun, off, 10*time.Millisecond)
			if l.count(warnsOff) != 2 {
				t.Fatalf("a second run did not warn: %q", l.lines())
			}
		})
	}
}

// A sample within a second breaks a run, so scattered outliers never warn.
func TestClockRunNeedsToBeUnbroken(t *testing.T) {
	t.Parallel()
	var l logged
	c := blob.NewClockCheck(l.log())
	for range 3 {
		feed(c, blob.ClockRun-1, 3*time.Second, 10*time.Millisecond)
		feed(c, 1, 0, 10*time.Millisecond)
	}
	if got := l.lines(); len(got) != 0 {
		t.Fatalf("scattered outliers were logged: %q", got)
	}
}

// A run of responses with no usable time goes blind and says so once, and a
// time again ends it. One miss among samples is not a run.
func TestClockGoesBlind(t *testing.T) {
	t.Parallel()
	var l logged
	c := blob.NewClockCheck(l.log())
	miss := errors.New("no date")
	for range blob.ClockRun - 1 {
		c.Observe(blob.ClockSample{}, miss)
	}
	feed(c, 1, 0, 10*time.Millisecond) // breaks the run
	for range blob.ClockRun - 1 {
		c.Observe(blob.ClockSample{}, miss)
	}
	if got := l.lines(); len(got) != 0 {
		t.Fatalf("short runs of misses were logged: %q", got)
	}
	c.Observe(blob.ClockSample{}, miss)
	if l.count(warnsBlind) != 1 {
		t.Fatalf("a run of misses did not warn: %q", l.lines())
	}
	for range 3 * blob.ClockRun {
		c.Observe(blob.ClockSample{}, miss)
	}
	if len(l.lines()) != 1 {
		t.Fatalf("a long run of misses warned again: %q", l.lines())
	}
	feed(c, 1, 0, 10*time.Millisecond)
	if l.count(seesTimeNow) != 1 || len(l.lines()) != 2 {
		t.Fatalf("a time did not end the blindness once: %q", l.lines())
	}
}

// A round trip over a second is no sample: it neither adds to a run nor breaks
// one, and a clock that is off is not recovered by it.
func TestClockIgnoresLongRoundTrips(t *testing.T) {
	t.Parallel()
	var l logged
	c := blob.NewClockCheck(l.log())
	feed(c, 3*blob.ClockRun, 3*time.Second, 1500*time.Millisecond)
	if got := l.lines(); len(got) != 0 {
		t.Fatalf("long round trips warned: %q", got)
	}
	feed(c, blob.ClockRun-1, 3*time.Second, 10*time.Millisecond)
	feed(c, 3, 0, 1500*time.Millisecond) // would break the run if it counted
	feed(c, 1, 3*time.Second, 10*time.Millisecond)
	if l.count(warnsOff) != 1 {
		t.Fatalf("long round trips broke the run: %q", l.lines())
	}
	feed(c, 3, 0, 1500*time.Millisecond)
	if len(l.lines()) != 1 {
		t.Fatalf("a long round trip recovered the clock: %q", l.lines())
	}
}

// withDate is a response carrying a Date header as given (none if empty).
func withDate(r *http.Response, date string) *http.Response {
	if date != "" {
		r.Header.Set("Date", date)
	}
	return r
}

// httpDate is the time as a store sends it.
func httpDate(t time.Time) string { return t.UTC().Format(http.TimeFormat) }

func loggedS3(t *testing.T, rt http.RoundTripper) (*blob.S3, *logged) {
	t.Helper()
	var l logged
	b, err := blob.NewS3(context.Background(), blob.S3Options{Bucket: "b", Endpoint: "http://s3.invalid", HTTPClient: &http.Client{Transport: rt}, Logger: l.log()})
	if err != nil {
		t.Fatal(err)
	}
	return b, &l
}

// Every response is a sample, an error's too: a run of any of them with a store
// a minute ahead warns once.
func TestS3SamplesEveryResponse(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, resp := range map[string]func() *http.Response{
		"success": func() *http.Response { return s3Object(`"e"`, "") },
		"error":   func() *http.Response { return s3Error(http.StatusNotFound, "NoSuchKey") },
		"refused": func() *http.Response { return s3Error(http.StatusForbidden, "AccessDenied") },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b, l := loggedS3(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
				wrote(r)
				return withDate(resp(), httpDate(time.Now().Add(time.Minute))), nil
			}))
			for range blob.ClockRun {
				b.Head(ctx, "k")
			}
			if l.count(warnsOff) != 1 || len(l.lines()) != 1 {
				t.Fatalf("%d responses a minute off: %q", blob.ClockRun, l.lines())
			}
		})
	}
}

// Each kind of call samples, the listing and the writes included.
func TestS3SamplesEveryCall(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	b, l := loggedS3(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		wrote(r)
		return withDate(s3Object(`"e"`, ""), httpDate(time.Now().Add(-time.Minute))), nil
	}))
	id := blob.WriteID{Node: "n", Epoch: 1, N: 1}
	b.Create(ctx, "k", strings.NewReader("v"), id)
	b.Update(ctx, "k", strings.NewReader("v"), "e", id)
	b.Get(ctx, "k")
	b.GetRange(ctx, "k", 0, 1)
	b.Head(ctx, "k")
	if len(l.lines()) != 1 {
		t.Fatalf("five calls a minute off logged %q, want one warning", l.lines())
	}
	b.Delete(ctx, "k", "")
	for range b.List(ctx, "", "") {
	}
	if l.count(warnsOff) != 1 {
		t.Fatalf("more calls warned again: %q", l.lines())
	}
}

// Responses without a usable time make the check blind, and the calls go on. A
// request that got no response has nothing to report.
func TestS3WithoutATime(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for name, date := range map[string]string{"omitted": "", "malformed": "yesterday-ish"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b, l := loggedS3(t, roundTripFunc(func(r *http.Request) (*http.Response, error) {
				wrote(r)
				return withDate(s3Object(`"e"`, ""), date), nil
			}))
			for range blob.ClockRun {
				if _, err := b.Head(ctx, "k"); err != nil {
					t.Fatalf("a response without a usable Date failed the call: %v", err)
				}
			}
			if l.count(warnsBlind) != 1 || len(l.lines()) != 1 {
				t.Fatalf("%d responses without a time: %q", blob.ClockRun, l.lines())
			}
		})
	}
	t.Run("no response", func(t *testing.T) {
		t.Parallel()
		b, l := loggedS3(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("connection refused")
		}))
		for range 3 * blob.ClockRun {
			if _, err := b.Head(ctx, "k"); err == nil {
				t.Fatal("a refused connection succeeded")
			}
		}
		if got := l.lines(); len(got) != 0 {
			t.Fatalf("requests with no response logged %q", got)
		}
	})
}
