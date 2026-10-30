package coord_test

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
)

// logBuf is a log the tests read what the watch said from.
type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func newWatch() (*coord.ClockWatch, *logBuf) {
	var l logBuf
	return coord.NewClockWatch(slog.New(slog.NewTextHandler(&l, nil))), &l
}

// clockSample is a response whose request took round, and whose store reads
// skew ahead of the node's clock at the request's midpoint, as a store sends it:
// in whole seconds.
func clockSample(skew, round time.Duration) blob.ClockSample {
	start := time.Now()
	mid := start.Round(0).Add(round / 2)
	return blob.ClockSample{Store: mid.Add(skew).Truncate(time.Second), Start: start, End: start.Add(round)}
}

// near reports whether got is within the second the store's time is good to.
func near(got, want time.Duration) bool {
	return got-want < time.Second/2+time.Millisecond && want-got < time.Second/2+time.Millisecond
}

func feed(w *coord.ClockWatch, n int, skew time.Duration) {
	for range n {
		w.Sink(clockSample(skew, 10*time.Millisecond), nil)
	}
}

func TestClockWatchSteadyIsQuiet(t *testing.T) {
	t.Parallel()
	w, log := newWatch()
	feed(w, 20, 0)
	r := w.Report()
	if r.Samples != 20 || r.Warning || r.Blind || !near(r.Offset, 0) {
		t.Fatalf("report %+v", r)
	}
	if log.String() != "" {
		t.Fatalf("a steady clock logged: %s", log)
	}
}

// An offset over a second warns once on crossing and once on coming back.
func TestClockWatchWarnsOnTheOffset(t *testing.T) {
	t.Parallel()
	w, log := newWatch()
	feed(w, 10, 0)
	feed(w, 10, 3*time.Second)
	r := w.Report()
	if !r.Warning || !near(r.Offset, 3*time.Second) {
		t.Fatalf("report %+v, want a warning at about 3s", r)
	}
	if n := strings.Count(log.String(), "from the store's: failover"); n != 1 {
		t.Fatalf("%d warnings for ten samples past the limit:\n%s", n, log)
	}
	// The other way too.
	feed(w, 10, -3*time.Second)
	if r := w.Report(); !r.Warning || !near(r.Offset, -3*time.Second) {
		t.Fatalf("report %+v, want a warning at about -3s", r)
	}
	feed(w, 10, 0)
	if r := w.Report(); r.Warning {
		t.Fatalf("report %+v, want it cleared", r)
	}
	if got := log.String(); strings.Count(got, "from the store's: failover") != 1 || strings.Count(got, "within a second of the store's again") != 1 {
		t.Fatalf("want one warning and one all-clear:\n%s", got)
	}
}

// The median of the latest samples decides the offset, so one odd response does
// not move it or warn (it is a disagreement, which the steadiness check reports).
func TestClockWatchIgnoresOneOutlier(t *testing.T) {
	t.Parallel()
	w, log := newWatch()
	feed(w, 10, 0)
	w.Sink(clockSample(30*time.Second, 10*time.Millisecond), nil)
	if r := w.Report(); r.Warning || !near(r.Offset, 0) || strings.Contains(log.String(), "from the store's") {
		t.Fatalf("one outlier moved the offset: %+v\n%s", r, log)
	}
}

// A sample from a long request says nothing finer than the noise in it.
func TestClockWatchDropsLongRoundTrips(t *testing.T) {
	t.Parallel()
	w, _ := newWatch()
	w.Sink(clockSample(time.Minute, 1500*time.Millisecond), nil)
	if r := w.Report(); r.Samples != 0 || r.Warning {
		t.Fatalf("report %+v, want the sample dropped", r)
	}
	w.Sink(clockSample(0, time.Second), nil)
	if r := w.Report(); r.Samples != 1 {
		t.Fatalf("report %+v, want a one-second request kept", r)
	}
}

// A store that stops sending a time makes the watch blind, which it says, and
// the next time it gets clears.
func TestClockWatchSaysWhenBlind(t *testing.T) {
	t.Parallel()
	w, log := newWatch()
	miss := errors.New("no Date")
	for range 4 {
		w.Sink(blob.ClockSample{}, miss)
	}
	if w.Report().Blind || log.String() != "" {
		t.Fatalf("four misses made it blind: %s", log)
	}
	w.Sink(clockSample(0, time.Millisecond), nil) // a good one breaks the run
	for range 4 {
		w.Sink(blob.ClockSample{}, miss)
	}
	if w.Report().Blind {
		t.Fatal("misses with a good sample between made it blind")
	}
	for range 3 {
		w.Sink(blob.ClockSample{}, miss)
	}
	if !w.Report().Blind || strings.Count(log.String(), "WARN") != 1 {
		t.Fatalf("a run of misses did not make it blind once:\n%s", log)
	}
	w.Sink(clockSample(0, time.Millisecond), nil)
	if w.Report().Blind || !strings.Contains(log.String(), "INFO") {
		t.Fatalf("a good sample did not clear it:\n%s", log)
	}
}

func TestClockWatchCheckStart(t *testing.T) {
	t.Parallel()
	ttl := 10 * time.Second
	w, _ := newWatch()
	if err := w.CheckStart(ttl); err != nil {
		t.Fatalf("without a sample: %v", err)
	}
	feed(w, 5, 5*time.Second) // a warning, but within the TTL
	if err := w.CheckStart(ttl); err != nil {
		t.Fatalf("within the TTL: %v", err)
	}
	feed(w, 5, 30*time.Second)
	if err := w.CheckStart(ttl); err == nil {
		t.Fatal("30s off with a 10s TTL started")
	}
	w2, _ := newWatch()
	feed(w2, 5, -30*time.Second)
	if err := w2.CheckStart(ttl); err == nil {
		t.Fatal("30s behind with a 10s TTL started")
	}
}

// at is a response d after t0 whose request took round and whose store read
// store at its midpoint, in whole seconds.
func at(t0 time.Time, d, round time.Duration, store time.Time) blob.ClockSample {
	start := t0.Add(d)
	return blob.ClockSample{Store: store.Truncate(time.Second), Start: start, End: start.Add(round)}
}

// run feeds one sample every step for total, the store reading what clock gives
// for the time since t0.
func run(w *coord.ClockWatch, t0 time.Time, from, total, step time.Duration, clock func(d time.Duration) time.Time) {
	for d := from; d < from+total; d += step {
		w.Sink(at(t0, d, 10*time.Millisecond, clock(d+5*time.Millisecond)), nil)
	}
}

// A store that keeps true time and a node that does, whatever the whole seconds
// do to where a sample falls, never reports a problem.
func TestClockWatchNoiseIsNotAProblem(t *testing.T) {
	t.Parallel()
	t0 := time.Now()
	for _, skew := range []time.Duration{0, 300 * time.Millisecond, -450 * time.Millisecond} {
		w, log := newWatch()
		// Samples a few ms apart cross the second boundary at every phase.
		run(w, t0, 0, 5*time.Second, 37*time.Millisecond, func(d time.Duration) time.Time { return t0.Add(d + skew) })
		if err := w.Check(); err != nil || w.Report().Unsteady || log.String() != "" {
			t.Fatalf("skew %v: noise was reported: %v %+v\n%s", skew, err, w.Report(), log)
		}
	}
}

// A store whose clock steps, as the node's would, is a problem that fails Check
// and is logged once, however many samples show it.
func TestClockWatchReportsAStep(t *testing.T) {
	t.Parallel()
	w, log := newWatch()
	t0 := time.Now()
	step := 20 * time.Second
	clock := func(d time.Duration) time.Time {
		if d >= step {
			d += 10 * time.Second
		}
		return t0.Add(d)
	}
	run(w, t0, 0, step, time.Second, clock)
	if err := w.Check(); err != nil {
		t.Fatalf("before the step: %v", err)
	}
	run(w, t0, step, 3*time.Second, time.Second, clock)
	if err := w.Check(); !errors.Is(err, coord.ErrClockUnsteady) || !w.Report().Unsteady {
		t.Fatalf("after the step: %v %+v", err, w.Report())
	}
	if n := strings.Count(log.String(), "clocks fail until"); n != 1 || !strings.Contains(log.String(), "moved") {
		t.Fatalf("want one warning that says what moved:\n%s", log)
	}
}

// A rate more than 20% off is a problem, and one inside the margin is not
// (though its offset warns once it grows past a second).
func TestClockWatchReportsARate(t *testing.T) {
	t.Parallel()
	ratio := func(r float64) func(t0 time.Time) func(time.Duration) time.Time {
		return func(t0 time.Time) func(time.Duration) time.Time {
			return func(d time.Duration) time.Time { return t0.Add(time.Duration(float64(d) * r)) }
		}
	}
	for name, c := range map[string]struct {
		rate     float64
		unsteady bool
	}{
		"half speed":   {0.5, true},
		"double":       {2, true},
		"a quarter":    {1.25, true},
		"a tenth":      {1.1, false},
		"a tenth slow": {0.9, false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			w, _ := newWatch()
			t0 := time.Now()
			run(w, t0, 0, 2*time.Minute, time.Second, ratio(c.rate)(t0))
			if got := w.Report().Unsteady; got != c.unsteady {
				t.Fatalf("rate %v: unsteady %v, want %v (%+v)", c.rate, got, c.unsteady, w.Report())
			}
		})
	}
}

// The problem clears once the samples since show a steady offset for a while,
// and a new disagreement before then starts the wait again.
func TestClockWatchRecovers(t *testing.T) {
	t.Parallel()
	w, log := newWatch()
	t0 := time.Now()
	stepped := func(extra time.Duration) func(time.Duration) time.Time {
		return func(d time.Duration) time.Time { return t0.Add(d + extra) }
	}
	run(w, t0, 0, 10*time.Second, time.Second, stepped(0))
	run(w, t0, 10*time.Second, time.Second, time.Second, stepped(30*time.Second))
	if !w.Report().Unsteady {
		t.Fatal("the step was not reported")
	}
	// Steady at the new offset, but not for long enough yet.
	run(w, t0, 11*time.Second, 3*time.Second, time.Second, stepped(30*time.Second))
	if !w.Report().Unsteady {
		t.Fatal("cleared after three samples")
	}
	// A second step starts the wait again.
	run(w, t0, 14*time.Second, 1*time.Second, time.Second, stepped(60*time.Second))
	run(w, t0, 15*time.Second, 4*time.Second, time.Second, stepped(60*time.Second))
	if !w.Report().Unsteady {
		t.Fatal("cleared in under the time it takes after the second step")
	}
	run(w, t0, 19*time.Second, 6*time.Second, time.Second, stepped(60*time.Second))
	if r := w.Report(); r.Unsteady || w.Check() != nil {
		t.Fatalf("not cleared after a steady while: %+v", r)
	}
	if got := log.String(); strings.Count(got, "clocks fail until") != 1 || strings.Count(got, "agree again") != 1 {
		t.Fatalf("want one warning and one all-clear for the problem:\n%s", got)
	}
}

// Many samples in a burst, as a busy node takes them, do not make a steady
// clock look unsteady, and do not clear a problem faster than time passes.
func TestClockWatchBurstsDoNotClear(t *testing.T) {
	t.Parallel()
	w, _ := newWatch()
	t0 := time.Now()
	clock := func(extra time.Duration) func(time.Duration) time.Time {
		return func(d time.Duration) time.Time { return t0.Add(d + extra) }
	}
	run(w, t0, 0, 10*time.Second, time.Second, clock(0))
	run(w, t0, 10*time.Second, time.Millisecond, time.Second, clock(30*time.Second))
	run(w, t0, 10*time.Second+time.Millisecond, 100*time.Millisecond, time.Millisecond, clock(30*time.Second))
	if !w.Report().Unsteady {
		t.Fatal("a burst of a hundred milliseconds cleared the problem")
	}
}

func TestClockWatchNilChecksNothing(t *testing.T) {
	t.Parallel()
	var w *coord.ClockWatch
	if err := w.Check(); err != nil {
		t.Fatal(err)
	}
}
