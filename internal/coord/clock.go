package coord

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
)

// The limits the watch holds the node's clock to. They come from the guide's own
// numbers and are not settings.
const (
	// clockWarnOffset is how far the node's clock may be from the store's before
	// the operator is told: the delay it adds to failover and to reminders on
	// other nodes.
	clockWarnOffset = time.Second
	// clockMaxRoundTrip is the longest request a sample is taken from. The store's
	// time has whole seconds, so a request longer than one says nothing finer than
	// the noise already in it.
	clockMaxRoundTrip = time.Second
	// clockWindow is how many recent samples the offset is the median of, and how
	// many responses in a row without a time make the watch blind.
	clockWindow = 5
	// clockNoise is what two offsets can differ by with nothing wrong: each is
	// good to half a second (the store's time has whole seconds), and each adds
	// half its round trip.
	clockNoise = time.Second
	// clockRateMargin is the share of elapsed time that the node's clock and the
	// store's may differ by: the lease's own margin, a fifth of the TTL.
	clockRateMargin = 5 // as a divisor
	// clockSteadyFor is how long the samples since a problem must span, with at
	// least clockWindow of them, to show the offset steady again.
	clockSteadyFor = 5 * time.Second
	// clockHistory is how many anchors the watch compares a sample against, at
	// least clockSpacing apart.
	clockHistory = 32
	clockSpacing = time.Second
)

// ErrClockUnsteady is the failure of the node's clock against the store's: the
// offset moved by more than the noise and the margin allow, which a clock whose
// rate is right and that has not stepped or paused never does.
var ErrClockUnsteady = errors.New("coord: the node's clock and the store's disagree")

// ClockReport is what a ClockWatch knows of the node's clock.
type ClockReport struct {
	// Samples is how many the watch has taken (those it dropped for a long round
	// trip are not counted).
	Samples int
	// Offset is the store's time less the node's, the median of the latest
	// samples and good to a second (the store's time has no finer resolution).
	// It is zero before the first sample.
	Offset time.Duration
	// Warning is whether the offset is over a second, either way.
	Warning bool
	// Unsteady is whether the offset moved by more than the noise and the 20%
	// margin allow since an earlier sample (a rate that is off, a step, or a pause
	// the monotonic clock did not count), and has not been steady since.
	Unsteady bool
	// Blind is whether the store's last responses carried no usable time, so the
	// clock is not being checked at all.
	Blind bool
}

// ClockWatch checks the node's wall clock against the store's, from the samples
// a backend takes of the responses it already gets (blob.ClockSample). It keeps
// the latest few, and logs when the offset crosses a second and back, and when
// the store stops giving a time. Its Sink is safe for concurrent use.
//
// The `none` and `peer` tiers rest on the node's clocks alone, and so the watch
// is where a clock the guide rules out ("working clocks") gets noticed.
type ClockWatch struct {
	log  *slog.Logger
	base time.Time // the watch's origin on both clocks, for the monotonic offsets

	mu       sync.Mutex
	recent   [clockWindow]time.Duration // offsets, a ring filled up to samples
	samples  int
	warning  bool
	missed   int // responses in a row without a usable time
	blind    bool
	lastMiss error

	// The anchors a sample is compared with, and the run of steady samples since
	// the last problem (the run only counts while unsteady).
	history    []clockPoint
	unsteady   bool
	steady     int
	steadyFrom time.Duration
}

// clockPoint is one sample's place on the node's monotonic clock (at, since the
// watch began) and its offsets against the store's by the wall clock and by the
// monotonic one, which differ by what the wall clock gained or lost.
type clockPoint struct {
	at, wall, mono, round time.Duration
}

// NewClockWatch returns a watch that logs to log (the default logger if nil).
func NewClockWatch(log *slog.Logger) *ClockWatch {
	if log == nil {
		log = slog.Default()
	}
	return &ClockWatch{log: log, base: time.Now()}
}

// Sink is the blob.ClockSink that feeds the watch.
func (w *ClockWatch) Sink(s blob.ClockSample, err error) {
	var say func()
	w.mu.Lock()
	if err != nil {
		say = w.missLocked(err)
	} else {
		say = w.sampleLocked(s)
	}
	w.mu.Unlock()
	if say != nil {
		say()
	}
}

// missLocked records a response with no usable time. Only a run of them makes the
// watch blind, so one odd response is not worth a log.
func (w *ClockWatch) missLocked(err error) func() {
	w.missed++
	w.lastMiss = err
	if w.missed < clockWindow || w.blind {
		return nil
	}
	w.blind = true
	return func() {
		w.log.Warn("the store's responses carry no usable time, so the node's clock is not being checked", "err", err)
	}
}

func (w *ClockWatch) sampleLocked(s blob.ClockSample) func() {
	round := s.End.Sub(s.Start)
	if round < 0 || round > clockMaxRoundTrip {
		return nil
	}
	var say func()
	w.missed = 0
	if w.blind {
		w.blind = false
		say = func() { w.log.Info("the store's responses carry a time again, the node's clock is being checked") }
	}
	// The store's time is truncated to the second, so on average it trails the
	// instant it was read by half of one. The request's midpoint, on the wall
	// clock, is the instant the node compares it with.
	mid := s.Start.Add(round / 2) // keeps the monotonic reading
	store := s.Store.Add(time.Second / 2)
	offset := store.Sub(mid.Round(0))
	p := clockPoint{at: mid.Sub(w.base), wall: offset, round: round}
	p.mono = store.Sub(w.base.Round(0).Add(p.at))
	say = chain(say, w.judgeLocked(p))
	w.recent[w.samples%clockWindow] = offset
	w.samples++
	was := w.warning
	w.warning = abs(w.offsetLocked()) > clockWarnOffset
	switch {
	case w.warning && !was:
		off := w.offsetLocked()
		say = chain(say, func() {
			w.log.Warn("the node's clock is more than a second from the store's: failover and reminders on other nodes are delayed", "offset", off.Round(time.Millisecond))
		})
	case !w.warning && was:
		say = chain(say, func() { w.log.Info("the node's clock is within a second of the store's again") })
	}
	return say
}

// judgeLocked compares a sample with the anchors. Any two offsets may differ by
// the noise and a fifth of the time between them: more is a clock that is off in
// rate, stepped, or paused where the monotonic clock did not count it. The
// history then starts again from this sample, and the problem lasts until the
// samples since show a steady offset for a while.
func (w *ClockWatch) judgeLocked(p clockPoint) func() {
	var say func()
	if moved := w.movedLocked(p); moved != "" {
		w.history, w.steady, w.steadyFrom = w.history[:0], 0, p.at
		if !w.unsteady {
			w.unsteady = true
			say = func() {
				w.log.Warn("the node's clock and the store's disagree: answers that rest on the clocks fail until the offset is steady", "moved", moved)
			}
		}
	} else if w.unsteady {
		w.steady++
		if w.steady >= clockWindow && p.at-w.steadyFrom >= clockSteadyFor {
			w.unsteady = false
			say = func() { w.log.Info("the node's clock and the store's agree again") }
		}
	}
	if n := len(w.history); n == 0 || abs(p.at-w.history[n-1].at) >= clockSpacing {
		if n == clockHistory {
			w.history = slices.Delete(w.history, 0, 1)
		}
		w.history = append(w.history, p)
	}
	return say
}

// movedLocked says how an anchor and p disagree, or "" if they do not.
func (w *ClockWatch) movedLocked(p clockPoint) string {
	for _, q := range w.history {
		gap := abs(p.at - q.at)
		allow := clockNoise + (p.round+q.round)/2 + gap/clockRateMargin
		if d := abs(p.wall - q.wall); d > allow {
			return fmt.Sprintf("offset by the wall clock %s in %s", d.Round(time.Millisecond), gap.Round(time.Millisecond))
		}
		if d := abs(p.mono - q.mono); d > allow {
			return fmt.Sprintf("offset by the monotonic clock %s in %s", d.Round(time.Millisecond), gap.Round(time.Millisecond))
		}
	}
	return ""
}

// Check returns ErrClockUnsteady (wrapped, with what moved) while the watch
// reports that the node's clock and the store's disagree, and nil otherwise. A
// nil watch checks nothing.
func (w *ClockWatch) Check() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.unsteady {
		return nil
	}
	return ErrClockUnsteady
}

func chain(a, b func()) func() {
	if b == nil {
		return a
	}
	if a == nil {
		return b
	}
	return func() { a(); b() }
}

// offsetLocked is the median of the samples in the window.
func (w *ClockWatch) offsetLocked() time.Duration {
	n := min(w.samples, clockWindow)
	if n == 0 {
		return 0
	}
	got := slices.Clone(w.recent[:n])
	slices.Sort(got)
	return got[n/2]
}

func abs(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// Report returns what the watch knows now.
func (w *ClockWatch) Report() ClockReport {
	w.mu.Lock()
	defer w.mu.Unlock()
	return ClockReport{Samples: w.samples, Offset: w.offsetLocked(), Warning: w.warning, Unsteady: w.unsteady, Blind: w.blind}
}

// CheckStart refuses a node whose clock is more than a lease TTL from the
// store's: its lease and the others' view of it would not mean the same time. It
// passes while there is no sample to judge by (the blind case is logged).
func (w *ClockWatch) CheckStart(ttl time.Duration) error {
	r := w.Report()
	if r.Samples == 0 || abs(r.Offset) <= ttl {
		return nil
	}
	return fmt.Errorf("the node's clock is %s from the store's, more than the lease TTL of %s: fix the host's clock before starting (offset_ms=%d)",
		abs(r.Offset).Round(time.Millisecond), ttl, r.Offset.Milliseconds())
}
