package coord

import (
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
)

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
	// Blind is whether the store's last responses carried no usable time, so the
	// clock is not being checked at all.
	Blind bool
}

// ClockWatch checks the node's wall clock against the store's, from the samples
// a backend takes of the responses it already gets (blob.ClockSample). It keeps
// the latest few, and logs when the offset crosses a second and back, and when
// the store stops giving a time. It fails nothing: it is where an operator
// learns of a clock the guide rules out ("working clocks"). Its Sink is safe
// for concurrent use.
type ClockWatch struct {
	log *slog.Logger

	mu      sync.Mutex
	recent  [clockWindow]time.Duration // offsets, a ring filled up to samples
	samples int
	warning bool
	missed  int // responses in a row without a usable time
	blind   bool
}

// NewClockWatch returns a watch that logs to log (the default logger if nil).
func NewClockWatch(log *slog.Logger) *ClockWatch {
	if log == nil {
		log = slog.Default()
	}
	return &ClockWatch{log: log}
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
	return ClockReport{Samples: w.samples, Offset: w.offsetLocked(), Warning: w.warning, Blind: w.blind}
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
