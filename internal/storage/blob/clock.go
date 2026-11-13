package blob

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// The limits the node's clock is held to. They come from the guide's own
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
	// clockRun is how many samples in a row must be off before the operator is
	// told, and how many responses in a row without a time make the check blind.
	// One odd response is noise and a handful in a row is not.
	clockRun = 5
)

// ClockSample is one reading of the store's clock: the store's time, and the
// node's readings of its own clock when the request began and when the response
// arrived. Start and End come from time.Now, so they carry the monotonic clock
// and End.Sub(Start) is the round trip. The store's time is only as fine as its
// source: S3 and RustFS send whole seconds, and every backend truncates to them.
type ClockSample struct {
	Store      time.Time
	Start, End time.Time
}

// clockCheck compares the node's wall clock with the store's, from the samples a
// backend takes of the responses it already gets. It logs when a run of samples
// is more than a second off and when one is back within it, and when a run of
// responses carries no usable time, so a store that stops sending one is
// reported and not mistaken for a quiet clock. It fails nothing: it is where an
// operator learns of a clock the guide rules out ("working clocks"). It is safe
// for concurrent use, and its logging is the backend's only side effect.
type clockCheck struct {
	log *slog.Logger

	mu     sync.Mutex
	off    int  // samples in a row more than a second off
	warned bool // the run reached clockRun and was said
	missed int  // responses in a row without a usable time
	blind  bool // the run of misses reached clockRun and was said
}

// newClockCheck returns a check that logs to log (the default logger if nil).
func newClockCheck(log *slog.Logger) *clockCheck {
	if log == nil {
		log = slog.Default()
	}
	return &clockCheck{log: log}
}

// observe takes a sample, or the failure to read one: err is set (and the sample
// empty) when a response had no usable time. It logs while it holds the lock,
// which nothing else is taken under, so the messages come out in order.
func (c *clockCheck) observe(s ClockSample, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.miss(err)
		return
	}
	round := s.End.Sub(s.Start)
	if round < 0 || round > clockMaxRoundTrip {
		return
	}
	c.missed = 0
	if c.blind {
		c.blind = false
		c.log.Info("the store's responses carry a time again, the node's clock is being checked")
	}
	// The store's time is truncated to the second, so on average it trails the
	// instant it was read by half of one. The request's midpoint, on the wall
	// clock, is the instant the node compares it with.
	mid := s.Start.Add(round / 2) // keeps the monotonic reading
	offset := s.Store.Add(time.Second / 2).Sub(mid.Round(0))
	if offset <= clockWarnOffset && offset >= -clockWarnOffset {
		c.off = 0
		if c.warned {
			c.warned = false
			c.log.Info("the node's clock is within a second of the store's again")
		}
		return
	}
	c.off++
	if c.off < clockRun || c.warned {
		return
	}
	c.warned = true
	c.log.Warn("the node's clock is more than a second from the store's: failover and reminders on other nodes are delayed", "offset", offset.Round(time.Millisecond))
}

// miss records a response with no usable time. Only a run of them makes the
// check blind, so one odd response is not worth a log.
func (c *clockCheck) miss(err error) {
	c.missed++
	if c.missed < clockRun || c.blind {
		return
	}
	c.blind = true
	c.log.Warn("the store's responses carry no usable time, so the node's clock is not being checked", "err", err)
}

// errNoDate and errBadDate are the failures to read a time off a response.
var (
	errNoDate  = errors.New("blob: the response has no Date header")
	errBadDate = errors.New("blob: the response has an unreadable Date header")
)

// responseSample reads the store's time off a response's headers.
func responseSample(h http.Header, start, end time.Time) (ClockSample, error) {
	v := h.Get("Date")
	if v == "" {
		return ClockSample{}, errNoDate
	}
	t, err := http.ParseTime(v)
	if err != nil {
		return ClockSample{}, fmt.Errorf("%w: %q", errBadDate, v)
	}
	return ClockSample{Store: t, Start: start, End: end}, nil
}

// clockSampler is the SDK middleware that takes a sample from every response.
// It sits at the front of the deserialize step, so it sees the response (an
// error's included) as soon as its headers are in, before the body is read, and
// the window it times is the HTTP exchange without the signing and the
// serializing around it.
type clockSampler struct{ check *clockCheck }

func (clockSampler) ID() string { return "shardliteClockSample" }

func (c clockSampler) HandleDeserialize(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (middleware.DeserializeOutput, middleware.Metadata, error) {
	start := time.Now()
	out, md, err := next.HandleDeserialize(ctx, in)
	end := time.Now()
	// A request that got no response (a refused connection, a timeout) has no
	// time to read and is nothing to report: the call's own error says so. The
	// SDK leaves an empty response in its place, which has no status.
	if resp, ok := out.RawResponse.(*smithyhttp.Response); ok && resp.Response != nil && resp.StatusCode != 0 {
		c.check.observe(responseSample(resp.Header, start, end))
	}
	return out, md, err
}

func addClockSampler(check *clockCheck) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Deserialize.Add(clockSampler{check}, middleware.Before)
	}
}
