package blob

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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

// ClockSink receives the samples a backend takes from the responses it already
// gets, and the failure to take one: err is set (and the sample empty) when a
// response had no usable time, so a store that stops sending one is reported
// and not mistaken for a quiet clock. It is called on the request's goroutine,
// so it must not block, and a backend calls it concurrently.
type ClockSink func(s ClockSample, err error)

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
type clockSampler struct{ sink ClockSink }

func (clockSampler) ID() string { return "shardliteClockSample" }

func (c clockSampler) HandleDeserialize(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (middleware.DeserializeOutput, middleware.Metadata, error) {
	start := time.Now()
	out, md, err := next.HandleDeserialize(ctx, in)
	end := time.Now()
	// A request that got no response (a refused connection, a timeout) has no
	// time to read and is nothing to report: the call's own error says so. The
	// SDK leaves an empty response in its place, which has no status.
	if resp, ok := out.RawResponse.(*smithyhttp.Response); ok && resp.Response != nil && resp.StatusCode != 0 {
		c.sink(responseSample(resp.Header, start, end))
	}
	return out, md, err
}

func addClockSampler(sink ClockSink) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Deserialize.Add(clockSampler{sink}, middleware.Before)
	}
}
