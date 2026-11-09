package shardlite

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Reason says why the node answered with an error of its own (the
// Shardlite-Error header and the guide's Status codes). Switch over it to tell
// causes apart. A node newer than the SDK may send a reason it does not know:
// that is ReasonOther, with the value in Error.RawReason.
type Reason string

const (
	// ReasonBadRequest is a malformed shard name or Shardlite-* header, or a
	// call the node refuses as made.
	ReasonBadRequest Reason = "bad-request"
	// ReasonUnknownShard is an undeclared kind, a kind a client may not name,
	// or a shard that does not exist and may not be created.
	ReasonUnknownShard Reason = "unknown-shard"
	// ReasonNotActive is a call with IfActive to a shard that is not active.
	ReasonNotActive Reason = "not-active"
	// ReasonNoEndpoint is a path the listener does not serve.
	ReasonNoEndpoint Reason = "no-endpoint"
	// ReasonHops is calls chained past the node's hop limit.
	ReasonHops Reason = "hops"
	// ReasonAppUnreachable is the node failing to reach the callee's app: the
	// app never had the request.
	ReasonAppUnreachable Reason = "app-unreachable"
	// ReasonAppFailed is the callee's app having the request and the connection
	// failing.
	ReasonAppFailed Reason = "app-failed"
	// ReasonOwnerUnreachable is the shard's owner node not answering, or being
	// dead and not adopted yet.
	ReasonOwnerUnreachable Reason = "owner-unreachable"
	// ReasonOwnerFailed is the owner node having the request and the connection
	// failing.
	ReasonOwnerFailed Reason = "owner-failed"
	// ReasonMoved is the shard moving or being claimed elsewhere meanwhile.
	ReasonMoved Reason = "moved"
	// ReasonDraining is the node stopping and unable to serve or place the
	// request.
	ReasonDraining Reason = "draining"
	// ReasonLeaseLost is the node's lease being lost or expired.
	ReasonLeaseLost Reason = "lease-lost"
	// ReasonBadFile is the shard's files failing their checksums.
	ReasonBadFile Reason = "bad-file"
	// ReasonSyncUnmet is the node being unable to meet the durability tier
	// asked for.
	ReasonSyncUnmet Reason = "sync-unmet"
	// ReasonNotReady is the node's readiness check failing.
	ReasonNotReady Reason = "not-ready"
	// ReasonTimeout is a timeout inside the node ending the request.
	ReasonTimeout Reason = "timeout"
	// ReasonInternal is any other failure of the node itself (storage, say).
	ReasonInternal Reason = "internal"
	// ReasonOther is a reason this SDK does not know, which a newer node may
	// send. Error.RawReason has it.
	ReasonOther Reason = ""
)

// reasonOf maps the value of Shardlite-Error to a Reason, ReasonOther if the
// SDK has no constant for it.
func reasonOf(raw string) Reason {
	switch r := Reason(raw); r {
	case ReasonBadRequest, ReasonUnknownShard, ReasonNotActive, ReasonNoEndpoint, ReasonHops, ReasonAppUnreachable, ReasonAppFailed,
		ReasonOwnerUnreachable, ReasonOwnerFailed, ReasonMoved, ReasonDraining, ReasonLeaseLost, ReasonBadFile, ReasonSyncUnmet,
		ReasonNotReady, ReasonTimeout, ReasonInternal:
		return r
	case ReasonOther:
	}
	return ReasonOther
}

// Error is an error the node answered with, as opposed to the callee app's own.
// NodeError builds it from a call's response and Node.Sync returns it, so find
// it with errors.As:
//
//	var nerr *shardlite.Error
//	if errors.As(err, &nerr) && nerr.Reason == shardlite.ReasonLeaseLost {
//		...
//	}
type Error struct {
	// Status is the HTTP status of the response.
	Status int
	// Reason is the node's reason, ReasonOther if the SDK has no constant for
	// it.
	Reason Reason
	// RawReason is the value of Shardlite-Error as the node sent it, the only
	// record of a ReasonOther.
	RawReason string
	// Message is the start of the response's body (up to 512 bytes), with
	// surrounding space trimmed.
	Message string

	retryAfter time.Duration
	hasRetry   bool
}

// RetryAfter is the node's Retry-After hint as it sent it: how long to wait
// before the next attempt, which may be zero (try again at once). The bool is
// false when the node gave none, and then the error says nothing about whether
// another attempt can succeed. The SDK has no retry policy of its own.
func (e *Error) RetryAfter() (time.Duration, bool) { return e.retryAfter, e.hasRetry }

func (e *Error) Error() string {
	msg := fmt.Sprintf("shardlite: the node answered %d (%s)", e.Status, e.RawReason)
	if e.Message != "" {
		msg += ": " + e.Message
	}
	if d, ok := e.RetryAfter(); ok {
		msg += fmt.Sprintf(" (retry after %s)", d)
	}
	return msg
}

// NodeError reads the node's error from the response of a call. It returns true
// when the response carries Shardlite-Error, which the node sets on every error
// of its own and strips from every app response, so false means the response is
// the app's answer.
//
// When it returns true it reads up to 512 bytes of the body for the message and
// leaves the rest, so the body is spent: the caller still closes it and reads
// it for nothing else. When it returns false it does not touch the body.
//
// Retry-After is read as the node writes it, a whole number of seconds. A
// response without one, or with one in another form, has no hint.
func NodeError(res *http.Response) (*Error, bool) {
	raw := res.Header.Values(headerError)
	if len(raw) == 0 {
		return nil, false
	}
	e := &Error{Status: res.StatusCode, RawReason: raw[0], Reason: reasonOf(raw[0])}
	if res.Body != nil {
		msg, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		e.Message = strings.TrimSpace(string(msg))
	}
	if secs, err := strconv.ParseInt(strings.TrimSpace(res.Header.Get("Retry-After")), 10, 64); err == nil && secs >= 0 && secs <= math.MaxInt64/int64(time.Second) {
		e.retryAfter, e.hasRetry = time.Duration(secs)*time.Second, true
	}
	return e, true
}
