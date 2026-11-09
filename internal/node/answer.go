package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// statusError is an answer the node gives with a status of its own.
type statusError struct {
	status int
	reason reason
	msg    string
}

func (e *statusError) Error() string { return e.msg }

// answer writes the answer for an error from one of route's steps.
func (s *Server) answer(w http.ResponseWriter, r *http.Request, own bool, err error) {
	if se, ok := errors.AsType[*statusError](err); ok {
		writeError(w, r, own, se.status, se.reason, se.msg)
		return
	}
	if ue, ok := errors.AsType[*unavailable](err); ok {
		w.Header().Set("Retry-After", strconv.Itoa(int((ue.retry+time.Second-1)/time.Second)))
		writeError(w, r, own, http.StatusServiceUnavailable, ue.reason, ue.msg)
		return
	}
	s.fail(w, r, own, err)
}

// fail answers with the status and the reason an error maps to.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, own bool, err error) {
	status, why := http.StatusBadGateway, reasonInternal
	if se, ok := errors.AsType[*statusError](err); ok {
		status, why = se.status, se.reason
	} else if ue, ok := errors.AsType[*unavailable](err); ok {
		// A tier the node can't meet: the retry hint is the default one.
		status, why = http.StatusServiceUnavailable, ue.reason
	} else {
		switch {
		case errors.Is(err, ErrUnknownKind):
			status, why = http.StatusNotFound, reasonUnknownShard
		case errors.Is(err, ErrBadTarget):
			status, why = http.StatusBadRequest, reasonBadRequest
		case errors.Is(err, ErrAppHolds), errors.Is(err, errShardsStopped):
			// A shard the node is no longer starting, or one the app committed to while
			// the stop was letting go of it (the stop is letting go of its shards while
			// it still answers).
			status, why = http.StatusServiceUnavailable, reasonDraining
		case errors.Is(err, errMoved):
			status, why = http.StatusServiceUnavailable, reasonMoved
		case errors.Is(err, lease.ErrLost), errors.Is(err, lease.ErrExpired):
			// A shard the node may not serve yet.
			status, why = http.StatusServiceUnavailable, reasonLeaseLost
		case errors.Is(err, shardfile.ErrBadFile):
			// A shard whose files are not the ones it holds cannot be served.
			status, why = http.StatusServiceUnavailable, reasonBadFile
		case errors.Is(err, context.DeadlineExceeded):
			status, why = http.StatusGatewayTimeout, reasonTimeout
		default:
			// Only a proxy failure or a lookup of the owner's address is a 502 with a
			// reason of its own (reasonError).
			if re, ok := errors.AsType[*reasonError](err); ok {
				why = re.reason
			}
			if neverReached(err) {
				// The dial to the app failed, so the app never had the request and a
				// retry is safe: a 502 with the default retry hint says so. A failure
				// after the request went out is a bare 502, since the app may have
				// acted on it.
				w.Header().Set("Retry-After", "1")
			}
		}
	}
	if status >= 500 {
		s.log.Warn("request failed", "method", r.Method, "path", r.URL.Path, "reason", why, "err", err)
		if status == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", "1")
		}
	}
	writeError(w, r, own, status, why, err.Error())
}

// neverReached says err is a failed dial, which is the one transport error that
// comes before anything was sent. The transport reports it as a net.OpError for
// the dial (a refused connection, an unreachable address, a name that doesn't
// resolve), and one that fails later (a reset, an EOF) is an op of read or write.
func neverReached(err error) bool {
	oe, ok := errors.AsType[*net.OpError](err)
	return ok && oe.Op == "dial"
}

// grpcCodes maps the statuses the node produces onto gRPC status codes.
var grpcCodes = map[int]int{
	http.StatusBadRequest:         3,  // INVALID_ARGUMENT
	http.StatusNotFound:           5,  // NOT_FOUND
	http.StatusConflict:           9,  // FAILED_PRECONDITION
	http.StatusLoopDetected:       8,  // RESOURCE_EXHAUSTED
	http.StatusTooManyRequests:    8,  // RESOURCE_EXHAUSTED
	http.StatusBadGateway:         14, // UNAVAILABLE
	http.StatusServiceUnavailable: 14, // UNAVAILABLE
	http.StatusGatewayTimeout:     4,  // DEADLINE_EXCEEDED
}

// writeError answers with an error the node produced itself, marked with its
// reason in Shardlite-Error. For a request with content-type application/grpc it
// is a gRPC status (a "trailers-only" response: HTTP 200 with grpc-status,
// grpc-message and shardlite-error), which is what a gRPC client understands. own
// says the request is the node's own (answered in process), which is not given a gRPC status.
func writeError(w http.ResponseWriter, r *http.Request, own bool, status int, why reason, msg string) {
	w.Header().Set(hdrError, string(why))
	// The node's own requests read the status themselves, whatever it carries.
	if isGRPC(r) && !own {
		code, ok := grpcCodes[status]
		if !ok {
			code = 13 // INTERNAL
		}
		h := w.Header()
		h.Set("Content-Type", "application/grpc")
		h.Set("Grpc-Status", strconv.Itoa(code))
		h.Set("Grpc-Message", grpcMessage(msg))
		w.WriteHeader(http.StatusOK)
		return
	}
	if strings.Contains(r.Header.Get("Accept"), "application/json") || strings.HasPrefix(r.URL.Path, apiPrefix) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(map[string]string{"error": msg})
		return
	}
	http.Error(w, msg, status)
}

// grpcMessage percent-encodes a grpc-message as the gRPC spec says.
func grpcMessage(s string) string {
	var b strings.Builder
	for _, c := range []byte(s) {
		if c >= 0x20 && c <= 0x7e && c != '%' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}
