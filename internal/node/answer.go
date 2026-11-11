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

// fail answers with the status and the reason an error maps to. An unavailable
// carries its own Retry-After, and every other 503 and a failed dial say 1.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	status, why, msg, retry := http.StatusBadGateway, reasonInternal, err.Error(), ""
	se, isStatus := errors.AsType[*statusError](err)
	ue, isUnavailable := errors.AsType[*unavailable](err)
	switch {
	case isStatus:
		status, why, msg = se.status, se.reason, se.msg
	case isUnavailable:
		status, why, msg = http.StatusServiceUnavailable, ue.reason, ue.msg
		retry = strconv.Itoa(int((ue.retry + time.Second - 1) / time.Second))
	case errors.Is(err, ErrUnknownKind):
		status, why = http.StatusNotFound, reasonUnknownShard
	case errors.Is(err, ErrBadTarget):
		status, why = http.StatusBadRequest, reasonBadRequest
	case errors.Is(err, ErrAppHolds), errors.Is(err, errShardsStopped):
		// A shard the node is no longer starting, or one the app committed to while
		// the stop was letting go of it (the stop is letting go of its shards while
		// it still answers).
		status, why = http.StatusServiceUnavailable, reasonDraining
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
			retry = "1"
		}
	}
	if status == http.StatusServiceUnavailable && retry == "" {
		retry = "1"
	}
	if retry != "" {
		w.Header().Set("Retry-After", retry)
	}
	// An unavailable is an answer the node means to give, so it is not logged.
	if status >= 500 && !isUnavailable {
		s.log.Warn("request failed", "method", r.Method, "path", r.URL.Path, "reason", why, "err", err)
	}
	writeError(w, r, status, why, msg)
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
// grpc-message and shardlite-error), which is what a gRPC client understands.
func writeError(w http.ResponseWriter, r *http.Request, status int, why reason, msg string) {
	if !isGRPC(r) {
		writePlain(w, r, status, why, msg)
		return
	}
	code, ok := grpcCodes[status]
	if !ok {
		code = 13 // INTERNAL
	}
	h := w.Header()
	h.Set(hdrError, string(why))
	h.Set("Content-Type", "application/grpc")
	h.Set("Grpc-Status", strconv.Itoa(code))
	h.Set("Grpc-Message", grpcMessage(msg))
	w.WriteHeader(http.StatusOK)
}

// writePlain is writeError with the status always the HTTP one, for an answer
// another node reads (a 421, or the refusal of a request that is no forward).
func writePlain(w http.ResponseWriter, r *http.Request, status int, why reason, msg string) {
	w.Header().Set(hdrError, string(why))
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
