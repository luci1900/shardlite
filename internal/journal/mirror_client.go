package journal

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/pos"
)

// The mirror protocol (docs/internals.md, Node journals and snapshots): one
// node's journal mirrored onto another node's disk, spoken over the peer
// listener the node already runs. The verbs and the statuses are the blob
// backend's, so a create is a PUT with If-None-Match: * (a number already
// taken answers 412, 409 a racing conditional write, 404 a missing segment),
// and the client reads back and retries in exactly the code it uses against a
// bucket. HTTP semantics, so HTTP/3 is possible later without changing the
// protocol.
const (
	// MirrorPath is where the peer listener serves the journals it mirrors.
	MirrorPath = "/_shardlite/journal/"
	// WriteIDHeader carries a segment's write id on the wire, as JSON: the
	// writer's node, epoch and counter, stored durably with the segment and
	// read back on a head.
	WriteIDHeader = "Shardlite-Write-Id"
)

// Mirror is another node's disk as a home for a journal: the journal's
// interface over HTTP to that node's peer listener. Like Blob and Local it is
// one journal, the one of the node it is constructed with, so a mirror of n1's
// journal and a mirror of n2's are two Mirrors against the same peer. The
// receiving side is in mirror_server.go.
//
// It is a Backend: single attempts, the outcomes of the table in internal/blob.
// A mirror's create returns only when the peer has committed the segment to its
// own database, so a nil from Create means the bytes are on that node's disk.
type Mirror struct {
	client *http.Client
	base   string       // the peer listener's base URL, http://host:port
	node   cluster.Node // whose journal this is
}

// NewMirror returns the journal of node as mirrored on the peer listener at
// base. The client is the caller's: a node uses one h2c client for its peers.
func NewMirror(client *http.Client, base string, node cluster.Node) *Mirror {
	return &Mirror{client: client, base: strings.TrimRight(base, "/"), node: node}
}

// Name is the node whose journal this is.
func (m *Mirror) Name() cluster.Node { return m.node }

var _ Backend = (*Mirror)(nil)

func (m *Mirror) path(seq pos.Seg) string {
	return fmt.Sprintf("%s%s%s/%0*d", m.base, MirrorPath, m.node, seqDigits, uint64(seq))
}

func (m *Mirror) listPath(from pos.Seg) string {
	if from < 1 {
		from = 1
	}
	return fmt.Sprintf("%s%s%s?from=%d", m.base, MirrorPath, m.node, uint64(from))
}

// Create writes the segment once, with If-None-Match: * and the write id in a
// header. It fails with blob.ErrExists when the number is taken.
func (m *Mirror) Create(ctx context.Context, seq pos.Seg, body blob.Body, id blob.WriteID) error {
	idWire, err := json.Marshal(id)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, m.path(seq), io.NewSectionReader(body, 0, body.Size()))
	if err != nil {
		return err
	}
	req.ContentLength = body.Size()
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("If-None-Match", "*")
	req.Header.Set(WriteIDHeader, string(idWire))
	resp, wasSent, err := m.do(ctx, req)
	if err != nil {
		return outcome(ctx, err, wasSent, false)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		return nil
	}
	return statusOutcome(m.node, seq, resp, false)
}

// Get returns the whole segment.
func (m *Mirror) Get(ctx context.Context, seq pos.Seg) (io.ReadCloser, error) {
	return m.get(ctx, seq, "")
}

// GetRange reads length bytes from offset (to the end if length is not
// positive or reaches past it). A negative offset asks for the last -offset
// bytes, the range read a footer is, and goes on the wire as HTTP's own
// suffix range.
func (m *Mirror) GetRange(ctx context.Context, seq pos.Seg, offset, length int64) (io.ReadCloser, error) {
	spec, err := rangeSpec(offset, length)
	if err != nil {
		return nil, err
	}
	return m.get(ctx, seq, spec)
}

// rangeSpec is the HTTP range of one Backend range: "" for a whole read.
func rangeSpec(offset, length int64) (string, error) {
	if offset < 0 {
		if length != 0 {
			return "", fmt.Errorf("journal: a range from the end takes no length (got %d)", length)
		}
		return "bytes=-" + strconv.FormatInt(-offset, 10), nil
	}
	if length <= 0 {
		return "bytes=" + strconv.FormatInt(offset, 10) + "-", nil
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+length-1), nil
}

func (m *Mirror) get(ctx context.Context, seq pos.Seg, rangeHeader string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.path(seq), nil)
	if err != nil {
		return nil, err
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	resp, wasSent, err := m.do(ctx, req)
	if err != nil {
		return nil, outcome(ctx, err, wasSent, true)
	}
	defer func() {
		if resp.Body != nil {
			resp.Body.Close()
		}
	}()
	switch {
	case resp.StatusCode == http.StatusRequestedRangeNotSatisfiable:
		// A range off the end of a segment is a caller's mistake, and
		// retrying it writes the mistake again: a plain error.
		return nil, fmt.Errorf("journal: mirror %s: the range of %d is not satisfiable", m.node, seq)
	case resp.StatusCode/100 == 2:
		body := resp.Body
		resp.Body = nil // the caller closes it
		return body, nil
	default:
		return nil, statusOutcome(m.node, seq, resp, true)
	}
}

// Head says the segment's size (0 is a fence) and write id.
func (m *Mirror) Head(ctx context.Context, seq pos.Seg) (SegmentInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, m.path(seq), nil)
	if err != nil {
		return SegmentInfo{}, err
	}
	resp, wasSent, err := m.do(ctx, req)
	if err != nil {
		return SegmentInfo{}, outcome(ctx, err, wasSent, true)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return SegmentInfo{}, statusOutcome(m.node, seq, resp, true)
	}
	var id blob.WriteID
	if wire := resp.Header.Get(WriteIDHeader); wire != "" {
		if err := json.Unmarshal([]byte(wire), &id); err != nil {
			return SegmentInfo{}, fmt.Errorf("journal: mirror %s: the write id of %d is bad: %w", m.node, seq, err)
		}
	}
	return SegmentInfo{Size: resp.ContentLength, WriteID: id}, nil
}

// Delete removes the segment.
func (m *Mirror) Delete(ctx context.Context, seq pos.Seg) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, m.path(seq), nil)
	if err != nil {
		return err
	}
	resp, wasSent, err := m.do(ctx, req)
	if err != nil {
		return outcome(ctx, err, wasSent, false)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		return nil
	}
	return statusOutcome(m.node, seq, resp, false)
}

// List returns the journal's segment numbers from from (inclusive) on, in
// number order. The answer is one number per line, ended by a line holding
// only ".": a stream cut short without it is an error, since a list that
// stops early would read as a journal that ends where the cut fell.
func (m *Mirror) List(ctx context.Context, from pos.Seg) iter.Seq2[pos.Seg, error] {
	return func(yield func(pos.Seg, error) bool) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.listPath(from), nil)
		if err != nil {
			yield(0, err)
			return
		}
		resp, wasSent, err := m.do(ctx, req)
		if err != nil {
			yield(0, outcome(ctx, err, wasSent, true))
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			yield(0, statusOutcome(m.node, 0, resp, true))
			return
		}
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64<<10), 64<<10)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "." {
				return
			}
			n, perr := strconv.ParseUint(line, 10, 64)
			if perr != nil || n == 0 {
				yield(0, fmt.Errorf("journal: mirror %s: a listing line is not a segment number: %q", m.node, line))
				return
			}
			if !yield(pos.Seg(n), nil) {
				return
			}
		}
		if err := sc.Err(); err != nil {
			yield(0, err)
			return
		}
		yield(0, fmt.Errorf("journal: mirror %s: the listing ended without its end mark", m.node))
	}
}

// do sends one request and says whether it was written out in full, so a
// failure without a response can be told from one that may have reached the
// peer.
func (m *Mirror) do(ctx context.Context, req *http.Request) (resp *http.Response, wasSent bool, err error) {
	var sent atomic.Bool
	req = req.WithContext(httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		WroteRequest: func(i httptrace.WroteRequestInfo) {
			if i.Err == nil {
				sent.Store(true)
			}
		},
	}))
	resp, err = m.client.Do(req)
	if err != nil {
		return nil, sent.Load(), err
	}
	return resp, true, nil
}

// outcome maps a transport failure onto the outcomes of a Backend, as the S3
// backend's classify does for its SDK: a request that was never fully sent
// (the peer is down, DNS, the context ended before the body was sent) is
// ErrNotApplied; one that was fully sent and then failed (a cut connection, a
// timeout of the transport's) is ErrUnknown; the caller's own context ending
// is returned as it is. A read that fails without a response is ErrNotApplied:
// it changed nothing.
func outcome(ctx context.Context, err error, wasSent, read bool) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		if ctx.Err() != nil {
			return err // the caller ended it: theirs to see
		}
		// A deadline of the transport's, while the caller's context is live.
		if read {
			return fmt.Errorf("%w: %w", blob.ErrNotApplied, err)
		}
		if wasSent {
			return fmt.Errorf("%w: %w", blob.ErrUnknown, err)
		}
		return fmt.Errorf("%w: %w", blob.ErrNotApplied, err)
	}
	if read {
		return fmt.Errorf("%w: %w", blob.ErrNotApplied, err)
	}
	if wasSent {
		return fmt.Errorf("%w: %w", blob.ErrUnknown, err)
	}
	return fmt.Errorf("%w: %w", blob.ErrNotApplied, err)
}

// statusOutcome maps a peer's status onto the outcomes, whose statuses they
// are: 412 is ErrExists for a create and ErrConflict for anything else, 404 is
// ErrNotFound, 409 and 429 are ErrNotApplied (the peer did nothing), 408 and
// every 5xx are ErrUnknown, and any other 4xx is a plain error, not retried.
func statusOutcome(node cluster.Node, seq pos.Seg, resp *http.Response, read bool) error {
	where := fmt.Sprintf("journal: mirror %s", node)
	if seq > 0 {
		where += fmt.Sprintf(": %d", seq)
	}
	switch status := resp.StatusCode; {
	case status == http.StatusNotFound:
		return fmt.Errorf("%s: %w: %s", where, blob.ErrNotFound, resp.Status)
	case status == http.StatusPreconditionFailed:
		if read {
			return fmt.Errorf("%s: %w: %s", where, blob.ErrConflict, resp.Status)
		}
		return fmt.Errorf("%s: %w: %s", where, blob.ErrExists, resp.Status)
	case status == http.StatusConflict, status == http.StatusTooManyRequests:
		return fmt.Errorf("%s: %w: %s", where, blob.ErrNotApplied, resp.Status)
	case status == http.StatusRequestTimeout, status >= 500:
		return fmt.Errorf("%s: %w: %s", where, blob.ErrUnknown, resp.Status)
	default:
		return fmt.Errorf("%s: %s", where, resp.Status)
	}
}
