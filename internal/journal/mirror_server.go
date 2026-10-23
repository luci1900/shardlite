package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/spool"
)

// The receiving side of the mirror protocol: the handler a node mounts on its
// peer listener to serve the journals it mirrors for other nodes. The client
// is Mirror, in mirror_client.go.

// MirrorSource is what a node serves the mirror protocol from: the backends
// holding the journals it mirrors, and its view of the journals' owners'
// leases (every node scans leases).
type MirrorSource interface {
	// Journal is the backend holding node's mirrored segments, nil if this
	// node holds none (a node that mirrors nothing, asked anyway).
	Journal(node cluster.Node) Backend
	// Epoch is this node's view of node's lease epoch, 0 if it has none. A
	// create whose write id names a strictly older epoch is refused: a stale
	// life of a node name is a fence to the mirror, not a duplicate. A view
	// that is merely expired, or not yet caught up with the owner's renewal,
	// never refuses.
	Epoch(ctx context.Context, node cluster.Node) pos.Epoch
}

// NewMirrorHandler serves the mirror protocol on a peer listener. spoolDir and
// spoolLimit are where a request's body is held while its create reads it at
// any offset (spool.DefaultLimit if the limit is zero).
//
// The handler answers after the backend has committed the segment, so a 2xx
// means the bytes are on this node's disk. A create whose number is taken
// answers 412 with the write id held under the number in a header; one whose
// write id names a stale epoch answers 412 with nothing under the number.
func NewMirrorHandler(src MirrorSource, spoolDir string, spoolLimit int) http.Handler {
	if spoolLimit <= 0 {
		spoolLimit = spool.DefaultLimit
	}
	return &mirrorHandler{src: src, dir: spoolDir, limit: spoolLimit}
}

type mirrorHandler struct {
	src   MirrorSource
	dir   string
	limit int
}

func (h *mirrorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	node, seq, ok := cutMirrorPath(r.URL.Path)
	if !ok {
		writeMirrorError(w, http.StatusBadRequest, "the path is not a journal's")
		return
	}
	b := h.src.Journal(node)
	if b == nil {
		writeMirrorError(w, http.StatusServiceUnavailable, "this node does not mirror "+string(node))
		return
	}
	if seq == 0 {
		// The listing: GET /_shardlite/journal/{node}?from=N.
		if r.Method != http.MethodGet {
			writeMirrorError(w, http.StatusMethodNotAllowed, "a journal's listing is a GET")
			return
		}
		h.list(w, r, b)
		return
	}
	switch r.Method {
	case http.MethodPut:
		h.create(w, r, b, node, seq)
	case http.MethodGet:
		h.get(w, r, b, seq)
	case http.MethodHead:
		h.head(w, r, b, seq)
	case http.MethodDelete:
		h.delete(w, r, b, seq)
	default:
		writeMirrorError(w, http.StatusMethodNotAllowed, "not a journal method")
	}
}

// cutMirrorPath splits /_shardlite/journal/{node}/{seq} into its parts. A path
// with no number (the listing route) gives seq 0.
func cutMirrorPath(p string) (node cluster.Node, seq pos.Seg, ok bool) {
	rest, found := strings.CutPrefix(p, MirrorPath)
	if !found {
		return "", 0, false
	}
	name, num, hasNum := strings.Cut(rest, "/")
	if !cluster.ValidNode(name) {
		return "", 0, false
	}
	if !hasNum || num == "" {
		return cluster.Node(name), 0, true
	}
	n, err := strconv.ParseUint(num, 10, 64)
	if err != nil || n == 0 {
		return "", 0, false
	}
	return cluster.Node(name), pos.Seg(n), true
}

// create is a PUT: create once, durable before the answer, and refused when
// the write id names a stale life of the journal's node.
func (h *mirrorHandler) create(w http.ResponseWriter, r *http.Request, b Backend, node cluster.Node, seq pos.Seg) {
	if r.Header.Get("If-None-Match") != "*" {
		writeMirrorError(w, http.StatusBadRequest, "a create is a PUT with If-None-Match: *")
		return
	}
	var id blob.WriteID
	if err := json.Unmarshal([]byte(r.Header.Get(WriteIDHeader)), &id); err != nil {
		writeMirrorError(w, http.StatusBadRequest, "the write id is missing or bad")
		return
	}
	// The epoch check before the body is read, so a refused write costs no IO.
	if view := h.src.Epoch(r.Context(), node); view > id.Epoch {
		writeMirrorErrorf(w, http.StatusPreconditionFailed,
			"the write id names epoch %d of %s and this node's view of its lease is %d", uint64(id.Epoch), node, uint64(view))
		return
	}
	body, err := spool.Collect(h.dir, h.limit, r.Body)
	if err != nil {
		writeMirrorError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer body.Close()
	if err := b.Create(r.Context(), seq, body.Reader(), id); err != nil {
		switch {
		case errors.Is(err, blob.ErrExists):
			// Whose segment holds the number goes back in a header, so the
			// client's read-back is one head and not two requests.
			if held, herr := b.Head(r.Context(), seq); herr == nil {
				if wire, merr := json.Marshal(held.WriteID); merr == nil {
					w.Header().Set(WriteIDHeader, string(wire))
				}
			}
			writeMirrorError(w, http.StatusPreconditionFailed, "the number is taken")
		case errors.Is(err, blob.ErrNotApplied):
			writeMirrorError(w, http.StatusConflict, err.Error())
		default:
			writeMirrorError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *mirrorHandler) get(w http.ResponseWriter, r *http.Request, b Backend, seq pos.Seg) {
	var (
		rc  io.ReadCloser
		err error
	)
	if spec := r.Header.Get("Range"); spec != "" {
		offset, length, rerr := parseRange(spec)
		if rerr != nil {
			writeMirrorError(w, http.StatusBadRequest, rerr.Error())
			return
		}
		rc, err = b.GetRange(r.Context(), seq, offset, length)
	} else {
		rc, err = b.Get(r.Context(), seq)
	}
	if err != nil {
		writeBackendError(w, err)
		return
	}
	defer rc.Close()
	if r.Header.Get("Range") != "" {
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	io.Copy(w, rc)
}

// parseRange reads HTTP's Range for the forms the journal's reads use:
// bytes=A-B, bytes=A- and bytes=-N (a tail). Anything else is refused.
func parseRange(spec string) (offset, length int64, err error) {
	rest, ok := strings.CutPrefix(spec, "bytes=")
	if !ok {
		return 0, 0, fmt.Errorf("the range is not bytes=")
	}
	first, last, ok := strings.Cut(rest, "-")
	if !ok {
		return 0, 0, fmt.Errorf("the range has no dash")
	}
	if first == "" {
		n, serr := strconv.ParseInt(last, 10, 64)
		if serr != nil || n <= 0 {
			return 0, 0, fmt.Errorf("the suffix range is %q", spec)
		}
		return -n, 0, nil
	}
	a, serr := strconv.ParseInt(first, 10, 64)
	if serr != nil || a < 0 {
		return 0, 0, fmt.Errorf("the range starts at %q", first)
	}
	if last == "" {
		return a, 0, nil
	}
	bnum, serr := strconv.ParseInt(last, 10, 64)
	if serr != nil || bnum < a {
		return 0, 0, fmt.Errorf("the range ends at %q", last)
	}
	return a, bnum - a + 1, nil
}

// head is a HEAD: the size (Content-Length) and the write id in a header.
func (h *mirrorHandler) head(w http.ResponseWriter, r *http.Request, b Backend, seq pos.Seg) {
	info, err := b.Head(r.Context(), seq)
	if err != nil {
		writeBackendError(w, err)
		return
	}
	if wire, merr := json.Marshal(info.WriteID); merr == nil {
		w.Header().Set(WriteIDHeader, string(wire))
	}
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	w.WriteHeader(http.StatusOK)
}

func (h *mirrorHandler) delete(w http.ResponseWriter, r *http.Request, b Backend, seq pos.Seg) {
	if err := b.Delete(r.Context(), seq); err != nil && !errors.Is(err, blob.ErrNotFound) {
		// A number with no segment is gone all the same, as the callers of a
		// delete accept of any store.
		writeBackendError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// list streams the journal's numbers from from, one per line, ended by ".".
// A failure midway ends the stream short of the mark, which the client reads
// as the error it is.
func (h *mirrorHandler) list(w http.ResponseWriter, r *http.Request, b Backend) {
	from, err := strconv.ParseUint(r.URL.Query().Get("from"), 10, 64)
	if err != nil || from < 1 {
		from = 1
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	for seq, lerr := range b.List(r.Context(), pos.Seg(from)) {
		if lerr != nil {
			return
		}
		if _, err := fmt.Fprintln(w, uint64(seq)); err != nil {
			return
		}
	}
	fmt.Fprintln(w, ".")
}

// writeBackendError answers with the status of a backend's outcome.
func writeBackendError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, blob.ErrNotFound):
		writeMirrorError(w, http.StatusNotFound, "no such segment")
	case errors.Is(err, blob.ErrExists):
		writeMirrorError(w, http.StatusPreconditionFailed, "the number is taken")
	case errors.Is(err, blob.ErrNotApplied):
		writeMirrorError(w, http.StatusConflict, err.Error())
	default:
		writeMirrorError(w, http.StatusInternalServerError, err.Error())
	}
}

func writeMirrorError(w http.ResponseWriter, status int, msg string) {
	writeMirrorErrorf(w, status, "%s", msg)
}

func writeMirrorErrorf(w http.ResponseWriter, status int, format string, args ...any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf(format, args...)})
}
