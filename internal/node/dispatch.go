package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/system/dispatch"
)

// Dispatches (docs/internals.md, System kinds): the call _dispatch
// makes to the node a due shard belongs on, which makes the shard active there so
// its reminders loop covers it.

// dispatchPath is on the peer listener: POST with a dispatchRequest.
const dispatchPath = apiPrefix + "dispatch"

// dispatchTimeout bounds one dispatch: an activation restores the shard.
const dispatchTimeout = 30 * time.Second

type dispatchRequest struct {
	Shard shard.Name `json:"shard"`
}

type dispatchReply struct {
	Activated bool `json:"activated"` // false if the shard was active already
}

// dispatchHere is a dispatch this node carries out: the shard is acquired, never
// created, as a wake does. Active here answers at once, cached here is
// reactivated, and an unowned or claimable shard is claimed here and restored. The
// shard is then active and its reminders loop covers it.
func (m *Catalog) dispatchHere(ctx context.Context, n shard.Name) (dispatch.Answer, error) {
	if m.IsActive(n) {
		return dispatch.Active, nil
	}
	hold, err := m.acquireExisting(ctx, n)
	if err != nil {
		if errors.Is(err, blob.ErrNotFound) {
			return dispatch.Unknown, dispatch.ErrNotFound
		}
		return dispatch.Unknown, err
	}
	hold.Release()
	return dispatch.Activated, nil
}

// dispatch sends a dispatch of the shard to node, in process when node is this one.
// A failure answers dispatch.Unknown, which is no answer at all.
func (s *Server) dispatch(ctx context.Context, node cluster.Node, n shard.Name) (dispatch.Answer, error) {
	ctx, cancel := context.WithTimeout(ctx, dispatchTimeout)
	defer cancel()
	if node == s.m.n.Name {
		return s.m.dispatchHere(ctx, n)
	}
	address, err := s.m.addrs.address(ctx, node)
	if err != nil {
		return dispatch.Unknown, fmt.Errorf("dispatching to %s: %w", node, err)
	}
	body, err := json.Marshal(dispatchRequest{Shard: n})
	if err != nil {
		return dispatch.Unknown, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+address+dispatchPath, bytes.NewReader(body))
	if err != nil {
		return dispatch.Unknown, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.peers.Do(req)
	if err != nil {
		return dispatch.Unknown, fmt.Errorf("dispatching to %s: %w", node, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var r dispatchReply
		if err := json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&r); err != nil {
			return dispatch.Unknown, fmt.Errorf("dispatching to %s: %w", node, err)
		}
		if r.Activated {
			return dispatch.Activated, nil
		}
		return dispatch.Active, nil
	case http.StatusNotFound:
		return dispatch.Unknown, dispatch.ErrNotFound
	}
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
	return dispatch.Unknown, fmt.Errorf("dispatching to %s: %d %s", node, resp.StatusCode, bytes.TrimSpace(msg))
}

// serveDispatch serves POST /_shardlite/dispatch: 200 once the shard is active
// here, 421 naming the owner if another live node owns it, 503 if its owner awaits
// adoption, and 404 if it has no manifest.
func (s *Server) serveDispatch(w http.ResponseWriter, r *http.Request) {
	var req dispatchRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
		writeError(w, r, true, http.StatusBadRequest, "a dispatch is {\"shard\": ...}")
		return
	}
	if req.Shard.IsZero() {
		writeError(w, r, true, http.StatusBadRequest, "a dispatch is {\"shard\": ...}")
		return
	}
	ans, err := s.m.dispatchHere(r.Context(), req.Shard)
	switch oe, owned := errors.AsType[*shardfile.OwnedElsewhereError](err); {
	case err == nil:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(dispatchReply{Activated: ans == dispatch.Activated})
	case errors.Is(err, dispatch.ErrNotFound):
		writeError(w, r, true, http.StatusNotFound, err.Error())
	case owned:
		w.Header().Set(hdrOwner, string(oe.Owner))
		writeError(w, r, true, http.StatusMisdirectedRequest, err.Error())
	case errors.Is(err, shardfile.ErrOwnerNotAdopted):
		w.Header().Set("Retry-After", "1")
		writeError(w, r, true, http.StatusServiceUnavailable, err.Error())
	default:
		s.fail(w, r, true, err)
	}
}
