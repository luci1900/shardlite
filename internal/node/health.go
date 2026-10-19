package node

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// Ready reports whether the node should get traffic: the app is healthy, the
// kinds file is valid and the node's lease is good.
func (s *Server) Ready() (bool, string) {
	switch {
	case !s.health.Healthy():
		return false, "the app is not healthy"
	case !s.kinds.Valid():
		return false, "the kinds file is not valid"
	case s.keeper.Check() != nil:
		return false, "the node's lease is not valid"
	}
	return true, "ok"
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ok, why := s.Ready()
	status := http.StatusOK
	if !ok {
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		io.WriteString(w, why+"\n")
	}
}

// health polls the app's health endpoint.
type health struct {
	url    string
	every  time.Duration
	client *http.Client
	log    *slog.Logger
	ok     atomic.Bool
}

func (*health) String() string { return "health poll" }

// Healthy reports whether the last poll got a 2xx answer.
func (h *health) Healthy() bool { return h.ok.Load() }

func (h *health) Serve(ctx context.Context) error {
	defer serving(h.log, h.String())()
	check := func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.url, nil)
		if err != nil {
			h.ok.Store(false)
			return
		}
		resp, err := h.client.Do(req)
		if err != nil {
			h.ok.Store(false)
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		h.ok.Store(resp.StatusCode/100 == 2)
	}
	check()
	t := time.NewTicker(h.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			check()
		}
	}
}
