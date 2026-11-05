package node

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/kind"
)

// Ready reports whether the node should get traffic: the app has answered its
// ready route with a declaration (a node with no app address skips this), and
// the node's lease is good.
func (s *Server) Ready() (bool, string) {
	if s.ready != nil && !s.ready.ok.Load() {
		if why := s.ready.why.Load(); why != nil {
			return false, *why
		}
		return false, "waiting for the app's first declaration"
	}
	if err := s.keeper.Check(); err != nil {
		return false, "the node's lease is not valid"
	}
	return true, "ok"
}

func (s *Server) readyRoute(w http.ResponseWriter, r *http.Request) {
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

// ready polls the app's ready route, which is both the health check and the
// source of the app's kinds: a 2xx answer parsed as a declaration
// (docs/guide.md, Shard kinds) marks the app healthy and makes its kinds
// current. A poll that fails marks the app unhealthy and keeps the last
// declaration that was good.
type ready struct {
	kinds  *Kinds
	url    string
	every  time.Duration
	client *http.Client
	log    *slog.Logger
	ok     atomic.Bool
	why    atomic.Pointer[string] // set while the last poll failed, with the reason
	decl   kind.App               // the last declaration seen, so a change is logged once
	first  bool                   // no declaration seen yet (only the poll's goroutine touches decl and first)
}

func (*ready) String() string { return "ready poll" }

func (r *ready) Serve(ctx context.Context) error {
	defer serving(r.log, r.String())()
	r.first = true
	check := func() {
		if why := r.poll(ctx); why != "" {
			r.why.Store(&why)
			r.ok.Store(false)
			return
		}
		r.why.Store(nil)
		r.ok.Store(true)
	}
	check()
	t := time.NewTicker(r.every)
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

// poll asks the app once. It returns "" for healthy, or why not.
func (r *ready) poll(ctx context.Context) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url, nil)
	if err != nil {
		return "the app's ready route is unreachable: " + err.Error()
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return "the app did not answer: " + err.Error()
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "the app's answer could not be read: " + err.Error()
	}
	if resp.StatusCode/100 != 2 {
		return "the app answered " + resp.Status
	}
	a, err := r.kinds.Parse(body)
	if err != nil {
		return "the app's declaration is not valid: " + err.Error()
	}
	r.declared(a)
	return ""
}

// declared makes the declaration current and logs a change of it. The poll's
// goroutine is the only caller.
func (r *ready) declared(a kind.App) {
	first := r.first
	r.first = false
	changed := first || a.Name != r.decl.Name || a.Version != r.decl.Version || len(a.Kinds) != len(r.decl.Kinds)
	if !changed {
		for name, k := range a.Kinds {
			if old, ok := r.decl.Kinds[name]; !ok || old.Sync != k.Sync || old.IdleTimeout != k.IdleTimeout || old.Public != k.Public {
				changed = true
				break
			}
		}
	}
	r.decl = a
	r.kinds.Update(a)
	if changed {
		r.log.Info("the app declared itself", "app", a.Name, "version", a.Version, "kinds", len(a.Kinds), "first", first)
	}
}
