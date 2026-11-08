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

// declarations polls the app's ready route, which is both the health check
// and the source of the app's kinds: a 2xx answer parsed as a declaration
// (docs/guide.md, Shard kinds) marks the app healthy and makes its kinds
// current. A poll that fails marks the app unhealthy and keeps the last
// declaration that was good. (The node's own readiness is Server.Ready.)
type declarations struct {
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

func (*declarations) String() string { return "declaration poll" }

func (r *declarations) Serve(ctx context.Context) error {
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
func (r *declarations) poll(ctx context.Context) string {
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
func (r *declarations) declared(a kind.App) {
	first := r.first
	r.first = false
	changed := first || a.Version != r.decl.Version || len(a.Kinds) != len(r.decl.Kinds)
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
		r.log.Info("the app declared itself", "version", a.Version, "kinds", len(a.Kinds), "first", first)
	}
}
