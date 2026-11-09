package node

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/notify"
)

// appLostAfter is how long the app may go without any HTTP answer to the
// poll, after it has answered once, before the node counts it as lost and
// stops. It is longer than a normal restart of one app container. (A constant,
// since the node observes whether its app is there and nothing gives a basis
// for another number.)
const appLostAfter = 30 * time.Second

// declarations polls the app's ready route, which is both the health check
// and the source of the app's kinds: a 2xx answer parsed as a declaration
// (docs/guide.md, Shard kinds) marks the app healthy and makes its kinds
// current. A poll that fails marks the app unhealthy and keeps the last
// declaration that was good. (The node's own readiness is Server.Ready.)
//
// The poll also tells when the app is lost: once the app has given any HTTP
// answer (a non-2xx one and a declaration that doesn't parse count, since the
// app is there and only not ready), a run of polls that get no answer at all
// (the dial fails, the connection resets, the client's timeout passes) lasting
// appLostAfter calls lost, once. Any answer ends the run. A node that has never
// heard from its app waits for it and never calls it lost.
type declarations struct {
	kinds  *Kinds
	url    string
	every  time.Duration
	client *http.Client
	log    *slog.Logger
	clock  Clock          // the poll's period and the run of unanswered polls, the real one if nil
	lost   func()         // called once, from the poll's goroutine, when the app is lost
	polls  notify.Counter // the polls that have finished
	ok     atomic.Bool
	why    atomic.Pointer[string] // set while the last poll failed, with the reason
	decl   kind.App               // the last declaration seen, so a change is logged once
	first  bool                   // no declaration seen yet

	// Only the poll's goroutine touches decl, first and these three.
	heard  bool      // the app has answered at least once
	silent time.Time // when the current run of unanswered polls began, zero if there is none
	gone   bool      // the app has been called lost
}

func (*declarations) String() string { return "declaration poll" }

func (r *declarations) Serve(ctx context.Context) error {
	defer serving(r.log, r.String())()
	if r.clock == nil {
		r.clock = realClock{}
	}
	r.first = true
	check := func() {
		why, answered := r.poll(ctx)
		if ctx.Err() == nil { // a poll cut off by the stop says nothing of the app
			r.observe(answered)
		}
		if why != "" {
			r.why.Store(&why)
			r.ok.Store(false)
		} else {
			r.why.Store(nil)
			r.ok.Store(true)
		}
		r.polls.Inc()
	}
	tick, stop := r.clock.NewTicker(r.every) // before the first poll, so a clock that moves after it is seen
	defer stop()
	check()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick:
			check()
		}
	}
}

// observe counts a poll towards the app being lost: whether it got any HTTP
// answer at all.
func (r *declarations) observe(answered bool) {
	if answered {
		r.heard = true
		r.silent = time.Time{}
		return
	}
	if !r.heard || r.gone {
		return
	}
	now := r.clock.Now()
	if r.silent.IsZero() {
		r.silent = now
	}
	if now.Sub(r.silent) < appLostAfter {
		return
	}
	r.gone = true
	r.log.Error("the app has not answered, so the node is stopping", "for", now.Sub(r.silent).Round(time.Second), "url", r.url)
	if r.lost != nil {
		r.lost()
	}
}

// poll asks the app once. It returns "" for healthy, or why not, and whether
// the app gave an HTTP answer at all (whatever it said).
func (r *declarations) poll(ctx context.Context) (why string, answered bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url, nil)
	if err != nil {
		return "the app's ready route is unreachable: " + err.Error(), false
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return "the app did not answer: " + err.Error(), false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "the app's answer could not be read: " + err.Error(), true
	}
	if resp.StatusCode/100 != 2 {
		return "the app answered " + resp.Status, true
	}
	a, err := r.kinds.Parse(body)
	if err != nil {
		return "the app's declaration is not valid: " + err.Error(), true
	}
	r.declared(a)
	return "", true
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
