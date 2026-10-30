// Package dispatch is the _dispatch system kind (docs/internals.md, System kinds):
// one shard, _system.dispatch/_cluster, that lists the reminder index up to the
// current minute and sends each due shard a dispatch, a call that makes the shard
// active on its owner or claims it on a node picked for load. It holds no state
// of its own beyond its reminder, so a move loses nothing.
package dispatch

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/remindex"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/system"
)

// Answer is what a node says to a dispatch it could carry out. The zero value
// is Unknown, which is no answer at all: Valid() is false for it, so a caller
// that lost the answer cannot mistake it for one the node gave
// (docs/internals.md, Failing loudly, the zero-value policy).
type Answer int

const (
	// Unknown is no answer: a dispatch that failed or was never answered.
	Unknown Answer = iota
	// Active means the shard was active on the node already.
	Active
	// Activated means the dispatch made it active.
	Activated
)

// String names an answer, for the logs and the errors.
func (a Answer) String() string {
	switch a {
	case Unknown:
		return "unknown"
	case Active:
		return "active"
	case Activated:
		return "activated"
	}
	return "unknown"
}

// Valid reports whether a is one of the answers a node gives. Unknown, the
// zero, is not one of them.
func (a Answer) Valid() bool {
	switch a {
	case Unknown:
		return false
	case Active, Activated:
		return true
	}
	return false
}

// ErrNotFound is the answer for a shard that has no manifest.
var ErrNotFound = errors.New("dispatch: the shard doesn't exist")

// maxBackoff is how far an entry moves at most, in minutes.
const maxBackoff = 16

// Options configure _dispatch. Zero values take the defaults.
type Options struct {
	// Rate is how many dispatches a second a pass makes at most, 1,000 if zero.
	Rate float64
	// InFlight is how many entries a pass handles at once, 256 if zero.
	InFlight int
	// Page is how many due entries a pass lists, 1,000 if zero.
	Page int
	// Budget is how long a pass runs before it stops and leaves the rest to the
	// next, 25 s if zero (inside the wake's 30 s).
	Budget time.Duration
	// Pace is the wait before the next pass when this one found due entries, 1 s
	// if zero.
	Pace time.Duration
	// Idle is the wait before the next pass when this one found nothing due, until
	// the start of the next minute if zero (and at least a second). Entries are
	// minutes, so only tests set it.
	Idle time.Duration
}

// Deps is what the kind uses of the node's.
type Deps struct {
	Store *blob.Store
	// Live names the nodes the lease scan's last listing counts as alive.
	Live func() []cluster.Node
	// Dispatch sends the dispatch for shard to node, in process when node is this
	// one. It returns ErrNotFound for a shard with no manifest.
	Dispatch func(ctx context.Context, node cluster.Node, name shard.Name) (Answer, error)
	Now      func() time.Time
	Log      *slog.Logger
}

// migrations: the kind has no tables of its own.
var migrations embed.FS

// New returns the _dispatch kind. Its tier is none: nothing acts on
// its answers, and a lost flush only repeats a dispatch.
func New(d Deps, opts Options) kind.Kind {
	p := newDispatcher(d, opts)
	return kind.Kind{Name: kind.SystemKind("dispatch"), Sync: kind.SyncNone, IDs: []shard.ID{"_cluster"}, Migrations: migrations, Handler: p.handler}
}

func newDispatcher(d Deps, opts Options) *dispatcher {
	if opts.Rate <= 0 {
		opts.Rate = 1000
	}
	if opts.InFlight <= 0 {
		opts.InFlight = 256
	}
	if opts.Page <= 0 {
		opts.Page = 1000
	}
	if opts.Budget <= 0 {
		opts.Budget = 25 * time.Second
	}
	if opts.Pace <= 0 {
		opts.Pace = time.Second
	}
	return &dispatcher{Deps: d, opts: opts, index: remindex.New(d.Store), manifests: shardfile.NewManifestStore(d.Store), load: map[cluster.Node]int{}}
}

type dispatcher struct {
	Deps
	opts      Options
	index     remindex.Index
	manifests shardfile.ManifestStore

	mu   sync.Mutex
	load map[cluster.Node]int // this dispatcher's dispatches in flight, by node
	slot time.Time            // when the next dispatch may go

	Passes, Dispatched, Failed atomic.Int64
}

func (p *dispatcher) handler(s *system.System) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /_shardlite/reminders", func(w http.ResponseWriter, r *http.Request) {
		busy, err := p.pass(r.Context())
		if err != nil {
			p.Log.Warn("a dispatch pass failed, trying again", "err", err)
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		now := p.Now()
		next := now.Truncate(time.Minute).Add(time.Minute)
		switch {
		case busy:
			next = now.Add(p.opts.Pace)
		case p.opts.Idle > 0:
			next = now.Add(p.opts.Idle)
		}
		if next.Unix() <= now.Unix() {
			next = now.Add(time.Second) // reminders are in seconds, and one due now counts as unanswered
		}
		err = s.Tx(r.Context(), func(tx *sql.Tx) error {
			_, err := tx.ExecContext(r.Context(), `INSERT INTO _shardlite_reminders (key, due) VALUES ('run', ?) ON CONFLICT (key) DO UPDATE SET due = excluded.due`, next.Unix())
			return err
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

// pass lists one page of due entries and handles each, at most InFlight at once,
// for at most Budget. It reports whether the page held entries, which a next pass
// should not wait a minute for.
func (p *dispatcher) pass(ctx context.Context) (busy bool, err error) {
	p.Passes.Add(1)
	now := p.Now()
	entries, err := p.index.Due(ctx, now, p.opts.Page)
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, p.opts.Budget)
	defer cancel()
	var g errgroup.Group
	g.SetLimit(p.opts.InFlight)
	for _, e := range entries {
		if ctx.Err() != nil {
			break
		}
		g.Go(func() error {
			p.handle(ctx, e, now)
			return nil // independent: one entry failing must not stop the others
		})
	}
	g.Wait()
	return len(entries) > 0, nil
}

// handle deals with one due entry: prune it if the manifest doesn't back it and
// the rule allows, otherwise dispatch the shard if it is backed, then move it
// forward.
func (p *dispatcher) handle(ctx context.Context, e remindex.Entry, now time.Time) {
	m, version, err := p.manifests.Get(ctx, e.Shard)
	exists := err == nil
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		p.fail(ctx, e, "could not read a manifest", err)
		return
	}
	var next remindex.Minute
	if exists {
		next = m.NextReminder
	}
	if !remindex.Backs(next, e) {
		if remindex.Prunable(exists, next, version, e) {
			if err := p.index.Delete(ctx, e); err != nil && ctx.Err() == nil {
				p.Log.Warn("could not delete a reminder entry, trying again", "shard", e.Shard, "err", err)
			}
			return
		}
		p.move(ctx, e, now, backoff(e.N))
		return
	}
	step := backoff(e.N)
	answer, err := p.send(ctx, e.Shard, m)
	switch {
	case err == nil:
		switch answer {
		case Active:
			step = maxBackoff // its reminders loop covers it
		case Activated:
		case Unknown:
			// A dispatch that answers nothing is one that failed: back off
			// and try again.
			p.Failed.Add(1)
			p.Log.Warn("a dispatch answered nothing, backing off", "shard", e.Shard, "moved", e.N)
		}
	case ctx.Err() != nil:
		return // the pass's budget ended: the entry stays due for the next one
	default:
		p.Failed.Add(1)
		p.Log.Warn("could not dispatch a shard, backing off", "shard", e.Shard, "moved", e.N, "err", err)
	}
	p.move(ctx, e, now, step)
}

func backoff(n int) int { return min(1<<min(n, 5), maxBackoff) }

func (p *dispatcher) move(ctx context.Context, e remindex.Entry, now time.Time, minutes int) {
	if err := p.index.Move(ctx, e, remindex.MinuteAt(now)+remindex.Minute(minutes)); err != nil && ctx.Err() == nil {
		p.Log.Warn("could not move a reminder entry, trying again", "shard", e.Shard, "err", err)
	}
}

func (p *dispatcher) fail(ctx context.Context, e remindex.Entry, msg string, err error) {
	if ctx.Err() == nil {
		p.Log.Warn(msg+", trying again", "shard", e.Shard, "err", err)
	}
}

// send dispatches the shard to the node it belongs on, within the rate limit. If
// the owner doesn't take it (a node that stopped is still listed as alive until its
// lease is a TTL old) it goes once to a node picked for load, which claims the
// shard if the owner's lease allows it and answers that the owner has it if not.
func (p *dispatcher) send(ctx context.Context, name shard.Name, m *shardfile.Manifest) (Answer, error) {
	if err := p.pace(ctx); err != nil {
		return Unknown, err
	}
	owner := cluster.Node("")
	for range 2 {
		node, ok := p.place(m.Owner, owner)
		if !ok {
			return Unknown, errors.New("no live node to dispatch to")
		}
		p.Dispatched.Add(1)
		answer, err := p.Dispatch(ctx, node, name)
		p.inflight(node, -1)
		if err == nil || ctx.Err() != nil || node != m.Owner {
			return answer, err
		}
		owner = node // the owner didn't take it: anyone else
	}
	return Unknown, errors.New("unreachable")
}

// pace waits for the next slot at Rate dispatches a second.
func (p *dispatcher) pace(ctx context.Context) error {
	p.mu.Lock()
	now := time.Now()
	at := now
	if p.slot.After(at) {
		at = p.slot
	}
	p.slot = at.Add(time.Duration(float64(time.Second) / p.opts.Rate))
	p.mu.Unlock()
	if d := at.Sub(now); d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return nil
}

// place picks the node for a dispatch and counts it in flight there, never the
// node not names: the shard's owner if the lease scan has it alive, and otherwise the better of two live
// nodes picked at random, the one with fewer of this dispatcher's dispatches in
// flight. Lease counts are seconds old and would send a herd to one node, so
// they aren't used, and the dispatcher's own node gets no preference.
func (p *dispatcher) place(owner, not cluster.Node) (cluster.Node, bool) {
	live := slices.DeleteFunc(slices.Clone(p.Live()), func(n cluster.Node) bool { return n == not })
	p.mu.Lock()
	defer p.mu.Unlock()
	pick := cluster.Node("")
	if owner != "" && slices.Contains(live, owner) {
		pick = owner
	}
	if pick == "" {
		switch len(live) {
		case 0:
			return "", false
		case 1:
			pick = live[0]
		default:
			i, j := rand.IntN(len(live)), rand.IntN(len(live)-1)
			if j >= i {
				j++ // two different nodes
			}
			pick = live[i]
			if p.load[live[j]] < p.load[pick] {
				pick = live[j]
			}
		}
	}
	p.load[pick]++
	return pick, true
}

func (p *dispatcher) inflight(node cluster.Node, d int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.load[node] += d; p.load[node] <= 0 {
		delete(p.load, node)
	}
}
