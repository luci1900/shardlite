package reconcile

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/luci1900/shardlite/internal/dueindex"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/shard"
)

// Answer is what a handler says becomes of the entry it was given: Done, Move or
// Leave. The loop applies it, so the lease check before an entry's delete or move
// and the writes themselves are in one place.
type Answer interface{ answer() }

// Done says the entry has been dealt with: the loop deletes it.
type Done struct{}

// Move says the entry stays but later: the loop creates the entry at Minute under
// Subject (in the same lane) and deletes the old one. The new entry must have
// another key than the old one, so a reminder's subject carries its move count and a
// move names the next.
type Move struct {
	Minute  dueindex.Minute
	Subject string
}

// Leave says the entry stays due as it is, for the next pass: the handler could not
// finish (the pass's budget ran out, or a read failed) and wrote nothing.
type Leave struct{}

func (*Done) answer()  {}
func (*Move) answer()  {}
func (*Leave) answer() {}

// Handler deals with one due entry of a reconciler's index. It reads the truth the
// entry points at and converges on it, so an entry handled twice, late or by two
// nodes at once is harmless, and answers what becomes of the entry. check is the
// node's lease check (Keeper.Check): the handler asks it before each delete or send
// of its own, and does none while it fails. A handler runs for at most the pass's
// budget (ctx) and must stop when it ends.
type Handler interface {
	Handle(ctx context.Context, e dueindex.Entry, check func() error) Answer
}

// Options are a reconciler's own. Zero values take the defaults. They are the
// node's own: each node runs its lanes at its own rate.
type Options struct {
	// Rate is how many entries a second a pass hands to the handler at most, 1,000
	// if zero.
	Rate float64
	// InFlight is how many entries a pass handles at once, 256 if zero.
	InFlight int
	// Page is how many due entries a pass lists over all its lanes together, 1,000
	// if zero.
	Page int
	// Budget is how long a pass runs before it stops and leaves the rest to the
	// next, 25 s if zero.
	Budget time.Duration
	// Pace is the wait before the next pass when this one found due entries, when
	// the node has no listing of the cluster yet and before a try again after a
	// failed pass (doubling up to a minute), 1 s if zero.
	Pace time.Duration
	// Every is how often the reconciler passes when nothing is due: at the next
	// multiple of Every on the clock (counted from midnight UTC), so a minute is the
	// start of each minute, when a minute's entries fall due. A minute if zero.
	Every time.Duration
}

func (o Options) withDefaults() Options {
	if o.Rate <= 0 {
		o.Rate = 1000
	}
	if o.InFlight <= 0 {
		o.InFlight = 256
	}
	if o.Page <= 0 {
		o.Page = 1000
	}
	if o.Budget <= 0 {
		o.Budget = 25 * time.Second
	}
	if o.Pace <= 0 {
		o.Pace = time.Second
	}
	if o.Every <= 0 {
		o.Every = time.Minute
	}
	return o
}

// Reconciler is a due index and the handler for its entries, with its own options.
type Reconciler struct {
	// Name is the reconciler's name in logs.
	Name    string
	Index   dueindex.Index
	Handler Handler
	Options Options

	// Passes counts the passes made (once each has ended and its next timer is set),
	// for tests.
	Passes notify.Counter
}

// Deps is what the loop uses of the node's.
type Deps struct {
	Clock Clock
	Log   *slog.Logger
	// Mine is the lanes this node ranks first for in the lease scan's last listing
	// (Mine), and whether there has been a listing yet. It is asked at the start of
	// each pass, so a change in the listing takes effect at the next one. A node with
	// a listing may rank first for no lane, as some nodes do in a cluster of more
	// nodes than lanes.
	Mine func() (lanes []shard.Lane, listed bool)
	// Check is the node's lease check (Keeper.Check): the loop asks it before it
	// deletes or moves an entry, and writes nothing while it fails.
	Check func() error
}

// Loop is the reconcile loop, which the shardset runs as a service on every node. It
// keeps no state of its own besides its counters, and two loops handling one entry
// leave at most a duplicate entry, so a change of the ranking loses nothing.
type Loop struct {
	Deps
	runs []*run
}

// run is a reconciler with its options filled in.
type run struct {
	*Reconciler
	opts Options
}

// New returns the loop over reconcilers, each with its own passes.
func New(d Deps, rs ...*Reconciler) *Loop {
	l := &Loop{Deps: d}
	for _, r := range rs {
		l.runs = append(l.runs, &run{Reconciler: r, opts: r.Options.withDefaults()})
	}
	return l
}

func (*Loop) String() string { return "reconcilers" }

// Serve makes a pass of each reconciler at once and then as each pass's wait ends,
// until ctx ends, each reconciler on its own. A pass that lists nothing waits for
// the reconciler's next Every, one that found entries or came before the node's
// first listing waits Pace, and one that failed waits Pace and then longer, up to a
// minute.
func (l *Loop) Serve(ctx context.Context) error {
	var g errgroup.Group
	for _, r := range l.runs {
		g.Go(func() error {
			l.serve(ctx, r)
			return nil
		})
	}
	return g.Wait()
}

func (l *Loop) serve(ctx context.Context, r *run) {
	var retry time.Duration
	for {
		var wait time.Duration
		lanes, listed := l.Mine()
		busy, err := l.pass(ctx, r, lanes)
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			l.Log.Warn("a pass failed, trying again", "reconciler", r.Name, "err", err)
			retry = min(max(2*retry, r.opts.Pace), time.Minute)
			wait = retry
		case busy || !listed:
			retry, wait = 0, r.opts.Pace
		default:
			now := l.Clock.Now()
			retry, wait = 0, now.Truncate(r.opts.Every).Add(r.opts.Every).Sub(now)
		}
		t, stop := l.Clock.NewTimer(wait)
		r.Passes.Inc() // once the next pass's timer is set, so a test moving the clock after it reaches that pass
		select {
		case <-ctx.Done():
			stop()
			return
		case <-t:
		}
	}
}

// pass lists one page of the due entries of lanes (the lanes in turn until the page
// is full) and hands each to the handler, at most InFlight at once, at Rate and for
// at most Budget, then applies the answers. It reports whether the page held
// entries, which a next pass should not wait for.
func (l *Loop) pass(ctx context.Context, r *run, lanes []shard.Lane) (busy bool, err error) {
	now := l.Clock.Now()
	var entries []dueindex.Entry
	for _, lane := range lanes {
		left := r.opts.Page - len(entries)
		if left <= 0 {
			break
		}
		got, err := r.Index.Due(ctx, lane, now, left)
		if err != nil {
			return false, err
		}
		entries = append(entries, got...)
	}
	ctx, cancel := context.WithTimeout(ctx, r.opts.Budget)
	defer cancel()
	lim := rate.NewLimiter(rate.Limit(r.opts.Rate), 1)
	var g errgroup.Group
	g.SetLimit(r.opts.InFlight)
	for _, e := range entries {
		if lim.Wait(ctx) != nil {
			break // the budget ended, or would before the next slot
		}
		g.Go(func() error {
			if ctx.Err() != nil {
				return nil // the budget ended while this one waited for a place
			}
			l.apply(ctx, r, e, r.Handler.Handle(ctx, e, l.Check))
			return nil // independent: one entry failing must not stop the others
		})
	}
	g.Wait()
	if err := l.Check(); err != nil {
		return len(entries) > 0, fmt.Errorf("the node's lease: %w", err)
	}
	return len(entries) > 0, nil
}

// apply does what the handler answered, unless the node's lease fails (pass reports
// that). A failed write leaves the entry as it was, to be handled again.
func (l *Loop) apply(ctx context.Context, r *run, e dueindex.Entry, a Answer) {
	var err error
	switch a := a.(type) {
	case *Done:
		if l.Check() != nil {
			return
		}
		err = r.Index.Delete(ctx, e)
	case *Move:
		if l.Check() != nil {
			return
		}
		err = r.Index.Move(ctx, e, a.Minute, a.Subject)
	case *Leave:
	}
	if err != nil && ctx.Err() == nil {
		l.Log.Warn("could not update an entry, trying again", "reconciler", r.Name, "subject", e.Subject, "err", err)
	}
}
