// Package dispatch is the dispatcher (docs/internals.md, The dispatcher): the service
// that wakes the inactive shards whose reminders are due. It lists the due entries
// of the lanes the node ranks first for (the ranking is in runner.go), and for each
// reads the shard's manifest and either prunes the entry by the rule or sends the
// shard a dispatch, a call with Shardlite-If-Exists to the path the node answers
// itself, which makes the shard active on its owner or claims it on a node picked
// for load, and moves the entry on by a backoff. It keeps no state of its own besides
// its counters, and two dispatchers handling one entry leave at most a duplicate
// entry, so a change of the ranking loses nothing.
package dispatch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// maxBackoff is how far an entry moves at most, in minutes.
const maxBackoff = 16

// The limits of a pass.
const (
	// maxRate is how many entries a second a pass handles at most.
	maxRate = 1000
	// maxInFlight is how many entries a pass handles at once.
	maxInFlight = 256
	// pageSize is how many due entries a pass lists over all its lanes together.
	pageSize = 1000
	// budget is how long a pass runs before it stops and leaves the rest to the next.
	budget = 25 * time.Second
)

// Options are the dispatcher's own. Zero values take the defaults. They are the
// node's own: each node runs its lanes at its own pace.
type Options struct {
	// Pace is the wait before the next pass when this one found due entries, when
	// the node has no listing of the cluster yet and before a try again after a
	// failed pass (doubling up to a minute), 1 s if zero.
	Pace time.Duration
	// Every is how often the dispatcher passes when nothing is due: at the next
	// multiple of Every on the clock (counted from midnight UTC), so a minute is the
	// start of each minute, when a minute's entries fall due. A minute if zero.
	Every time.Duration
}

func (o Options) withDefaults() Options {
	if o.Pace <= 0 {
		o.Pace = time.Second
	}
	if o.Every <= 0 {
		o.Every = time.Minute
	}
	return o
}

// Deps is what the dispatcher uses of the node's.
type Deps struct {
	Store *blob.Store
	Clock Clock
	Log   *slog.Logger
	// Call sends a request of the node's own through its routing, in process or
	// forwarded to the shard's owner, and returns the whole answer (Server.call).
	Call func(*http.Request) (*http.Response, error)
	// Mine is the lanes this node ranks first for in the lease scan's last listing
	// (Mine), and whether there has been a listing yet. It is asked at the start of
	// each pass, so a change in the listing takes effect at the next one. A node with
	// a listing may rank first for no lane, as some nodes do in a cluster of more
	// nodes than lanes.
	Mine func() (lanes []shard.Lane, listed bool)
	// Check is the node's lease check (Keeper.Check): the dispatcher asks it before
	// each send, delete or move of its own, and does none while it fails.
	Check func() error
}

// New returns the dispatcher over the reminder index.
func New(d Deps, o Options) *Dispatcher {
	return &Dispatcher{Deps: d, opts: o.withDefaults(), index: shardfile.NewReminderIndex(d.Store), manifests: shardfile.NewManifestStore(d.Store)}
}

// Dispatcher wakes the inactive shards whose reminders are due. The shardset runs
// it as a service on every node.
type Dispatcher struct {
	Deps
	opts      Options
	index     shardfile.ReminderIndex
	manifests shardfile.ManifestStore

	// Passes counts the passes made (once each has ended and its next timer is set),
	// for tests.
	Passes notify.Counter
	// Dispatched counts the dispatches sent and Failed those that failed, for tests.
	Dispatched, Failed atomic.Int64
}

func (*Dispatcher) String() string { return "dispatcher" }

// Serve makes a pass at once and then as each pass's wait ends, until ctx ends. A
// pass that lists nothing waits for the next Every, one that found entries or came
// before the node's first listing waits Pace, and one that failed waits Pace and
// then longer, up to a minute.
func (d *Dispatcher) Serve(ctx context.Context) error {
	var retry time.Duration
	for {
		var wait time.Duration
		lanes, listed := d.Mine()
		busy, err := d.pass(ctx, lanes)
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			d.Log.Warn("a pass failed, trying again", "err", err)
			retry = min(max(2*retry, d.opts.Pace), time.Minute)
			wait = retry
		case busy || !listed:
			retry, wait = 0, d.opts.Pace
		default:
			now := d.Clock.Now()
			retry, wait = 0, now.Truncate(d.opts.Every).Add(d.opts.Every).Sub(now)
		}
		t, stop := d.Clock.NewTimer(wait)
		d.Passes.Inc() // once the next pass's timer is set, so a test moving the clock after it reaches that pass
		select {
		case <-ctx.Done():
			stop()
			return nil
		case <-t:
		}
	}
}

// pass lists one page of the due entries of lanes (the lanes in turn until the page
// is full) and handles each, at most maxInFlight at once, at maxRate and for at most
// the budget. It reports whether the page held entries, which a next pass should not
// wait for, and fails if the node's lease does at its end.
func (d *Dispatcher) pass(ctx context.Context, lanes []shard.Lane) (busy bool, err error) {
	now := d.Clock.Now()
	var entries []shardfile.Reminder
	for _, lane := range lanes {
		left := pageSize - len(entries)
		if left <= 0 {
			break
		}
		got, err := d.index.Due(ctx, lane, now, left)
		if err != nil {
			return false, err
		}
		entries = append(entries, got...)
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	lim := rate.NewLimiter(rate.Limit(maxRate), 1)
	var g errgroup.Group
	g.SetLimit(maxInFlight)
	for _, e := range entries {
		if lim.Wait(ctx) != nil {
			break // the budget ended, or would before the next slot
		}
		g.Go(func() error {
			if ctx.Err() != nil {
				return nil // the budget ended while this one waited for a place
			}
			d.handle(ctx, e)
			return nil // independent: one entry failing must not stop the others
		})
	}
	g.Wait()
	if err := d.Check(); err != nil {
		return len(entries) > 0, fmt.Errorf("the node's lease: %w", err)
	}
	return len(entries) > 0, nil
}

// handle deals with one due reminder. It reads the manifest, never trusting the
// entry: it prunes the entry if the manifest doesn't back it and the rule allows,
// otherwise dispatches the shard if it is backed, then moves the entry forward. It
// sends, deletes and moves nothing while the node's lease fails (pass reports
// that), and leaves the entry for the next pass when it can't tell or the pass's
// budget ends. A failed write leaves the entry as it was, to be handled again.
func (d *Dispatcher) handle(ctx context.Context, r shardfile.Reminder) {
	m, version, err := d.manifests.Get(ctx, r.Shard)
	exists := err == nil
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		if ctx.Err() == nil {
			d.Log.Warn("could not read a manifest, trying again", "shard", r.Shard, "err", err)
		}
		return
	}
	var next shardfile.Minute
	if exists {
		next = m.NextReminder
	}
	if !shardfile.Backs(next, r) {
		if shardfile.Prunable(exists, next, version, r) {
			d.write(ctx, r, func() error { return d.index.Delete(ctx, r) })
			return
		}
		d.move(ctx, r, backoff(r.N))
		return
	}
	step := backoff(r.N)
	if d.Check() != nil {
		return
	}
	active, err := d.send(ctx, r.Shard)
	switch {
	case err == nil:
		if active {
			step = maxBackoff // its reminders loop covers it
		}
	case ctx.Err() != nil:
		return // the pass's budget ended: the entry stays due for the next one
	default:
		d.Failed.Add(1)
		d.Log.Warn("could not dispatch a shard, backing off", "shard", r.Shard, "moved", r.N, "err", err)
	}
	d.move(ctx, r, step)
}

func backoff(n int) int { return min(1<<min(n, 5), maxBackoff) }

// move puts the reminder back minutes on, counted one more move so that its key is
// new.
func (d *Dispatcher) move(ctx context.Context, r shardfile.Reminder, minutes int) {
	next := r
	next.Minute = shardfile.MinuteAt(d.Clock.Now()) + shardfile.Minute(minutes)
	next.N++
	d.write(ctx, r, func() error { return d.index.Move(ctx, r, next) })
}

// write does a delete or a move of the reminder r, unless the node's lease fails.
func (d *Dispatcher) write(ctx context.Context, r shardfile.Reminder, do func() error) {
	if d.Check() != nil {
		return
	}
	if err := do(); err != nil && ctx.Err() == nil {
		d.Log.Warn("could not update an entry, trying again", "entry", r.Key(), "err", err)
	}
}

// send dispatches the shard: a call that the node serves
// on the shard's owner, or on a node picked for load if nobody live owns it. It
// never creates the shard, and it has no calling shard, so nothing gates it. It
// reports whether the shard was active already (204) or the call made it active
// (200).
func (d *Dispatcher) send(ctx context.Context, name shard.Name) (active bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost"+Activate, http.NoBody)
	if err != nil {
		return false, err
	}
	req.Header.Set(Target, name.String())
	req.Header.Set(IfExists, "true")
	d.Dispatched.Add(1)
	resp, err := d.Call(req)
	if err != nil {
		return false, fmt.Errorf("dispatching %s: %w", name, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return false, nil
	case http.StatusNoContent:
		return true, nil
	}
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
	return false, fmt.Errorf("dispatching %s: %d %s", name, resp.StatusCode, bytes.TrimSpace(msg))
}
