// Package dispatch is the dispatcher, the reminder index's handler
// (docs/internals.md, Reconcilers): for each due entry it reads the shard's manifest
// and either prunes the entry by the rule or sends the shard a dispatch, a call with
// Shardlite-If-Exists to the path the node answers itself, which makes the shard
// active on its owner or claims it on a node picked for load, and moves the entry on
// by a backoff. The reconcile loop (internal/reconcile) lists the entries, paces the
// handler and writes the entry the dispatcher answers for. It keeps no state of its
// own, and two dispatchers handling one entry leave at most a duplicate entry, so a
// change of the ranking loses nothing.
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

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/dueindex"
	"github.com/luci1900/shardlite/internal/reconcile"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// maxBackoff is how far an entry moves at most, in minutes.
const maxBackoff = 16

// Deps is what the dispatcher uses of the node's.
type Deps struct {
	Store *blob.Store
	Clock reconcile.Clock
	Log   *slog.Logger
	// Call sends a request of the node's own through its routing, in process or
	// forwarded to the shard's owner, and returns the whole answer (Server.call).
	Call func(*http.Request) (*http.Response, error)
}

// New returns the dispatcher, which the shardset hands to the reconcile loop as the
// reminder index's handler.
func New(d Deps) *Dispatcher {
	return &Dispatcher{Deps: d, manifests: shardfile.NewManifestStore(d.Store)}
}

// Dispatcher wakes the inactive shards whose reminders are due.
type Dispatcher struct {
	Deps
	manifests shardfile.ManifestStore

	// Dispatched counts the dispatches sent and Failed those that failed, for tests.
	Dispatched, Failed atomic.Int64
}

// Handle deals with one due reminder: prune it if the manifest doesn't back it and
// the rule allows (done), otherwise dispatch the shard if it is backed, then move
// the entry forward. It sends nothing while the node's lease fails (the loop
// reports that), and leaves the entry for the next pass when it can't tell.
func (d *Dispatcher) Handle(ctx context.Context, e dueindex.Entry, check func() error) reconcile.Answer {
	r, err := shardfile.ParseReminder(e)
	if err != nil {
		d.Log.Warn("an entry of the reminder index is not a reminder, leaving it", "subject", e.Subject, "err", err)
		return &reconcile.Leave{}
	}
	m, version, err := d.manifests.Get(ctx, r.Shard)
	exists := err == nil
	if err != nil && !errors.Is(err, blob.ErrNotFound) {
		if ctx.Err() == nil {
			d.Log.Warn("could not read a manifest, trying again", "shard", r.Shard, "err", err)
		}
		return &reconcile.Leave{}
	}
	var next dueindex.Minute
	if exists {
		next = m.NextReminder
	}
	if !shardfile.Backs(next, r) {
		if shardfile.Prunable(exists, next, version, r) {
			return &reconcile.Done{}
		}
		return d.move(r, backoff(r.N))
	}
	step := backoff(r.N)
	if check() != nil {
		return &reconcile.Leave{}
	}
	active, err := d.send(ctx, r.Shard)
	switch {
	case err == nil:
		if active {
			step = maxBackoff // its reminders loop covers it
		}
	case ctx.Err() != nil:
		return &reconcile.Leave{} // the pass's budget ended: the entry stays due for the next one
	default:
		d.Failed.Add(1)
		d.Log.Warn("could not dispatch a shard, backing off", "shard", r.Shard, "moved", r.N, "err", err)
	}
	return d.move(r, step)
}

func backoff(n int) int { return min(1<<min(n, 5), maxBackoff) }

// move is the answer that puts the reminder back minutes on, counted one more move
// so that its key is new.
func (d *Dispatcher) move(r shardfile.Reminder, minutes int) *reconcile.Move {
	r.Minute = dueindex.MinuteAt(d.Clock.Now()) + dueindex.Minute(minutes)
	r.N++
	return &reconcile.Move{Minute: r.Minute, Subject: r.Entry().Subject}
}

// send dispatches the shard: a call that the node serves
// on the shard's owner, or on a node picked for load if nobody live owns it. It
// never creates the shard, and it has no calling shard, so nothing gates it. It
// reports whether the shard was active already (204) or the call made it active
// (200).
func (d *Dispatcher) send(ctx context.Context, name shard.Name) (active bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost"+reconcile.Activate, http.NoBody)
	if err != nil {
		return false, err
	}
	req.Header.Set(reconcile.Target, name.String())
	req.Header.Set(reconcile.IfExists, "true")
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
