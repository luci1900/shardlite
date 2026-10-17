package node

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// errShardsStopped is what a request gets once the shards have stopped.
var errShardsStopped = errors.New("node: the shards have stopped")

// table maps each shard's name to its entry (docs/concurrency.md, The tree), and
// runs the shards' loops until it stops. Each loop runs its shard's messages
// owner. Nothing locks the whole table: an activation adds its entry with LoadOrStore,
// and whoever finds one already there waits for it; removal marks the entry
// gone under the entry's own mutex, then deletes it with CompareAndDelete, so it
// can only remove the entry it means to. A request that found an entry just
// before it was marked gone looks the name up again.
type table struct {
	entries syncMap[string, *entry]
	log     *slog.Logger

	// The shards' loops run on ctx until stop. mu guards running and stopped, so
	// that no loop starts once stop has begun.
	ctx     context.Context
	cancel  context.CancelFunc
	loops   errgroup.Group
	mu      sync.Mutex
	stopped bool
	running map[*shard]struct{}
}

func newTable(log *slog.Logger) *table {
	ctx, cancel := context.WithCancel(context.Background())
	return &table{log: log, ctx: ctx, cancel: cancel, running: map[*shard]struct{}{}}
}

// run starts a shard's loop, unless the table has stopped.
func (t *table) run(a *shard) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return false
	}
	t.running[a] = struct{}{}
	t.loops.Go(func() error {
		a.Serve(t.ctx)
		t.mu.Lock()
		delete(t.running, a)
		t.mu.Unlock()
		return nil
	})
	return true
}

// stop ends the loops of the shards still running and waits for them until ctx
// ends. A loop still running then is logged by name and left behind.
func (t *table) stop(ctx context.Context) {
	t.mu.Lock()
	t.stopped = true
	list := slices.Collect(maps.Keys(t.running))
	t.mu.Unlock()
	t.cancel()
	for _, a := range list {
		select {
		case <-a.ended:
		case <-ctx.Done():
		}
		select {
		case <-a.ended:
		default:
			t.log.Error("a shard did not stop in time", "shard", a.Name)
		}
	}
}

// entryState is where an entry is in its life.
type entryState int

const (
	entryActivating entryState = iota // the shard's loop is activating it: requests wait
	entryActive                       // requests take references
	entryBusy                         // being let go of or deleted, or a cached shard being evicted or deleted: requests wait
	entryStopped                      // the shard's loop ended without letting go of it (the shards stopped): requests fail
	entryGone                         // removed: whoever finds it looks the name up again
)

// entry is one name in the table: the shard's state and its references, under
// its own mutex. Requests take and release references here, never through the
// shard's loop, so a delta or a release never delays them.
type entry struct {
	name string
	sh   *shard // the shard, nil for the entry of a cached shard being evicted or deleted; set before the entry is in the table

	mu      sync.Mutex
	state   entryState
	refs    int
	lastUse time.Time
	err     error         // why the activation failed, once gone
	changed chan struct{} // closed and replaced whenever state changes
}

func newEntry(name string, state entryState) *entry {
	return &entry{name: name, state: state, changed: make(chan struct{})}
}

// set changes the state and wakes whoever waits for a change. The caller holds mu.
func (e *entry) set(s entryState) {
	e.state = s
	close(e.changed)
	e.changed = make(chan struct{})
}

// take takes a reference if the shard is active (ok). Otherwise wait is closed
// when the entry changes, or nil if the entry is gone or stopped, with err
// saying why its activation failed (if it did) or that the shards stopped.
func (e *entry) take(now time.Time) (ok bool, wait <-chan struct{}, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch e.state {
	case entryActive:
		e.refs++
		e.lastUse = now
		return true, nil, nil
	case entryGone:
		return false, nil, e.err
	case entryStopped:
		return false, nil, errShardsStopped
	}
	return false, e.changed, nil
}

// put gives a reference back.
func (e *entry) put(now time.Time) {
	e.mu.Lock()
	e.refs--
	e.lastUse = now
	e.mu.Unlock()
}

// use is the number of references and the time of the last use.
func (e *entry) use() (refs int, last time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.refs, e.lastUse
}

func (e *entry) is(states ...entryState) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range states {
		if e.state == s {
			return true
		}
	}
	return false
}

// activated makes the entry active, counting its activation as a use.
func (e *entry) activated(now time.Time) {
	e.mu.Lock()
	e.lastUse = now
	e.set(entryActive)
	e.mu.Unlock()
}

// close makes an active entry busy if nobody holds it, so that its shard can be
// let go of or deleted. It reports false, changing nothing, if a request holds
// it.
func (e *entry) close() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != entryActive || e.refs > 0 {
		return false
	}
	e.set(entryBusy)
	return true
}

// reopen makes a busy entry active again: the shard was not let go of.
func (e *entry) reopen() {
	e.mu.Lock()
	e.set(entryActive)
	e.mu.Unlock()
}

// stop marks the entry of a shard whose loop ended without letting go of it.
func (e *entry) stop() {
	e.mu.Lock()
	e.set(entryStopped)
	e.mu.Unlock()
}

func (t *table) load(name string) *entry {
	e, _ := t.entries.Load(name)
	return e
}

// start adds an entry for name and starts its shard (made by mk), unless the
// name has an entry already, which it returns instead.
func (t *table) start(name string, mk func(*entry) *shard) *entry {
	e := newEntry(name, entryActivating)
	e.sh = mk(e)
	if had, loaded := t.entries.LoadOrStore(name, e); loaded {
		return had
	}
	if !t.run(e.sh) {
		t.remove(e, errShardsStopped)
	}
	return e
}

// claim adds a busy entry with no shard for name, for an operation on a shard
// that isn't active (eviction, the delete of a cached shard, the sweep of a file
// from before a restart). It reports false if the name has an entry already.
// The caller removes the entry when it is done.
func (t *table) claim(name string) (*entry, bool) {
	e := newEntry(name, entryBusy)
	if _, loaded := t.entries.LoadOrStore(name, e); loaded {
		return nil, false
	}
	return e, true
}

// remove marks the entry gone, then deletes it from the table (if it is still
// the one there). err is why its activation failed, nil otherwise.
func (t *table) remove(e *entry, err error) {
	e.mu.Lock()
	e.err = err
	e.set(entryGone)
	e.mu.Unlock()
	t.entries.CompareAndDelete(e.name, e)
}

// forget deletes an entry that is gone, if it is still there: whoever finds a
// gone entry helps its removal along before looking the name up again.
func (t *table) forget(e *entry) { t.entries.CompareAndDelete(e.name, e) }

// shards calls fn for the shard of every entry in one of the given states.
func (t *table) shards(fn func(*shard), states ...entryState) {
	t.entries.Range(func(_ string, e *entry) bool {
		if e.sh != nil && e.is(states...) {
			fn(e.sh)
		}
		return true
	})
}
