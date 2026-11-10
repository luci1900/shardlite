package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/kill"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/peers"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/reconcile"
	"github.com/luci1900/shardlite/internal/reconcile/dispatch"
	"github.com/luci1900/shardlite/internal/service"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

var (
	// ErrBusy: the shard is in use (shutdown could not deactivate it).
	ErrBusy = errors.New("shard is in use")
	// ErrAppHolds: the app committed to a shard while a stop was letting go of it (a release that found a commit made during it), so it was not let go of.
	ErrAppHolds = errors.New("another process has the shard's file open")
)

// tempDirName is the directory of Data that holds the temporary files of deltas,
// snapshots and downloads. The node empties it at startup, and no kind may have
// its name.
const tempDirName = ".tmp"

// ShardsetOptions configure the shardset: what the shardset itself needs, and an options
// struct for each of its parts.
type ShardsetOptions struct {
	Kinds  *Kinds
	Data   string // directory of the active shards' files
	Logger *slog.Logger
	Clock  Clock // the time of reminders, next_reminder, reminder ages, reminder backoff, idle shards, the shards' timers, watchdog and lease scan periods, the real one if nil

	Flush     FlushOptions
	Lifecycle LifecycleOptions
	Reminders ReminderOptions // docs/guide.md, Reminders
	Leases    peers.ScanOptions

	// Dispatch, if set, runs the dispatcher, the reminder index's reconciler, which
	// wakes the shards whose reminders are due while they are inactive
	// (docs/internals.md, Reconcilers).
	Dispatch *reconcile.Options

	// OwnerCache is how many shards' owners the node remembers, 100,000 if zero
	// (docs/internals.md, Routing and forwarding).
	OwnerCache int

	limits shardLimits // the node-wide limits, the constants below if zero (tests set them)
}

// shardLimits are the node-wide limits on what shards' loops do at once.
type shardLimits struct {
	deactivate, snapshot, rebase int
}

// sems are the counting semaphores for shardLimits, one channel each: a loop
// takes a place by sending (acquireSem) and gives it back by receiving.
type sems struct {
	deactivate, snapshot, rebase chan struct{}
}

func newSems(l shardLimits) sems {
	return sems{
		deactivate: make(chan struct{}, l.deactivate),
		snapshot:   make(chan struct{}, l.snapshot),
		rebase:     make(chan struct{}, l.rebase),
	}
}

// Stats counts what the shards have done, for tests (nothing else reads them
// yet). The shardset owns them and hands them to the parts that count.
type Stats struct {
	Activations    atomic.Int64   // shards activated
	Restores       atomic.Int64   // ... by restoring from blob storage
	Creates        atomic.Int64   // shards created (activated with no manifest)
	Deletes        atomic.Int64   // shards deleted on deactivation because the app never wrote to them
	TailAlerts     atomic.Int64   // alerts raised for a tail older than the alert age
	TailBytes      atomic.Int64   // bytes of tail held by active shards, as of each one's last look of its delta timer
	TailBytesMax   atomic.Int64   // the highest TailBytes seen
	LeaseScans     notify.Counter // listings of the node leases
	Adoptions      notify.Counter // dead nodes this node finished adopting
	Watchdog       notify.Counter // passes of the watchdog (the tail alert)
	BadPeerHeaders notify.Counter // peer headers (Shardlite-Node and Shardlite-Placement) ignored because they did not read
	Unanswered     notify.Counter // forwards that failed before the owner's node answered (no connection, or no 102 within the answer timeout)
	Waits          notify.Counter // requests that found their shard being activated or let go of, and waited for it

	ReminderWakes        atomic.Int64   // wakes of apps for reminders, however they ended
	ReminderWakeFailures atomic.Int64   // wakes that failed and will be repeated after a backoff
	ChecksumMismatches   atomic.Int64   // snapshots whose checksum was not the capture's: the capture went wrong somewhere
	Placements           atomic.Int64   // claims this node sent to another node, picked at random
	LetGoHeld            atomic.Int64   // shards a stop let go of though the app had their file open (the idle path never does)
	HeldPastIdle         atomic.Int64   // holds of a shard's file by the app that outlasted an idle timeout and were warned of, once per hold (the shard stays active until the app closes it)
	LeakedFiles          notify.Counter // LTX files the node knew no manifest lists and could not delete (a failed best-effort delete, a delete in a rebase's sweep or of a deleted shard's files), and each failed read that stopped such a delete (docs/internals.md, Garbage)
}

// Shardset owns the node's shards, which are the active ones (docs/internals.md,
// Concurrency). Its parts are the table of shards and their actors, the flush loop,
// the lease scan, the watchdog and the reconcilers,
// which Serve runs. The reminders' limits are its too. Each part gets what it
// depends on passed in, and Shardset orders its own stop (Serve).
type Shardset struct {
	n     *Node
	opts  ShardsetOptions
	log   *slog.Logger
	Stats Stats

	tab         *table
	flush       *Flusher
	rem         *reminders
	leases      *peers.Scan
	watch       *watchdog
	reconcilers *reconcile.Loop       // nil unless Dispatch is set
	dispatching *reconcile.Reconciler // the reminder index's, nil unless Dispatch is set
	dispatcher  *dispatch.Dispatcher  // its handler, nil unless Dispatch is set
	deps        *actorDeps            // what every shard is given

	// owners maps a shard's name to the node that owned it when this node last
	// found out. It is a hint: a wrong entry costs a misdirected request, never
	// correctness.
	owners *lru.Cache[shard.Name, cluster.Node]
	addrs  *peers.Addresses // where the nodes listen for forwards
	placer *peers.Placer    // the peers to avoid, and the node for a claim that isn't made here
	// draining is set when the node is told to stop (Drain): readiness fails, its peers are told to place nothing on it (Shardlite-Placement: avoid) and a claim is placed on another node. The placer leaves this node out of its picks while it is set.
	draining atomic.Bool

	// heldAtStop, if set, is called each time the stop finds a shard that a request still
	// holds (the control and peer servers serve while the shards are let go of), with how
	// long that shard has been held. Run uses it to end such requests after a while.
	heldAtStop func(held time.Duration)

	cancelBase context.CancelFunc // ends deps.base
	abandon    atomic.Bool        // tests: Serve stops as a crash would, letting go of nothing
}

// NewShardset prepares the data directory and clears what a crash
// left behind. Nothing runs until Serve.
//
// newRouting is the way back into the node's own router, which the Server
// provides. Its callbacks run a request through that router in process (wake is
// Server.wakeApp and call is Server.call, both through serveInternal), so neither
// can be a client built before the Server. The router needs the shards and the
// shards need the callbacks, so newRouting is called with the shards while they
// are being built (the Server may only keep them), and what it returns is what
// the shards are built with. It is how build order is handled when two parts need
// each other, not an import cycle.
func NewShardset(n *Node, opts ShardsetOptions, newRouting func(*Shardset) routing) (*Shardset, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Clock == nil {
		opts.Clock = realClock{}
	}
	lc, fl, mo := &opts.Lifecycle, &opts.Flush, &opts.Reminders
	orDefault(&fl.Interval, 250*time.Millisecond)
	orDefault(&fl.Min, DefaultFlushMin)
	orDefault(&fl.Coalesce, DefaultFlushCoalesce)
	orDefault(&lc.DeltaAge, shardfile.DefaultDeltaAge)
	orDefault(&lc.DeltaSize, shardfile.DefaultDeltaSize)
	orDefault(&lc.MaxFiles, shardfile.DefaultMaxFiles)
	orDefault(&lc.SnapshotEvery, 2*time.Second)
	orDefault(&lc.HoldOff, 5*time.Second)
	orDefault(&lc.IdleRecheck, time.Second)
	orDefault(&opts.OwnerCache, 100_000)
	orDefault(&opts.Leases.Every, 2*time.Second)
	orDefault(&opts.Leases.TTL, lease.DefaultTTL)
	orDefault(&mo.Backoff, time.Second)
	orDefault(&mo.MaxBackoff, time.Minute)
	orDefault(&mo.MaxAge, 24*time.Hour)
	orDefault(&mo.NodeLimit, 256)
	orDefault(&opts.limits.deactivate, deactivateLimit)
	orDefault(&opts.limits.snapshot, snapshotLimit)
	orDefault(&opts.limits.rebase, rebaseLimit)
	if err := os.MkdirAll(opts.Data, 0o755); err != nil {
		return nil, err
	}
	// A restart is treated as a crash: nothing on disk from before it is trusted.
	if err := cleanDataDir(opts.Data); err != nil {
		return nil, err
	}
	m := &Shardset{n: n, opts: opts, log: opts.Logger, tab: newTable(opts.Logger)}
	n.Files.Now = opts.Clock.Now
	log, clk, st := opts.Logger, opts.Clock, &m.Stats
	n.Files.Log, n.Files.Leaked = log, &st.LeakedFiles
	m.owners, _ = lru.New[shard.Name, cluster.Node](opts.OwnerCache) // only a size below 1 fails
	rt := newRouting(m)
	m.rem = newReminders(opts.Reminders, clk, log, st, rt.wake)
	m.flush = newFlusher(m.tab, n.Writer, n.Keeper, n.Confirmer, log, opts.Flush)
	self := func() lease.Incarnation { l := n.Keeper.Lease(); return l.Incarnation() }
	ad := &adopter{reg: n.Registry, files: n.Files, store: n.Store, self: self, nodeErr: n.Err, clock: clk, log: log, adopted: &st.Adoptions}
	m.leases = &peers.Scan{
		Registry: n.Registry, Self: self, Clock: clk, Options: opts.Leases, Log: log,
		Scans: &st.LeaseScans, Adopt: ad.adopt, Died: m.died,
	}
	var rs []*reconcile.Reconciler
	if opts.Dispatch != nil {
		m.dispatcher = dispatch.New(dispatch.Deps{Store: n.Store, Clock: clk, Log: log, Call: rt.call})
		m.dispatching = &reconcile.Reconciler{Name: "dispatcher", Index: shardfile.ReminderIndex(n.Store), Handler: m.dispatcher, Options: *opts.Dispatch}
		rs = append(rs, m.dispatching)
	}
	if len(rs) > 0 {
		m.reconcilers = reconcile.New(reconcile.Deps{Clock: clk, Log: log, Mine: m.lanes, Check: n.Keeper.Check}, rs...)
	}
	m.placer = &peers.Placer{
		Self: n.Name, Draining: &m.draining, Listing: m.leases.Nodes, Clock: clk, TTL: opts.Leases.TTL, Log: log,
		Bad: &st.BadPeerHeaders, Fault: n.Faults.PeerHeader,
	}
	m.addrs = &peers.Addresses{Get: n.Registry.Get, Nodes: m.leases.Nodes}
	m.watch = &watchdog{tab: m.tab, clock: clk, every: lc.SnapshotEvery, alertAge: tailAlertAge(lc.DeltaAge), log: log, stats: st}
	var base context.Context
	base, m.cancelBase = context.WithCancel(context.Background())
	m.deps = &actorDeps{
		opts: opts.Lifecycle, kinds: opts.Kinds, clock: clk, log: log, stats: st,
		tab: m.tab, data: opts.Data, flush: m.flush, rem: m.rem,
		files: n.Files, writer: n.Writer, tempDir: n.Files.Temp,
		base: base,
		sems: newSems(opts.limits),
	}
	n.SetTails(m)
	return m, nil
}

// routing is the shards' way back into the node's own router, which the Server
// fills in because the router needs the shards. Both callbacks run a request
// through that router in process (serveInternal in origin.go, as a wake or as a
// call from the node), so there is no client to build first. Wake is the wake of
// an app for its shard's reminders (Server.wakeApp) and call is the dispatcher's
// request (Server.call).
type routing struct {
	wake wakeFunc
	call func(*http.Request) (*http.Response, error)
}

// orDefault sets an option that is zero (or negative) to its default.
func orDefault[T int | int64 | time.Duration](v *T, def T) {
	if *v <= 0 {
		*v = def
	}
}

// Serve runs the shards until ctx ends, then stops in order
// (docs/internals.md, Concurrency: Shutdown). The public server has ended by then, but the
// control and peer servers may still be serving (Run ends ctx before it stops them after a
// stop that was asked for): a request that needs a shard finds the table closed to new
// ones and is placed elsewhere or refused. First the
// lease scan and the watchdog stop, so that nothing activates any
// more; then the table is closed and every shard is let go of (released, so that the next node to need it
// claims it), with the flusher still running; then the flusher. It returns why some shard could not be let go of,
// if one couldn't. Once the node may not write any more (its lease is gone or
// its writer fenced) it lets go of none.
func (m *Shardset) Serve(ctx context.Context) error {
	flusher := service.New(context.Background(), m.log)
	flusher.Add(m.flush)
	loops := service.New(context.Background(), m.log)
	for _, s := range []service.Service{m.leases, m.watch} {
		loops.Add(s)
	}
	if m.reconcilers != nil {
		loops.Add(m.reconcilers)
	}
	<-ctx.Done()

	m.stopPart(loops)
	var err error
	if jerr := m.n.Err(); m.abandon.Load() || jerr != nil {
		// Nothing may be written once the lease is gone, and a crash lets go of
		// nothing: what the shards are doing fails, and they stay as they are.
		if jerr != nil {
			err = fmt.Errorf("not letting go of the shards: %w", jerr)
		}
		m.cancelBase()
	} else {
		// The deadline starts now, whatever the steps of the shutdown before
		// this one took.
		sctx, cancel := context.WithTimeout(context.Background(), letGoDeadline)
		m.tab.close()
		err = m.letGo(sctx)
		cancel()
	}
	tctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	m.tab.stop(tctx) // the shards that could not be let go of end their loops
	cancel()
	m.stopPart(flusher)
	m.cancelBase()
	return err
}

// stopPart stops a group of the shardset's parts, waiting for it up to
// stopTimeout (the group names a part that doesn't stop by then).
func (m *Shardset) stopPart(g *service.Group) {
	ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()
	if err := g.Stop(ctx); err != nil {
		m.log.Error("a part of the shards failed or did not stop in time", "err", err)
	}
}

// letGo lets go of every shard, at most the deactivation limit at once. Every
// shard's reminders loop stops (and a wake in progress ends) before any shard is
// let go of, so no wake holds a reference to its shard while it is let go of. A
// transient storage failure (the final sync, the delta or the release) is tried again until
// ctx ends, and so is a shard a request holds (the control and peer servers may still have
// requests in flight, which end). Other failures are not.
func (m *Shardset) letGo(ctx context.Context) error {
	var list []*actor
	m.tab.shards(func(a *actor) { list = append(list, a) }, entryActivating, entryActive, entryBusy)
	var g errgroup.Group
	g.SetLimit(m.opts.limits.deactivate)
	for _, a := range list {
		g.Go(func() error {
			select {
			case <-a.ready:
				if err := a.quiet(ctx); err != nil {
					m.log.Error("could not stop a shard's reminders at shutdown", "shard", a.Name, "err", err)
				}
			case <-a.ended: // its activation failed
			case <-ctx.Done():
			}
			return nil
		})
	}
	g.Wait()
	errs := make([]error, len(list))
	for i, a := range list {
		g.Go(func() error {
			errs[i] = m.letGoOf(ctx, a)
			return nil // independent: one shard failing must not stop the others
		})
	}
	g.Wait()
	return errors.Join(errs...)
}

func (m *Shardset) letGoOf(ctx context.Context, a *actor) error {
	wait := 100 * time.Millisecond
	var heldSince time.Time
	for {
		// A stop goes ahead over a file the app keeps open, since the app is
		// stopping too. Only a commit made while it is let go of (ErrAppHolds) is
		// tried again, as the app is plainly using the file.
		_, err := a.evict(ctx) // not done: it was let go of already
		busy, used := errors.Is(err, ErrBusy), errors.Is(err, ErrAppHolds)
		if err != nil && (busy || used || blob.Transient(err)) && ctx.Err() == nil {
			if busy {
				if heldSince.IsZero() {
					heldSince = time.Now()
				}
				if m.heldAtStop != nil {
					m.heldAtStop(time.Since(heldSince))
				}
			} else {
				m.log.Warn("could not let go of a shard at shutdown, trying again", "shard", a.Name, "err", err)
			}
			if sleep(ctx, wait) {
				wait = min(2*wait, 2*time.Second)
				continue
			}
		}
		if err != nil {
			return fmt.Errorf("%s: %w", a.Name, err)
		}
		a.d.at(kill.StopLetGo)
		return nil
	}
}

// Drain is the first step of a stop that was asked for rather than forced (docs/internals.md, Takeover). Readiness fails and the node's answers and forwards say Shardlite-Placement: avoid, so the load balancer sends it no new clients and the other nodes place nothing on it. A claim that reaches the node is placed elsewhere (acquire), so the shards the stop lets go of are claimed by the nodes that need them. With no other node to pick (none is live, or each says avoid) the node claims for itself, since there is nowhere else to send the request.
func (m *Shardset) Drain() {
	m.draining.Store(true)
}

// Hold keeps an active shard active until it is released. A request holds one
// while it runs, and a WebSocket, a stream or a streaming call for as long as it
// is open.
type Hold struct {
	m    *Shardset
	a    *actor
	once sync.Once
	// proven is set once a storage chunk has passed the gate: the node then
	// knew it still owned its shards after that chunk was read, which is after
	// the request began, and the shard's file only grows. Later chunks of the
	// request have nothing more to prove, only to wait for their own commits.
	proven atomic.Bool
	// started is set when the acquisition that made this hold started the
	// shard's entry, rather than finding one: a shard activated for this request.
	started bool
}

// Path is the opaque path of the shard's file, for Shardlite-File.
func (h *Hold) Path() string { return h.a.Path }

// Sync is the shard's kind's durability tier: where its output waits for the
// commits before it to be durable.
func (h *Hold) Sync() kind.Sync {
	k, err := h.m.opts.Kinds.Lookup(h.a.Name.Kind())
	if err != nil {
		return kind.SyncStorage
	}
	return k.Sync
}

// Release gives up the hold.
func (h *Hold) Release() {
	h.once.Do(func() { h.a.e.put(h.m.now()) })
}

// Acquire activates the shard if it is not active and holds it. However many
// requests ask at once, the shard is activated once: the first adds its entry
// and the others wait for it. The activation runs in the shard's own loop, so
// one caller giving up doesn't fail the others. A shard another node owns is not
// activated at all: the manifest is read before the entry is added, and the
// owner is remembered (*shardfile.OwnedElsewhereError).
func (m *Shardset) Acquire(ctx context.Context, n shard.Name) (*Hold, error) {
	return m.take(ctx, n, acquireOpts{})
}

// acquireOpts say how a request acquires a shard.
type acquireOpts struct {
	// existing: a shard with no manifest is not created (blob.ErrNotFound).
	existing bool
	// anywhere: a shard that is not active here and that nobody else owns
	// is claimed on a node picked at random, which may not be this one. The error is
	// then a *placedError naming it. The request that asks is a call from a shard
	// (docs/internals.md, Activation), never one that has been placed already.
	anywhere bool
	// sendBack, if set, is the node that forwarded the request for a client, which has the
	// client's session: a shard that is not active here and whose manifest names
	// no owner (whoever had it let it go, and the forwarding node's owner cache still names that node) is claimed there, so the error is a *placedError naming it.
	sendBack cluster.Node
}

// placedError says an acquisition belongs on another node: it was placed there
// (acquireOpts.anywhere, or any claim while the node drains). The request goes there
// as a forward, and the node claims the shard.
type placedError struct {
	Shard shard.Name
	Node  cluster.Node
}

func (e *placedError) Error() string { return fmt.Sprintf("%s is placed on %s", e.Shard, e.Node) }

// forwardTo is the node a request goes to when acquiring its shard failed because
// another node owns it (*shardfile.OwnedElsewhereError) or it was placed there
// (*placedError).
func forwardTo(err error) (cluster.Node, bool) {
	if oe, ok := errors.AsType[*shardfile.OwnedElsewhereError](err); ok {
		return oe.Owner, true
	}
	if pe, ok := errors.AsType[*placedError](err); ok {
		return pe.Node, true
	}
	return "", false
}

func (m *Shardset) take(ctx context.Context, n shard.Name, o acquireOpts) (*Hold, error) {
	if _, err := m.opts.Kinds.Lookup(n.Kind()); err != nil {
		return nil, err
	}
	hold, err := m.acquire(ctx, n, o)
	// The next call for the shard goes straight to its owner or the node picked,
	// with no manifest read. A wrong entry costs a 421 and a retry.
	if node, ok := forwardTo(err); ok && node != o.sendBack {
		m.owners.Add(n, node)
	}
	return hold, err
}

func (m *Shardset) acquire(ctx context.Context, n shard.Name, o acquireOpts) (*Hold, error) {
	for {
		e := m.tab.load(n)
		started := false
		if e == nil {
			if err := m.n.Keeper.Check(); err != nil {
				return nil, err
			}
			read, err := m.readManifest(ctx, n, o.existing)
			if err != nil {
				return nil, err
			}
			if o.sendBack != "" && read != nil && read.Manifest.Owner == "" {
				return nil, &placedError{Shard: n, Node: o.sendBack} // whoever had it let it go (the forwarding node's owner cache is stale), so it is not a shard this node owns and lost
			}
			// Only a shard that is not active here, and that no other live node owns (readManifest has failed for those), is placed. While the node drains every claim is placed, whatever its origin. The placer never picks this node while it drains, so with nowhere else to go the node claims the shard itself.
			if o.anywhere || m.draining.Load() {
				if node := m.placer.Place(); node != m.n.Name {
					m.Stats.Placements.Add(1)
					return nil, &placedError{Shard: n, Node: node}
				}
			}
			m.owners.Remove(n) // this node claims it now
			e, started = m.tab.start(n, func(e *entry) *actor {
				a := newActor(m.deps, e)
				a.read = read
				return a
			})
		}
		// The failure of an activation this request waited for is its own. One that
		// had failed before it looked is somebody else's.
		waited := false
		ok, wait, err := e.take(m.now())
		if wait != nil {
			m.Stats.Waits.Inc()
		}
		for wait != nil {
			waited = true
			select {
			case <-wait:
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			}
			ok, wait, err = e.take(m.now())
		}
		switch {
		case ok:
			return &Hold{m: m, a: e.a, started: started}, nil
		case err != nil && waited && errors.Is(err, blob.ErrConflict):
			return nil, m.lostClaim(ctx, n, err)
		case err != nil && (waited || errors.Is(err, errShardsStopped)):
			return nil, err
		}
		m.tab.forget(e) // gone: look again
	}
}

// lostClaim is what a request answers when the activation it waited for lost its claim to another node's (the compare-and-swap on the manifest conflicted, so somebody changed it first). The app never saw the request, so it is not a failure: the manifest is read again, and if the winner owns the shard the request goes to it like any request for a shard another node owns. If the manifest does not show an owner yet (it changed to something else) the client is told to ask again at once.
func (m *Shardset) lostClaim(ctx context.Context, n shard.Name, lost error) error {
	if _, err := m.readManifest(ctx, n, false); err != nil {
		return err
	}
	return &unavailable{reasonMoved, fmt.Sprintf("%s was claimed by another node meanwhile: %v", n, lost), 0}
}

// readManifest reads the manifest of a shard that has no entry, for the
// activation to start from, and fails if another node owns the shard. The
// handle is nil for a shard with no manifest yet, which is
// blob.ErrNotFound if it must exist.
func (m *Shardset) readManifest(ctx context.Context, n shard.Name, existing bool) (*shardfile.Handle, error) {
	h, err := m.n.Files.Attach(ctx, n)
	if errors.Is(err, blob.ErrNotFound) && !existing {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return h, h.CheckClaimable(ctx)
}

// tryHold takes a hold on an active shard, nil if it isn't (or is being let go of).
func (m *Shardset) tryHold(n shard.Name) *Hold {
	e := m.tab.load(n)
	if e == nil {
		return nil
	}
	if ok, _, _ := e.take(m.now()); !ok {
		return nil
	}
	return &Hold{m: m, a: e.a}
}

// IsActive reports whether the shard is active (for Shardlite-If-Active).
func (m *Shardset) IsActive(n shard.Name) bool {
	e := m.tab.load(n)
	return e != nil && e.is(entryActive, entryBusy)
}

// Tails is what the node's lease publishes as the journal range's low end: the
// lowest segment the captured shards' tails still need (0 if none does).
func (m *Shardset) Tails() pos.Seg {
	var from pos.Seg
	m.tab.entries.Range(func(_ shard.Name, e *entry) bool {
		if sh := e.a.cap.lease.Load(); sh != nil {
			if f := sh.TailFrom(); f != 0 && (from == 0 || f < from) {
				from = f
			}
		}
		return true
	})
	return from
}

// deactivateLimit is how many shards are deactivated at once. Each mostly
// waits: for a flush cycle (shared by all of them), then for a delta and a
// manifest write in blob storage, about half a second in all. At 8 a node could
// only let go of about 20 shards a second, less than players leave after a
// launch evening, and idle shards keep their descriptors and memory meanwhile.
const deactivateLimit = 256

// snapshotLimit is how many deltas and rebases run at once. Each is a few blob
// storage requests (about 100 ms) and a little CPU. Every active shard needs a
// delta every few minutes: 10,000 active shards need about 55 a second, so the
// limit has to allow well over that.
const snapshotLimit = 64

// rebaseLimit is how many rebases run at once on the node: each writes (and
// uploads) a file the size of the shard's database in the data directory.
const rebaseLimit = 4

// Active returns the names of the active shards.
func (m *Shardset) Active() []shard.Name {
	var out []shard.Name
	m.tab.shards(func(a *actor) { out = append(out, a.Name) }, entryActive, entryBusy)
	return out
}

// Nodes returns the lease scan's last listing of the cluster's nodes, nil
// before the first.
func (m *Shardset) Nodes() *peers.List { return m.leases.Nodes() }

// died is told each node the lease scan first finds dead. Every entry of the
// owner cache that names it goes: its shards are claimed elsewhere or here, and
// a request for one goes through Acquire rather than to a node that can't answer
// or no longer owns it. It walks the owner cache once, which a dead node is rare
// enough to afford.
func (m *Shardset) died(node cluster.Node) {
	for _, n := range m.owners.Keys() {
		if owner, ok := m.owners.Peek(n); ok && owner == node {
			m.owners.Remove(n)
		}
	}
}

// isLive reports whether the lease scan's last listing has node and doesn't
// count it as dead.
func (m *Shardset) isLive(node cluster.Node) bool {
	return slices.Contains(m.leases.Nodes().Live(), node)
}

// lanes is the lanes this node runs the reconcilers for: the ones it ranks first for among the nodes of the lease scan's last listing (reconcile.Mine), and whether there has been a listing yet.
func (m *Shardset) lanes() (lanes []shard.Lane, listed bool) {
	return reconcile.Mine(m.n.Name, m.leases.Nodes().Live()), m.leases.Nodes() != nil
}

// Flusher is the flush loop, for the gates.
func (m *Shardset) Flusher() *Flusher { return m.flush }
