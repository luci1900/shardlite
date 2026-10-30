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
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/service"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/system/dispatch"
)

var (
	// ErrBusy: the shard is in use (shutdown could not deactivate it).
	ErrBusy = errors.New("shard is in use")
	// ErrAppHolds: another process has the shard's file open, which the node never lets go of or takes over.
	ErrAppHolds = errors.New("another process has the shard's file open")
	// ErrNotHere: the shard is neither active nor cached on this node.
	ErrNotHere = errors.New("shard is not on this node")
)

// tempDirName is the directory of Data that holds the temporary files of deltas,
// snapshots and downloads. The node empties it at startup, and no kind may have
// its name.
const tempDirName = ".tmp"

// CatalogOptions configure Shards: what Shards itself needs, and an options
// struct for each of its parts.
type CatalogOptions struct {
	Kinds  *Kinds
	Data   string // directory of shard files, which is the local cache
	Logger *slog.Logger
	Clock  Clock // the time of reminders, next_reminder, reminder ages, reminder backoff, idle shards, the shards' timers and the cache, watchdog and lease scan periods, the real one if nil

	Cache     CacheOptions
	Flush     FlushOptions
	Lifecycle LifecycleOptions
	Reminders ReminderOptions // docs/guide.md, Reminders
	Leases    LeaseScanOptions
	Claims    ClaimOptions

	// Dispatch, if set, adds the _dispatch system kind that wakes the shards whose
	// reminders are due while they are inactive (docs/internals.md, System kinds).
	Dispatch *dispatch.Options

	// System are the system kinds the node runs (docs/internals.md, System kinds).
	System []kind.Kind

	// OwnerCache is how many shards' owners the node remembers, 100,000 if zero
	// (docs/internals.md, Routing and forwarding).
	OwnerCache int
	// OwnerCheck is how long a read of an unreachable owner's lease answers the
	// requests for its shards before it is read again, 1 s if zero.
	OwnerCheck time.Duration

	limits shardLimits // the node-wide limits, the constants below if zero (tests set them)
}

// shardLimits are the node-wide limits on what shards' loops do at once.
type shardLimits struct {
	deactivate, snapshot, rebase int
}

// Stats counts what the shards have done, for tests (nothing else reads them
// yet). The Catalog owns them and hands them to the parts that count.
type Stats struct {
	Activations  atomic.Int64   // shards activated
	Restores     atomic.Int64   // ... by restoring from blob storage
	CachedHits   atomic.Int64   // ... from a cached file
	Creates      atomic.Int64   // shards created (activated with no manifest)
	Deletes      atomic.Int64   // shards deleted on deactivation because the app never wrote to them
	Discards     atomic.Int64   // cached files discarded as out of date
	TailAlerts   atomic.Int64   // alerts raised for a tail older than the alert age
	TailBytes    atomic.Int64   // bytes of tail held by active shards, as of each one's last look of its delta timer
	TailBytesMax atomic.Int64   // the highest TailBytes seen
	CachePasses  notify.Counter // passes of the cache loop (the cache trimmed, held files looked at)
	LeaseScans   notify.Counter // listings of the node leases
	Adoptions    notify.Counter // dead nodes this node finished adopting
	Watchdog     notify.Counter // passes of the watchdog (the tail alert)
	ClaimPasses  notify.Counter // passes of the claimer

	ReminderWakes        atomic.Int64 // wakes of apps for reminders, however they ended
	ReminderWakeFailures atomic.Int64 // wakes that failed and will be repeated after a backoff
	ChecksumMismatches   atomic.Int64 // snapshots whose checksum was not the capture's: the capture went wrong somewhere
}

// Catalog owns the node's shards, active and cached (docs/internals.md,
// Concurrency). Its parts are the table of shards and their actors, the flush loop,
// the cache loop, the lease scan, the watchdog and the claimer,
// which Serve runs. The reminders' limits and the cache index are its too. Each part gets what it
// depends on passed in, and Catalog orders its own stop (Serve).
type Catalog struct {
	n     *Node
	opts  CatalogOptions
	log   *slog.Logger
	Stats Stats

	tab    *table
	cache  *cache
	flush  *Flusher
	rem    *reminders
	leases *leaseScan
	watch  *watchdog
	claim  *claimer
	sys    systemKinds
	deps   *actorDeps // what every shard is given

	// owners maps a shard's name to the node that owned it when this node last
	// found out. It is a hint: a wrong entry costs a misdirected request, never
	// correctness.
	owners *lru.Cache[shard.Name, cluster.Node]
	addrs  *addresses   // where the nodes listen for forwards
	checks *ownerChecks // the leases of owners that don't answer forwards

	cancelBase context.CancelFunc // ends deps.base
	abandon    atomic.Bool        // tests: Serve stops as a crash would, letting go of nothing
}

// NewCatalog prepares the data directory and the index and recovers what a crash
// left behind. Nothing runs until Serve.
//
// The wake of an app for its shard's reminders and the node's calls to system
// shards are the server's routing, and the server needs the shards, so
// newRouting is called with the shards while they are being built (the server
// may only keep them) and what it returns is what the shards are built with.
func NewCatalog(n *Node, opts CatalogOptions, newRouting func(*Catalog) routing) (*Catalog, error) {
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
	orDefault(&opts.Cache.Every, time.Second)
	orDefault(&opts.OwnerCache, 100_000)
	orDefault(&opts.OwnerCheck, time.Second)
	orDefault(&opts.Leases.Every, 2*time.Second)
	orDefault(&opts.Leases.TTL, coord.DefaultTTL)
	orDefault(&opts.Claims.Every, 10*time.Minute)
	orDefault(&opts.Claims.Retry, 2*time.Second)
	orDefault(&mo.Backoff, time.Second)
	orDefault(&mo.MaxBackoff, time.Minute)
	orDefault(&mo.MaxAge, 24*time.Hour)
	orDefault(&mo.NodeLimit, 256)
	orDefault(&opts.limits.deactivate, deactivateLimit)
	orDefault(&opts.limits.snapshot, snapshotLimit)
	orDefault(&opts.limits.rebase, rebaseLimit)
	var m *Catalog
	var rt routing
	if opts.Dispatch != nil {
		opts.System = append(slices.Clone(opts.System), dispatch.New(dispatch.Deps{
			Store: n.Store,
			Live:  func() []cluster.Node { return m.liveNodes() },
			Dispatch: func(ctx context.Context, node cluster.Node, name shard.Name) (dispatch.Answer, error) {
				return rt.dispatch(ctx, node, name)
			},
			Now: opts.Clock.Now,
			Log: opts.Logger,
		}, *opts.Dispatch))
	}
	sysApp, sys, err := newSystemApp(opts.System, opts.Kinds.defaultIdle)
	if err != nil {
		return nil, err
	}
	opts.Kinds.setSystem(sysApp)
	if err := os.MkdirAll(opts.Data, 0o755); err != nil {
		return nil, err
	}
	m = &Catalog{n: n, opts: opts, log: opts.Logger, tab: newTable(opts.Logger), sys: sys}
	n.Files.Now = opts.Clock.Now
	log, clk, st := opts.Logger, opts.Clock, &m.Stats
	// A restart is treated as a crash: nothing on disk from before it is trusted.
	m.owners, _ = lru.New[shard.Name, cluster.Node](opts.OwnerCache) // only a size below 1 fails
	m.cache = &cache{data: opts.Data, tab: m.tab, files: n.Files, node: n.Name, clock: clk, log: log, stats: st}
	if err := m.cache.open(opts.Cache, n.LocalDB); err != nil {
		return nil, err
	}
	rt = newRouting(m)
	m.rem = newReminders(opts.Reminders, clk, log, st, rt.wake)
	m.flush = newFlusher(m.tab, n.Writer, n.Keeper, n.Confirmer, n.Clock, log, opts.Flush)
	m.leases = &leaseScan{
		reg: n.Registry, files: n.Files, store: n.Store, nodeErr: n.Err,
		self:  func() coord.Incarnation { l := n.Keeper.Lease(); return l.Incarnation() },
		clock: clk, opts: opts.Leases, log: log, scans: &st.LeaseScans, adopted: &st.Adoptions, died: m.died,
		mirror: newMirrorDialer(n.Registry, n.Files.JournalOptions),
	}
	m.claim = &claimer{targets: sys.targets(), active: m.IsActive, owner: m.owners.Get, live: m.isLive, acquire: m.Acquire,
		clock: clk, opts: opts.Claims, log: log, kick: notify.NewKick(), adopted: &st.Adoptions, passes: &st.ClaimPasses}
	m.addrs = &addresses{get: n.Registry.Get, nodes: m.leases.Nodes}
	m.checks = &ownerChecks{get: n.Registry.Get, now: clk.Now, every: opts.OwnerCheck, log: log}
	m.watch = &watchdog{tab: m.tab, clock: clk, every: lc.SnapshotEvery, alertAge: tailAlertAge(lc.DeltaAge), log: log, stats: st}
	var base context.Context
	base, m.cancelBase = context.WithCancel(context.Background())
	m.deps = &actorDeps{
		opts: opts.Lifecycle, kinds: opts.Kinds, sys: sys, call: rt.call, clock: clk, log: log, stats: st,
		tab: m.tab, cache: m.cache, flush: m.flush, rem: m.rem,
		files: n.Files, writer: n.Writer, tempDir: n.Files.Temp,
		base:          base,
		deactivations: make(chan struct{}, opts.limits.deactivate),
		snapshots:     make(chan struct{}, opts.limits.snapshot),
		rebases:       make(chan struct{}, opts.limits.rebase),
	}
	n.SetTails(m)
	return m, nil
}

// routing is what the shards use of the server's routing: the wake of an app
// for its shard's reminders (Server.wakeApp), the node's dispatches
// (Server.dispatch) and its calls to system shards (Server.call).
type routing struct {
	wake     wakeFunc
	dispatch func(ctx context.Context, node cluster.Node, name shard.Name) (dispatch.Answer, error)
	call     func(*http.Request) (*http.Response, error)
}

// orDefault sets an option that is zero (or negative) to its default.
func orDefault[T int | int64 | time.Duration](v *T, def T) {
	if *v <= 0 {
		*v = def
	}
}

// Serve runs the shards until ctx ends, then stops in order
// (docs/internals.md, Concurrency: Shutdown). Every request has been joined by then (Run
// ends ctx only once both servers have ended). First the
// lease scan, the watchdog and the cache loop stop, so that nothing activates any
// more; then every shard is let go of, with the flusher still running; then the flusher,
// and the cache index closes. It returns why some shard could not be let go of,
// if one couldn't. Once the node may not write any more (its lease is gone or
// its writer fenced) it lets go of none.
func (m *Catalog) Serve(ctx context.Context) error {
	flusher := service.New(context.Background(), m.log)
	flusher.Add(m.flush)
	loops := service.New(context.Background(), m.log)
	for _, s := range []service.Service{m.leases, m.watch, m.cache, m.claim} {
		loops.Add(s)
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
		err = m.letGo(sctx)
		cancel()
	}
	tctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	m.tab.stop(tctx) // the shards that could not be let go of end their loops
	cancel()
	m.stopPart(flusher)
	m.cancelBase()
	// The local database is not closed here: the journal's writer is still
	// running (the journal's stage stops after this one), and it writes to it.
	return err
}

// stopPart stops a group of the catalog's parts, waiting for it up to
// stopTimeout (the group names a part that doesn't stop by then).
func (m *Catalog) stopPart(g *service.Group) {
	ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()
	if err := g.Stop(ctx); err != nil {
		m.log.Error("a part of the shards failed or did not stop in time", "err", err)
	}
}

// letGo lets go of every shard, at most the deactivation limit at once. Every
// shard's reminders loop stops (and a wake in progress ends) before any shard is
// let go of, so no wake holds a reference to its shard while it is let go of. A
// transient storage failure (the final sync or the delta) is tried again until
// ctx ends. Other failures are not.
func (m *Catalog) letGo(ctx context.Context) error {
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

func (m *Catalog) letGoOf(ctx context.Context, a *actor) error {
	wait := 100 * time.Millisecond
	for {
		ok, err := a.deactivateNow(ctx)
		if err != nil && blob.Transient(err) && ctx.Err() == nil {
			m.log.Warn("could not deactivate a shard at shutdown, trying again", "shard", a.Name, "err", err)
			if sleep(ctx, wait) {
				wait = min(2*wait, 2*time.Second)
				continue
			}
		}
		switch {
		case err != nil:
			return fmt.Errorf("%s: %w", a.Name, err)
		case !ok:
			return fmt.Errorf("%s: %w", a.Name, ErrBusy)
		}
		return nil
	}
}

// Evict lets go of an active or cached shard and gives up its ownership (its
// manifest is released), so that the next node to need it, for a request or a
// reminder, claims it (docs/internals.md, Takeover). An active shard is
// deactivated first. A shard in use is not evicted (ErrBusy), nor one the app
// has open (ErrAppHolds), and one that isn't here is ErrNotHere.
func (m *Catalog) Evict(ctx context.Context, n shard.Name) error {
	for {
		e := m.tab.load(n)
		switch {
		case e == nil:
			row, ok, err := m.cache.idx.get(n)
			switch {
			case err != nil:
				return err
			case !ok || row.State != stateCached:
				return fmt.Errorf("%w: %s", ErrNotHere, n)
			}
			if evicted, err := m.cache.evict(ctx, row); evicted || err != nil {
				return err
			}
			// An entry was added meanwhile: look again.
		case e.a == nil: // a cached shard being evicted or deleted
			if _, wait, _ := e.take(m.now()); wait != nil {
				select {
				case <-wait:
				case <-ctx.Done():
					return context.Cause(ctx)
				}
			} else {
				m.tab.forget(e) // gone: look again
			}
		case e.is(entryStopped):
			return errShardsStopped
		default:
			if done, err := e.a.evict(ctx); done {
				return err
			}
		}
	}
}

// fileSig is what stat says about a shard's files. Comparing two of them is how
// the node tells whether a shard has new frames without opening anything.
type fileSig struct {
	wal, db       int64
	walMod, dbMod int64
}

func statSig(path string) fileSig {
	var s fileSig
	if fi, err := os.Stat(path); err == nil {
		s.db, s.dbMod = fi.Size(), fi.ModTime().UnixNano()
	}
	if size, mod, ok := capture.WALSidecarsOf(path).WALStat(); ok {
		s.wal, s.walMod = size, mod
	}
	return s
}

// Hold keeps an active shard active until it is released. A request holds one
// while it runs, and a WebSocket, a stream or a streaming call for as long as it
// is open.
type Hold struct {
	m    *Catalog
	a    *actor
	once sync.Once
	// proven is set once a storage chunk has passed the gate: the node then
	// knew it still owned its shards after that chunk was read, which is after
	// the request began, and the shard's file only grows. Later chunks of the
	// request have nothing more to prove, only to wait for their own commits.
	proven atomic.Bool
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
func (m *Catalog) Acquire(ctx context.Context, n shard.Name) (*Hold, error) {
	return m.take(ctx, n, false)
}

// acquireExisting is Acquire for a shard that exists already: one with no
// manifest that isn't active or cached here is not created, and the error is
// blob.ErrNotFound. Wakes use it, since a hint never creates a shard.
func (m *Catalog) acquireExisting(ctx context.Context, n shard.Name) (*Hold, error) {
	return m.take(ctx, n, true)
}

func (m *Catalog) take(ctx context.Context, n shard.Name, existing bool) (*Hold, error) {
	if _, err := m.opts.Kinds.Lookup(n.Kind()); err != nil {
		return nil, err
	}
	// A system kind has only its fixed ids.
	if k := m.sys[n.Kind()]; k != nil && !slices.Contains(k.IDs, n.ID()) {
		return nil, fmt.Errorf("%w: %q is not an id of %s", ErrBadTarget, n.ID(), n.Kind())
	}
	hold, err := m.acquire(ctx, n, existing)
	if oe, ok := errors.AsType[*shardfile.OwnedElsewhereError](err); ok {
		m.owners.Add(n, oe.Owner)
	}
	return hold, err
}

func (m *Catalog) acquire(ctx context.Context, n shard.Name, existing bool) (*Hold, error) {
	for {
		e := m.tab.load(n)
		if e == nil {
			if err := m.n.Keeper.Check(); err != nil {
				return nil, err
			}
			read, err := m.readManifest(ctx, n, existing)
			if err != nil {
				return nil, err
			}
			m.owners.Remove(n) // this node claims it now
			e = m.tab.start(n, func(e *entry) *actor {
				a := newActor(m.deps, e)
				a.read = read
				return a
			})
		}
		// The failure of an activation this request waited for is its own. One that
		// had failed before it looked is somebody else's.
		waited := false
		ok, wait, err := e.take(m.now())
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
			return &Hold{m: m, a: e.a}, nil
		case err != nil && (waited || errors.Is(err, errShardsStopped)):
			return nil, err
		}
		m.tab.forget(e) // gone: look again
	}
}

// readManifest reads the manifest of a shard that has no entry, for the
// activation to start from, and fails if another node owns the shard. The
// handle is nil for a cached shard (its file is reopened with no storage call)
// and for one with no manifest yet, which is blob.ErrNotFound if it must exist.
func (m *Catalog) readManifest(ctx context.Context, n shard.Name, existing bool) (*shardfile.Handle, error) {
	if row, ok, err := m.cache.idx.get(n); err != nil || (ok && row.State == stateCached) {
		return nil, err
	}
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
func (m *Catalog) tryHold(n shard.Name) *Hold {
	e := m.tab.load(n)
	if e == nil || e.a == nil {
		return nil
	}
	if ok, _, _ := e.take(m.now()); !ok {
		return nil
	}
	return &Hold{m: m, a: e.a}
}

// IsActive reports whether the shard is active (for Shardlite-If-Active).
func (m *Catalog) IsActive(n shard.Name) bool {
	e := m.tab.load(n)
	return e != nil && e.a != nil && e.is(entryActive, entryBusy)
}

// Tails is what the node's lease counts: the captured shards and the lowest
// segment their tails still need.
func (m *Catalog) Tails() (shards int, from pos.Seg) {
	m.tab.entries.Range(func(_ shard.Name, e *entry) bool {
		if e.a == nil {
			return true
		}
		if sh := e.a.cap.lease.Load(); sh != nil {
			shards++
			if f := sh.TailFrom(); f != 0 && (from == 0 || f < from) {
				from = f
			}
		}
		return true
	})
	return shards, from
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
func (m *Catalog) Active() []shard.Name {
	var out []shard.Name
	m.tab.shards(func(a *actor) { out = append(out, a.Name) }, entryActive, entryBusy)
	return out
}

// Cached returns the names of the deactivated shards on disk.
func (m *Catalog) Cached() ([]shard.Name, error) { return m.cache.cached() }

// SetCacheLimit changes the disk limit for deactivated shards (0 for none).
func (m *Catalog) SetCacheLimit(n int64) { m.cache.limit.Store(n) }

// Nodes returns the lease scan's last listing of the cluster's nodes, nil
// before the first.
func (m *Catalog) Nodes() *NodeList { return m.leases.Nodes() }

// died is told each node the lease scan first finds dead. Every entry of the
// owner cache that names it goes: its shards are claimed elsewhere or here, and
// a request for one goes through Acquire rather than to a node that can't answer
// or no longer owns it. It walks the cache once, which a dead node is rare
// enough to afford. The claimer then looks for system shards the node owned.
func (m *Catalog) died(node cluster.Node) {
	for _, n := range m.owners.Keys() {
		if owner, ok := m.owners.Peek(n); ok && owner == node {
			m.owners.Remove(n)
		}
	}
	m.claim.kick.Kick()
}

// isLive reports whether the lease scan's last listing has node and doesn't
// count it as dead.
func (m *Catalog) isLive(node cluster.Node) bool { return slices.Contains(m.liveNodes(), node) }

// liveNodes is the names of the nodes the lease scan's last listing doesn't
// count as dead.
func (m *Catalog) liveNodes() []cluster.Node {
	var out []cluster.Node
	for _, v := range m.leases.Nodes().nodes() {
		if !v.Dead {
			out = append(out, v.Name)
		}
	}
	return out
}

// Flusher is the flush loop, for the gates.
func (m *Catalog) Flusher() *Flusher { return m.flush }
