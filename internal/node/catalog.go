package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/service"
	"github.com/luci1900/shardlite/internal/shardfile"
)

var (
	// ErrBusy: the shard is in use (a request or connection holds it, or the app has the file open).
	ErrBusy = errors.New("shard is in use")
	// ErrAppHolds: another process has the shard's file open, which the node never lets go of or takes over.
	ErrAppHolds = errors.New("another process has the shard's file open")
)

// tempDirName is the directory of Data that holds the temporary files of deltas,
// snapshots and downloads (not Scratch, which is tmpfs and so memory too). The
// node empties it at startup, and no kind may have its name.
const tempDirName = ".tmp"

// CatalogOptions configure Shards: what Shards itself needs, and an options
// struct for each of its parts.
type CatalogOptions struct {
	Kinds   *Kinds
	Data    string        // directory of shard files, which is the local cache
	Scratch string        // capture's disposable working files, Litestream's metadata directories (tmpfs in production)
	Drain   time.Duration // how long Drain keeps delivering at shutdown, 10 s if zero
	Logger  *slog.Logger
	Clock   Clock // the time of inbox rows, wake hints, row ages, message backoff, idle shards, the shards' timers and the cache, watchdog and wake scan periods, the real one if nil

	Cache     CacheOptions
	Flush     FlushOptions
	Lifecycle LifecycleOptions
	Messages  MessageOptions // docs/guide.md, Messages
	Wakes     WakeOptions
	Leases    LeaseScanOptions

	// OwnerCache is how many shards' owners the node remembers, 100,000 if zero
	// (docs/internals.md, Routing and forwarding).
	OwnerCache int

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

	MessageAttempts   atomic.Int64 // intakes and calls to handlers
	MessagesDelivered atomic.Int64 // outbox rows acknowledged by the receiver's intake
	MessagesRetried   atomic.Int64 // attempts that will be repeated
	MessagesGivenUp   atomic.Int64 // outbox rows given up on
	InboxDelivered    atomic.Int64 // calls to handlers answered with a 2xx
	InboxGivenUp      atomic.Int64 // inbox rows given up on
}

// Catalog owns the node's shards, active and cached (docs/concurrency.md, The
// tree). Its parts are the table of shards and their actors, the flush loop,
// the cache loop, the wake scan, the lease scan and the watchdog, which Serve
// runs. The messenger and the cache index are its too. Each part gets what it
// depends on passed in, and Catalog orders its own stop (Serve).
type Catalog struct {
	n     *Node
	opts  CatalogOptions
	log   *slog.Logger
	Stats Stats

	tab    *table
	cache  *cache
	flush  *Flusher
	msgs   *messenger
	wakes  *wakeScan
	leases *leaseScan
	watch  *watchdog
	deps   *shardDeps // what every shard is given

	// owners maps a shard's name to the node that owned it when this node last
	// found out. It is a hint: a wrong entry costs a misdirected request, never
	// correctness.
	owners *lru.Cache[string, string]
	addrs  *addresses // where the nodes listen for forwards

	cancelBase context.CancelFunc // ends deps.base
	abandon    atomic.Bool        // tests: Serve stops as a crash would, letting go of nothing
}

// NewCatalog prepares the data directory and the index and recovers what a crash
// left behind. Nothing runs until Serve.
//
// The messages' transport is the server's routing, and the server needs the
// shards, so newTransport is called with the shards while they are being built
// (the server may only keep them) and its transport is what the messenger is
// built with.
func NewCatalog(n *Node, opts CatalogOptions, newTransport func(*Catalog) *transport) (*Catalog, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Clock == nil {
		opts.Clock = realClock{}
	}
	lc, fl, mo, wo := &opts.Lifecycle, &opts.Flush, &opts.Messages, &opts.Wakes
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
	orDefault(&opts.Drain, 10*time.Second)
	orDefault(&opts.OwnerCache, 100_000)
	orDefault(&wo.Every, time.Minute)
	orDefault(&wo.ActivationLimit, 32)
	orDefault(&opts.Leases.Every, 2*time.Second)
	orDefault(&opts.Leases.TTL, coord.DefaultTTL)
	orDefault(&mo.Backoff, time.Second)
	orDefault(&mo.MaxBackoff, time.Minute)
	orDefault(&mo.MaxAge, 24*time.Hour)
	orDefault(&mo.ReceiverLimit, 32)
	orDefault(&mo.NodeLimit, 256)
	orDefault(&opts.limits.deactivate, deactivateLimit)
	orDefault(&opts.limits.snapshot, snapshotLimit)
	orDefault(&opts.limits.rebase, rebaseLimit)
	// Litestream's metadata never needs to outlive the process.
	if err := os.RemoveAll(opts.Scratch); err != nil {
		return nil, err
	}
	for _, d := range []string{opts.Data, opts.Scratch} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	m := &Catalog{n: n, opts: opts, log: opts.Logger, tab: newTable(opts.Logger)}
	log, clk, st := opts.Logger, opts.Clock, &m.Stats
	// A restart is treated as a crash: nothing on disk from before it is trusted.
	m.owners, _ = lru.New[string, string](opts.OwnerCache) // only a size below 1 fails
	m.cache = &cache{data: opts.Data, tab: m.tab, files: n.Files, node: n.Name, clock: clk, log: log, stats: st}
	if err := m.cache.open(opts.Cache); err != nil {
		return nil, err
	}
	m.msgs = newMessenger(opts.Messages, opts.Kinds, clk, log, st, newTransport(m))
	m.flush = newFlusher(m.tab, n.Writer, n.Keeper, log, opts.Flush)
	hints := hintStore{n.Store}
	m.wakes = &wakeScan{store: n.Store, hints: hints, files: n.Files, keeper: n.Keeper, kinds: opts.Kinds, acquire: m.Acquire, self: n.Name, active: m.IsActive, clock: clk, opts: opts.Wakes, log: log}
	m.leases = &leaseScan{
		reg: n.Registry, files: n.Files, store: n.Store, nodeErr: n.Err,
		self:  func() coord.Incarnation { l := n.Keeper.Lease(); return l.Incarnation() },
		clock: clk, opts: opts.Leases, log: log, scans: &st.LeaseScans, adopted: &st.Adoptions,
	}
	m.addrs = &addresses{get: n.Registry.Get, nodes: m.leases.Nodes}
	m.watch = &watchdog{tab: m.tab, clock: clk, every: lc.SnapshotEvery, alertAge: tailAlertAge(lc.DeltaAge), log: log, stats: st}
	var base context.Context
	base, m.cancelBase = context.WithCancel(context.Background())
	m.deps = &shardDeps{
		opts: opts.Lifecycle, kinds: opts.Kinds, clock: clk, log: log, stats: st,
		tab: m.tab, cache: m.cache, flush: m.flush, msgs: m.msgs, hints: hints,
		files: n.Files, writer: n.Writer, scratch: opts.Scratch, tempDir: n.TempDir, spool: n.SpoolLimit,
		base:          base,
		deactivations: make(chan struct{}, opts.limits.deactivate),
		snapshots:     make(chan struct{}, opts.limits.snapshot),
		rebases:       make(chan struct{}, opts.limits.rebase),
	}
	n.SetTails(m)
	return m, nil
}

// orDefault sets an option that is zero (or negative) to its default.
func orDefault[T int | int64 | time.Duration](v *T, def T) {
	if *v <= 0 {
		*v = def
	}
}

// Serve runs the shards until ctx ends, then stops in order
// (docs/concurrency.md, Shutdown). Every request has been joined by then (Run
// ends ctx only once both servers have ended). First the wake scan, the
// watchdog and the cache loop stop, so that nothing activates any more; then
// every shard is let go of, with the flusher still running; then the flusher,
// and the cache index closes. It returns why some shard could not be let go of,
// if one couldn't. Once the node may not write any more (its lease is gone or
// its writer fenced) it lets go of none.
func (m *Catalog) Serve(ctx context.Context) error {
	flusher := service.New(context.Background(), m.log)
	flusher.Add(m.flush)
	loops := service.New(context.Background(), m.log)
	for _, s := range []service.Service{m.wakes, m.leases, m.watch, m.cache} {
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
	m.cache.idx.close()
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
// shard's messages owner stops (and its deliveries are joined) before any shard
// is let go of, so no delivery holds a reference to the shard it sends to while
// that one is let go of. The releases write the wake hints themselves. A
// transient storage failure (the final sync, the delta or a hint) is tried again
// until ctx ends. Other failures are not.
func (m *Catalog) letGo(ctx context.Context) error {
	var list []*shard
	m.tab.shards(func(a *shard) { list = append(list, a) }, entryActivating, entryActive, entryBusy)
	var g errgroup.Group
	g.SetLimit(m.opts.limits.deactivate)
	for _, a := range list {
		g.Go(func() error {
			select {
			case <-a.ready:
				if err := a.quiet(ctx); err != nil {
					m.log.Error("could not stop a shard's messages at shutdown", "shard", a.Name, "err", err)
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

func (m *Catalog) letGoOf(ctx context.Context, a *shard) error {
	wait := 100 * time.Millisecond
	for {
		ok, err := a.release(ctx)
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
	if fi, err := os.Stat(path + "-wal"); err == nil {
		s.wal, s.walMod = fi.Size(), fi.ModTime().UnixNano()
	}
	return s
}

// Ref is a hold on an active shard: it stays active until the last Ref is
// released. A request holds one while it runs, and a WebSocket, a stream or a
// streaming call for as long as it is open.
type Ref struct {
	m    *Catalog
	a    *shard
	once sync.Once
}

// Name is the shard's kind/id.
func (r *Ref) Name() string { return r.a.Name }

// Path is the opaque path of the shard's file, for Shardlite-Path.
func (r *Ref) Path() string { return r.a.Path }

// Sync reports whether the shard's kind has sync on right now.
func (r *Ref) Sync() bool {
	k, err := r.m.opts.Kinds.Lookup(r.a.Kind)
	return err == nil && k.Sync
}

// Release gives up the hold.
func (r *Ref) Release() {
	r.once.Do(func() { r.a.e.put(r.m.now()) })
}

// Acquire activates the shard if it is not active and holds it. target is a
// Shardlite-Target value. However many requests ask at once, the shard is
// activated once: the first adds its entry and the others wait for it. The
// activation runs in the shard's own loop, so one caller giving up doesn't fail
// the others. A shard another node owns is not activated at all: the manifest
// is read before the entry is added, and the owner is remembered
// (*shardfile.OwnedElsewhereError).
func (m *Catalog) Acquire(ctx context.Context, target string) (*Ref, error) {
	ref, err := m.acquire(ctx, target)
	if oe, ok := errors.AsType[*shardfile.OwnedElsewhereError](err); ok {
		m.owners.Add(target, oe.Owner)
	}
	return ref, err
}

func (m *Catalog) acquire(ctx context.Context, target string) (*Ref, error) {
	kind, id, err := ParseTarget(target)
	if err != nil {
		return nil, err
	}
	if _, err := m.opts.Kinds.Lookup(kind); err != nil {
		return nil, err
	}
	for {
		e := m.tab.load(target)
		if e == nil {
			if err := m.n.Keeper.Check(); err != nil {
				return nil, err
			}
			read, err := m.readManifest(ctx, target)
			if err != nil {
				return nil, err
			}
			m.owners.Remove(target) // this node claims it now
			e = m.tab.start(target, func(e *entry) *shard {
				a := newShard(m.deps, e, kind, id)
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
			return &Ref{m: m, a: e.sh}, nil
		case err != nil && (waited || errors.Is(err, errShardsStopped)):
			return nil, err
		}
		m.tab.forget(e) // gone: look again
	}
}

// readManifest reads the manifest of a shard that has no entry, for the
// activation to start from, and fails if another node owns the shard. The
// handle is nil for a cached shard (its file is reopened with no storage call)
// and for one with no manifest yet.
func (m *Catalog) readManifest(ctx context.Context, target string) (*shardfile.Handle, error) {
	if row, ok, err := m.cache.idx.get(target); err != nil || (ok && row.State == stateCached) {
		return nil, err
	}
	h, err := m.n.Files.Attach(ctx, target)
	if errors.Is(err, blob.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return h, h.CheckClaimable(ctx)
}

// hold takes a hold on an active shard, nil if it isn't (or is being let go of).
func (m *Catalog) hold(target string) *Ref {
	e := m.tab.load(target)
	if e == nil || e.sh == nil {
		return nil
	}
	if ok, _, _ := e.take(m.now()); !ok {
		return nil
	}
	return &Ref{m: m, a: e.sh}
}

// IsActive reports whether the shard is active (for Shardlite-If-Active).
func (m *Catalog) IsActive(target string) bool {
	e := m.tab.load(target)
	return e != nil && e.sh != nil && e.is(entryActive, entryBusy)
}

// Tails is what the node's lease counts: the captured shards and the lowest
// segment their tails still need.
func (m *Catalog) Tails() (shards int, from uint64) {
	m.tab.entries.Range(func(_ string, e *entry) bool {
		if e.sh == nil {
			return true
		}
		if sh := e.sh.cap.lease.Load(); sh != nil {
			shards++
			if f := sh.TailFrom(); f != 0 && (from == 0 || f < from) {
				from = f
			}
		}
		return true
	})
	return shards, from
}

// Delete removes a shard: it is deactivated, its manifest is deleted by
// conditional delete, then its generation's LTX files and the local files. A
// shard in use, or one the app still has open, is refused with ErrBusy. An
// active shard is deleted by its own loop, a cached one under an entry of its
// own.
func (m *Catalog) Delete(ctx context.Context, target string) error {
	kind, _, err := ParseTarget(target)
	if err != nil {
		return err
	}
	if _, err := m.opts.Kinds.Lookup(kind); err != nil {
		return err
	}
	for {
		e := m.tab.load(target)
		if e == nil {
			e, ok := m.tab.claim(target)
			if !ok {
				continue // somebody added one meanwhile: look again
			}
			err := m.cache.deleteCached(ctx, target)
			m.tab.remove(e, nil)
			return err
		}
		if e.sh != nil && e.is(entryActive) {
			a := e.sh
			err, lerr := do(ctx, a.ops, a.ended, func() error { return a.deleteActive(ctx) })
			switch {
			case errors.Is(lerr, errLoopEnded) || errors.Is(err, errLoopEnded):
				continue // let go of meanwhile: look again
			case lerr != nil:
				return lerr
			case err != nil:
				return err
			}
			return a.waitEnded(ctx) // until the name is free
		}
		ok, wait, err := e.take(m.now())
		switch {
		case ok:
			e.put(m.now()) // it became active since the look above: the next look sends it the delete
		case wait != nil:
			select {
			case <-wait:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		case errors.Is(err, errShardsStopped):
			return err
		default:
			m.tab.forget(e)
		}
	}
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
func (m *Catalog) Active() []string {
	var out []string
	m.tab.shards(func(a *shard) { out = append(out, a.Name) }, entryActive, entryBusy)
	return out
}

// Cached returns the names of the deactivated shards on disk.
func (m *Catalog) Cached() ([]string, error) { return m.cache.cached() }

// SetCacheLimit changes the disk limit for deactivated shards (0 for none).
func (m *Catalog) SetCacheLimit(n int64) { m.cache.limit.Store(n) }

// Nodes returns the lease scan's last listing of the cluster's nodes, nil
// before the first.
func (m *Catalog) Nodes() *NodeList { return m.leases.Nodes() }

// Flusher is the flush loop, for the gates.
func (m *Catalog) Flusher() *Flusher { return m.flush }
