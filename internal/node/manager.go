package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
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

// ManagerOptions configure a Manager.
type ManagerOptions struct {
	Kinds      *Kinds
	Data       string        // directory of shard files, which is the local cache
	Scratch    string        // capture's disposable working files, Litestream's metadata directories (tmpfs in production)
	CacheLimit int64         // bytes of deactivated shards to keep, 0 for no limit
	Flush      time.Duration // flush interval, segment.DefaultFlush if zero
	// FlushMin is the shortest time between the starts of two flush cycles, DefaultFlushMin if zero.
	FlushMin time.Duration
	// FlushCoalesce is how long a cycle that output is waiting for is held back so
	// that output arriving together shares it, DefaultFlushCoalesce if zero.
	FlushCoalesce time.Duration
	// NoEarlyFlush makes cycles run on the interval alone, for tests that decide when one runs.
	NoEarlyFlush bool
	// A shard gets a delta when its tail is DeltaAge old or holds DeltaSize bytes,
	// whichever comes first. A tail older than 3 times DeltaAge raises an alert.
	DeltaAge   time.Duration // shardfile.DefaultDeltaAge if zero
	DeltaSize  int64         // shardfile.DefaultDeltaSize if zero
	MaxFiles   int           // files a manifest may list before the next snapshot rebases the shard, shardfile.DefaultMaxFiles if zero
	Janitor    time.Duration // how often idle shards are looked for, 1 s if zero
	Background time.Duration // how often snapshots (deltas and rebases) are looked for, 2 s if zero
	HoldOff    time.Duration // how long to leave a shard alone after it couldn't be deactivated, 5 s if zero
	Logger     *slog.Logger

	// Messages (docs/guide.md, Messages).
	MessageBackoff       time.Duration // first wait before an outbox or inbox row is retried, 1 s if zero
	MessageMaxBackoff    time.Duration // longest wait, 1 min if zero
	MessageMaxAge        time.Duration // an outbox row older than this is given up on, and an inbox row failing this long past its due time is given up on, 24 h if zero
	MessageReceiverLimit int           // requests in flight to one receiver, 32 if zero
	MessageNodeLimit     int           // requests in flight on the node, 256 if zero
	ActivationLimit      int           // shards activated at once by the scan for wake hints, 32 if zero
	Drain                time.Duration // how long Drain keeps delivering at shutdown, 10 s if zero
	WakeEvery            time.Duration // how often wake hints are looked for, 1 min if zero
	Clock                Clock         // the time of inbox rows, wake hints, row ages, message backoff, idle shards and the janitor and wake scan periods, the real one if nil
}

// Stats counts what the manager has done, for tests and metrics.
type Stats struct {
	Activations  atomic.Int64 // shards activated
	Restores     atomic.Int64 // ... by restoring from blob storage
	CachedHits   atomic.Int64 // ... from a cached file
	Creates      atomic.Int64 // shards created (activated with no manifest)
	Deletes      atomic.Int64 // shards deleted on deactivation because the app never wrote to them
	AutoDeletes  atomic.Int64 // shards deleted on deactivation because of auto_delete
	Discards     atomic.Int64 // cached files discarded as out of date
	TailAlerts   atomic.Int64 // alerts raised for a tail older than the alert age
	TailBytes    atomic.Int64 // bytes of tail held by active shards, as of the last pass of the background loop
	TailBytesMax atomic.Int64 // the highest TailBytes seen
	Janitor      atomic.Int64 // passes of the janitor (idle shards deactivated, the cache trimmed)
	Background   atomic.Int64 // passes of the background loop (deltas and rebases)

	MessageAttempts   atomic.Int64 // intakes and calls to handlers
	MessagesDelivered atomic.Int64 // outbox rows acknowledged by the receiver's intake
	MessagesRetried   atomic.Int64 // attempts that will be repeated
	MessagesGivenUp   atomic.Int64 // outbox rows given up on
	SnapshotHints     atomic.Int64 // wake hints written because a snapshot was about to take the tail of a shard with a live outbox row
	InboxDelivered    atomic.Int64 // calls to handlers answered with a 2xx
	InboxGivenUp      atomic.Int64 // inbox rows given up on
}

// Manager owns the node's shards: activation and creation, the flush loop,
// deactivation, the local cache and delete.
type Manager struct {
	n    *Node
	opts ManagerOptions
	log  *slog.Logger
	idx  *index

	mu     sync.Mutex
	active map[string]*ActiveShard
	locks  keyedLock
	sf     singleflight.Group

	flush      *Flusher
	rebases    chan struct{} // a slot for each rebase running
	msgs       *messenger
	cacheLimit atomic.Int64
	Stats      Stats
	held       map[string]bool // shard files from before the restart that an app still had open (guarded by mu)

	ctx    context.Context
	cancel context.CancelFunc
	g      errgroup.Group // the loops and background jobs the manager owns; Close joins them
}

// NewManager prepares the data directory and the index, recovers what a crash
// left behind and starts the flush loop and the background loops.
func NewManager(n *Node, opts ManagerOptions) (*Manager, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Flush <= 0 {
		opts.Flush = 250 * time.Millisecond
	}
	if opts.DeltaAge <= 0 {
		opts.DeltaAge = shardfile.DefaultDeltaAge
	}
	if opts.DeltaSize <= 0 {
		opts.DeltaSize = shardfile.DefaultDeltaSize
	}
	if opts.MaxFiles <= 0 {
		opts.MaxFiles = shardfile.DefaultMaxFiles
	}
	if opts.FlushMin <= 0 {
		opts.FlushMin = DefaultFlushMin
	}
	if opts.FlushCoalesce <= 0 {
		opts.FlushCoalesce = DefaultFlushCoalesce
	}
	if opts.Janitor <= 0 {
		opts.Janitor = time.Second
	}
	if opts.HoldOff <= 0 {
		opts.HoldOff = 5 * time.Second
	}
	if opts.Background <= 0 {
		opts.Background = 2 * time.Second
	}
	if opts.Clock == nil {
		opts.Clock = realClock{}
	}
	for _, d := range []struct {
		v   *time.Duration
		def time.Duration
	}{{&opts.Drain, 10 * time.Second}, {&opts.WakeEvery, time.Minute}, {&opts.MessageBackoff, time.Second}, {&opts.MessageMaxBackoff, time.Minute}, {&opts.MessageMaxAge, 24 * time.Hour}} {
		if *d.v <= 0 {
			*d.v = d.def
		}
	}
	for _, d := range []struct {
		v   *int
		def int
	}{{&opts.MessageReceiverLimit, 32}, {&opts.MessageNodeLimit, 256}, {&opts.ActivationLimit, 32}} {
		if *d.v <= 0 {
			*d.v = d.def
		}
	}
	// Litestream's metadata never needs to outlive the process.
	if err := os.RemoveAll(opts.Scratch); err != nil {
		return nil, err
	}
	for _, d := range []string{opts.Data, opts.Scratch} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	// A restart is treated as a crash: nothing on disk from before it is trusted.
	left, err := clearCache(opts.Data, opts.Logger)
	if err != nil {
		return nil, err
	}
	idx, err := openIndex(filepath.Join(opts.Data, indexName))
	if err != nil {
		return nil, err
	}
	m := &Manager{n: n, opts: opts, log: opts.Logger, idx: idx, active: map[string]*ActiveShard{}, rebases: make(chan struct{}, rebaseLimit), held: left}
	m.cacheLimit.Store(opts.CacheLimit)
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.flush = newFlusher(m, opts.Flush, opts.FlushMin, opts.FlushCoalesce, opts.NoEarlyFlush)
	m.msgs = newMessenger(m)
	m.start(m.alertLoop)
	m.start(m.flush.loop)
	m.start(m.janitorLoop)
	m.start(m.backgroundLoop)
	m.start(m.wakeLoop)
	return m, nil
}

func (m *Manager) start(f func(ctx context.Context)) {
	m.g.Go(func() error {
		f(m.ctx)
		return nil
	})
}

// indexName is the cache index's file in the data directory.
const indexName = ".index.db"

// clearCache is what a start does to the data directory, after the node has
// adopted its own journal: it deletes the cache index (and its journal files),
// the contents of the temporary directory and every shard file that no process
// has open, so the node starts with an empty cache, as a new one would. The
// index runs with synchronous=OFF and the apps' files with NORMAL, so after an
// OS crash neither can be trusted to agree with the other or with blob storage.
//
// A file that an app still has open (from before the restart) is never deleted:
// the app could go on committing to it with no error. The returned set holds
// their paths; the janitor looks at them again until the app has closed them,
// and activating the shard meanwhile is refused (ErrAppHolds).
func clearCache(data string, log *slog.Logger) (held map[string]bool, err error) {
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		if err := os.Remove(filepath.Join(data, indexName+suffix)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	if ents, err := os.ReadDir(filepath.Join(data, tempDirName)); err == nil {
		for _, e := range ents {
			if err := os.RemoveAll(filepath.Join(data, tempDirName, e.Name())); err != nil {
				return nil, err
			}
		}
	}
	bases := map[string]bool{} // the database file of each set of shard files
	err = filepath.WalkDir(data, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p == filepath.Join(data, tempDirName) {
				return filepath.SkipDir
			}
			return nil
		}
		for _, suffix := range []string{"-wal", "-shm", ".restoring"} {
			p = strings.TrimSuffix(p, suffix)
		}
		bases[p] = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	held = map[string]bool{}
	for p := range bases {
		if isHeld, err := capture.ProbeOnce(p); err != nil {
			return nil, err
		} else if isHeld {
			held[p] = true
			log.Error("a shard file from before the restart is still open in another process, leaving it alone until it is closed", "path", p)
			continue
		}
		removeLocal(p)
	}
	return held, nil
}

// sweepHeld deletes the shard files that were still open at the start, as soon
// as no process has them open any more.
func (m *Manager) sweepHeld() {
	m.mu.Lock()
	paths := make([]string, 0, len(m.held))
	for p := range m.held {
		paths = append(paths, p)
	}
	m.mu.Unlock()
	for _, p := range paths {
		if isHeld, err := capture.ProbeOnce(p); err != nil || isHeld {
			continue
		}
		removeLocal(p)
		m.mu.Lock()
		delete(m.held, p)
		m.mu.Unlock()
	}
}

func removeLocal(path string) {
	for _, p := range []string{path, path + "-wal", path + "-shm", path + ".restoring"} {
		os.RemoveAll(p)
	}
}

// pathFor is the opaque name of a shard's file: it carries the generation, so
// a shard deleted and created again never reuses a file. Kinds and ids need no
// escaping (see ParseTarget).
func (m *Manager) pathFor(kind, id, generation string) string {
	return filepath.Join(m.opts.Data, kind, id+"."+generation+".db")
}

// keyedLock is a mutex per key that costs nothing for keys nobody holds.
type keyedLock struct {
	mu sync.Mutex
	m  map[string]*klEntry
}

type klEntry struct {
	mu sync.Mutex
	n  int
}

func (k *keyedLock) lock(key string) (unlock func()) {
	k.mu.Lock()
	if k.m == nil {
		k.m = map[string]*klEntry{}
	}
	e := k.m[key]
	if e == nil {
		e = &klEntry{}
		k.m[key] = e
	}
	e.n++
	k.mu.Unlock()
	e.mu.Lock()
	return func() {
		e.mu.Unlock()
		k.mu.Lock()
		if e.n--; e.n == 0 {
			delete(k.m, key)
		}
		k.mu.Unlock()
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

// ActiveShard is a shard open on this node.
type ActiveShard struct {
	m              *Manager
	Name, Kind, ID string
	Gen, Path      string
	autoDelete     bool        // the manifest has auto_delete, set when the shard was created (the kind's setting is looked up when it is needed)
	keptNoted      atomic.Bool // the log line saying why an auto-delete shard is kept has been written
	opMu           sync.Mutex  // delta, rebase and deactivation
	intakeMu       sync.Mutex  // serialises the intakes inserted into the inbox, so none waits on SQLite's busy handler
	h              *shardfile.Handle
	msgs           *shardMsgs
	cap            atomic.Pointer[capture.Shard]
	syncErr        atomic.Pointer[error]
	sigMu          sync.Mutex
	last           capture.Header // the wal-index header as of the last completed capture
	lastOK         bool           // last is valid (a header that couldn't be read, or an unseen one, is not)
	refs           int            // guarded by Manager.mu, as are the rest
	lastUse        time.Time
	closing        bool
	delay          time.Time // do not try to deactivate before this
	alertedAt      time.Time // when the tail-age alert was last raised (only alertLoop uses it)
}

// header reads the wal-index header of a captured shard: ok is false if it
// could not be read (the shard counts as changed then).
func (a *ActiveShard) header() (h capture.Header, ok bool) {
	sh := a.cap.Load()
	if sh == nil {
		return h, false
	}
	h, err := sh.Header()
	return h, err == nil
}

// captured records the header read before a capture started: commits that
// landed after the read are captured too, and just cause one more sync.
func (a *ActiveShard) captured(h capture.Header, ok bool) {
	a.sigMu.Lock()
	a.last, a.lastOK = h, ok
	a.sigMu.Unlock()
}

// changed reports whether the header differs from the last completed capture,
// and returns the header it read.
func (a *ActiveShard) changed() (h capture.Header, ok, changed bool) {
	h, ok = a.header()
	a.sigMu.Lock()
	defer a.sigMu.Unlock()
	return h, ok, !ok || !a.lastOK || h != a.last
}

// hasNewFrames reports whether the shard has commits that are not captured.
func (a *ActiveShard) hasNewFrames() bool {
	_, _, c := a.changed()
	return c
}

// Ref is a hold on an active shard: it stays active until the last Ref is
// released. A request holds one while it runs, and a WebSocket, a stream or a
// streaming call for as long as it is open.
type Ref struct {
	m    *Manager
	a    *ActiveShard
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
	r.once.Do(func() {
		m, a := r.m, r.a
		m.mu.Lock()
		a.refs--
		a.lastUse = m.now()
		m.mu.Unlock()
	})
}

// Acquire activates the shard if it is not active and holds it. target is a
// Shardlite-Target value.
func (m *Manager) Acquire(ctx context.Context, target string) (*Ref, error) {
	kind, id, err := ParseTarget(target)
	if err != nil {
		return nil, err
	}
	if _, err := m.opts.Kinds.Lookup(kind); err != nil {
		return nil, err
	}
	for {
		if r := m.hold(target); r != nil {
			return r, nil
		}
		if err := m.n.Keeper.Check(); err != nil {
			return nil, err
		}
		// One activation per shard, however many requests ask. It runs on the
		// manager's context so that one caller giving up doesn't fail the others.
		ch := m.sf.DoChan(target, func() (any, error) { return nil, m.activate(m.ctx, target, kind, id) })
		select {
		case res := <-ch:
			if res.Err != nil {
				return nil, res.Err
			}
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		}
	}
}

// hold takes a hold on an active shard, nil if it isn't (or is being let go of).
func (m *Manager) hold(target string) *Ref {
	m.mu.Lock()
	defer m.mu.Unlock()
	a := m.active[target]
	if a == nil || a.closing {
		return nil
	}
	a.refs++
	a.lastUse = m.now()
	return &Ref{m: m, a: a}
}

// IsActive reports whether the shard is active (for Shardlite-If-Active).
func (m *Manager) IsActive(target string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	a := m.active[target]
	return a != nil
}

func (m *Manager) register(a *ActiveShard) {
	a.lastUse = m.now()
	m.mu.Lock()
	m.active[a.Name] = a
	m.mu.Unlock()
	m.Stats.Activations.Add(1)
}

func (m *Manager) removeActive(a *ActiveShard) {
	m.mu.Lock()
	if m.active[a.Name] == a {
		delete(m.active, a.Name)
	}
	m.mu.Unlock()
	a.msgs.cancel() // ends the deliveries of its messages
	a.msgs.stopTimer()
}

func (m *Manager) activeIs(target string, a *ActiveShard) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active[target] == a
}

func (m *Manager) putRow(a *ActiveShard, state string, pos uint64) error {
	row := indexRow{Name: a.Name, Path: a.Path, Generation: a.Gen, Position: pos, State: state, LastUsed: time.Now()}
	if state == stateCached {
		mj, err := json.Marshal(a.h.Manifest)
		if err != nil {
			return err
		}
		if _, err := os.Stat(a.Path); err != nil {
			return err
		}
		sig := statSig(a.Path)
		row.Manifest, row.ETag = string(mj), string(a.h.ETag)
		row.Size, row.MTimeNs, row.WalSize, row.WalMTimeNs = sig.db, sig.dbMod, sig.wal, sig.walMod
		row.Bytes = sig.db + sig.wal
	}
	return m.idx.put(row)
}

// activate follows the Activation in docs/internals.md for a single node. The caller holds
// no lock. Under the shard's lock it re-checks that nobody else has activated
// the shard, then:
//
//  1. a file cached since this process started is opened again, with no blob
//     storage call;
//  2. any other shard is claimed by compare-and-swap on its manifest and restored;
//  3. a shard with no manifest is created: a conditional create of its manifest
//     claims it, and its file starts empty. If another node creates it first,
//     it is claimed like any other shard.
func (m *Manager) activate(ctx context.Context, target, kind, id string) error {
	unlock := m.locks.lock(target)
	defer unlock()
	m.mu.Lock()
	_, there := m.active[target]
	m.mu.Unlock()
	if there {
		return nil
	}
	k, err := m.opts.Kinds.Lookup(kind)
	if err != nil {
		return err
	}
	a := &ActiveShard{m: m, Name: target, Kind: kind, ID: id}
	a.msgs = newShardMsgs(m, a)
	defer func() {
		if m.activeIs(target, a) {
			return
		}
		a.msgs.cancel() // not activated
	}()

	if row, ok, err := m.idx.get(target); err != nil {
		return err
	} else if ok && row.State == stateCached {
		done, err := m.activateCached(ctx, a, row)
		if done || err != nil {
			return err
		}
	}

	created := false
	h, err := m.n.Files.Attach(ctx, target)
	if errors.Is(err, blob.ErrNotFound) {
		h, created, err = m.n.Files.Create(ctx, target, k.AutoDelete)
	}
	if err != nil {
		return err
	}
	noTail := h.Manifest.Journal == "" // known before the claim sets it
	if !created {
		if err := h.Claim(ctx); err != nil {
			return err
		}
	}
	a.Gen, a.Path = h.Manifest.Generation, m.pathFor(kind, id, h.Manifest.Generation)
	a.autoDelete = h.Manifest.AutoDelete
	if err := m.clearLocal(a.Path); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(a.Path), 0o755); err != nil {
		return err
	}
	a.h = h
	var restored *capture.Restored
	if created {
		m.Stats.Creates.Add(1)
	} else {
		res, err := m.n.Files.Restore(ctx, target, a.Path, shardfile.RestoreOptions{NoJournal: noTail})
		switch {
		case errors.Is(err, shardfile.ErrNothingToRestore):
			// A manifest with nothing durable behind it (its node died before the
			// first flush): the shard starts empty, as a new one does.
		case err != nil:
			return err
		default:
			a.h, restored = res.Handle, res.Capture
		}
		m.Stats.Restores.Add(1)
	}
	if err := m.startCapture(ctx, a, restored); err != nil {
		return err
	}
	if err := m.putRow(a, stateActive, 0); err != nil {
		return err
	}
	m.register(a)
	return nil
}

// activateCached reuses a cached file. done is false when the file had to be
// discarded and the shard needs the normal path.
func (m *Manager) activateCached(ctx context.Context, a *ActiveShard, row indexRow) (done bool, err error) {
	// Litestream leaves the -wal in place when it closes, and it is part of the
	// file's state, so both must be as they were at deactivation.
	_, serr := os.Stat(row.Path)
	if sig := statSig(row.Path); serr != nil || sig != (fileSig{db: row.Size, dbMod: row.MTimeNs, wal: row.WalSize, walMod: row.WalMTimeNs}) {
		m.log.Warn("cached shard files changed since it was deactivated, discarding them", "shard", a.Name, "path", row.Path, "err", serr)
		return false, m.discardLocal(row)
	}
	// Nothing else may have the file open (the node's own handles are closed).
	if held, err := capture.ProbeOnce(row.Path); err != nil {
		return false, err
	} else if held {
		m.log.Error("a process has a deactivated shard's file open", "shard", a.Name, "path", row.Path)
		return false, fmt.Errorf("%w: %s", ErrAppHolds, a.Name)
	}
	// The row was written by this process (the cache starts empty), so the manifest
	// it recorded is the one the node holds, and nothing needs asking of blob storage.
	var man shardfile.Manifest
	if err := json.Unmarshal([]byte(row.Manifest), &man); err != nil {
		return false, m.discardLocal(row)
	}
	h := m.n.Files.HandleFor(a.Name, &man, blob.ETag(row.ETag))
	a.h, a.Gen, a.Path, a.autoDelete = h, row.Generation, row.Path, h.Manifest.AutoDelete
	if err := m.startCapture(ctx, a, &capture.Restored{Position: row.Position}); err != nil {
		return false, err
	}
	m.Stats.CachedHits.Add(1)
	if err := m.putRow(a, stateActive, 0); err != nil {
		return false, err
	}
	m.register(a)
	return true, nil
}

// startCapture starts capturing the shard's file and runs Litestream's first
// sync before the app can open it. A restored (or cached) shard is capture's
// seeded start: Litestream continues from the restored state without reading
// the database, so that first sync writes nothing and the position stays the
// restored one. A new shard (restored is nil) gets an empty file first, and its
// first sync, of an empty database holding only Litestream's tables, is its
// first position.
func (m *Manager) startCapture(ctx context.Context, a *ActiveShard, restored *capture.Restored) error {
	if restored == nil {
		// The node holds no locks on a file it has just created, so closing the
		// descriptor drops nothing.
		f, err := os.OpenFile(a.Path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		f.Close()
	}
	sh, err := capture.New(capture.Config{
		Name: a.Name, Path: a.Path, Generation: a.h.Manifest.Generation,
		MetaRoot: m.opts.Scratch, Writer: m.n.Writer, Restored: restored, Logger: m.log, Durable: m.flush.WaitCycle,
		TempDir: m.n.TempDir, SpoolLimit: m.n.SpoolLimit,
	})
	if err != nil {
		return err
	}
	// Tracked before anything is captured, so that the lease's journal range
	// covers the first file from the moment it is queued.
	m.n.Track(sh)
	if err := sh.Capture(ctx); err != nil {
		m.n.Untrack(sh)
		sh.Close(ctx)
		sh.Discard()
		return err
	}
	// The reserved table goes in after the first sync, before the app has the shard.
	if err := m.ensureMessages(ctx, sh); err != nil {
		m.n.Untrack(sh)
		sh.Close(ctx)
		sh.Discard()
		return err
	}
	a.cap.Store(sh)
	a.captured(a.header())
	return nil
}

// clearLocal removes leftovers at path, unless another process has the file open.
func (m *Manager) clearLocal(path string) error {
	if held, err := capture.ProbeOnce(path); err != nil {
		return err
	} else if held {
		return fmt.Errorf("%w: %s", ErrAppHolds, path)
	}
	removeLocal(path)
	return nil
}

func (m *Manager) discardLocal(row indexRow) error {
	m.Stats.Discards.Add(1)
	if err := m.clearLocal(row.Path); err != nil {
		return err
	}
	return m.idx.delete(row.Name)
}

// resume starts capture again on a shard whose capture was let go of but whose
// deactivation couldn't be finished, carrying the tail over.
func (m *Manager) resume(ctx context.Context, a *ActiveShard, old *capture.Shard) error {
	// The release that failed may have failed because ctx ended (shutdown cancels
	// the janitor's deactivations). Capture has to be back whatever happens: a
	// shard left with a closed database can never be let go of.
	ctx = context.WithoutCancel(ctx)
	err := m.startCapture(ctx, a, &capture.Restored{Position: old.Position(), Tail: old.Tail()})
	old.Discard() // the new capture holds the tail's files now (or New gave them up)
	return err
}

// deactivate lets go of an idle shard: Release (a final sync and closing every
// handle, then the -shm check), a delta for its tail, and a row in the cache
// index. A shard the app never wrote to is left with an empty manifest instead,
// so requests for ids that don't exist leave almost nothing behind. It reports whether the shard was
// deactivated. A shard the app still holds, or one in use, stays active.
func (m *Manager) deactivate(ctx context.Context, a *ActiveShard) (bool, error) {
	unlock := m.locks.lock(a.Name)
	defer unlock()
	m.mu.Lock()
	if m.active[a.Name] != a {
		m.mu.Unlock()
		return true, nil
	}
	if a.refs > 0 {
		m.mu.Unlock()
		return false, nil
	}
	a.closing = true
	m.mu.Unlock()
	done := false
	defer func() {
		if !done {
			m.mu.Lock()
			a.closing = false
			m.mu.Unlock()
		}
	}()
	a.opMu.Lock()
	defer a.opMu.Unlock()

	// An auto-delete shard with rows in its outbox or inbox is released like any
	// other, and carries on at its next activation. Nothing is written for one
	// that is deleted.
	deleteIt := false
	if a.autoDeletes() {
		live, err := m.msgs.busy(ctx, a, true)
		if err != nil {
			return false, err
		}
		deleteIt = !live
	}
	// The hints that wake the shard again go first (a stale one is harmless). If
	// one can't be written the shard stays active.
	if err := m.msgs.hintsBeforeRelease(ctx, a); err != nil {
		return false, err
	}
	sh := a.cap.Load()
	released, err := sh.Release(ctx)
	if err != nil {
		if rerr := m.resume(ctx, a, sh); rerr != nil {
			m.log.Error("could not resume capture after a failed release", "shard", a.Name, "err", rerr)
		}
		return false, err
	}
	if !released {
		m.holdOff(a)
		m.log.Warn("the app still has the shard open, keeping it active", "shard", a.Name)
		return false, nil
	}
	m.n.Untrack(sh)
	if sh.Empty() {
		// The app never wrote to it: the shard leaves nothing behind. A conflict
		// means somebody else changed the manifest, and the shard is kept.
		if err := a.h.Delete(ctx); err == nil || errors.Is(err, blob.ErrNotFound) {
			sh.Discard()
			removeLocal(a.Path)
			m.idx.delete(a.Name)
			m.Stats.Deletes.Add(1) // before the shard stops being active, so whoever sees it gone sees the count
			m.removeActive(a)
			done = true
			return true, nil
		} else if ctx.Err() == nil {
			m.log.Warn("could not delete the manifest of a shard the app never wrote to, keeping it", "shard", a.Name, "err", err)
		}
	}
	if deleteIt {
		// No final delta: the tail goes with the journal.
		defer sh.Discard() // the shard is deleted, or resumed with the tail handed on
		resume := func() error { return m.resume(ctx, a, sh) }
		if err := m.deleteReleased(ctx, a.Name, a.Path, a.h, resume); err != nil {
			return false, err
		}
		m.Stats.AutoDeletes.Add(1) // before the shard stops being active, as Deletes is
		m.removeActive(a)
		done = true
		return true, nil
	}
	// A tail is a delta by the time the shard is cached, so the lease's journal range
	// never has to cover a shard that isn't active.
	if err := a.h.WriteDelta(ctx, sh, shardfile.Hooks{}); err != nil {
		if rerr := m.resume(ctx, a, sh); rerr != nil {
			m.log.Error("could not resume capture after a failed delta", "shard", a.Name, "err", rerr)
		}
		return false, err
	}
	if err := m.putRow(a, stateCached, a.h.Manifest.LastPosition()); err != nil {
		return false, err
	}
	m.removeActive(a)
	done = true
	return true, nil
}

// holdOff delays the next attempt to deactivate a shard that couldn't be.
func (m *Manager) holdOff(a *ActiveShard) {
	m.mu.Lock()
	a.delay = m.now().Add(m.opts.HoldOff)
	m.mu.Unlock()
}

// evict makes room: the shard's manifest is released by compare-and-swap (the
// delta was written when it was deactivated), then its files are deleted.
func (m *Manager) evict(ctx context.Context, row indexRow) error {
	unlock := m.locks.lock(row.Name)
	defer unlock()
	m.mu.Lock()
	_, active := m.active[row.Name]
	m.mu.Unlock()
	if active {
		return nil
	}
	cur, ok, err := m.idx.get(row.Name)
	if err != nil || !ok || cur.State != stateCached {
		return err
	}
	if held, err := capture.ProbeOnce(cur.Path); err != nil {
		return err
	} else if held {
		return fmt.Errorf("%w: %s", ErrAppHolds, cur.Name)
	}
	h, err := m.n.Files.Attach(ctx, cur.Name)
	switch {
	case errors.Is(err, blob.ErrNotFound):
	case err != nil:
		return err
	case h.Manifest.Owner == m.n.Name:
		if err := h.ReleaseOwnership(ctx); err != nil && !errors.Is(err, blob.ErrConflict) {
			return err // storage trouble: keep the files, try again later
		}
	}
	removeLocal(cur.Path)
	return m.idx.delete(cur.Name)
}

// evictIfNeeded removes the least recently used cached shards while the cache
// is over its limit.
func (m *Manager) evictIfNeeded(ctx context.Context) {
	limit := m.cacheLimit.Load()
	if limit <= 0 {
		return
	}
	for {
		used, err := m.idx.cachedBytes()
		if err != nil || used <= limit {
			return
		}
		rows, err := m.idx.leastRecentlyUsed(16)
		if err != nil || len(rows) == 0 {
			return
		}
		progress := false
		for _, r := range rows {
			if err := m.evict(ctx, r); err != nil {
				m.log.Warn("could not evict a cached shard", "shard", r.Name, "err", err)
				continue
			}
			progress = true
			if used, err = m.idx.cachedBytes(); err != nil || used <= limit {
				return
			}
		}
		if !progress {
			return
		}
	}
}

// Delete removes a shard: it is deactivated, its manifest is deleted by
// conditional delete, then its generation's LTX files and the local files. A shard in use, or one the app still has open, is
// refused with ErrBusy. An auto-delete shard is deleted this way too when it
// deactivates (deleteReleased).
func (m *Manager) Delete(ctx context.Context, target string) error {
	kind, _, err := ParseTarget(target)
	if err != nil {
		return err
	}
	if _, err := m.opts.Kinds.Lookup(kind); err != nil {
		return err
	}
	unlock := m.locks.lock(target)
	defer unlock()
	var path string
	var resume func() error
	m.mu.Lock()
	a := m.active[target]
	if a != nil && a.refs > 0 {
		m.mu.Unlock()
		return fmt.Errorf("%w: %s has requests or connections", ErrBusy, target)
	}
	if a != nil {
		a.closing = true
	}
	m.mu.Unlock()
	if a != nil {
		defer func() {
			m.mu.Lock()
			a.closing = false
			m.mu.Unlock()
		}()
		a.opMu.Lock()
		defer a.opMu.Unlock()
		path = a.Path
		sh := a.cap.Load()
		released, err := sh.Release(ctx)
		if err != nil || !released {
			if err == nil {
				err = fmt.Errorf("%w: the app has %s open", ErrBusy, target)
			}
			return err
		}
		m.n.Untrack(sh)
		resume = func() error { return m.resume(ctx, a, sh) }
		defer sh.Discard() // the shard is deleted, or resumed with the tail handed on
	} else if row, ok, err := m.idx.get(target); err != nil {
		return err
	} else if ok && row.Path != "" {
		path = row.Path
		if held, err := capture.ProbeOnce(path); err != nil || held {
			if err == nil {
				err = fmt.Errorf("%w: the app has %s open", ErrBusy, target)
			}
			return err
		}
	}

	var h *shardfile.Handle
	if a != nil {
		h = a.h
	}
	if err := m.deleteReleased(ctx, target, path, h, resume); err != nil {
		return err
	}
	if a != nil {
		m.removeActive(a)
	}
	return nil
}

// deleteReleased deletes a shard that nothing has open: its manifest by
// conditional delete (at h's version if there is a handle, which an active shard
// has), then its generation's LTX files, then the local files at path and the
// cache index row. If the manifest is still there afterwards, nothing was
// deleted and resume (if given) starts capture again.
//
// A crash after the manifest is deleted leaves LTX files that nothing refers to
// in blob storage, and the local files, which the next start finds in the index
// as left by an earlier process and discards. A crash before it leaves the shard
// as it was.
func (m *Manager) deleteReleased(ctx context.Context, target, path string, h *shardfile.Handle, resume func() error) error {
	var err error
	if h != nil {
		err = h.DeleteShard(ctx)
	} else {
		err = m.n.Files.DeleteShard(ctx, target)
	}
	if err != nil {
		// If the manifest is gone the shard is deleted, whatever else failed.
		if _, _, gerr := m.n.Files.GetManifest(ctx, target); !errors.Is(gerr, blob.ErrNotFound) {
			if resume != nil {
				if rerr := resume(); rerr != nil {
					m.log.Error("could not resume capture after a failed delete", "shard", target, "err", rerr)
				}
			}
			return err
		}
		m.log.Error("the shard's manifest is deleted but some of its files are not", "shard", target, "err", err)
	}
	if path != "" {
		removeLocal(path)
	}
	m.idx.delete(target)
	return nil
}

// autoDeletes reports whether the shard is deleted when it deactivates and has
// no rows: its kind has auto_delete and so did the kind when the shard was
// created. Either alone is not enough, so that marking a kind auto_delete can't
// delete shards written before, and a shard created as auto-delete is kept once
// its kind drops the flag. The first time a shard is kept for that, it is logged.
func (a *ActiveShard) autoDeletes() bool {
	k, err := a.m.opts.Kinds.Lookup(a.Kind)
	if err != nil || k.AutoDelete == a.autoDelete {
		return err == nil && k.AutoDelete
	}
	if a.keptNoted.CompareAndSwap(false, true) {
		if k.AutoDelete {
			a.m.log.Warn("keeping a shard that is deactivating: its kind is auto_delete, but the shard was created before that", "shard", a.Name)
		} else {
			a.m.log.Warn("keeping a shard that is deactivating: it was created as auto-delete, but its kind no longer is", "shard", a.Name)
		}
	}
	return false
}

// janitorLoop deactivates idle shards and evicts from the cache.
func (m *Manager) janitorLoop(ctx context.Context) {
	tick, stop := m.opts.Clock.NewTicker(m.opts.Janitor)
	defer stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
		}
		m.deactivateIdle(ctx)
		m.evictIfNeeded(ctx)
		m.sweepHeld()
		m.Stats.Janitor.Add(1)
	}
}

func (m *Manager) idleShards(now time.Time) []*ActiveShard {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*ActiveShard
	for _, a := range m.active {
		if a.refs > 0 || a.closing || now.Before(a.delay) {
			continue
		}
		idle := DefaultIdleTimeout
		if k, err := m.opts.Kinds.Lookup(a.Kind); err == nil {
			idle = k.IdleTimeout
		}
		if now.Sub(a.lastUse) >= idle {
			out = append(out, a)
		}
	}
	return out
}

// deactivateLimit is how many idle shards are deactivated at once. Each mostly
// waits: for a flush cycle (shared by all of them), then for a delta and a
// manifest write in blob storage, about half a second in all. At 8 a node could
// only let go of about 20 shards a second, less than players leave after a
// launch evening, and idle shards keep their descriptors and memory meanwhile.
const deactivateLimit = 256

// backgroundLimit is how many deltas and rebases run at once. Each is a few blob
// storage requests (about 100 ms) and a little CPU. Every active shard needs a
// delta every few minutes: 10,000 active shards need about 55 a second, so the
// limit has to allow well over that.
const backgroundLimit = 64

// rebaseLimit is how many rebases run at once on the node: each writes (and
// uploads) a file the size of the shard's database in the data directory.
const rebaseLimit = 4

func (m *Manager) deactivateIdle(ctx context.Context) {
	idle := m.idleShards(m.now())
	var g errgroup.Group
	g.SetLimit(deactivateLimit)
	for _, a := range idle {
		g.Go(func() error {
			if m.msgs.holds(ctx, a) {
				return nil // undelivered messages keep the shard active
			}
			if _, err := m.deactivate(ctx, a); err != nil && ctx.Err() == nil {
				m.log.Warn("could not deactivate an idle shard", "shard", a.Name, "err", err)
				m.holdOff(a)
			}
			return nil // independent: one shard failing must not stop the others
		})
	}
	g.Wait()
}

// backgroundLoop takes the snapshots of active shards: a delta for a tail that
// has aged, or a rebase instead when the shard's base is old or it has many
// files (docs/internals.md, Node journals and snapshots).
func (m *Manager) backgroundLoop(ctx context.Context) {
	t := time.NewTicker(m.opts.Background)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		m.mu.Lock()
		list := make([]*ActiveShard, 0, len(m.active))
		for _, a := range m.active {
			list = append(list, a)
		}
		m.mu.Unlock()
		var total int64
		for _, a := range list {
			if sh := a.cap.Load(); sh != nil {
				total += sh.TailBytes()
			}
		}
		m.Stats.TailBytes.Store(total)
		if total > m.Stats.TailBytesMax.Load() {
			m.Stats.TailBytesMax.Store(total)
		}
		var g errgroup.Group
		g.SetLimit(backgroundLimit)
		for _, a := range list {
			if !a.opMu.TryLock() {
				continue // being deactivated, or busy with its own delta
			}
			g.Go(func() error {
				defer a.opMu.Unlock()
				sh := a.cap.Load()
				if sh == nil {
					return nil
				}
				policy := shardfile.Policy{MaxFiles: m.opts.MaxFiles, MaxBytes: m.opts.DeltaSize}
				if a.h.RebaseDue(policy) {
					// A rebase moves the whole database through a temporary file, so only
					// a few run at once whatever the background limit.
					select {
					case m.rebases <- struct{}{}:
						defer func() { <-m.rebases }()
					case <-ctx.Done():
						return nil
					}
				}
				// The tail is what tells an adopter a shard may have work, so a shard with a live outbox row gets its
				// wake hint before the snapshot takes the tail away.
				hooks := shardfile.Hooks{AfterFileWrite: func() error { return m.msgs.hintLiveOutbox(ctx, a) }}
				if _, err := a.h.MaybeSnapshot(ctx, sh, m.opts.DeltaAge, policy, hooks); err != nil && ctx.Err() == nil {
					m.log.Warn("snapshot failed", "shard", a.Name, "err", err)
				}
				return nil // independent children: errors are logged per shard
			})
		}
		g.Wait()
		m.Stats.Background.Add(1)
	}
}

// tailAlertAge is how old a tail gets before it raises the alert: three times
// the age at which a delta is written, so a healthy shard never raises it.
func tailAlertAge(deltaAge time.Duration) time.Duration { return 3 * deltaAge }

// JournalExpiry is how long the node keeps its journal segments before its
// sweeper deletes them: 24 hours, or four times the tail alert age if that is
// longer, so the alert comes in time to act.
func JournalExpiry(deltaAge time.Duration) time.Duration {
	return max(24*time.Hour, 4*tailAlertAge(deltaAge))
}

// tailAlertEvery is how often a shard repeats the tail-age alert.
const tailAlertEvery = time.Minute

// alertLoop raises the tail-age alert on its own timer, apart from the
// background loop: a shard whose delta keeps failing (or hangs, holding its
// operation lock) must still raise it.
func (m *Manager) alertLoop(ctx context.Context) {
	t := time.NewTicker(m.opts.Background)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		m.mu.Lock()
		list := make([]*ActiveShard, 0, len(m.active))
		for _, a := range m.active {
			list = append(list, a)
		}
		m.mu.Unlock()
		for _, a := range list {
			m.checkTailAge(a)
		}
	}
}

// checkTailAge raises an alert (an error-level log line) if the shard's oldest
// tail file is older than the tail alert age: its commits are only in journal
// segments, which expire, and deltas should have replaced them long ago. Only
// alertLoop calls it.
func (m *Manager) checkTailAge(a *ActiveShard) {
	sh := a.cap.Load()
	if sh == nil {
		return
	}
	n, oldest := sh.TailInfo()
	if n == 0 {
		return
	}
	age := time.Since(oldest)
	if age < tailAlertAge(m.opts.DeltaAge) || time.Since(a.alertedAt) < tailAlertEvery {
		return
	}
	a.alertedAt = time.Now()
	m.Stats.TailAlerts.Add(1)
	m.log.Error("a shard's tail is too old, its deltas are not being written", "shard", a.Name, "tail_age", age, "alert_age", tailAlertAge(m.opts.DeltaAge))
}

// Active returns the names of the active shards.
func (m *Manager) Active() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.active))
	for n := range m.active {
		out = append(out, n)
	}
	return out
}

// Cached returns the names of the deactivated shards on disk.
func (m *Manager) Cached() ([]string, error) {
	rows, err := m.idx.rows(`WHERE state = ?`, stateCached)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Name
	}
	return out, nil
}

// SetCacheLimit changes the disk limit for deactivated shards (0 for none).
func (m *Manager) SetCacheLimit(n int64) { m.cacheLimit.Store(n) }

// Flusher is the flush loop, for the gates.
func (m *Manager) Flusher() *Flusher { return m.flush }

// Close stops the loops and deactivates every shard that can be (shards in use
// or open in the app stay as they are). It does not release the node's lease.
func (m *Manager) Close(ctx context.Context) error {
	m.cancel()
	m.g.Wait()
	m.mu.Lock()
	list := make([]*ActiveShard, 0, len(m.active))
	for _, a := range m.active {
		list = append(list, a)
	}
	m.mu.Unlock()
	var errs []error
	for _, a := range list {
		// A transient storage failure (the final sync, the delta or a hint) is
		// tried again until the shutdown timeout runs out. Other failures are not.
		wait := 100 * time.Millisecond
		for {
			ok, err := m.deactivate(ctx, a)
			if err != nil && blob.Transient(err) && ctx.Err() == nil {
				m.log.Warn("could not deactivate a shard at shutdown, trying again", "shard", a.Name, "err", err)
				select {
				case <-time.After(wait):
					wait = min(2*wait, 2*time.Second)
					continue
				case <-ctx.Done():
				}
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", a.Name, err))
			} else if !ok {
				errs = append(errs, fmt.Errorf("%s: %w", a.Name, ErrBusy))
			}
			break
		}
	}
	m.idx.close()
	return errors.Join(errs...)
}
