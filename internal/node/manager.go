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
	// ErrKindChanged: the shard holds data that its kind would no longer ship (the
	// kind became ephemeral after the shard was written).
	ErrKindChanged = errors.New("shard has data but its kind is now ephemeral")
)

// ephemeralDir is the directory of Data that holds the files of ephemeral shards.
// The node empties it at startup, and no kind may have its name.
const ephemeralDir = "ephemeral"

// ManagerOptions configure a Manager.
type ManagerOptions struct {
	Kinds      *Kinds
	Data       string        // directory of shard files, which is the local cache
	Meta       string        // Litestream's metadata directories (tmpfs in production)
	CacheLimit int64         // bytes of deactivated shards to keep, 0 for no limit
	Flush      time.Duration // flush interval, segment.DefaultFlush if zero
	DeltaAge   time.Duration // how old a tail gets before a delta is written, shardfile.DefaultDeltaAge if zero
	TailAlert  time.Duration // a shard whose oldest tail is older than this raises an alert, 15 min if zero
	MaxFiles   int           // files a manifest may list before they are merged, shardfile.DefaultMaxFiles if zero
	Janitor    time.Duration // how often idle shards are looked for, 1 s if zero
	Background time.Duration // how often deltas and merges are looked for, 2 s if zero
	HoldOff    time.Duration // how long to leave a shard alone after it couldn't be deactivated, 5 s if zero
	Logger     *slog.Logger
}

// Stats counts what the manager has done, for tests and metrics.
type Stats struct {
	Activations atomic.Int64 // shards activated
	Restores    atomic.Int64 // ... by restoring from blob storage
	CachedHits  atomic.Int64 // ... from a cached file
	Creates     atomic.Int64 // shards created (activated with no manifest)
	Deletes     atomic.Int64 // shards deleted on deactivation because the app never wrote to them
	Discards    atomic.Int64 // cached files discarded as out of date
	TailAlerts  atomic.Int64 // alerts raised for a tail older than the alert age
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
	cacheLimit atomic.Int64
	Stats      Stats

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
	if opts.TailAlert <= 0 {
		opts.TailAlert = 15 * time.Minute
	}
	if opts.MaxFiles <= 0 {
		opts.MaxFiles = shardfile.DefaultMaxFiles
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
	// Litestream's metadata never needs to outlive the process, and neither does
	// the state of ephemeral shards: the next activation starts empty.
	for _, d := range []string{opts.Meta, filepath.Join(opts.Data, ephemeralDir)} {
		if err := os.RemoveAll(d); err != nil {
			return nil, err
		}
	}
	for _, d := range []string{opts.Data, opts.Meta} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	idx, err := openIndex(filepath.Join(opts.Data, ".index.db"))
	if err != nil {
		return nil, err
	}
	m := &Manager{n: n, opts: opts, log: opts.Logger, idx: idx, active: map[string]*ActiveShard{}}
	m.cacheLimit.Store(opts.CacheLimit)
	m.ctx, m.cancel = context.WithCancel(context.Background())
	if err := m.recover(); err != nil {
		idx.close()
		return nil, err
	}
	m.flush = newFlusher(m, opts.Flush)
	m.start(m.alertLoop)
	m.start(m.flush.loop)
	m.start(m.janitorLoop)
	m.start(m.backgroundLoop)
	return m, nil
}

func (m *Manager) start(f func(ctx context.Context)) {
	m.g.Go(func() error {
		f(m.ctx)
		return nil
	})
}

// recover deals with the data directory after a start. Rows left "active" by
// an earlier process are files that may hold commits that never reached the
// journal, so they are discarded (unless another process has them open). Files the
// index doesn't know are logged, never deleted.
func (m *Manager) recover() error {
	rows, err := m.idx.all()
	if err != nil {
		return err
	}
	known := map[string]bool{filepath.Join(m.opts.Data, ".index.db"): true}
	for _, r := range rows {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			known[r.Path+suffix] = true
		}
		if r.State != stateActive {
			continue
		}
		held, err := capture.ProbeOnce(r.Path)
		if err != nil || held {
			m.log.Error("a shard left active by the previous process is still open in another process, leaving its files alone", "shard", r.Name, "path", r.Path, "err", err)
			continue
		}
		removeLocal(r.Path)
		if err := m.idx.delete(r.Name); err != nil {
			return err
		}
	}
	return filepath.WalkDir(m.opts.Data, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || known[p] || strings.HasPrefix(filepath.Base(p), ".index.db") {
			return nil
		}
		// A file from an active row that was just discarded is gone already.
		m.log.Error("file in the data directory is not one the node knows", "path", p)
		return nil
	})
}

func removeLocal(path string) {
	for _, p := range []string{path, path + "-wal", path + "-shm", path + ".restoring"} {
		os.RemoveAll(p)
	}
}

// pathFor is the opaque name of a shard's file: it carries the generation, so
// a shard deleted and created again never reuses a file. Kinds and ids need no
// escaping (see ParseTarget).
func (m *Manager) pathFor(kind, id, generation string, ephemeral bool) string {
	if ephemeral {
		return filepath.Join(m.opts.Data, ephemeralDir, kind, id+"."+generation+".db")
	}
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
	ephemeral      bool       // an ephemeral kind: nothing is captured or shipped (cap stays nil)
	opMu           sync.Mutex // delta, merge and deactivation
	h              *shardfile.Handle
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
	return err == nil && k.Sync && !k.Ephemeral
}

// Release gives up the hold.
func (r *Ref) Release() {
	r.once.Do(func() {
		m, a := r.m, r.a
		m.mu.Lock()
		a.refs--
		a.lastUse = time.Now()
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
	a.lastUse = time.Now()
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
	a.lastUse = time.Now()
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
}

func (m *Manager) putRow(a *ActiveShard, state string, pos uint64) error {
	row := indexRow{Name: a.Name, Path: a.Path, Generation: a.Gen, Position: pos, Epoch: m.n.Keeper.Lease().Epoch, State: state, LastUsed: time.Now()}
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

// activate follows design.md's Activation for a single node. The caller holds
// no lock. Under the shard's lock it re-checks that nobody else has activated
// the shard, then:
//
//  1. a cached file this node still owns is opened again, with no blob storage
//     call if it was cached in this epoch;
//  2. an unowned shard is claimed by compare-and-swap on its manifest and
//     restored, and a manifest this node owns in its current epoch is used as it is;
//  3. a shard with no manifest is created: a conditional create of its manifest
//     claims it, and its file starts empty. If another node creates it first,
//     it is claimed like any other shard.
//
// A shard of an ephemeral kind goes through the same steps for its manifest, then
// gets an empty file and nothing else: no restore, no capture.
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
	a := &ActiveShard{m: m, Name: target, Kind: kind, ID: id, ephemeral: k.Ephemeral}

	var cachedRow *indexRow // a cached file of a shard whose kind has become ephemeral
	if row, ok, err := m.idx.get(target); err != nil {
		return err
	} else if ok {
		switch {
		case row.State == stateCached && k.Ephemeral:
			cachedRow = &row
		case row.State == stateCached:
			done, err := m.activateCached(ctx, a, row)
			if done || err != nil {
				return err
			}
		default:
			// Left active by an earlier process (recover has dealt with the ones
			// it could): the file may be ahead of what was flushed.
			if err := m.discardLocal(row); err != nil {
				return err
			}
		}
	}

	created := false
	h, err := m.n.Files.Attach(ctx, target)
	if errors.Is(err, blob.ErrNotFound) {
		h, created, err = m.n.Files.CreateAs(ctx, target, k.Ephemeral)
	}
	if err != nil {
		return err
	}
	if k.Ephemeral && !h.Manifest.Ephemeral && (len(h.Manifest.Files) > 0 || h.Manifest.Journal != "") {
		// Activating it would stop shipping the writes of a shard that has data.
		m.log.Error("refusing to activate a shard that has data: its kind is now ephemeral, which would stop shipping its writes. Make the kind durable again, or delete the shard",
			"shard", target, "kind", kind, "files", len(h.Manifest.Files), "journal", h.Manifest.Journal)
		return fmt.Errorf("%w: %s", ErrKindChanged, target)
	}
	noTail := h.Manifest.Journal == "" // known before the claim sets it
	if !created {
		if k.Ephemeral {
			err = h.ClaimAs(ctx, true)
		} else {
			err = h.Claim(ctx) // also takes an ephemeral manifest over as a durable one
		}
		if err != nil {
			return err
		}
	}
	a.Gen, a.Path = h.Manifest.Generation, m.pathFor(kind, id, h.Manifest.Generation, k.Ephemeral)
	if err := m.clearLocal(a.Path); err != nil {
		return err
	}
	if cachedRow != nil {
		if err := m.discardLocal(*cachedRow); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Dir(a.Path), 0o755); err != nil {
		return err
	}
	a.h = h
	if k.Ephemeral {
		return m.startEphemeral(a, created)
	}
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

// startEphemeral gives an ephemeral shard its empty file and registers it. The
// node keeps no handle on the file and watches nothing: the app has it to itself.
func (m *Manager) startEphemeral(a *ActiveShard, created bool) error {
	f, err := os.OpenFile(a.Path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	f.Close()
	if created {
		m.Stats.Creates.Add(1)
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
	var h *shardfile.Handle
	if row.Epoch == m.n.Keeper.Lease().Epoch {
		var man shardfile.Manifest
		if err := json.Unmarshal([]byte(row.Manifest), &man); err != nil {
			return false, m.discardLocal(row)
		}
		h = m.n.Files.HandleFor(a.Name, &man, blob.ETag(row.ETag))
	} else {
		// The file is from an earlier life of the node. It is still good if it ends
		// where the manifest's files end, and is reclaimed by CAS.
		h, err = m.n.Files.Attach(ctx, a.Name)
		if errors.Is(err, blob.ErrNotFound) {
			return false, m.discardLocal(row)
		} else if err != nil {
			return false, err
		}
		if h.Manifest.Generation != row.Generation || h.Manifest.LastPosition() != row.Position {
			m.log.Warn("cached shard doesn't match its manifest, discarding it", "shard", a.Name, "file_position", row.Position, "manifest_position", h.Manifest.LastPosition())
			return false, m.discardLocal(row)
		}
		if err := h.Claim(ctx); err != nil {
			return false, err
		}
	}
	a.h, a.Gen, a.Path = h, row.Generation, row.Path
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
// sync before the app can open it. For a restored (or cached) shard that sync
// is a snapshot of exactly the restored state and isn't uploaded. A new shard
// (restored is nil) gets an empty file first, and its first sync, of an empty
// database holding only Litestream's tables, is its first position.
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
		MetaRoot: m.opts.Meta, Writer: m.n.Writer, Restored: restored, Logger: m.log, Durable: m.flush.WaitCycle,
	})
	if err != nil {
		return err
	}
	if err := sh.Capture(ctx); err != nil {
		sh.Close(ctx)
		return err
	}
	m.n.Track(sh)
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
	return m.startCapture(ctx, a, &capture.Restored{Position: old.Position(), Tail: old.Tail()})
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

	if a.ephemeral {
		return m.deactivateEphemeral(ctx, a, &done)
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
			removeLocal(a.Path)
			m.idx.delete(a.Name)
			m.removeActive(a)
			m.Stats.Deletes.Add(1)
			done = true
			return true, nil
		} else if ctx.Err() == nil {
			m.log.Warn("could not delete the manifest of a shard the app never wrote to, keeping it", "shard", a.Name, "err", err)
		}
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

// deactivateEphemeral lets go of an idle ephemeral shard: if no other process has
// its file open the manifest is deleted by conditional delete (a conflict keeps the
// shard) and then the file. No final sync, no delta and no cached file. The caller holds the
// shard's lock and has marked it closing.
func (m *Manager) deactivateEphemeral(ctx context.Context, a *ActiveShard, done *bool) (bool, error) {
	if held, err := capture.ProbeOnce(a.Path); err != nil {
		return false, err
	} else if held {
		m.holdOff(a)
		m.log.Warn("the app still has the shard open, keeping it active", "shard", a.Name)
		return false, nil
	}
	if err := a.h.Delete(ctx); err != nil && !errors.Is(err, blob.ErrNotFound) {
		if errors.Is(err, blob.ErrConflict) {
			m.log.Warn("the manifest of an ephemeral shard was changed by somebody else, keeping the shard", "shard", a.Name)
		}
		return false, err
	}
	removeLocal(a.Path)
	m.removeActive(a)
	*done = true
	return true, nil
}

// holdOff delays the next attempt to deactivate a shard that couldn't be.
func (m *Manager) holdOff(a *ActiveShard) {
	m.mu.Lock()
	a.delay = time.Now().Add(m.opts.HoldOff)
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
// refused with ErrBusy. An active ephemeral shard has no files in blob storage, so
// its manifest and local file are deleted, as at deactivation.
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
		if a.ephemeral {
			done := false
			_, err := m.deactivateEphemeral(ctx, a, &done)
			if err == nil && !done {
				err = fmt.Errorf("%w: the app has %s open", ErrBusy, target)
			}
			return err
		}
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

	if err := m.n.Files.DeleteShard(ctx, target); err != nil {
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
	if a != nil {
		m.removeActive(a)
	}
	return nil
}

// janitorLoop deactivates idle shards and evicts from the cache.
func (m *Manager) janitorLoop(ctx context.Context) {
	t := time.NewTicker(m.opts.Janitor)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		m.deactivateIdle(ctx)
		m.evictIfNeeded(ctx)
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

// backgroundLimit is how many deltas and merges run at once. Each is a few blob
// storage requests (about 100 ms) and a little CPU. Every active shard needs a
// delta every few minutes: 10,000 active shards need about 55 a second, so the
// limit has to allow well over that.
const backgroundLimit = 64

func (m *Manager) deactivateIdle(ctx context.Context) {
	idle := m.idleShards(time.Now())
	var g errgroup.Group
	g.SetLimit(deactivateLimit)
	for _, a := range idle {
		g.Go(func() error {
			if _, err := m.deactivate(ctx, a); err != nil && ctx.Err() == nil {
				m.log.Warn("could not deactivate an idle shard", "shard", a.Name, "err", err)
				m.holdOff(a)
			}
			return nil // independent: one shard failing must not stop the others
		})
	}
	g.Wait()
}

// backgroundLoop writes deltas for tails that have aged and merges long
// manifests, for the active shards.
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
				if _, err := a.h.MaybeWriteDelta(ctx, sh, m.opts.DeltaAge, shardfile.Hooks{}); err != nil && ctx.Err() == nil {
					m.log.Warn("delta failed", "shard", a.Name, "err", err)
					return nil
				}
				if _, err := a.h.MergeIfNeeded(ctx, m.opts.MaxFiles, shardfile.Hooks{}); err != nil && ctx.Err() == nil {
					m.log.Warn("merge failed", "shard", a.Name, "err", err)
				}
				return nil // independent children: errors are logged per shard
			})
		}
		g.Wait()
	}
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
	tail := sh.Tail()
	if len(tail) == 0 {
		return
	}
	age := time.Since(tail[0].At)
	if age < m.opts.TailAlert || time.Since(a.alertedAt) < tailAlertEvery {
		return
	}
	a.alertedAt = time.Now()
	m.Stats.TailAlerts.Add(1)
	m.log.Error("a shard's tail is too old, its deltas are not being written", "shard", a.Name, "tail_age", age, "alert_age", m.opts.TailAlert)
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
		if ok, err := m.deactivate(ctx, a); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", a.Name, err))
		} else if !ok {
			errs = append(errs, fmt.Errorf("%s: %w", a.Name, ErrBusy))
		}
	}
	m.idx.close()
	return errors.Join(errs...)
}
