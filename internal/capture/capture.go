// Package capture wraps Litestream for one shard. The node drives it: there
// are no timers and no polling, and every sync goes into the node's pending
// segment through the shard's ReplicaClient.
//
// Litestream is started afresh on every activation of a shard: a new metadata
// directory (on tmpfs in production) and a client that reports no files, so
// Litestream numbers its commits within the activation. Those numbers mean
// nothing outside it. A new shard starts from nothing and its first sync is its
// first position. A restored shard starts from a seed (see seed.go): Litestream
// is told, with a file of the node's own, that the database as restored is
// already synced, so it never reads the database to snapshot it. The shard's own
// positions are assigned by the client as commits are captured.
package capture

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/spool"
)

// Restored describes a shard that was restored from its files before this
// activation. Litestream is started from that state (see seed) and doesn't
// snapshot it.
type Restored struct {
	Position uint64 // the shard's position as restored: its manifest files plus Tail
	// Tail is the commits already in the node journal but not yet in a delta. New
	// takes over the references of its files: they are closed when the tail is
	// dropped or discarded, or at once if New fails.
	Tail []TailFile
}

// Writer is what a shard needs of the node's journal (segment.Writer).
type Writer interface {
	NextSeq() uint64
	AppendFile(shard, generation string, position uint64, ltxFile *spool.File) error
	Flush(ctx context.Context) error
}

// Config describes one shard to capture.
type Config struct {
	Name       string // kind/id, for example users/42
	Path       string // the SQLite file
	Generation string
	// MetaRoot holds one Litestream metadata directory per shard. The node
	// keeps it on tmpfs: nothing in it needs to outlive the node process.
	MetaRoot string
	Writer   Writer
	Restored *Restored // set for a shard restored from its files, nil for a new one
	// A commit file is held in memory up to SpoolLimit bytes (spool.DefaultLimit
	// if zero) and in a temporary file in TempDir beyond that: a single commit
	// can be as large as the database. TempDir is never the scratch directory,
	// which is memory. Without one a commit larger than SpoolLimit fails.
	TempDir    string
	SpoolLimit int
	// At, if set, is called at named points of an activation of a restored shard:
	// "seed-folded", "seed-wal", "seed-file" and "litestream-opened". The fault
	// tests kill the process at them.
	At     func(point string)
	Logger *slog.Logger
	Tune   func(*litestream.DB) // optional, to change Litestream settings in tests
	// Durable waits until everything queued in Writer so far is in blob storage.
	// Release and Close call it after their final sync. The node sets it to wait
	// for its next flush cycle, so deactivations share segments with the rest;
	// by default the shard flushes the writer itself.
	Durable func(ctx context.Context) error
}

func (c Config) spoolLimit() int {
	if c.SpoolLimit > 0 {
		return c.SpoolLimit
	}
	return spool.DefaultLimit
}

// Shard is one captured SQLite database. Its lock (mu) guards Litestream's
// database, the mapped wal-index and the client's state: everything that
// touches Litestream runs holding it, one at a time, on the caller's goroutine
// (do). Litestream calls the client from inside a sync, a snapshot or a close,
// so those calls run under the lock too. The activation runs from New until
// Close, or a Release that leaves capture closed (or Abandon). Once it is over,
// the shard is its caller's alone (whoever closed or released it): Tail,
// DropTail, Discard and DB still take the lock (call), but nothing else waits
// for it.
type Shard struct {
	cfg     Config
	metaDir string
	client  *client

	// mu is the lock: a channel with room for one, so a caller can stop waiting
	// for it when its context ends.
	mu   chan struct{}
	over atomic.Bool // the activation is over: do runs nothing more

	// Under mu (see above).
	db *litestream.DB
	// wi maps the wal-index header (see WalIndex). It is made before Litestream
	// opens the shard and dropped after Litestream closes it. Only holders of mu
	// changes it, under wiMu, so Header can read it from any goroutine.
	wiMu sync.RWMutex
	wi   *WalIndex
	// kept is the Litestream commit number of the level 0 file DropTail has left
	// because it was the newest: the seed of a restored shard to begin with.
	kept uint64
}

// MetaDir is a shard's Litestream metadata directory under root. It has the
// same name as the shard's file (kind/id.generation, see the node's pathFor),
// so the two are easy to match up, and ids that differ only by case never
// share a directory, even on a case-insensitive filesystem.
func MetaDir(root, shard, generation string) string {
	return filepath.Join(root, shard+"."+generation)
}

// New starts capturing cfg.Path in a fresh metadata directory. The file must
// exist: a new shard's is created empty by the node, before the app gets it.
func New(cfg Config) (sh *Shard, err error) {
	defer func() {
		if err != nil && cfg.Restored != nil {
			CloseTail(cfg.Restored.Tail)
		}
	}()
	if cfg.Writer == nil {
		return nil, fmt.Errorf("capture: writer required")
	}
	if cfg.MetaRoot == "" {
		return nil, fmt.Errorf("capture: metadata root required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	s := &Shard{cfg: cfg, metaDir: MetaDir(cfg.MetaRoot, cfg.Name, cfg.Generation), client: newClient(cfg), mu: make(chan struct{}, 1)}
	// A fresh start: whatever an earlier activation left is gone.
	if err := os.RemoveAll(s.metaDir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(s.metaDir, 0o755); err != nil {
		return nil, err
	}
	if cfg.Restored != nil {
		s.kept = uint64(seedTXID)
	}
	if s.db, err = s.openLitestream(cfg.Restored != nil); err != nil {
		os.RemoveAll(s.metaDir)
		return nil, err
	}
	return s, nil
}

// do runs fn holding the lock and returns its error, or ErrClosed once the
// activation is over. It gives up only while it waits for the lock, when ctx
// ends. fn runs to the end once it has started, and ctx only ends the wait for
// whatever holds the lock before it.
//
// A panic in fn ends the activation before it goes on: a caller that recovers
// it (net/http recovers a handler's) leaves Litestream in whatever state the
// panic left, so nothing may use the shard again.
func (s *Shard) do(ctx context.Context, fn func() error) error {
	select {
	case s.mu <- struct{}{}:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	defer func() { <-s.mu }()
	if s.over.Load() {
		return ErrClosed
	}
	finished := false
	defer func() {
		if !finished {
			s.over.Store(true)
		}
	}()
	err := fn()
	finished = true
	return err
}

// call runs fn holding the lock, whether or not the activation is over (the
// shard is its caller's alone then, see Shard).
func (s *Shard) call(fn func()) {
	s.mu <- struct{}{}
	defer func() { <-s.mu }()
	fn()
}

// end marks the activation over and drops the mapping. Its caller holds the lock.
func (s *Shard) end() {
	s.over.Store(true)
	s.unmap()
}

// Header reads the shard's wal-index header: it differs from an earlier read
// if the database was committed to since (and may differ without that, never
// the other way round). An error means the header can't be trusted, and
// callers should treat the shard as changed: Litestream hasn't opened the
// database yet, or the shard is closed (ErrClosed). The mapping is read on the
// caller's goroutine without the lock, so a flush cycle or a gate never waits
// behind a sync, and the shard is never reported busy because of one; the only
// wait is for an unmap, which is a system call.
func (s *Shard) Header() (Header, error) {
	s.wiMu.RLock()
	defer s.wiMu.RUnlock()
	if s.wi == nil {
		return Header{}, ErrClosed
	}
	return s.header()
}

// header reads the mapping. Holders of the lock call it directly (they are the
// only writers of s.wi); others go through Header.
func (s *Shard) header() (Header, error) {
	h, err := s.wi.Read()
	if err == nil && h == (Header{}) {
		err = errors.New("capture: wal-index not initialised")
	}
	return h, err
}

func (s *Shard) at(point string) {
	if s.cfg.At != nil {
		s.cfg.At(point)
	}
}

func (s *Shard) unmap() {
	s.wiMu.Lock()
	defer s.wiMu.Unlock()
	s.wi.Close()
	s.wi = nil
}

// openLitestream opens the shard with a new Litestream database. With seeded
// set the shard is a restored one, and Litestream is given the position to
// continue from (see seed) before it opens. Otherwise the metadata directory
// is as the last activation or sync left it, or empty.
func (s *Shard) openLitestream(seeded bool) (*litestream.DB, error) {
	db := litestream.NewDB(s.cfg.Path)
	db.SetLogger(s.cfg.Logger)
	db.SetMetaPath(s.metaDir)
	if seeded {
		// Nobody else has the shard open: the caller has checked, and nothing in
		// this process has opened it yet, so what seed opens and closes drops no
		// locks.
		if held, err := ProbeOnce(s.cfg.Path); err != nil {
			return nil, err
		} else if held {
			return nil, fmt.Errorf("capture: %s is open in another process", s.cfg.Name)
		}
		if err := seed(s.cfg.Path, db, s.at); err != nil {
			return nil, err
		}
	}
	// The node holds no locks on the shard yet, so the descriptor MapWalIndex
	// opens and closes drops none.
	wi, err := MapWalIndex(s.cfg.Path)
	if err != nil {
		return nil, err
	}
	s.wiMu.Lock()
	s.wi = wi
	s.wiMu.Unlock()
	// The node drives Litestream. No background polling.
	db.MonitorInterval = 0
	// The client only fails for good (the writer is fenced or closed), so
	// Litestream's retries on close, 30 seconds by default, could only hold up
	// a release. One attempt.
	db.ShutdownSyncTimeout = 0
	if s.cfg.Tune != nil {
		s.cfg.Tune(db)
	}
	replica := litestream.NewReplicaWithClient(db, s.client)
	// The replica syncs only when called.
	replica.MonitorEnabled = false
	db.Replica = replica
	if err := db.Open(); err != nil {
		s.unmap()
		return nil, err
	}
	s.at("litestream-opened")
	return db, nil
}

// Capture syncs the shard: new commits become LTX files, and the files are
// queued in the segment writer and in the shard's tail. It returns when they
// are all queued, however many that is, without waiting for any segment. The
// commits are durable once the writer has been flushed after this returns.
//
// SyncAndWait is db.Sync followed by Replica.Sync (see litestream db.go), which
// is what the node wants, so we use it as is. Litestream's sync runs on a
// context that is never cancelled: a sync cut off in the middle of its work
// would leave Litestream unsure what the replica has. It does no network I/O
// (the client only queues), so ctx only ends the wait for whatever holds the
// shard's lock before it.
func (s *Shard) Capture(ctx context.Context) error {
	return s.do(ctx, func() error {
		if s.db == nil {
			return ErrClosed
		}
		return s.db.SyncAndWait(context.Background())
	})
}

// Sync is Capture followed by a wait until the segments holding the commits are
// in blob storage (it flushes the segment writer).
func (s *Shard) Sync(ctx context.Context) error {
	if err := s.Capture(ctx); err != nil {
		return err
	}
	return s.cfg.Writer.Flush(ctx)
}

// durable waits until what is queued is in blob storage.
func (s *Shard) durable(ctx context.Context) error {
	if s.cfg.Durable != nil {
		return s.cfg.Durable(ctx)
	}
	return s.cfg.Writer.Flush(ctx)
}

// closeForRelease is the first step of Release, under the lock: Litestream's
// connection is closed (its final sync queues what is left), and what the
// database looked like just after is read. The mapping is kept: Release
// compares this header with one read after its -shm check. A mapping holds no
// locks, so keeping it costs nothing there.
func (s *Shard) closeForRelease(ctx context.Context, read func(*sql.DB) error) (c closed, err error) {
	if s.db == nil {
		return c, ErrClosed
	}
	// Whether the shard holds anything of the app's, and the caller's read, are
	// read between two syncs. The first captures what was committed before the
	// reads, so if the final sync (Litestream's close) captures anything more,
	// something was written after them.
	serr := s.db.SyncAndWait(context.Background())
	empty, marked, eerr := s.holdsNothing(ctx)
	rerr := errors.New("no read")
	if read != nil {
		rerr = s.use(read)
	}
	pos := s.client.position()
	err = s.db.Close(context.WithoutCancel(ctx))
	s.db = nil
	exact := serr == nil && s.client.position() == pos
	c.Empty = exact && eerr == nil && empty
	c.Marked = c.Empty && marked
	if exact && rerr == nil {
		c.At = pos
	}
	c.hdr, c.hdrErr = s.header()
	return c, err
}

// closed is what closeForRelease learned about the database just after
// Litestream closed it.
type closed struct {
	Released
	// hdr is the wal-index header, to tell whether the app committed after
	// Litestream's final sync.
	hdr    Header
	hdrErr error
}

// MarkedQuery says whether the app has marked the shard for deletion (a
// _shardlite_delete table).
const MarkedQuery = `SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE name = '_shardlite_delete' AND type = 'table')`

// holdsNothing reports whether the database holds nothing of the app's. Either
// the app marked it for deletion (a _shardlite_delete table, whatever else is in
// it), or it has no tables or indexes besides Litestream's own and shardlite's
// (the _shardlite_ tables; an index counts as its table's, so the automatic one
// of a reserved table's text key is ignored), user_version and application_id
// are both 0, and _shardlite_reminders has no rows (a reminder the node gave up
// on is the app's data too). marked says which it was. It asks through Litestream's own
// connection, so the node opens nothing of the shard's (see shm.go for why that
// matters).
func (s *Shard) holdsNothing(ctx context.Context) (empty, marked bool, err error) {
	sqldb := s.db.SQLDB()
	if sqldb == nil {
		return false, false, errors.New("capture: litestream has not opened the database")
	}
	if err := sqldb.QueryRowContext(ctx, MarkedQuery).Scan(&marked); err != nil || marked {
		return marked, marked, err
	}
	var objects, userVersion, applicationID int64
	err = sqldb.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM sqlite_schema WHERE name NOT IN ('_litestream_seq', '_litestream_lock', 'sqlite_sequence') AND tbl_name NOT LIKE '\_shardlite\_%' ESCAPE '\'),
		(SELECT user_version FROM pragma_user_version),
		(SELECT application_id FROM pragma_application_id)`).Scan(&objects, &userVersion, &applicationID)
	if err != nil || objects != 0 || userVersion != 0 || applicationID != 0 {
		return false, false, err
	}
	// Reminders (dead or not) are the app's data.
	var exists bool
	if err := sqldb.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE name = '_shardlite_reminders' AND type = 'table')`).Scan(&exists); err != nil {
		return false, false, err
	}
	if !exists {
		return true, false, nil
	}
	var rows bool
	if err := sqldb.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM _shardlite_reminders)`).Scan(&rows); err != nil || rows {
		return false, false, err
	}
	return true, false, nil
}

// ErrClosed is what Capture, Use and Snapshot return for a shard whose
// database is closed.
var ErrClosed = errors.New("capture: the shard's database is closed")

// Use runs fn with Litestream's connection pool (never a descriptor of the
// node's own), under the shard's lock. So it never runs during a sync, a snapshot
// or the final sync of a release, and what fn commits is captured by the next
// sync. fn must not call the shard.
func (s *Shard) Use(ctx context.Context, fn func(*sql.DB) error) error {
	return s.do(ctx, func() error { return s.use(fn) })
}

// UseAt is Use, and also reports the position fn's reads saw, if they saw
// exactly a captured state. Under the same lock, a sync before fn captures what
// is committed so far, and a sync after it that captures nothing new shows that
// nothing was committed in between, so fn saw exactly the position the first
// sync reached (no other sync runs meanwhile). at is 0 if the second sync
// captured more: fn may have seen commits that weren't captured. A checkpoint
// changes nothing here, since it commits nothing.
func (s *Shard) UseAt(ctx context.Context, fn func(*sql.DB) error) (at uint64, err error) {
	err = s.do(ctx, func() error {
		if s.db == nil {
			return ErrClosed
		}
		if err := s.db.SyncAndWait(context.Background()); err != nil {
			return err
		}
		before := s.client.view.Load().position
		if err := s.use(fn); err != nil {
			return err
		}
		if err := s.db.SyncAndWait(context.Background()); err != nil {
			return err
		}
		if s.client.view.Load().position == before {
			at = before
		}
		return nil
	})
	return at, err
}

// use runs fn on Litestream's connection. The caller holds the lock.
func (s *Shard) use(fn func(*sql.DB) error) error {
	db := s.db
	if db == nil {
		return ErrClosed
	}
	if db.SQLDB() == nil {
		// Litestream opens its connection at the first sync after Open, so a shard
		// whose capture was reopened (a release that did not go through) has none
		// if the sync that followed failed. Sync it now, as the next flush cycle
		// would.
		if err := db.SyncAndWait(context.Background()); err != nil {
			return err
		}
		if db.SQLDB() == nil {
			return ErrClosed
		}
	}
	return fn(db.SQLDB())
}

// Snapshot returns a full snapshot of the shard as an LTX file, and the
// shard's position that it covers: every commit captured up to it, and none
// after, though the app keeps writing while the snapshot is read. The reader
// must be closed: until it is, Litestream holds off its checkpoints.
//
// The position is the one the client has numbered, and it is right because of
// the order of things. In one step under the shard's lock, which keeps syncs out,
// a sync brings Litestream and the client level (every commit Litestream has
// written locally has been through the client), SnapshotReader fixes the
// position it covers (Litestream's own commit number, which is only meaningful
// within this activation) and holds off checkpoints in one step, and the two
// numbers are compared: Litestream's commit number must be the highest the
// client has seen. Only then is the client's position (the position of that
// same commit) returned. A later sync adds commits above both.
func (s *Shard) Snapshot(ctx context.Context) (position uint64, r io.ReadCloser, err error) {
	err = s.do(ctx, func() error {
		db := s.db
		if db == nil {
			return ErrClosed
		}
		if err := db.SyncAndWait(context.WithoutCancel(ctx)); err != nil {
			return err
		}
		txid, pos := s.client.point()
		lpos, rc, err := db.SnapshotReader(ctx)
		if err != nil {
			return fmt.Errorf("capture: snapshot of %s: %w", s.cfg.Name, err)
		}
		if uint64(lpos.TXID) != txid {
			rc.Close()
			return fmt.Errorf("capture: snapshot of %s is at Litestream commit %d, but the last captured is %d", s.cfg.Name, lpos.TXID, txid)
		}
		position, r = pos, rc
		return nil
	})
	return position, r, err
}

// Close ends the activation: a final sync, then Litestream's connection and
// file handles are closed and its metadata directory is deleted. It ends the
// activation whatever ctx says, and returns nil for a shard that is closed or
// released already.
func (s *Shard) Close(ctx context.Context) error {
	open := false
	var err error
	if s.do(context.WithoutCancel(ctx), func() error {
		if open = s.db != nil; open {
			err = s.db.Close(context.WithoutCancel(ctx))
			s.db = nil
		}
		s.end()
		return nil
	}) != nil {
		return nil
	}
	if !open {
		return nil
	}
	if err == nil {
		err = s.durable(ctx) // the final sync queued its files
	}
	if rerr := os.RemoveAll(s.metaDir); err == nil {
		err = rerr
	}
	return err
}

// Abandon ends the activation as a crash would, for a node that stops without
// letting go of the shard: nothing is synced or written, and Litestream keeps the
// shard's files open until the process exits. Only the activation ends.
func (s *Shard) Abandon() {
	s.do(context.Background(), func() error { s.end(); return nil })
}

// Release is how the node lets go of a shard (deactivation, move, eviction).
// It does a final sync and closes Litestream's connection and every file
// handle the node has on the shard, waits until the files the final sync queued
// are in blob storage, and only then asks whether another process still has the
// shard open (see shm.go for why the order matters).
//
// If one does, or the wait failed, capture is reopened where it left off and
// Release returns false: the node must not let go. Otherwise the activation is
// over, the metadata directory is deleted and Release returns true. If
// Litestream's close or the reopen fails, the activation is over too, and
// Release returns the error (Open says which).
//
// read, if not nil, reads the database at its final state, beside the check
// for empty (see Released.At). An error from it only makes At 0.
func (s *Shard) Release(ctx context.Context, read func(*sql.DB) error) (released bool, f Released, err error) {
	var c closed
	if err := s.do(ctx, func() error {
		var err error
		if c, err = s.closeForRelease(ctx, read); err != nil && !errors.Is(err, ErrClosed) {
			s.end() // Litestream is closed or half closed: capture stays closed
		}
		return err
	}); err != nil {
		return false, f, err
	}
	// Without the lock: the flush cycle this waits for may be waiting for it to
	// sync this shard, which finds the database closed and returns.
	ferr := s.durable(ctx)

	// The last step runs even if ctx ended: the shard has to end up in one state.
	err = s.do(context.WithoutCancel(ctx), func() error {
		var err error
		released, err = s.settle(c, ferr)
		return err
	})
	if err != nil || !released {
		return false, f, err
	}
	return true, c.Released, os.RemoveAll(s.metaDir)
}

// Released is what Release found about the shard's final state. It is
// meaningful when the shard was released.
type Released struct {
	// Empty says the shard held nothing of the app's (the app never wrote to it,
	// removed everything it wrote or marked it for deletion), so it can be
	// deleted rather than kept.
	Empty bool
	// Marked says Empty is because the app marked the shard for deletion.
	Marked bool
	// At is the position Release's read saw exactly, which is the shard's final
	// position, or 0 if the read failed or something was committed after it
	// (Litestream's close captured more).
	At uint64
}

// settle is the last step of Release, under the lock, with nothing of the node's
// open on the shard: the -shm check, then the header compared with the one
// read after Litestream closed. It ends the activation if the shard can be let
// go of, and otherwise reopens capture.
func (s *Shard) settle(c closed, ferr error) (released bool, err error) {
	held, perr := ProbeOnce(s.cfg.Path)
	// A commit after Litestream's final sync changes the header, even if the app
	// has closed the file since (the mapping outlives a deleted -shm). If the app
	// opened it again, SQLite reset -shm and the header reads differently or not
	// at all. Either way the commit isn't captured, so capture resumes.
	hdr, herr := s.header()
	s.unmap()
	if herr != nil || c.hdrErr != nil || hdr != c.hdr {
		held = true
	}
	if ferr == nil && perr == nil && !held {
		s.end()
		return true, nil
	}
	// Resume where Litestream left off, or snapshot if the app checkpointed in between.
	db, err := s.openLitestream(false)
	if err != nil {
		s.end()
		return false, err
	}
	s.db = db
	// Litestream opens its connection, and with it the read lock that keeps
	// the app's checkpoints behind what it has captured, only at its first
	// sync. Without one, the app closing the file would be the last
	// connection: SQLite would checkpoint and reset the WAL, and the next sync
	// would have to snapshot the whole database.
	if err := db.SyncAndWait(context.Background()); err != nil && ferr == nil && perr == nil {
		return false, err
	}
	if ferr != nil {
		return false, ferr
	}
	return false, perr
}

// Open reports whether Litestream has the shard open: false once it is closed
// or released, or after a release that failed before capture was reopened.
func (s *Shard) Open() bool { return !s.over.Load() }

// DB exposes the current Litestream DB for tests and measurements. It waits
// for whatever holds the lock. A test that syncs with it without the lock must
// not do so at the shard's first sync, when Litestream asks the
// client what it has.
func (s *Shard) DB() (db *litestream.DB) {
	s.call(func() { db = s.db })
	return db
}

// Name is the shard's kind/id.
func (s *Shard) Name() string { return s.cfg.Name }

// Position is the shard's position after the last commit captured so far.
func (s *Shard) Position() uint64 { return s.client.view.Load().position }

// TailFrom is the lowest node journal segment that may hold one of the shard's
// tail files (including files still being written, and one being queued), or
// 0 if the shard has no tail. The node's lease records the lowest over all its
// shards.
func (s *Shard) TailFrom() uint64 { return s.client.view.Load().from }

// Tail returns the commits captured since the shard's last delta, in position
// order (earlier activations first, then this one). The caller holds a
// reference to each file and closes them (CloseTail) when done.
func (s *Shard) Tail() (tail []TailFile) {
	s.call(func() { tail = s.client.tailCopy() })
	return tail
}

// TailInfo is how many files the tail has and the age of the oldest, without
// taking references to them.
func (s *Shard) TailInfo() (n int, oldest time.Time) {
	v := s.client.view.Load()
	return v.files, v.oldest
}

// TailBytes is how many bytes the tail's files hold (in memory, or in temporary
// files when they are large).
func (s *Shard) TailBytes() int64 { return s.client.view.Load().bytes }

// Discard gives up the shard's tail without writing it anywhere: for a shard
// that is finished with or whose tail has been handed to a new Shard (Tail).
// Closing or releasing a shard leaves its tail, since the node still writes it
// as a delta then.
func (s *Shard) Discard() { s.call(s.client.discard) }

// DropTail forgets tail files that end at or before position, once a delta
// holds them. It also deletes their local level 0 copies, except the newest,
// which Litestream needs to find its place (and the one kept by an earlier call,
// once a newer one has taken its place).
func (s *Shard) DropTail(position uint64) { s.call(func() { s.dropTail(position) }) }

func (s *Shard) dropTail(position uint64) {
	dropped, newest := s.client.dropTail(position)
	db := s.db
	if db == nil {
		return
	}
	stale := s.kept
	for _, f := range dropped {
		if f.local && f.maxTXID == newest {
			s.kept = newest
		}
	}
	keep := s.kept
	for _, f := range dropped {
		if f.local && f.maxTXID != newest {
			_ = os.Remove(db.LTXPath(0, ltx.TXID(f.minTXID), ltx.TXID(f.maxTXID)))
		}
	}
	if stale != 0 && stale != keep {
		_ = os.Remove(db.LTXPath(0, ltx.TXID(stale), ltx.TXID(stale)))
	}
	if keep != 0 && keep != uint64(seedTXID) && len(dropped) > 0 {
		// A commit as large as the database leaves a file that large in the scratch
		// directory, which only needs to say where Litestream is.
		if err := shrinkNewest(db, keep); err != nil {
			s.cfg.Logger.Warn("could not shrink Litestream's newest file", "shard", s.cfg.Name, "err", err)
		}
	}
}
