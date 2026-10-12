// Package capture wraps Litestream for one shard. The node drives it: there
// are no timers and no polling, and every sync goes into the node's pending
// segment through the shard's ReplicaClient.
//
// Litestream is started from scratch on every activation of a shard: a fresh,
// empty metadata directory (on tmpfs in production) and a client that reports
// no files, so Litestream treats the shard as a new database and numbers its
// commits from 1. Those numbers mean nothing outside the activation. The
// shard's own positions are assigned by the client as commits are captured.
package capture

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/segment"
)

// Restored describes a shard that was restored from its files before this
// activation, so the first sync (a snapshot of exactly that state) is not
// uploaded.
type Restored struct {
	Position uint64     // the shard's position as restored: its manifest files plus Tail
	Tail     []TailFile // commits already in the node journal but not yet in a delta
}

// Config describes one shard to capture.
type Config struct {
	Name       string // kind/id, for example users/42
	Path       string // the SQLite file
	Generation string
	// MetaRoot holds one Litestream metadata directory per shard. The node
	// keeps it on tmpfs: nothing in it needs to outlive the node process.
	MetaRoot string
	Writer   *segment.Writer
	Restored *Restored // set for a shard restored from its files, nil for a new one
	Logger   *slog.Logger
	Tune     func(*litestream.DB) // optional, to change Litestream settings in tests
	// Durable waits until everything queued in Writer so far is in blob storage.
	// Release and Close call it after their final sync. The node sets it to wait
	// for its next flush cycle, so deactivations share segments with the rest;
	// by default the shard flushes the writer itself.
	Durable func(ctx context.Context) error
}

// Shard is one captured SQLite database.
type Shard struct {
	cfg     Config
	metaDir string
	client  *client

	mu     sync.Mutex
	db     *litestream.DB
	closed bool
	empty  bool // as of the last closeDB: the shard held nothing of the app's

	// closedHdr is the wal-index header read just after Litestream closed, to
	// tell whether the app committed after Litestream's final sync.
	closedHdr    Header
	closedHdrErr error

	// wi maps the wal-index header (see WalIndex). It is made before Litestream
	// opens the shard and dropped after Litestream closes it. wiMu keeps a read
	// from racing the unmap.
	wiMu sync.RWMutex
	wi   *WalIndex

	// syncing is held by the goroutine while it is inside Litestream, so
	// closing the database can wait for it.
	syncing chan struct{}
}

// MetaDir returns the directory name used for a shard under a metadata root.
// MetaDir is a shard's Litestream metadata directory under root. It has the
// same name as the shard's file (kind/id.generation, see the node's pathFor),
// so the two are easy to match up, and ids that differ only by case never
// share a directory, even on a case-insensitive filesystem.
func MetaDir(root, shard, generation string) string {
	return filepath.Join(root, shard+"."+generation)
}

// New starts capturing cfg.Path in a fresh metadata directory. The file must
// exist: a new shard's is created empty by the node, before the app gets it.
func New(cfg Config) (*Shard, error) {
	if cfg.Writer == nil {
		return nil, fmt.Errorf("capture: writer required")
	}
	if cfg.MetaRoot == "" {
		return nil, fmt.Errorf("capture: metadata root required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	s := &Shard{cfg: cfg, metaDir: MetaDir(cfg.MetaRoot, cfg.Name, cfg.Generation), client: newClient(cfg), syncing: make(chan struct{}, 1)}
	// A fresh start: whatever an earlier activation left is gone.
	if err := os.RemoveAll(s.metaDir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(s.metaDir, 0o755); err != nil {
		return nil, err
	}
	var err error
	if s.db, err = s.openLitestream(); err != nil {
		os.RemoveAll(s.metaDir)
		return nil, err
	}
	return s, nil
}

// Header reads the shard's wal-index header: it differs from an earlier read
// if the database was committed to since (and may differ without that, never
// the other way round). An error means the header can't be trusted, for
// example because Litestream hasn't opened the database yet, and callers
// should treat the shard as changed.
func (s *Shard) Header() (Header, error) {
	s.wiMu.RLock()
	defer s.wiMu.RUnlock()
	h, err := s.wi.Read()
	if err == nil && h == (Header{}) {
		err = errors.New("capture: wal-index not initialised")
	}
	return h, err
}

func (s *Shard) mapWalIndex() error {
	wi, err := MapWalIndex(s.cfg.Path)
	if err != nil {
		return err
	}
	s.wiMu.Lock()
	s.wi = wi
	s.wiMu.Unlock()
	return nil
}

func (s *Shard) unmapWalIndex() {
	s.wiMu.Lock()
	s.wi.Close()
	s.wi = nil
	s.wiMu.Unlock()
}

func (s *Shard) openLitestream() (*litestream.DB, error) {
	// The node holds no locks on the shard yet, so the descriptor MapWalIndex
	// opens and closes drops none.
	if err := s.mapWalIndex(); err != nil {
		return nil, err
	}
	db := litestream.NewDB(s.cfg.Path)
	db.SetLogger(s.cfg.Logger)
	db.SetMetaPath(s.metaDir)
	// The node drives Litestream. No background polling.
	db.MonitorInterval = 0
	if s.cfg.Tune != nil {
		s.cfg.Tune(db)
	}
	replica := litestream.NewReplicaWithClient(db, s.client)
	// The replica syncs only when called.
	replica.MonitorEnabled = false
	db.Replica = replica
	if err := db.Open(); err != nil {
		s.unmapWalIndex()
		return nil, err
	}
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
// (the client only queues), so ctx only ends the wait for a sync that is
// already running on the shard.
func (s *Shard) Capture(ctx context.Context) error {
	done, err := s.quiesce(ctx)
	if err != nil {
		return err
	}
	defer done()
	s.mu.Lock()
	db := s.db
	s.mu.Unlock()
	if db == nil {
		return fmt.Errorf("capture: %s is closed", s.cfg.Name)
	}
	return db.SyncAndWait(context.Background())
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

// quiesce waits until no sync is inside Litestream and keeps new ones out
// until the returned function is called.
func (s *Shard) quiesce(ctx context.Context) (func(), error) {
	select {
	case s.syncing <- struct{}{}:
		return func() { <-s.syncing }, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

// closeDB closes Litestream's connection (its final sync queues what is left)
// and drops the mapping. Nothing is held on return, so a flush cycle that is
// waiting for this shard's sync, which the caller then waits for, can finish.
func (s *Shard) closeDB(ctx context.Context, strict bool) (open bool, err error) {
	done, err := s.quiesce(ctx)
	if err != nil {
		return false, err
	}
	defer done()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false, nil
	}
	if s.db == nil {
		if !strict {
			return false, nil
		}
		return false, fmt.Errorf("capture: %s has no open database (an earlier release failed)", s.cfg.Name)
	}
	// Whether the app ever wrote is read before Litestream's final sync. If that
	// sync captures anything more, something was written after the read.
	empty, eerr := s.holdsNothing(ctx)
	pos := s.client.position()
	err = s.db.Close(context.WithoutCancel(ctx))
	s.empty = eerr == nil && empty && s.client.position() == pos
	s.db = nil
	// The mapping is kept: Release compares this header with one read after its
	// -shm check. A mapping holds no locks, so keeping it costs nothing there.
	s.closedHdr, s.closedHdrErr = s.Header()
	return true, err
}

// holdsNothing reports whether the database holds nothing of the app's: no
// tables or indexes besides Litestream's own, and user_version and
// application_id both 0. It asks through Litestream's own connection, so the
// node opens nothing of the shard's (see shm.go for why that matters).
func (s *Shard) holdsNothing(ctx context.Context) (bool, error) {
	sqldb := s.db.SQLDB()
	if sqldb == nil {
		return false, errors.New("capture: litestream has not opened the database")
	}
	var objects, userVersion, applicationID int64
	err := sqldb.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM sqlite_schema WHERE name NOT IN ('_litestream_seq', '_litestream_lock')),
		(SELECT user_version FROM pragma_user_version),
		(SELECT application_id FROM pragma_application_id)`).Scan(&objects, &userVersion, &applicationID)
	return objects == 0 && userVersion == 0 && applicationID == 0, err
}

// Empty reports whether the shard held nothing of the app's when it was last
// released or closed: the app never wrote to it (or removed everything it
// wrote). A shard like that can be deleted rather than kept.
func (s *Shard) Empty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.empty
}

// Close ends the activation: a final sync, then Litestream's connection and
// file handles are closed and its metadata directory is deleted.
func (s *Shard) Close(ctx context.Context) error {
	open, err := s.closeDB(ctx, false)
	if open {
		s.unmapWalIndex()
	}
	if !open {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		return err
	}
	if err == nil {
		err = s.durable(ctx) // the final sync queued its files
	}
	// Whatever happened, the activation is over.
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	if rerr := os.RemoveAll(s.metaDir); err == nil {
		err = rerr
	}
	return err
}

// Release is how the node lets go of a shard (deactivation, move, eviction).
// It does a final sync and closes Litestream's connection and every file
// handle the node has on the shard, waits until the files the final sync queued
// are in blob storage, and only then asks whether another process still has the
// shard open (see shm.go for why the order matters).
//
// If one does, or the wait failed, capture is reopened where it left off and
// Release returns false: the node must not let go. Otherwise the activation is
// over, the metadata directory is deleted and Release returns true.
func (s *Shard) Release(ctx context.Context) (released bool, err error) {
	open, err := s.closeDB(ctx, true)
	if open && err != nil {
		s.unmapWalIndex()
	}
	if !open || err != nil {
		return !open && err == nil, err
	}
	ferr := s.durable(ctx)

	// The last step runs even if ctx ended: the shard has to end up in one state.
	ctx = context.WithoutCancel(ctx)
	done, err := s.quiesce(ctx)
	if err != nil {
		return false, err
	}
	defer done()
	s.mu.Lock()
	defer s.mu.Unlock()
	held, perr := ProbeOnce(s.cfg.Path)
	// A commit after Litestream's final sync changes the header, even if the app
	// has closed the file since (the mapping outlives a deleted -shm). If the app
	// opened it again, SQLite reset -shm and the header reads differently or not
	// at all. Either way the commit isn't captured, so capture resumes.
	hdr, herr := s.Header()
	s.unmapWalIndex()
	if herr != nil || s.closedHdrErr != nil || hdr != s.closedHdr {
		held = true
	}
	if ferr != nil || perr != nil || held {
		// Resume where Litestream left off, or snapshot if the app checkpointed in between.
		db, err := s.openLitestream()
		if err != nil {
			return false, err
		}
		s.db = db
		if ferr != nil {
			return false, ferr
		}
		return false, perr
	}
	s.closed = true
	return true, os.RemoveAll(s.metaDir)
}

// DB exposes the current Litestream DB for tests and measurements.
func (s *Shard) DB() *litestream.DB {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db
}

// Name is the shard's kind/id.
func (s *Shard) Name() string { return s.cfg.Name }

// Position is the shard's position after the last commit captured so far.
func (s *Shard) Position() uint64 { return s.client.position() }

// TailFrom is the lowest node journal segment that may hold one of the shard's
// tail files (including files still being written), or 0 if the shard has no
// tail. The node's lease records the lowest over all its shards.
func (s *Shard) TailFrom() uint64 { return s.client.tailFrom() }

// Tail returns the commits captured since the shard's last delta, in position
// order (earlier activations first, then this one).
func (s *Shard) Tail() []TailFile { return s.client.tailCopy() }

// DropTail forgets tail files that end at or before position, once a delta
// holds them. It also deletes their local level 0 copies, except the newest,
// which Litestream needs to find its place.
func (s *Shard) DropTail(position uint64) {
	dropped, newest := s.client.dropTail(position)
	db := s.DB()
	if db == nil {
		return
	}
	for _, f := range dropped {
		if !f.local || f.maxTXID == newest {
			continue
		}
		_ = os.Remove(db.LTXPath(0, ltx.TXID(f.minTXID), ltx.TXID(f.maxTXID)))
	}
}
