// Package capture captures one SQLite shard. The node drives it: there are no
// timers and no polling, and every sync goes into the node's pending segment.
//
// A sync reads the committed frames the app has added to the shard's -wal file
// since the last one, and encodes them as one LTX file numbered with the shard's
// own positions. What keeps SQLite from copying or wiping a frame before it is
// read is a read transaction on mark 0 that the node holds while the shard is
// captured (see "The read rule" in docs/internals.md and specs/capture.qnt). With
// it held the WAL only grows, so when it is large the node takes the write lock,
// reads what is left, ends the transaction, has everything copied into the
// database file and begins a new transaction (restart).
package capture

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/superfly/ltx"
	"modernc.org/sqlite"

	"github.com/luci1900/shardlite/internal/kill"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// Restored describes a shard that was restored from its files before this
// activation. Capture continues from its position, and doesn't read the database.
type Restored struct {
	Position pos.Position // the shard's position as restored: its manifest files plus Tail
	// Tail is the commits already in the node journal but not yet in a delta.
	// New takes it over: its logs are closed when the tail is dropped or
	// discarded, or at once if New fails. A Restored reused after
	// New holds no files.
	Tail TailFiles
	// Checksums is the CRC of each page of the restored database, which the
	// restore computed as it applied the files. Capture continues from it, so
	// the activation reads no pages. It is required, and New keeps it, so the
	// caller must not use it again.
	Checksums *Checksums
}

// Writer is what a shard needs of the node's journal (journal.Writer).
type Writer interface {
	NextSeq() pos.Seg
	AppendFile(name shard.Name, generation shard.Gen, position pos.Position, ltxFile tempfile.File) error
	Flush(ctx context.Context) error
}

// DefaultRestartFrames is the size of the WAL, in frames, above which the node
// restarts it (SQLite's own default for automatic checkpoints).
const DefaultRestartFrames = 1000

// Config describes one shard to capture.
type Config struct {
	Name       shard.Name // kind/id, for example users/42
	Path       string     // the SQLite file
	Generation shard.Gen
	Writer     Writer
	Restored   *Restored // set for a shard restored from its files, nil for a new one
	// TempDir is where the shard's tail is kept: a commit is appended to a log
	// there, and a single commit can be as large as the database. It is required.
	TempDir *tempfile.Dir
	// RestartFrames is the size of the WAL, in frames, above which a sync
	// restarts it (DefaultRestartFrames if zero).
	RestartFrames Frame
	// At, if set, is called at named points of an activation of a restored
	// shard: Folded and ReadHeld. The fault tests kill the process at them.
	At     func(kill.Point)
	Logger *slog.Logger
	// ChecksumMismatch is called when a snapshot's checksum is not the
	// capture's, which means the capture went wrong somewhere. The node counts
	// them.
	ChecksumMismatch func(want, got ltx.Checksum)
	// SkewChecksum makes the capture's checksum wrong, so that a snapshot's
	// checksum is not the capture's. It is a test hook: what the node does when
	// the two differ is what a test of it needs.
	SkewChecksum bool
	// Durable waits until everything queued in Writer so far is in blob storage.
	// Release and Close call it after their final sync. The node sets it to wait
	// for its next flush cycle, so deactivations share segments with the rest;
	// by default the shard flushes the writer itself.
	Durable func(ctx context.Context) error
	// Now is the clock for the LTX headers' timestamps and the tail files' times
	// (time.Now if nil). With the same commits and the same clock, capture
	// writes the same bytes.
	Now func() time.Time
}

func (c Config) restartFrames() Frame {
	if c.RestartFrames > 0 {
		return c.RestartFrames
	}
	return DefaultRestartFrames
}

// Shard is one captured SQLite database. Its lock (mu) guards the connections,
// the descriptors, where capture has read to, the mapped wal-index and the tail:
// everything runs holding it, one at a time, on the caller's goroutine (do). The
// activation runs from New until Close, or a Release that leaves capture closed
// (or Abandon). Once it is over, the shard is its caller's alone (whoever closed
// or released it): Tail, DropTail and Discard still take the lock (call), but
// nothing else waits for it.
type Shard struct {
	cfg  Config
	side WALSidecars // the WAL and wal-index beside cfg.Path, whose names nothing else spells
	tail *tail

	// mu is the lock: a channel with room for one, so a caller can stop waiting
	// for it when its context ends.
	mu   chan struct{}
	over atomic.Bool // the activation is over: do runs nothing more

	// wi maps the wal-index (see WALIndex). It is made before the connections
	// open and dropped after they close. Only holders of mu change it, under
	// wiMu, so Header can read it from any goroutine.
	wiMu sync.RWMutex
	wi   *WALIndex

	// Under mu (see above). pool is nil once capture is closed.
	pool *sql.DB
	// rtx is the connection that holds the read transaction (reading says it does).
	// The transaction is on mark 0 whenever it was begun with everything copied.
	rtx     *sql.Conn
	reading bool
	// dbF and walF are descriptors kept open until the end: closing any descriptor
	// on a file drops all of this process's fcntl locks on it, the connections'
	// included (see walsidecar.go). walF is opened when the WAL first has frames.
	dbF, walF *os.File
	pageSize  PageSize

	// cur is where capture has read the shard's WAL to, nil before it has
	// read a frame of one (see walCursor).
	cur *walCursor
	// dbPages is the database's size in pages after the last captured commit.
	dbPages Pages
	// cks is the CRC of each page of the database and the running checksum of
	// the whole of it, which every file capture writes carries.
	cks *Checksums
	// known is what the wal-index said at the last sync: a release that is
	// refused resumes if the WAL is the same one and has at least as many frames.
	knownSalt   [2]uint32
	knownFrames Frame
	// back is how many frames were copied into the database file at the last sync.
	back Frame
	// snaps is the number of snapshots being read, which hold off a restart.
	snaps int
	// openAtRelease is set by a release that went ahead over a handle another process kept open (OpenAtRelease).
	openAtRelease bool
}

// ErrClosed is what Capture, Use and Snapshot return for a shard whose
// database is closed.
var ErrClosed = errors.New("capture: the shard's database is closed")

// New starts capturing cfg.Path. The file must exist: a new shard's is created
// empty by the node, before the app gets it.
func New(cfg Config) (sh *Shard, err error) {
	defer func() {
		if err != nil && cfg.Restored != nil {
			cfg.Restored.Tail.Close()
		}
	}()
	if cfg.Writer == nil {
		return nil, fmt.Errorf("capture: writer required")
	}
	if cfg.TempDir == nil {
		return nil, fmt.Errorf("capture: temporary directory required")
	}
	if cfg.Restored != nil && cfg.Restored.Checksums == nil {
		return nil, fmt.Errorf("capture: the restored shard's checksums are required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	s := &Shard{cfg: cfg, side: WALSidecarsOf(cfg.Path), tail: newTail(cfg.Restored), mu: make(chan struct{}, 1)}
	if cfg.Restored != nil {
		// Nobody else has the shard open: the caller has checked, and nothing in
		// this process has opened it yet, so what foldWAL opens and closes drops no
		// locks. The database file holds everything it has after.
		if held, err := s.side.Held(); err != nil {
			return nil, err
		} else if held {
			return nil, fmt.Errorf("capture: %s is open in another process", cfg.Name)
		}
		if err := foldWAL(cfg.Path); err != nil {
			return nil, err
		}
		s.at(kill.Folded)
	}
	if err := s.mapWALIndex(); err != nil {
		return nil, err
	}
	if err := s.open(context.Background()); err != nil {
		s.unmap()
		return nil, err
	}
	// The checksums of the database: the ones the restore computed for a
	// restored shard (the caller's own copy, which the restore or the old
	// capture gave up), and an empty database's for a new one.
	if cfg.Restored == nil {
		s.cks = NewChecksums()
	} else {
		s.cks = cfg.Restored.Checksums
	}
	// The WAL is empty: the transaction gets mark 0, and nothing the app writes
	// from here on is copied or wiped before a sync reads it.
	if err := s.beginRead(context.Background()); err != nil {
		s.closeConns()
		s.unmap()
		return nil, err
	}
	info, err := s.wi.ReadInfo()
	if err != nil {
		s.closeConns()
		s.unmap()
		return nil, err
	}
	s.note(info)
	s.at(kill.ReadHeld)
	return s, nil
}

// Close ends the activation: a final sync, then the connections and descriptors
// are closed. It ends the activation whatever ctx says, and returns nil for a
// shard that is closed or released already.
func (s *Shard) Close(ctx context.Context) error {
	open := false
	var err error
	if s.do(context.WithoutCancel(ctx), func() error {
		if open = s.pool != nil; open {
			err = s.syncing(context.WithoutCancel(ctx))
			s.closeConns()
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
	return err
}

// Abandon ends the activation as a crash would, for a node that stops without
// letting go of the shard: nothing is synced or written, and the shard's files
// stay open until the process exits. Only the activation ends.
func (s *Shard) Abandon() {
	s.do(context.Background(), func() error { s.end(); return nil })
}

// Open reports whether capture has the shard open: false once it is closed
// or released, or after a release that failed before capture was reopened.
func (s *Shard) Open() bool { return !s.over.Load() }

// Position is the shard's position after the last commit captured so far.
func (s *Shard) Position() pos.Position { return s.tail.view.Load().position }

// Checksums is the CRC of each page of the database and the running checksum
// of the whole of it, as of the last commit captured. It is a copy: the
// capture's own goes on with the next sync.
func (s *Shard) Checksums() *Checksums {
	var cks *Checksums
	s.call(func() { cks = s.cks.Clone() })
	return cks
}

// Use runs fn with capture's connection pool (never a descriptor of the node's
// own), under the shard's lock. So it never runs during a sync, a snapshot or
// the final sync of a release, and what fn commits is captured by the next
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
func (s *Shard) UseAt(ctx context.Context, fn func(*sql.DB) error) (at pos.Position, err error) {
	err = s.do(ctx, func() error {
		if err := s.syncing(ctx); err != nil {
			return err
		}
		before := s.tail.position()
		if err := s.use(fn); err != nil {
			return err
		}
		if err := s.sync(); err != nil {
			return err
		}
		if s.tail.position() == before {
			at = before
		}
		return nil
	})
	return at, err
}

// use runs fn on capture's connections. The caller holds the lock.
func (s *Shard) use(fn func(*sql.DB) error) error {
	if s.pool == nil {
		return ErrClosed
	}
	return fn(s.pool)
}

// do runs fn holding the lock and returns its error, or ErrClosed once the
// activation is over. It gives up only while it waits for the lock, when ctx
// ends. fn runs to the end once it has started, and ctx only ends the wait for
// whatever holds the lock before it.
//
// A panic in fn ends the activation before it goes on: a caller that recovers
// it (net/http recovers a handler's) leaves capture in whatever state the
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
// callers should treat the shard as changed: the connection hasn't opened the
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

func (s *Shard) at(point kill.Point) {
	if s.cfg.At != nil {
		s.cfg.At(point)
	}
}

// mapWALIndex maps the -shm file. The node holds no locks on the shard yet, so
// the descriptor it opens and closes drops none.
func (s *Shard) mapWALIndex() error {
	wi, err := MapWALIndex(s.cfg.Path)
	if err != nil {
		return err
	}
	s.wiMu.Lock()
	s.wi = wi
	s.wiMu.Unlock()
	return nil
}

func (s *Shard) unmap() {
	s.wiMu.Lock()
	defer s.wiMu.Unlock()
	s.wi.Close()
	s.wi = nil
}

// open opens the connections and the descriptors. The WAL mode is set here for a
// new shard, whose file is empty.
func (s *Shard) open(ctx context.Context) (err error) {
	pool := sql.OpenDB(persistWAL{dsn: fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=wal_autocheckpoint(0)", s.cfg.Path)})
	defer func() {
		if err != nil {
			pool.Close()
		}
	}()
	var mode string
	if err := pool.QueryRowContext(ctx, `PRAGMA journal_mode = wal`).Scan(&mode); err != nil {
		return err
	} else if mode != "wal" {
		return fmt.Errorf("capture: could not set WAL mode on %s (%q)", s.cfg.Name, mode)
	}
	var pageSize PageSize
	if err := pool.QueryRowContext(ctx, `PRAGMA page_size`).Scan(&pageSize); err != nil {
		return err
	}
	dbF, err := os.Open(s.cfg.Path)
	if err != nil {
		return err
	}
	if fi, err := dbF.Stat(); err != nil {
		dbF.Close()
		return err
	} else {
		s.dbPages = Pages(fi.Size() / int64(pageSize))
	}
	s.pool, s.dbF, s.pageSize = pool, dbF, pageSize
	return nil
}

// sqliteDriver is the SQLite driver capture opens the shard's connections with,
// a driver of its own so that nothing registered on the global one (a test's
// function, say) runs inside the shard.
var sqliteDriver = &sqlite.Driver{}

// persistWAL is the connector the shard's connections come from: every one
// keeps the WAL file when it closes (SQLITE_FCNTL_PERSIST_WAL), so that the
// last of them to close leaves the WAL and the -shm beside the database file.
//
// Left in place, they are what a release trusts and the next activation
// reuses: the app's next open reuses the -shm, so a commit after the final
// sync shows in the wal-index header Release compares (settle), and the WAL
// left beside the file is folded into the database when capture is resumed
// (foldWAL). Deleted instead, which is what SQLite does when
// the last connection closes, the app's next open would make a new -shm the
// header comparison never sees, and a commit made in between would be let go
// uncaptured.
type persistWAL struct{ dsn string }

// Connect opens one connection and keeps its WAL file.
func (c persistWAL) Connect(context.Context) (driver.Conn, error) {
	conn, err := sqliteDriver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	fc, ok := conn.(sqlite.FileControl)
	if !ok {
		conn.Close()
		return nil, errors.New("capture: the SQLite driver cannot keep a WAL file")
	}
	if _, err := fc.FileControlPersistWAL("main", 1); err != nil {
		conn.Close()
		return nil, fmt.Errorf("capture: keep the WAL file: %w", err)
	}
	return conn, nil
}

// Driver is the driver the connections come from.
func (c persistWAL) Driver() driver.Driver { return sqliteDriver }

// beginRead begins the read transaction, if there is none.
func (s *Shard) beginRead(ctx context.Context) error {
	if s.reading {
		return nil
	}
	if s.rtx == nil {
		c, err := s.pool.Conn(ctx)
		if err != nil {
			return err
		}
		s.rtx = c
	}
	if _, err := s.rtx.ExecContext(ctx, `BEGIN`); err != nil {
		return err
	}
	var n int
	if err := s.rtx.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_schema`).Scan(&n); err != nil {
		s.rtx.ExecContext(ctx, `ROLLBACK`)
		return err
	}
	s.reading = true
	return nil
}

// endRead ends the read transaction, if there is one.
func (s *Shard) endRead() {
	if s.reading {
		s.rtx.ExecContext(context.Background(), `ROLLBACK`)
		s.reading = false
	}
}

// closeConns closes the connections and then the descriptors, in that order:
// closing a descriptor drops the connections' locks. A connection that closes
// last on the database checkpoints it; the WAL and the -shm stay in place for
// the next capture to fold (persistWAL).
func (s *Shard) closeConns() {
	s.endRead()
	if s.rtx != nil {
		s.rtx.Close()
		s.rtx = nil
	}
	if s.pool != nil {
		s.pool.Close()
		s.pool = nil
	}
	if s.walF != nil {
		s.walF.Close()
		s.walF = nil
	}
	if s.dbF != nil {
		s.dbF.Close()
		s.dbF = nil
	}
}

// note records what the wal-index says, as of a sync that read all of it.
func (s *Shard) note(info Info) {
	s.knownSalt, s.knownFrames, s.back = info.Salt, info.MxFrame, info.NBackfill
	if info.MxFrame == 0 {
		// A WAL with no frames (the app truncated it): a cursor on the one before
		// would read frames the file no longer has.
		s.cur = nil
	}
}

// durable waits until what is queued is in blob storage.
func (s *Shard) durable(ctx context.Context) error {
	if s.cfg.Durable != nil {
		return s.cfg.Durable(ctx)
	}
	return s.cfg.Writer.Flush(ctx)
}
