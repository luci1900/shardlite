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
	"bufio"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/superfly/ltx"
	"modernc.org/sqlite"

	"github.com/luci1900/shardlite/internal/kill"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/spool"
)

// Restored describes a shard that was restored from its files before this
// activation. Capture continues from its position, and doesn't read the database.
type Restored struct {
	Position pos.Position // the shard's position as restored: its manifest files plus Tail
	// Tail is the commits already in the node journal but not yet in a delta.
	// New takes it over: the files' references are closed when the tail is
	// dropped or discarded, or at once if New fails. A Restored reused after
	// New holds no files.
	Tail TailFiles
	// Checksums is the CRC of each page of the restored database, which the
	// restore computed as it applied the files. Capture continues from it, so
	// the activation reads no pages. It is nil for a shard whose checksums are
	// not known, and capture reads them from the file then.
	Checksums *Checksums
}

// Writer is what a shard needs of the node's journal (segment.Writer).
type Writer interface {
	NextSeq() pos.Seg
	AppendFile(name shard.Name, generation shard.Gen, position pos.Position, ltxFile *spool.File) error
	Flush(ctx context.Context) error
}

// DefaultRestartFrames is the size of the WAL, in frames, above which the node
// restarts it (SQLite's own default for automatic checkpoints).
const DefaultRestartFrames = 1000

// Config describes one shard to capture.
type Config struct {
	Name       shard.Name // kind/id, for example game.users/42
	Path       string     // the SQLite file
	Generation shard.Gen
	Writer     Writer
	Restored   *Restored // set for a shard restored from its files, nil for a new one
	// A commit file is held in memory up to SpoolLimit bytes (spool.DefaultLimit
	// if zero) and in a temporary file in TempDir beyond that: a single commit
	// can be as large as the database. Without a TempDir a commit larger than
	// SpoolLimit fails.
	TempDir    string
	SpoolLimit uint64
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
}

func (c Config) spoolLimit() int {
	if c.SpoolLimit > 0 {
		return int(c.SpoolLimit)
	}
	return spool.DefaultLimit
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
}

// ErrClosed is what Capture, Use and Snapshot return for a shard whose
// database is closed.
var ErrClosed = errors.New("capture: the shard's database is closed")

// errBusy says the write lock could not be had in time.
var errBusy = errors.New("capture: the shard is being written")

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
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
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
	// restored shard, the ones the cache index holds for a cached one, and the
	// ones read from the file for a shard whose are not known.
	switch {
	case cfg.Restored == nil:
		s.cks = NewChecksums() // a new shard: an empty database
	case cfg.Restored.Checksums != nil:
		// A copy: the capture's own goes on with the next sync, and the
		// caller's is the restore's.
		s.cks = cfg.Restored.Checksums.Clone()
	default:
		if s.cks, err = readChecksums(cfg.Path, s.pageSize, s.dbPages); err != nil {
			s.closeConns()
			s.unmap()
			return nil, err
		}
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
// left beside the file is folded into the database at the next activation of
// a cached shard (foldWAL). Deleted instead, which is what SQLite does when
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
// the next activation to fold (persistWAL).
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
}

// ensureWAL opens the WAL file. A connection creates it, so it is there once
// the app has written.
func (s *Shard) ensureWAL() error {
	if s.walF != nil {
		return nil
	}
	f, err := s.side.openWAL()
	if err != nil {
		return err
	}
	s.walF = f
	return nil
}

// readWAL reads the WAL the wal-index names and the frames it holds, and
// returns a cursor for it with the window the frames hold.
//
// The wal-index and the WAL are separate files that SQLite updates in turn
// when it restarts the WAL, so a read that straddles the restart sees them
// name different WALs, or the frames of one where the other's are. That is
// errRestarted, and the caller reads the pair again.
//
// Reading across a restart is safe: the WAL only restarts once every frame of
// it is captured and in the database file (the read rule), so the frames it
// held are in the tail already.
func (s *Shard) readWAL(info Info) (walCursor, window, error) {
	if info.MxFrame == 0 {
		return walCursor{}, window{}, nil
	}
	if err := s.ensureWAL(); err != nil {
		return walCursor{}, window{}, fmt.Errorf("capture: open the WAL of %s: %w", s.cfg.Name, err)
	}
	// A cursor for the WAL the wal-index names: the one capture was reading, if
	// it is the same one, or a fresh one. A fresh one means SQLite restarted the
	// WAL, and the read rule means every frame of the last one was read before
	// that could happen.
	c, err := walAt(s.walF, info, s.pageSize, s.cur)
	if err != nil {
		return walCursor{}, window{}, err
	}
	w, err := c.readTo(s.walF, info.MxFrame)
	if err != nil {
		return walCursor{}, window{}, err
	}
	return c, w, nil
}

// sync reads the frames the app has committed since the last one, and queues
// them as one file. The caller holds the lock and the read transaction.
func (s *Shard) sync() error {
	first := s.tail.nextPos == 1 // the first file is a snapshot and has every page
	c, info, w, err := walRead(s.wi.ReadInfo, s.readWAL)
	if err != nil {
		return err
	}
	if w.commits == 0 && (!first || s.dbPages == 0) {
		s.note(info)
		return nil
	}
	count, commit := pos.Count(w.commits), w.commit
	if w.commits == 0 {
		// A database that predates the WAL: the first file has its every page.
		count, commit = 1, s.dbPages
	}
	if err := s.queueFile(count, commit, w.pf, c.h, first); err != nil {
		return err
	}
	if info.MxFrame > 0 {
		s.cur = &c
	}
	s.dbPages = commit
	s.note(info)
	return nil
}

// queueFile encodes a file of count commits, for the frames pf of the WAL h on
// top of the database file, and queues it in the journal and the tail. With all
// set every page of the database is in it, those the WAL has no frame for read
// from the database file.
//
// The file carries the checksum of the database before it and the one after it,
// which the pages it writes give: the CRCs of the pages capture has already
// seen are in memory, so no page is read for this.
func (s *Shard) queueFile(count pos.Count, commit Pages, pf pageFrames, h walHeader, all bool) error {
	pos := s.tail.nextPos
	w := spool.NewWriter(s.cfg.TempDir, s.cfg.spoolLimit())
	pg := pages{wal: s.walF, db: s.dbF, pageSize: s.pageSize, h: h, pf: pf}
	hdr := ltx.Header{
		Version:   ltx.Version,
		PageSize:  uint32(s.pageSize),
		Commit:    uint32(commit),
		MinTXID:   ltx.TXID(pos),
		MaxTXID:   ltx.TXID(pos.Add(count)),
		Timestamp: time.Now().UnixMilli(),
	}
	// A snapshot has no database before it, so it has no pre-apply checksum.
	// Every other file has the one the database has now.
	if pos > 1 {
		hdr.PreApplyChecksum = s.cks.Checksum()
	}
	p := newPending(s.cks, all)
	if _, err := s.writeLTX(w, hdr, pg, p); err != nil {
		w.Abort()
		return err
	}
	body, err := w.Finish()
	if err != nil {
		return err
	}
	defer body.Close()
	// The file is queued in the writer and recorded in the tail at once, without
	// waiting for its segment. The writer never drops a queued file: it retries
	// until the segment is written, or the node is fenced and has to stop. Whoever
	// needs the commits durable flushes the writer after the sync.
	//
	// The lease reads the writer's next number before the shards' TailFrom, so
	// TailFrom counts the file from before it is queued: a lease that reads the
	// number after the file is queued finds it in TailFrom.
	seg := s.cfg.Writer.NextSeq()
	s.tail.publish(seg)
	if err := s.cfg.Writer.AppendFile(s.cfg.Name, s.cfg.Generation, pos, body); err != nil {
		s.tail.publish(0) // refused: the writer is closed or fenced
		return err
	}
	s.tail.add(body, count, seg)
	// The file is in the journal, so the database it holds is the one capture
	// has now: its checksums take the pages the file wrote.
	s.cks.apply(p, commit)
	return nil
}

// pages reads the pages of a database as of one point in its history: each
// page from the newest frame of its WAL that pf names, or from the database
// file, which the read transaction on mark 0 holds exactly as it was when it
// began (the read rule, docs/internals.md): every page committed after that is
// in the WAL, and no frame of the WAL is copied over or wiped while the
// transaction is held. A page pf doesn't name and the file doesn't hold is
// past the database's end.
type pages struct {
	wal      io.ReaderAt // the WAL, whose frames pf names (nil if none was read)
	db       io.ReaderAt // the database file
	pageSize PageSize    // the database's page size
	h        walHeader   // which WAL wal is, zero with no WAL read
	pf       pageFrames  // the newest frame of each page it covers
}

// page reads page pgno into buf, which is one page long.
func (p pages) page(pgno Pgno, buf []byte) error {
	if f := p.pf.frame(pgno); f != 0 {
		if err := p.h.page(p.wal, f, buf); err != nil {
			return fmt.Errorf("capture: page %d from WAL frame %d: %w", pgno, f, err)
		}
		return nil
	}
	if _, err := p.db.ReadAt(buf, int64(pgno-1)*int64(p.pageSize)); err != nil {
		return fmt.Errorf("capture: page %d from the database file: %w", pgno, err)
	}
	return nil
}

// writeLTX writes the LTX file hdr to w, with the CRC of every page it writes
// in p. With all set every page of the database up to hdr.Commit is in it,
// those pf has no frame for read from the database file; otherwise only the
// pages pf covers.
//
// It returns the file's post-apply checksum: the checksum of the database once
// the file's pages are written.
func (s *Shard) writeLTX(w io.Writer, hdr ltx.Header, pg pages, p *pending) (ltx.Checksum, error) {
	enc, err := ltx.NewEncoder(w)
	if err != nil {
		return 0, err
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		return 0, err
	}
	page := make([]byte, hdr.PageSize)
	put := func(pgno Pgno) error {
		if err := pg.page(pgno, page); err != nil {
			return err
		}
		if err := enc.EncodePage(ltx.PageHeader{Pgno: uint32(pgno)}, page); err != nil {
			return err
		}
		p.add(pgno, ltx.ChecksumPageWithHasher(p.hasher, uint32(pgno), page))
		return nil
	}
	if p.all {
		lock := Pgno(ltx.LockPgno(hdr.PageSize))
		for pgno := Pgno(1); pgno <= Pgno(hdr.Commit); pgno++ {
			if pgno == lock {
				continue
			}
			if err := put(pgno); err != nil {
				return 0, err
			}
		}
	} else {
		for _, e := range pg.pf {
			if pgno := e.pgno(); pgno <= Pgno(hdr.Commit) {
				if err := put(pgno); err != nil {
					return 0, err
				}
			}
		}
	}
	post := p.cks.after(p, Pages(hdr.Commit))
	enc.SetPostApplyChecksum(post)
	if err := enc.Close(); err != nil {
		return 0, err
	}
	return post, nil
}

// syncing is a sync that first makes sure the read transaction is held: after a
// restart that failed part-way, or a release that did not go through, it is not,
// and the app may have got a checkpoint and a restart in between.
func (s *Shard) syncing(ctx context.Context) error {
	if s.pool == nil {
		return ErrClosed
	}
	if !s.reading {
		if err := s.locked(ctx, true); err != nil {
			return err
		}
	}
	return s.sync()
}

// locked takes the database's write lock (BEGIN IMMEDIATE on one connection
// of the pool): while it is held no writer can commit, so no frame is added
// and nothing about the WAL can change. Under that lock it syncs, ends the
// read transaction, has every frame copied into the database file and begins
// a new transaction before it lets go, which gets read mark 0 because nothing
// is left to copy; the app's next write then restarts the WAL. The write lock
// is held for one sync and one checkpoint, which is also what the app's
// writes wait on.
//
// The copy runs on another connection of the pool, PASSIVE, because RESTART
// and TRUNCATE take the write lock themselves, which x already holds, and
// frames committed in the gap of letting it go would be copied and wiped
// unread. An app's reader can hold the copy back: the new transaction then
// gets a nonzero read mark, which is as safe (everything up to it is
// captured) and leaves the WAL as it is until the next sync tries again.
//
// With resume set the read transaction was not held (a release that did not
// go through), so the WAL may not be the one capture was reading. Nothing can
// change while the write lock is held, so the check against what the last sync
// saw is exact: a WAL with the same salts and at least the frames capture had
// read is the same one with the frames past what was read still in it, and
// they are synced; any other WAL means the app checkpointed and restarted it
// while the node held nothing, commits may have gone uncaptured, and the
// whole database is written as one file instead. The two cases differ only in
// what is read, never in what is held: both end with the checkpoint, the new
// read transaction and the lock let go.
func (s *Shard) locked(ctx context.Context, resume bool) (err error) {
	x, err := s.pool.Conn(ctx)
	if err != nil {
		return err
	}
	defer x.Close()
	wait := 5000 // a release must conclude, so a resumed lock waits as the pool does
	if !resume {
		wait = 20 // a restart that cannot get the lock is tried again at the next sync
	}
	if _, err := x.ExecContext(ctx, fmt.Sprintf(`PRAGMA busy_timeout = %d`, wait)); err != nil {
		return err
	}
	if _, err := x.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil { // the write lock
		if isBusy(err) {
			return errBusy
		}
		return err
	}
	defer x.ExecContext(context.Background(), `ROLLBACK`)

	info, err := s.wi.ReadInfo()
	if err != nil {
		return err
	}
	if resume && (info.Salt != s.knownSalt || info.MxFrame < s.knownFrames) {
		err = s.writeAll(info)
	} else {
		err = s.sync()
	}
	if err != nil {
		return err
	}
	s.endRead() // our reader out of the way of the copy
	var busy, logFrames, done int
	if err := s.pool.QueryRowContext(ctx, `PRAGMA wal_checkpoint(PASSIVE)`).Scan(&busy, &logFrames, &done); err != nil {
		s.cfg.Logger.Warn("checkpoint failed", "shard", s.cfg.Name, "err", err)
	}
	if err := s.beginRead(ctx); err != nil { // begun before the lock goes: everything is copied, so mark 0
		return err
	}
	if info, err = s.wi.ReadInfo(); err == nil {
		s.back = info.NBackfill
	}
	return nil
}

// isBusy reports whether err is SQLite's "database is locked".
func isBusy(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "SQLITE_BUSY") || strings.Contains(err.Error(), "database is locked"))
}

// writeAll queues every page of the database as one file counted as one
// commit, from the WAL and the database file as they are. The caller holds
// the write lock, so the WAL is not restarted while it reads. The WAL is
// read with a fresh cursor, never the one capture was reading (the WAL may
// not be the one it was), and verified frame by frame like any sync.
func (s *Shard) writeAll(info Info) error {
	var (
		c      walCursor
		w      window
		commit = s.dbPages
		err    error
	)
	if info.MxFrame > 0 {
		if err := s.ensureWAL(); err != nil {
			return fmt.Errorf("capture: open the WAL of %s: %w", s.cfg.Name, err)
		}
		if c, err = walAt(s.walF, info, s.pageSize, nil); err != nil {
			return err
		}
		if w, err = c.readTo(s.walF, info.MxFrame); err != nil {
			return err
		}
		commit = w.commit
	} else {
		fi, err := s.dbF.Stat()
		if err != nil {
			return err
		}
		commit = Pages(fi.Size() / int64(s.pageSize))
	}
	if err := s.queueFile(1, commit, w.pf, c.h, true); err != nil {
		return err
	}
	if info.MxFrame > 0 {
		s.cur = &c
	}
	s.dbPages = commit
	s.note(info)
	return nil
}

// Capture syncs the shard: new commits become LTX files, and the files are
// queued in the segment writer and in the shard's tail. It returns when they
// are all queued, however many that is, without waiting for any segment. The
// commits are durable once the writer has been flushed after this returns.
//
// It also restarts the WAL once it is large. Reading the WAL does no network
// I/O (the writer only queues), so ctx only ends the wait for whatever holds the
// shard's lock before it.
func (s *Shard) Capture(ctx context.Context) error {
	return s.do(ctx, func() error {
		if err := s.syncing(ctx); err != nil {
			return err
		}
		s.maybeRestart(ctx)
		return nil
	})
}

// maybeRestart restarts the WAL if it is large and nothing needs the database
// file as it is. The commits are captured already, so a restart that fails is
// only tried again at a later sync.
func (s *Shard) maybeRestart(ctx context.Context) {
	if s.snaps > 0 || s.cur == nil || s.cur.frames <= s.cfg.restartFrames() || s.back >= s.cur.frames {
		return
	}
	if err := s.locked(ctx, false); err != nil && !errors.Is(err, errBusy) {
		s.cfg.Logger.Warn("could not restart the WAL", "shard", s.cfg.Name, "err", err)
	}
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

// closeForRelease is the first step of Release, under the lock: a final sync, the
// connections and descriptors are closed, and what the database looked like just
// after is read. The mapping is kept: Release compares this header with one read
// after its -shm check. A mapping holds no locks, so keeping it costs nothing there.
func (s *Shard) closeForRelease(ctx context.Context, read func(*sql.DB) error) (c closed, err error) {
	if s.pool == nil {
		return c, ErrClosed
	}
	// Whether the shard holds anything of the app's, and the caller's read, are
	// read between two syncs. The first captures what was committed before the
	// reads, so if the final sync captures anything more, something was written
	// after them.
	serr := s.syncing(ctx)
	empty, marked, eerr := s.holdsNothing(ctx)
	rerr := errors.New("no read")
	if read != nil {
		rerr = read(s.pool)
	}
	pos := s.tail.position()
	err = s.sync()
	s.closeConns()
	exact := serr == nil && err == nil && s.tail.position() == pos
	switch {
	case exact && eerr == nil && empty:
		c.Released = &Deleted{Marked: marked}
	case exact && rerr == nil:
		c.Released = &Kept{At: pos}
	default:
		c.Released = &Uncertain{}
	}
	c.hdr, c.hdrErr = s.header()
	return c, err
}

// closed is what closeForRelease learned about the database just after it
// closed.
type closed struct {
	// Released is what the release found about the shard's final state. It is a
	// field and not an embedded interface, so that closed does not implement
	// Released itself and show up as a variant of it.
	Released Released
	// hdr is the wal-index header, to tell whether the app committed after
	// the final sync.
	hdr    Header
	hdrErr error
}

// MarkedQuery says whether the app has marked the shard for deletion (a
// _shardlite_delete table).
const MarkedQuery = `SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE name = '_shardlite_delete' AND type = 'table')`

// holdsNothing reports whether the database holds nothing of the app's. Either
// the app marked it for deletion (a _shardlite_delete table, whatever else is in
// it), or it has no tables or indexes besides shardlite's (the _shardlite_
// tables; an index counts as its table's, so the automatic one of a reserved
// table's text key is ignored), user_version and application_id are both 0, and
// _shardlite_reminders has no rows (a reminder the node gave up on is the app's
// data too). marked says which it was. It asks through capture's own connections,
// so the node opens nothing of the shard's (see walsidecar.go for why that matters).
func (s *Shard) holdsNothing(ctx context.Context) (empty, marked bool, err error) {
	sqldb := s.pool
	if sqldb == nil {
		return false, false, ErrClosed
	}
	if err := sqldb.QueryRowContext(ctx, MarkedQuery).Scan(&marked); err != nil || marked {
		return marked, marked, err
	}
	var objects, userVersion, applicationID int64
	err = sqldb.QueryRowContext(ctx, `SELECT
		(SELECT count(*) FROM sqlite_schema WHERE name NOT IN ('sqlite_sequence') AND tbl_name NOT LIKE '\_shardlite\_%' ESCAPE '\'),
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

// Snapshot returns a full snapshot of the shard as an LTX file, and the
// shard's position that it covers: every commit captured up to it, and none
// after, though the app keeps writing while the snapshot is read. The reader
// must be closed: until it is, the WAL is not restarted.
//
// Under the shard's lock a sync captures what is committed, and the position it
// reaches is the snapshot's. The pages are then read without the lock, each from
// the newest WAL frame up to that point that the database file doesn't hold yet,
// or from the database file. The read transaction keeps the database file as it
// was when the transaction began, and the WAL from being restarted, which is
// what makes the pages exact. Later syncs add commits above the position.
//
// The snapshot carries the checksum of the database it holds, which is compared
// with the capture's own at the same position: they are the same database, and
// a difference means the capture went wrong somewhere.
func (s *Shard) Snapshot(ctx context.Context) (position pos.Position, r io.ReadCloser, err error) {
	err = s.do(ctx, func() error {
		if err := s.syncing(ctx); err != nil {
			return err
		}
		position = s.tail.position()
		if position == 0 {
			r = io.NopCloser(eofReader{})
			return nil
		}
		var pf pageFrames
		var h walHeader
		if s.cur != nil && s.cur.frames > s.back {
			var err error
			if pf, err = s.cur.newestFrames(s.walF, s.back); err != nil {
				return err
			}
			h = s.cur.h
		}
		s.snaps++
		pr, pw := io.Pipe()
		sr := &snapshotReader{PipeReader: pr, done: make(chan struct{}), s: s}
		hdr := ltx.Header{
			Version:   ltx.Version,
			PageSize:  uint32(s.pageSize),
			Commit:    uint32(s.dbPages),
			MinTXID:   1,
			MaxTXID:   ltx.TXID(position),
			Timestamp: time.Now().UnixMilli(),
		}
		pg := pages{wal: s.walF, db: s.dbF, pageSize: s.pageSize, h: h, pf: pf}
		// The checksum of the database at the snapshot's position, which the
		// snapshot's own checksum is compared with, and the checksums it is
		// compared against: the pages are read without the shard's lock.
		if s.cfg.SkewChecksum {
			s.cks.sum ^= skewMask
		}
		want, cks := s.cks.Checksum(), s.cks
		go func() {
			defer close(sr.done)
			bw := bufio.NewWriterSize(pw, 1<<20)
			// The pending is the goroutine's own: the pages are read without the
			// shard's lock, and a sync may be computing CRCs at the same time.
			p := newPending(cks, true)
			post, err := s.writeLTX(bw, hdr, pg, p)
			if err == nil {
				err = bw.Flush()
			}
			if err == nil && post != want {
				s.mismatch(want, post, p, Pages(hdr.Commit))
			}
			pw.CloseWithError(err)
		}()
		r = sr
		return nil
	})
	return position, r, err
}

// mismatch is what a snapshot does when its checksum is not the capture's: the
// capture went wrong somewhere, and the snapshot is the database as SQLite
// reads it, so it is the truth. The error is logged and counted, and the
// capture carries on from the snapshot's CRCs.
func (s *Shard) mismatch(want, got ltx.Checksum, p *pending, commit Pages) {
	s.cfg.Logger.Error("a snapshot's checksum is not the capture's: the capture went wrong somewhere",
		"shard", s.cfg.Name, "capture", want.String(), "snapshot", got.String())
	if s.cfg.ChecksumMismatch != nil {
		s.cfg.ChecksumMismatch(want, got)
	}
	s.do(context.Background(), func() error {
		cks := &Checksums{}
		cks.apply(p, commit)
		s.cks = cks
		return nil
	})
}

type eofReader struct{}

func (eofReader) Read([]byte) (int, error) { return 0, io.EOF }

// snapshotReader reads the LTX file a snapshot is writing. Closing it ends the
// snapshot and lets the WAL be restarted again.
type snapshotReader struct {
	*io.PipeReader
	done chan struct{}
	s    *Shard
	once sync.Once
}

func (r *snapshotReader) Close() error {
	r.once.Do(func() {
		r.PipeReader.CloseWithError(io.ErrClosedPipe)
		<-r.done
		r.s.call(func() { r.s.snaps-- })
	})
	return nil
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

// Release is how the node lets go of a shard (deactivation, move, eviction).
// It does a final sync and closes every connection and file handle the node has
// on the shard, waits until the files the final sync queued are in blob storage,
// and only then asks whether another process still has the shard open (see
// walsidecar.go for why the order matters).
//
// If one does, or the wait failed, capture is reopened where it left off and
// Release returns false: the node must not let go. Otherwise the activation is
// over and Release returns true. If the close or the reopen fails, the
// activation is over too, and Release returns the error (Open says which).
//
// read, if not nil, reads the database at its final state, beside the check
// for empty (see Released.At). An error from it only makes At 0.
func (s *Shard) Release(ctx context.Context, read func(*sql.DB) error) (released bool, f Released, err error) {
	var c closed
	if err := s.do(ctx, func() error {
		var err error
		if c, err = s.closeForRelease(ctx, read); err != nil && !errors.Is(err, ErrClosed) {
			s.end() // capture is closed or half closed: it stays closed
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
	return true, c.Released, nil
}

// Released is what Release found about the shard's final state. It is
// meaningful when the shard was released.
type Released interface {
	released()
}

// Deleted says the shard holds nothing of the app's (the app never wrote to
// it, removed everything it wrote or marked it for deletion), so it is deleted
// rather than kept. Marked says the app marked the shard for deletion.
type Deleted struct{ Marked bool }

func (*Deleted) released() {}

// Kept says the shard is kept, and the release's read saw exactly this
// position, which is the shard's final position.
type Kept struct{ At pos.Position }

func (*Kept) released() {}

// Uncertain says the shard is kept, and the read did not see exactly its final
// state: the read failed, or the final sync captured more.
type Uncertain struct{}

func (*Uncertain) released() {}

// settle is the last step of Release, under the lock, with nothing of the node's
// open on the shard: the -shm check, then the header compared with the one
// read after the final sync. It ends the activation if the shard can be let
// go of, and otherwise reopens capture.
func (s *Shard) settle(c closed, ferr error) (released bool, err error) {
	held, perr := s.side.Held()
	// A commit after the final sync changes the header, even if the app has
	// closed the file since (the mapping outlives a deleted -shm). If the app
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
	// Resume where capture left off.
	if err := s.mapWALIndex(); err != nil {
		s.end()
		return false, err
	}
	if err := s.open(context.Background()); err != nil {
		s.end()
		return false, err
	}
	// The read transaction comes with the write lock, which also decides whether
	// the WAL is the one capture was reading (see locked). Without it, the app
	// closing the file would be the last connection: SQLite would checkpoint and
	// restart the WAL under capture. If it can't be had, the next sync tries again.
	if err := s.locked(context.Background(), true); err != nil && ferr == nil && perr == nil {
		return false, err
	}
	if ferr != nil {
		return false, ferr
	}
	return false, perr
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

// TailFrom is the lowest node journal segment that may hold one of the shard's
// tail files (including files still being written, and one being queued), or
// 0 if the shard has no tail. The node's lease records the lowest over all its
// shards.
func (s *Shard) TailFrom() pos.Seg { return s.tail.view.Load().from }

// Tail returns the commits captured since the shard's last delta, in position
// order (earlier activations first, then this one). The caller holds a
// reference to each file and closes them (TailFiles.Close) when done.
func (s *Shard) Tail() (files TailFiles) {
	s.call(func() { files = s.tail.files.Retained() })
	return files
}

// TailInfo is how many files the tail has and the age of the oldest, without
// taking references to them.
func (s *Shard) TailInfo() (n int, oldest time.Time) {
	v := s.tail.view.Load()
	return v.files, v.oldest
}

// TailBytes is how many bytes the tail's files hold (in memory, or in temporary
// files when they are large).
func (s *Shard) TailBytes() int64 { return s.tail.view.Load().bytes }

// Discard gives up the shard's tail without writing it anywhere: for a shard
// that is finished with or whose tail has been handed to a new Shard (Tail).
// Closing or releasing a shard leaves its tail, since the node still writes it
// as a delta then.
func (s *Shard) Discard() { s.call(s.tail.discard) }

// DropTail forgets tail files that end at or before position, once a delta
// holds them.
func (s *Shard) DropTail(position pos.Position) { s.call(func() { s.tail.drop(position) }) }
