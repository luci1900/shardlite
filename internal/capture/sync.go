package capture

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/model/pos"
)

// errBusy says the write lock could not be had in time.
var errBusy = errors.New("capture: the shard is being written")

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
	log, err := s.tail.log(s.cfg.TempDir)
	if err != nil {
		return err
	}
	w, err := log.Append()
	if err != nil {
		return err
	}
	pg := pages{wal: s.walF, db: s.dbF, pageSize: s.pageSize, h: h, pf: pf}
	hdr := ltx.Header{
		Version:   ltx.Version,
		PageSize:  uint32(s.pageSize),
		Commit:    uint32(commit),
		MinTXID:   ltx.TXID(pos),
		MaxTXID:   ltx.TXID(pos.Add(count)),
		Timestamp: s.cfg.Now().UnixMilli(),
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
	s.tail.add(body, count, seg, s.cfg.Now())
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
	before   func()      // if set, called before each page is read
}

// page reads page pgno into buf, which is one page long.
func (p pages) page(pgno Pgno, buf []byte) error {
	if p.before != nil {
		p.before()
	}
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
// queued in the journal writer and in the shard's tail. It returns when they
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
// in blob storage (it flushes the journal writer).
func (s *Shard) Sync(ctx context.Context) error {
	if err := s.Capture(ctx); err != nil {
		return err
	}
	return s.cfg.Writer.Flush(ctx)
}
