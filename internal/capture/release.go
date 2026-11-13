package capture

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/luci1900/shardlite/internal/pos"
)

// IfHeld says what Release does when another process still has the shard open.
type IfHeld int

const (
	// ResumeIfHeld is the zero value: capture is reopened where it left off and
	// Release reports the shard as not released.
	ResumeIfHeld IfHeld = iota
	// GoAheadIfHeld lets go though the other process keeps its handle, which the
	// node does only at a stop (the app is stopping too, and a commit through
	// the handle afterwards is lost). The wal-index header is still compared
	// before and after, so a commit made during the release resumes capture
	// as it does for ResumeIfHeld. OpenAtRelease says whether the file was open.
	GoAheadIfHeld
)

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

// Release is how the node lets go of a shard (a deactivation or a stop).
// It does a final sync and closes every connection and file handle the node has
// on the shard, waits until the files the final sync queued are in blob storage,
// and only then asks whether another process still has the shard open (see
// walsidecar.go for why the order matters).
//
// If one does (and ifHeld is ResumeIfHeld), a commit was made after the final
// sync, or the wait failed, capture is reopened where it left off and Release
// returns false: the node must not let go. Otherwise the activation is
// over and Release returns true. If the close or the reopen fails, the
// activation is over too, and Release returns the error (Open says which).
//
// read, if not nil, reads the database at its final state, beside the check
// for empty (see Released.At). An error from it only makes At 0.
func (s *Shard) Release(ctx context.Context, read func(*sql.DB) error, ifHeld IfHeld) (released bool, f Released, err error) {
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
		released, err = s.settle(c, ferr, ifHeld)
		return err
	})
	if err != nil || !released {
		return false, f, err
	}
	return true, c.Released, nil
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
	if eerr != nil && err == nil {
		// A marked check that could not run (the app dropped _shardlite_delete)
		// fails the release, so the shard is kept and the error is the caller's
		// to report, rather than the shard reading as unmarked.
		err = eerr
	}
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

// MarkedQuery says whether the app has marked the shard for deletion (a row in
// _shardlite_delete). It asks the table rather than sqlite_schema, so a shard
// whose table is missing (the app dropped it) fails instead of reading as
// unmarked.
const MarkedQuery = `SELECT EXISTS (SELECT 1 FROM _shardlite_delete)`

// holdsNothing reports whether the database holds nothing of the app's. Either
// the app marked it for deletion (a row in _shardlite_delete, whatever else is
// in it), or it has no tables or indexes besides shardlite's (the _shardlite_
// tables, empty or not; an index counts as its table's, so the automatic one of
// a reserved table's text key is ignored), user_version and application_id are
// both 0, and _shardlite_reminders has no rows (a reminder the node gave up on
// is the app's data too). marked says which it was. It asks through capture's
// own connections, so the node opens nothing of the shard's (see walsidecar.go
// for why that matters), and an _shardlite_delete that is missing fails the
// marked check rather than reading the shard as unmarked.
func (s *Shard) holdsNothing(ctx context.Context) (empty, marked bool, err error) {
	sqldb := s.pool
	if sqldb == nil {
		return false, false, ErrClosed
	}
	if err := sqldb.QueryRowContext(ctx, MarkedQuery).Scan(&marked); err != nil || marked {
		if err != nil {
			err = fmt.Errorf("asking for the delete marker: %w", err)
		}
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

// OpenAtRelease says whether a Release told to go ahead (GoAheadIfHeld) let go of
// the shard though another process still had it open. It is false for a shard that
// was not released, or released with nobody holding it.
func (s *Shard) OpenAtRelease() (open bool) {
	s.call(func() { open = s.openAtRelease })
	return open
}

// settle is the last step of Release, under the lock, with nothing of the node's
// open on the shard: the -shm check, then the header compared with the one
// read after the final sync. It ends the activation if the shard can be let
// go of, and otherwise reopens capture.
func (s *Shard) settle(c closed, ferr error, ifHeld IfHeld) (released bool, err error) {
	held, perr := s.side.Held()
	// A commit after the final sync changes the header, even if the app has
	// closed the file since (the mapping outlives a deleted -shm). If the app
	// opened it again, SQLite reset -shm and the header reads differently or not
	// at all. Either way the commit isn't captured, so capture resumes, whether
	// or not the node goes ahead over an open file.
	hdr, herr := s.header()
	s.unmap()
	changed := herr != nil || c.hdrErr != nil || hdr != c.hdr
	if ferr == nil && perr == nil && !changed && (!held || ifHeld == GoAheadIfHeld) {
		s.openAtRelease = held
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
