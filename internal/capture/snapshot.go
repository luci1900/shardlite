package capture

import (
	"bufio"
	"context"
	"io"
	"sync"

	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/kill"
	"github.com/luci1900/shardlite/internal/pos"
)

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
		sr := &snapshotReader{PipeReader: pr, s: s}
		hdr := ltx.Header{
			Version:   ltx.Version,
			PageSize:  uint32(s.pageSize),
			Commit:    uint32(s.dbPages),
			MinTXID:   1,
			MaxTXID:   ltx.TXID(position),
			Timestamp: s.cfg.Now().UnixMilli(),
		}
		pg := pages{wal: s.walF, db: s.dbF, pageSize: s.pageSize, h: h, pf: pf, before: func() { s.at(kill.SnapshotPage) }}
		// The checksum of the database at the snapshot's position, which the
		// snapshot's own checksum is compared with, and the checksums it is
		// compared against: the pages are read without the shard's lock.
		if s.cfg.SkewChecksum {
			s.cks.sum ^= skewMask
		}
		want, cks := s.cks.Checksum(), s.cks
		sr.eg.Go(func() error {
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
			pw.CloseWithError(err) // the reader gets the error, so the goroutine has none
			return nil
		})
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
	eg   errgroup.Group // the goroutine writing the file
	s    *Shard
	once sync.Once
}

func (r *snapshotReader) Close() error {
	r.once.Do(func() {
		r.PipeReader.CloseWithError(io.ErrClosedPipe)
		r.eg.Wait()
		r.s.call(func() { r.s.snaps-- })
	})
	return nil
}
