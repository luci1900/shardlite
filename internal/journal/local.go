package journal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/pos"
)

// Local is the node's own disk as a home for a journal: the segments in the
// node's local database (internal/localdb), in fixed-size chunks.
//
// A segment's bytes are a run of chunks, not one value: SQLite's substr over a
// large blob materialises the whole value, so a range read of a segment kept
// as one blob would cost the whole segment, which the Backend's contract
// forbids. The chunks are 64 KB and their boundaries are at multiples of that,
// so a range read finds its chunks from the offset alone and needs no index of
// them.
//
// A create is one transaction that inserts the segment's row and its chunks:
// the segment appears whole or not at all. The write id is a column, so the
// read-back that tells our own earlier attempt from somebody else's segment is
// a query and there is no sidecar file to keep consistent.
//
// A local store decides nothing. The fence is created in the record, and a
// local segment is a copy of what is below it.
type Local struct {
	db   *sql.DB
	node cluster.Node // whose journal this is
}

// NewLocal returns the node's own disk as a journal's home. db is the node's
// local database, which holds the journal's segments (and the cache index's
// rows).
func NewLocal(db *sql.DB, node cluster.Node) *Local { return &Local{db: db, node: node} }

// Name is the node whose journal this is.
func (x *Local) Name() cluster.Node { return x.node }

// chunkSize is how large a chunk of a segment's bytes is. The chunks are at
// multiples of it, so a range read finds them from the offset alone. It is
// part of the format the bytes are stored in, since the boundaries are at
// multiples of it: a change to it is a migration that drops the chunks,
// because nothing else can find them.
const chunkSize = 64 << 10

// Create writes the segment and returns only when it is durable: SQLite's
// commit with synchronous=FULL, so the bytes are on disk when it returns. It
// fails with blob.ErrExists if the number is taken, and the segment's row and
// its chunks go in one transaction, so a reader never sees half a segment.
func (x *Local) Create(ctx context.Context, seq pos.Seg, body blob.Body, id blob.WriteID) error {
	wire, err := json.Marshal(id)
	if err != nil {
		return err
	}
	tx, err := x.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `INSERT INTO segments (node, seq, write_id, size) VALUES (?, ?, ?, ?)
		ON CONFLICT (node, seq) DO NOTHING`, x.node, int64(seq), string(wire), body.Size())
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return blob.ErrExists // somebody else's segment holds the number
	}
	buf := make([]byte, chunkSize)
	for off, idx := int64(0), 0; off < body.Size(); idx++ {
		m, err := body.ReadAt(buf, off)
		if m > 0 {
			if _, err := tx.ExecContext(ctx, `INSERT INTO segment_chunks (node, seq, idx, data) VALUES (?, ?, ?, ?)`,
				x.node, int64(seq), idx, buf[:m]); err != nil {
				return err
			}
			off += int64(m)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // the last chunk is short
			}
			return err
		}
		if m == 0 {
			return fmt.Errorf("journal: the body of segment %d gave no bytes", seq)
		}
	}
	return tx.Commit()
}

// Get returns the whole segment, blob.ErrNotFound if there is no such segment.
func (x *Local) Get(ctx context.Context, seq pos.Seg) (io.ReadCloser, error) {
	if _, err := x.size(ctx, seq); err != nil {
		return nil, err
	}
	return x.read(ctx, seq, 0, 0)
}

// GetRange reads length bytes from offset (to the end if length is not
// positive or reaches past it). A negative offset asks for the last -offset
// bytes of the segment, and the length must then be zero.
func (x *Local) GetRange(ctx context.Context, seq pos.Seg, offset, length int64) (io.ReadCloser, error) {
	size, err := x.size(ctx, seq)
	if err != nil {
		return nil, err
	}
	if offset < 0 {
		if length != 0 {
			return nil, fmt.Errorf("journal: a range from the end takes no length (got %d)", length)
		}
		offset = max(size+offset, 0)
		length = size - offset
	}
	if offset > size {
		return nil, fmt.Errorf("journal: range offset %d out of bounds (size %d)", offset, size)
	}
	if length <= 0 || offset+length > size {
		length = size - offset
	}
	return x.read(ctx, seq, offset, length)
}

// Head says the segment's size (0 is a fence) and write id.
func (x *Local) Head(ctx context.Context, seq pos.Seg) (SegmentInfo, error) {
	var size int64
	var wire string
	err := x.db.QueryRowContext(ctx, `SELECT size, write_id FROM segments WHERE node = ? AND seq = ?`,
		x.node, int64(seq)).Scan(&size, &wire)
	if errors.Is(err, sql.ErrNoRows) {
		return SegmentInfo{}, blob.ErrNotFound
	}
	if err != nil {
		return SegmentInfo{}, err
	}
	var id blob.WriteID
	if err := json.Unmarshal([]byte(wire), &id); err != nil {
		return SegmentInfo{}, fmt.Errorf("journal: the write id of segment %d is bad: %w", seq, err)
	}
	return SegmentInfo{Size: size, WriteID: id}, nil
}

// Delete removes the segment. A number with no segment is done: the callers
// accept both answers.
func (x *Local) Delete(ctx context.Context, seq pos.Seg) error {
	tx, err := x.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM segment_chunks WHERE node = ? AND seq = ?`, x.node, int64(seq)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM segments WHERE node = ? AND seq = ?`, x.node, int64(seq)); err != nil {
		return err
	}
	return tx.Commit()
}

// List returns the journal's segment numbers from from (inclusive) on, in
// number order.
func (x *Local) List(ctx context.Context, from pos.Seg) iter.Seq2[pos.Seg, error] {
	if from < 1 {
		from = 1
	}
	return func(yield func(pos.Seg, error) bool) {
		rows, err := x.db.QueryContext(ctx, `SELECT seq FROM segments WHERE node = ? AND seq >= ? ORDER BY seq`,
			x.node, int64(from))
		if err != nil {
			yield(0, err)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var s int64
			if err := rows.Scan(&s); err != nil {
				yield(0, err)
				return
			}
			if !yield(pos.Seg(s), nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(0, err)
		}
	}
}

// size is the segment's size, blob.ErrNotFound if there is no such segment.
func (x *Local) size(ctx context.Context, seq pos.Seg) (int64, error) {
	var size int64
	err := x.db.QueryRowContext(ctx, `SELECT size FROM segments WHERE node = ? AND seq = ?`, x.node, int64(seq)).Scan(&size)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, blob.ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return size, nil
}

// read returns a reader over the segment's bytes from offset, at most length
// bytes (all of them if length is not positive). It reads the chunks the
// range covers and no others.
func (x *Local) read(ctx context.Context, seq pos.Seg, offset, length int64) (io.ReadCloser, error) {
	first := offset / chunkSize
	rows, err := x.db.QueryContext(ctx, `SELECT data FROM segment_chunks WHERE node = ? AND seq = ? AND idx >= ? ORDER BY idx`,
		x.node, int64(seq), int64(first))
	if err != nil {
		return nil, err
	}
	return &chunkReader{rows: rows, skip: int(offset % chunkSize), left: length}, nil
}

// chunkReader reads a segment's chunks in order, as one stream of bytes.
type chunkReader struct {
	rows *sql.Rows
	skip int   // bytes to skip in the first chunk
	left int64 // bytes left to read, 0 for all of them
	buf  []byte
	pos  int
	done bool
}

func (r *chunkReader) Read(p []byte) (int, error) {
	for {
		if r.done {
			return 0, io.EOF
		}
		if r.pos < len(r.buf) {
			n := copy(p, r.buf[r.pos:])
			if r.left > 0 && int64(n) > r.left {
				n = int(r.left)
			}
			r.pos += n
			if r.left > 0 {
				r.left -= int64(n)
				if r.left == 0 {
					r.done = true
				}
			}
			return n, nil
		}
		if !r.rows.Next() {
			if err := r.rows.Err(); err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
		var data []byte
		if err := r.rows.Scan(&data); err != nil {
			return 0, err
		}
		r.buf, r.pos = data, 0
		if r.skip > 0 {
			if r.skip >= len(r.buf) {
				r.skip -= len(r.buf)
				r.buf = nil
				continue
			}
			r.pos = r.skip
			r.skip = 0
		}
	}
}

func (r *chunkReader) Close() error { return r.rows.Close() }
