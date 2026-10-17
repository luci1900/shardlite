package capture

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/spool"
)

// TailFile is one captured commit file not yet in a delta.
type TailFile struct {
	Position uint64 // the shard's position of its first commit
	Count    uint64 // the number of commits in the file
	// Body is the LTX file: in memory if it is small, in a temporary file if it is
	// larger than the spool limit. Whoever holds a TailFile holds a reference to
	// it and closes it (CloseTail).
	Body *spool.File
	At   time.Time
	// Seg is a lower bound on the number of the node journal segment holding the
	// file: the writer's next number when the file was queued. The node's lease
	// records the oldest one still needed.
	Seg uint64

	// Litestream's commit numbers, meaningful only within the activation that
	// captured the file. Zero unless local.
	minTXID, maxTXID uint64
	local            bool // has a level 0 copy in this activation's metadata directory
}

// Last is the shard's position of its last commit.
func (t TailFile) Last() uint64 { return t.Position + t.Count - 1 }

// CloseTail gives up the references a list of tail files holds (what Tail
// returns, and what a restore hands to Config.Restored).
func CloseTail(files []TailFile) {
	for _, f := range files {
		f.Body.Close()
	}
}

// client is the ReplicaClient for one shard. It writes level 0 files into the
// node's pending segment, numbers them with the shard's own positions and
// keeps them in memory as the shard's tail.
//
// Litestream calls it from inside a sync, a snapshot or a close, which run
// under the shard's lock, and the shard calls it under the lock too, so the
// lock guards its state (see Shard). Others read view.
type client struct {
	cfg Config

	// tail is in position order: the files restored from the journal (from
	// earlier activations, not local) first, then this activation's.
	tail    []TailFile
	high    uint64 // highest Litestream commit number uploaded in this activation
	nextPos uint64

	view atomic.Pointer[tailView] // published under the shard's lock whenever the tail or the position changes
}

// tailView is what the shard tells others without its lock: TailFrom (the
// lease reads it), TailInfo and TailBytes (the watchdog and the delta timer)
// and Position. It is never changed once published.
type tailView struct {
	position uint64
	from     uint64
	files    int
	oldest   time.Time
	bytes    int64
}

func newClient(cfg Config) *client {
	c := &client{cfg: cfg, nextPos: 1}
	if r := cfg.Restored; r != nil {
		c.tail = append([]TailFile(nil), r.Tail...) // the client owns their references now
		c.nextPos = r.Position + 1
		c.high = uint64(seedTXID) // Litestream continues from the file that seeded it
	}
	c.publish(0)
	return c
}

// publish makes the tail and position as they are now the view. queuing, if
// not 0, is the segment bound of a file about to be queued, which TailFrom
// counts already.
func (c *client) publish(queuing uint64) {
	v := &tailView{position: c.nextPos - 1}
	note := func(seg uint64) {
		if seg != 0 && (v.from == 0 || seg < v.from) {
			v.from = seg
		}
	}
	note(queuing)
	for _, f := range c.tail {
		note(f.Seg)
		v.bytes += f.Body.Size()
	}
	if len(c.tail) > 0 {
		v.files, v.oldest = len(c.tail), c.tail[0].At
	}
	c.view.Store(v)
}

func (c *client) Type() string                   { return "shardlite" }
func (c *client) Init(ctx context.Context) error { return nil }
func (c *client) SetLogger(logger *slog.Logger)  {}
func (c *client) DeleteAll(ctx context.Context) error {
	return errors.New("capture: DeleteAll not supported")
}

// DeleteLTXFiles does nothing: compaction is off, and the tail is dropped by
// the node when a delta is written.
func (c *client) DeleteLTXFiles(ctx context.Context, a []*ltx.FileInfo) error { return nil }

func (c *client) position() uint64 { return c.nextPos - 1 }

// point is where the client is: the highest Litestream commit number it has
// been given in this activation, and the shard's position after it.
func (c *client) point() (txid, position uint64) { return c.high, c.nextPos - 1 }

func (c *client) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	if level != 0 {
		return nil, fmt.Errorf("capture: level %d not supported (compaction is off)", level)
	}
	// A commit can be as large as the database (an import, a VACUUM), so the file
	// is collected into a temporary file when it is large and never held whole in
	// memory.
	body, err := spool.Collect(c.cfg.TempDir, c.cfg.spoolLimit(), r)
	if err != nil {
		return nil, err
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
	pos, seg := c.nextPos, c.cfg.Writer.NextSeq()
	c.publish(seg)
	if err := c.cfg.Writer.AppendFile(c.cfg.Name, c.cfg.Generation, pos, body); err != nil {
		c.publish(0) // refused: the writer is closed or fenced
		return nil, err
	}
	count := uint64(maxTXID-minTXID) + 1
	now := time.Now()
	c.tail = append(c.tail, TailFile{Position: pos, Count: count, Body: body.Retain(), At: now, Seg: seg, minTXID: uint64(minTXID), maxTXID: uint64(maxTXID), local: true})
	c.nextPos = pos + count
	c.high = max(c.high, uint64(maxTXID))
	c.publish(0)
	return &ltx.FileInfo{Level: 0, MinTXID: minTXID, MaxTXID: maxTXID, Size: body.Size(), CreatedAt: now}, nil
}

// LTXFiles lists this activation's files. A restored shard's earlier
// activations are not listed, so Litestream starts from nothing, or from the
// seed (see seed). Litestream's replica works out what it has "uploaded" from
// the highest commit number listed, so that is always the highest it has been
// given in this activation, though the files before it have been dropped from
// the tail since: a replica made anew over the same metadata directory (after a
// release that did not go through) then goes on from there, and doesn't look for
// level 0 files that were deleted.
func (c *client) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	var infos []*ltx.FileInfo
	if level == 0 {
		var top uint64
		for _, f := range c.tail {
			if f.local {
				infos = append(infos, &ltx.FileInfo{Level: 0, MinTXID: ltx.TXID(f.minTXID), MaxTXID: ltx.TXID(f.maxTXID), Size: f.Body.Size(), CreatedAt: f.At})
				top = max(top, f.maxTXID)
			}
		}
		if c.high > top {
			// Everything up to here has been through the client: the seed, or tail
			// files that are in a delta by now.
			infos = append(infos, &ltx.FileInfo{Level: 0, MinTXID: ltx.TXID(c.high), MaxTXID: ltx.TXID(c.high), CreatedAt: time.Now()})
		}
		if seek > 0 {
			kept := infos[:0]
			for _, info := range infos {
				if info.MinTXID >= seek {
					kept = append(kept, info)
				}
			}
			infos = kept
		}
	}
	return ltx.NewFileInfoSliceIterator(infos), nil
}

func (c *client) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	var body *spool.File
	if level == 0 {
		for _, f := range c.tail {
			if f.local && f.minTXID == uint64(minTXID) && f.maxTXID == uint64(maxTXID) {
				body = f.Body.Retain()
			}
		}
	}
	if body == nil {
		return nil, fmt.Errorf("capture: ltx file %s: %w", ltx.FormatFilename(minTXID, maxTXID), litestream.ErrLTXMissing)
	}
	rd := body.Reader()
	if offset > body.Size() {
		offset = body.Size()
	}
	if _, err := rd.Seek(offset, io.SeekStart); err != nil {
		body.Close()
		return nil, err
	}
	var r io.Reader = rd
	if size > 0 && size < body.Size()-offset {
		r = io.LimitReader(rd, size)
	}
	return &bodyReader{Reader: r, body: body}, nil
}

// bodyReader reads a tail file and gives its reference back when closed.
type bodyReader struct {
	io.Reader
	body *spool.File
}

func (b *bodyReader) Close() error { return b.body.Close() }

// tailCopy lists the tail, each file with a reference of its own for the caller.
func (c *client) tailCopy() []TailFile {
	out := make([]TailFile, 0, len(c.tail))
	for _, f := range c.tail {
		f.Body.Retain()
		out = append(out, f)
	}
	return out
}

// discard gives up every file of the tail. The client holds none afterwards.
func (c *client) discard() {
	CloseTail(c.tail)
	c.tail = nil
	c.publish(0)
}

// dropTail removes tail files that end at or before position and closes their
// references. It returns them (their Bodies are closed: only the numbers are
// meaningful) and the highest Litestream commit number of this activation.
func (c *client) dropTail(position uint64) (dropped []TailFile, newest uint64) {
	i := 0
	for i < len(c.tail) && c.tail[i].Last() <= position {
		i++
	}
	dropped = c.tail[:i]
	c.tail = append([]TailFile(nil), c.tail[i:]...)
	CloseTail(dropped)
	c.publish(0)
	return dropped, c.high
}

var _ litestream.ReplicaClient = (*client)(nil)
