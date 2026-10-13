package capture

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
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
type client struct {
	cfg Config

	mu      sync.Mutex
	carried []TailFile // from earlier activations, restored from the journal
	tail    []TailFile // captured in this activation
	high    uint64     // highest Litestream commit number uploaded in this activation
	nextPos uint64
}

func newClient(cfg Config) *client {
	c := &client{cfg: cfg, nextPos: 1}
	if r := cfg.Restored; r != nil {
		c.carried = append([]TailFile(nil), r.Tail...) // the client owns their references now
		c.nextPos = r.Position + 1
		c.high = uint64(seedTXID) // Litestream continues from the file that seeded it
	}
	return c
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

func (c *client) position() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nextPos - 1
}

// point is where the client is: the highest Litestream commit number it has
// been given in this activation, and the shard's position after it.
func (c *client) point() (txid, position uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.high, c.nextPos - 1
}

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

	c.mu.Lock()
	// The file is queued in the writer and recorded in the tail at once, without
	// waiting for its segment. The writer never drops a queued file: it retries
	// until the segment is written, or the node is fenced and has to stop. Whoever
	// needs the commits durable flushes the writer after the sync.
	pos, seg := c.nextPos, c.cfg.Writer.NextSeq()
	done := c.cfg.Writer.AppendFile(c.cfg.Name, c.cfg.Generation, pos, body)
	select {
	case err := <-done:
		// Refused: the writer is closed or fenced, or the node's lease has run out.
		c.mu.Unlock()
		return nil, err
	default:
	}
	count := uint64(maxTXID-minTXID) + 1
	now := time.Now()
	c.tail = append(c.tail, TailFile{Position: pos, Count: count, Body: body.Retain(), At: now, Seg: seg, minTXID: uint64(minTXID), maxTXID: uint64(maxTXID), local: true})
	c.nextPos = pos + count
	c.high = max(c.high, uint64(maxTXID))
	c.mu.Unlock()
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
		c.mu.Lock()
		var top uint64
		for _, f := range c.tail {
			infos = append(infos, &ltx.FileInfo{Level: 0, MinTXID: ltx.TXID(f.minTXID), MaxTXID: ltx.TXID(f.maxTXID), Size: f.Body.Size(), CreatedAt: f.At})
			top = max(top, f.maxTXID)
		}
		if c.high > top {
			// Everything up to here has been through the client: the seed, or tail
			// files that are in a delta by now.
			infos = append(infos, &ltx.FileInfo{Level: 0, MinTXID: ltx.TXID(c.high), MaxTXID: ltx.TXID(c.high), CreatedAt: time.Now()})
		}
		c.mu.Unlock()
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
		c.mu.Lock()
		for _, f := range c.tail {
			if f.minTXID == uint64(minTXID) && f.maxTXID == uint64(maxTXID) {
				body = f.Body.Retain()
			}
		}
		c.mu.Unlock()
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

// tailFrom is the lowest segment number that may hold a file of this shard's
// tail (files still in the writer's queue are in it too), or 0 if there are none.
func (c *client) tailFrom() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var low uint64
	note := func(seg uint64) {
		if seg != 0 && (low == 0 || seg < low) {
			low = seg
		}
	}
	for _, f := range c.carried {
		note(f.Seg)
	}
	for _, f := range c.tail {
		note(f.Seg)
	}
	return low
}

// tailCopy lists the tail, each file with a reference of its own for the caller.
func (c *client) tailCopy() []TailFile {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]TailFile, 0, len(c.carried)+len(c.tail))
	for _, f := range append(append([]TailFile(nil), c.carried...), c.tail...) {
		f.Body.Retain()
		out = append(out, f)
	}
	return out
}

// tailInfo is how many files the tail has and the age of the oldest.
func (c *client) tailInfo() (n int, oldest time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	all := append(append([]TailFile(nil), c.carried...), c.tail...)
	if len(all) == 0 {
		return 0, time.Time{}
	}
	return len(all), all[0].At
}

// tailBytes is the size of the files of the tail.
func (c *client) tailBytes() (n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, f := range c.carried {
		n += f.Body.Size()
	}
	for _, f := range c.tail {
		n += f.Body.Size()
	}
	return n
}

// discard gives up every file of the tail. The client holds none afterwards.
func (c *client) discard() {
	c.mu.Lock()
	defer c.mu.Unlock()
	CloseTail(c.carried)
	CloseTail(c.tail)
	c.carried, c.tail = nil, nil
}

// dropTail removes tail files that end at or before position and closes their
// references. It returns them (their Bodies are closed: only the numbers are
// meaningful) and the highest Litestream commit number of this activation.
func (c *client) dropTail(position uint64) (dropped []TailFile, newest uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	newest = c.high
	cut := func(a []TailFile) []TailFile {
		i := 0
		for i < len(a) && a[i].Last() <= position {
			i++
		}
		dropped = append(dropped, a[:i]...)
		return append([]TailFile(nil), a[i:]...)
	}
	c.carried = cut(c.carried)
	c.tail = cut(c.tail)
	CloseTail(dropped)
	return dropped, newest
}

var _ litestream.ReplicaClient = (*client)(nil)
