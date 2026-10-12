package capture

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/superfly/ltx"
)

// TailFile is one captured commit file not yet in a delta.
type TailFile struct {
	Position uint64 // the shard's position of its first commit
	Count    uint64 // the number of commits in the file
	Data     []byte
	At       time.Time
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

// client is the ReplicaClient for one shard. It writes level 0 files into the
// node's pending segment, numbers them with the shard's own positions and
// keeps them in memory as the shard's tail.
type client struct {
	cfg Config

	mu        sync.Mutex
	carried   []TailFile // from earlier activations, restored from the journal
	tail      []TailFile // captured in this activation
	high      uint64     // highest Litestream commit number uploaded in this activation
	nextPos   uint64
	skipFirst bool // restored, and Litestream's first snapshot hasn't been written yet
}

func newClient(cfg Config) *client {
	c := &client{cfg: cfg, nextPos: 1}
	if r := cfg.Restored; r != nil {
		c.carried = append([]TailFile(nil), r.Tail...)
		c.nextPos = r.Position + 1
		c.skipFirst = true
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

func (c *client) WriteLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, r io.Reader) (*ltx.FileInfo, error) {
	if level != 0 {
		return nil, fmt.Errorf("capture: level %d not supported (compaction is off)", level)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.skipFirst {
		// The first file of an activation of a restored shard is Litestream's
		// snapshot of exactly what was restored. It is not uploaded.
		if _, full, err := isFullSnapshot(data); err != nil {
			c.mu.Unlock()
			return nil, err
		} else if minTXID != 1 || !full {
			c.mu.Unlock()
			return nil, fmt.Errorf("capture: first file of a restored shard is %s and not a full snapshot", ltx.FormatFilename(minTXID, maxTXID))
		}
		c.skipFirst = false
		c.high = uint64(maxTXID)
		c.mu.Unlock()
		return &ltx.FileInfo{Level: 0, MinTXID: minTXID, MaxTXID: maxTXID, Size: int64(len(data)), CreatedAt: time.Now()}, nil
	}
	// The file is queued in the writer and recorded in the tail at once, without
	// waiting for its segment. The writer never drops a queued file: it retries
	// until the segment is written, or the node is fenced and has to stop. Whoever
	// needs the commits durable flushes the writer after the sync.
	pos, seg := c.nextPos, c.cfg.Writer.NextSeq()
	done := c.cfg.Writer.Append(c.cfg.Name, c.cfg.Generation, pos, data)
	select {
	case err := <-done:
		// Refused: the writer is closed or fenced, or the node's lease has run out.
		c.mu.Unlock()
		return nil, err
	default:
	}
	count := uint64(maxTXID-minTXID) + 1
	now := time.Now()
	c.tail = append(c.tail, TailFile{Position: pos, Count: count, Data: data, At: now, Seg: seg, minTXID: uint64(minTXID), maxTXID: uint64(maxTXID), local: true})
	c.nextPos = pos + count
	c.high = max(c.high, uint64(maxTXID))
	c.mu.Unlock()
	return &ltx.FileInfo{Level: 0, MinTXID: minTXID, MaxTXID: maxTXID, Size: int64(len(data)), CreatedAt: now}, nil
}

// LTXFiles lists this activation's files. A restored shard's earlier
// activations are not listed, so Litestream starts from nothing.
func (c *client) LTXFiles(ctx context.Context, level int, seek ltx.TXID, useMetadata bool) (ltx.FileIterator, error) {
	var infos []*ltx.FileInfo
	if level == 0 {
		c.mu.Lock()
		for _, f := range c.tail {
			infos = append(infos, &ltx.FileInfo{Level: 0, MinTXID: ltx.TXID(f.minTXID), MaxTXID: ltx.TXID(f.maxTXID), Size: int64(len(f.Data)), CreatedAt: f.At})
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
	var data []byte
	if level == 0 {
		c.mu.Lock()
		for _, f := range c.tail {
			if f.minTXID == uint64(minTXID) && f.maxTXID == uint64(maxTXID) {
				data = f.Data
			}
		}
		c.mu.Unlock()
	}
	if data == nil {
		return nil, fmt.Errorf("capture: ltx file %s: %w", ltx.FormatFilename(minTXID, maxTXID), litestream.ErrLTXMissing)
	}
	if offset > int64(len(data)) {
		offset = int64(len(data))
	}
	data = data[offset:]
	if size > 0 && size < int64(len(data)) {
		data = data[:size]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

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

func (c *client) tailCopy() []TailFile {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]TailFile, 0, len(c.carried)+len(c.tail))
	out = append(out, c.carried...)
	return append(out, c.tail...)
}

// dropTail removes tail files that end at or before position. It returns
// them and the highest Litestream commit number of this activation.
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
	return dropped, newest
}

// isFullSnapshot reports whether an LTX file holds every page of the database.
func isFullSnapshot(data []byte) (ltx.Header, bool, error) {
	dec := ltx.NewDecoder(bytes.NewReader(data))
	if err := dec.DecodeHeader(); err != nil {
		return ltx.Header{}, false, err
	}
	hdr := dec.Header()
	var n uint32
	buf := make([]byte, hdr.PageSize)
	for {
		var ph ltx.PageHeader
		if err := dec.DecodePage(&ph, buf); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return hdr, false, err
		}
		n++
	}
	lock := uint32(0)
	if hdr.LockPgno() <= hdr.Commit {
		lock = 1 // the lock page is never stored
	}
	return hdr, n+lock == hdr.Commit, nil
}

var _ litestream.ReplicaClient = (*client)(nil)
