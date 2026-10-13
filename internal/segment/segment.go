// Package segment writes and reads a node's journal: numbered objects that each
// hold every shard's LTX files from one flush interval.
//
// Layout of a segment object:
//
//	LTX file bytes, exactly as Litestream produced them, back to back
//	footer: JSON array of Entry
//	8 bytes: footer length, big-endian
//	8 bytes: magic "SHLSEG01"
//
// An empty object is a fence (see docs/internals.md, Fencing).
package segment

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/spool"
)

const (
	magic         = "SHLSEG01"
	trailerSize   = 16
	DefaultFlush  = 250 * time.Millisecond
	seqKeyDigits  = 20
	maxFooterSize = 1 << 30
)

var (
	// ErrFenced means another writer took a segment number: this writer must stop for good.
	ErrFenced = errors.New("segment: writer fenced")
	// ErrClosed is returned to waiters of a writer that was closed.
	ErrClosed = errors.New("segment: writer closed")
	// ErrFence is returned by ReadFooter for an empty object.
	ErrFence = errors.New("segment: fence")
	// ErrCorrupt is returned for a segment that doesn't parse.
	ErrCorrupt = errors.New("segment: corrupt")
)

// Entry locates one LTX file inside a segment. Position is the shard's own
// position of the entry's first commit and Count the number of commits it
// holds, so the entry covers Position to Position+Count-1. Both are assigned
// by the node, and restore compares them with the manifest's files. Nothing of
// Litestream's numbering is stored.
type Entry struct {
	Shard      string `json:"shard"`
	Generation string `json:"generation"`
	Position   uint64 `json:"position"`
	Count      uint64 `json:"count"`
	Offset     int64  `json:"offset"`
	Length     int64  `json:"length"`
}

// Last is the position of the entry's last commit.
func (e Entry) Last() uint64 { return e.Position + e.Count - 1 }

// Prefix is the key prefix of a node's journal.
func Prefix(node string) string { return "journal/" + node + "/" }

// Key returns the object key of a node's segment.
func Key(node string, seq uint64) string {
	return fmt.Sprintf("journal/%s/%0*d", node, seqKeyDigits, seq)
}

// Options configure a Writer.
type Options struct {
	Interval time.Duration // flush interval, DefaultFlush if zero
	StartSeq uint64        // first segment number, 1 if zero
	Retry    time.Duration // first wait before retrying an upload whose outcome is unknown, 25 ms if zero (doubling up to 40 times that)
	// Check, if set, is asked before a segment's waiters are told it was written
	// and before new files are accepted. A node whose lease is no longer good
	// returns an error here, and nothing more is acknowledged.
	Check  func() error
	Logger *slog.Logger // slog.Default() if nil
	// A segment is built in memory up to SpoolLimit bytes (spool.DefaultLimit if
	// zero) and in a temporary file in TempDir beyond it, and so are the files
	// appended to it. A segment is never held in the scratch directory, which is
	// memory. Without a TempDir a segment cannot be larger than SpoolLimit.
	TempDir    string
	SpoolLimit int
}

type pendingEntry struct {
	Entry
	body *spool.File // a reference of its own, closed once the segment holds it
}

// Writer batches appended LTX files into one segment per flush interval.
type Writer struct {
	store *blob.Store
	node  string
	opts  Options

	uploadMu sync.Mutex // one segment upload at a time

	mu       sync.Mutex
	pending  []pendingEntry
	waiters  []chan error
	inflight *upload // a segment whose outcome isn't known yet, retried before anything newer
	seq      uint64
	stopErr  error // set once fenced or closed

	flushes atomic.Int64
	ctx     context.Context // done when the writer is closed
	cancel  context.CancelFunc
	g       errgroup.Group // owns the flush loop; Close joins it
}

// upload is one segment on its way to blob storage. A retry sends the same
// body under the same write id, so blob.Store can tell a lost response from
// somebody else's object.
type upload struct {
	seq     uint64
	body    *spool.File // closed once the segment is written, or the writer is
	id      blob.WriteID
	waiters []chan error
}

// NewWriter starts a writer whose flush loop runs until Close.
func NewWriter(store *blob.Store, node string, opts Options) *Writer {
	if opts.Interval <= 0 {
		opts.Interval = DefaultFlush
	}
	if opts.Retry <= 0 {
		opts.Retry = 25 * time.Millisecond
	}
	if opts.StartSeq == 0 {
		opts.StartSeq = 1
	}
	if opts.SpoolLimit <= 0 {
		opts.SpoolLimit = spool.DefaultLimit
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &Writer{store: store, node: node, opts: opts, seq: opts.StartSeq, ctx: ctx, cancel: cancel}
	w.g.Go(func() error { w.loop(ctx); return nil })
	return w
}

// Flushes is the number of segments written so far.
func (w *Writer) Flushes() int64 { return w.flushes.Load() }

// NextSeq is the number the next segment will use.
func (w *Writer) NextSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq
}

// Append queues one LTX file. The returned channel receives nil once the
// segment holding it has been written, or the error that stopped it.
func (w *Writer) Append(shard, generation string, position uint64, ltxBytes []byte) <-chan error {
	f := spool.Bytes(ltxBytes)
	defer f.Close()
	return w.AppendFile(shard, generation, position, f)
}

// AppendFile is Append for a file that may be in a temporary file. The writer
// takes a reference of its own, so the caller still closes its own.
func (w *Writer) AppendFile(shard, generation string, position uint64, ltxFile *spool.File) <-chan error {
	ch := make(chan error, 1)
	hdr, _, err := ltx.PeekHeader(ltxFile.Reader())
	if err != nil {
		ch <- fmt.Errorf("segment: bad ltx file for %s: %w", shard, err)
		return ch
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopErr != nil {
		ch <- w.stopErr
		return ch
	}
	if w.opts.Check != nil {
		if err := w.opts.Check(); err != nil {
			ch <- err
			return ch
		}
	}
	w.pending = append(w.pending, pendingEntry{
		Entry: Entry{Shard: shard, Generation: generation, Position: position, Count: uint64(hdr.MaxTXID-hdr.MinTXID) + 1, Length: ltxFile.Size()},
		body:  ltxFile.Retain(),
	})
	w.waiters = append(w.waiters, ch)
	return ch
}

func (w *Writer) loop(ctx context.Context) {
	t := time.NewTicker(w.opts.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = w.Flush(ctx)
		}
	}
}

// Flush writes the pending entries as one segment now, if there are any, and
// returns once it is written. Concurrent flushes run one after the other.
//
// A segment whose upload fails is retried with the same body and write id
// until the outcome is known, and nothing newer is written before it. If ctx
// ends first, Flush returns its error and the segment stays in flight for the
// next Flush (or the flush loop) to finish. Only a fence (somebody else's
// object under the segment's number) or Close fails the segment's waiters.
func (w *Writer) Flush(ctx context.Context) error {
	w.uploadMu.Lock()
	defer w.uploadMu.Unlock()

	// ctx ends when the caller's does or when the writer is closed.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stop := context.AfterFunc(w.ctx, func() { cancel(ErrClosed) })
	defer stop()

	// A segment left in flight goes first, then whatever was queued behind it.
	for {
		w.mu.Lock()
		if w.stopErr != nil {
			err := w.stopErr
			w.mu.Unlock()
			return err
		}
		resumed := w.inflight != nil
		if !resumed {
			if len(w.pending) == 0 {
				w.mu.Unlock()
				return nil
			}
			// The segment is built without the lock: it can be a lot of copying, and
			// other shards keep appending meanwhile. Only this flush builds segments
			// (it holds uploadMu), so nothing else touches the inflight upload.
			pending, waiters := w.pending, w.waiters
			w.pending, w.waiters = nil, nil
			w.mu.Unlock()
			body, err := w.encode(pending)
			w.mu.Lock()
			if err != nil {
				// Nothing is lost: the entries go back, ahead of what came in meanwhile.
				w.pending = append(pending, w.pending...)
				w.waiters = append(waiters, w.waiters...)
				w.mu.Unlock()
				return err
			}
			for _, p := range pending {
				p.body.Close() // the segment has its own copy
			}
			if w.stopErr != nil {
				// Closed or fenced while it was being built.
				err := w.stopErr
				w.mu.Unlock()
				body.Close()
				for _, ch := range waiters {
					ch <- err
				}
				return err
			}
			w.inflight = &upload{seq: w.seq, body: body, id: w.store.NewWriteID(), waiters: waiters}
		}
		up := w.inflight
		w.mu.Unlock()

		err := w.send(ctx, up)

		w.mu.Lock()
		switch {
		case err == nil:
			w.inflight = nil
			w.seq++
			w.flushes.Add(1)
			up.body.Close()
		case errors.Is(err, blob.ErrExists):
			// Someone else's object holds our number (blob.Store has already ruled out
			// our own earlier attempts), so a newer writer owns this journal. Fail
			// whatever queued up behind it too.
			err = ErrFenced
			w.stopErr = ErrFenced
			waiters := append(up.waiters, w.waiters...)
			up.body.Close()
			w.closePending()
			w.inflight, w.pending, w.waiters = nil, nil, nil
			w.mu.Unlock()
			for _, ch := range waiters {
				ch <- err
			}
			return err
		default:
			// The writer was closed (Close fails the waiters) or the caller gave
			// up: the segment stays in flight.
			w.mu.Unlock()
			if w.ctx.Err() != nil {
				return ErrClosed
			}
			return err
		}
		w.mu.Unlock()
		// The segment is written, but a node whose lease has run out doesn't know
		// it still owns what the segment holds, so it acknowledges nothing.
		var ack error
		if w.opts.Check != nil {
			ack = w.opts.Check()
		}
		for _, ch := range up.waiters {
			ch <- ack
		}
		if !resumed {
			return nil
		}
	}
}

// send retries one segment's create until the outcome is known: it was
// written (nil), somebody else holds the number (blob.ErrExists) or ctx ended.
func (w *Writer) send(ctx context.Context, up *upload) error {
	backoff := w.opts.Retry
	for {
		_, err := w.store.CreateWithID(ctx, Key(w.node, up.seq), up.body.Reader(), up.id)
		if err == nil || errors.Is(err, blob.ErrExists) {
			return err
		}
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if !blob.Transient(err) {
			// Retrying won't fix this (a bad key, no permission), but the segment
			// can't be given up either: its commits are in nobody else's hands.
			w.opts.Logger.Error("segment upload failed with an error retrying cannot fix, will keep trying",
				"node", w.node, "segment", up.seq, "err", err)
		}
		t := time.NewTimer(backoff)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return context.Cause(ctx)
		}
		backoff = min(backoff*2, 40*w.opts.Retry)
	}
}

// encode lays out a segment: the LTX files back to back, then the footer. It
// is built in memory if it is small, in a temporary file if it is not. It does
// not close the entries' files.
func (w *Writer) encode(pending []pendingEntry) (*spool.File, error) {
	sw := spool.NewWriter(w.opts.TempDir, w.opts.SpoolLimit)
	footer := make([]Entry, len(pending))
	for i, p := range pending {
		p.Offset = sw.Written()
		if _, err := io.Copy(sw, p.body.Reader()); err != nil {
			sw.Abort()
			return nil, err
		}
		footer[i] = p.Entry
	}
	fb, err := json.Marshal(footer)
	if err != nil {
		sw.Abort()
		return nil, err
	}
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(fb)))
	for _, b := range [][]byte{fb, n[:], []byte(magic)} {
		if _, err := sw.Write(b); err != nil {
			sw.Abort()
			return nil, err
		}
	}
	return sw.Finish()
}

// closePending closes the files of entries that were never written. The caller
// holds w.mu.
func (w *Writer) closePending() {
	for _, p := range w.pending {
		p.body.Close()
	}
}

// Close stops the flush loop and fails whatever is still pending with ErrClosed.
func (w *Writer) Close() {
	w.cancel()
	w.g.Wait()
	w.uploadMu.Lock()
	defer w.uploadMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopErr == nil {
		w.stopErr = ErrClosed
	}
	if w.inflight != nil {
		for _, ch := range w.inflight.waiters {
			ch <- ErrClosed
		}
		w.inflight.body.Close()
		w.inflight = nil
	}
	for _, ch := range w.waiters {
		ch <- ErrClosed
	}
	w.closePending()
	w.pending, w.waiters = nil, nil
}

// ReadFooter returns the entries of a segment. A fence returns ErrFence.
func ReadFooter(ctx context.Context, store *blob.Store, node string, seq uint64) ([]Entry, error) {
	key := Key(node, seq)
	// Head first: a range read on a zero-byte object misbehaves on S3.
	info, err := store.Head(ctx, key)
	if err != nil {
		return nil, err
	}
	if info.Size == 0 {
		return nil, ErrFence
	}
	if info.Size < trailerSize {
		return nil, fmt.Errorf("%w: %s is %d bytes", ErrCorrupt, key, info.Size)
	}
	tr, err := readRange(ctx, store, key, info.Size-trailerSize, trailerSize)
	if err != nil {
		return nil, err
	}
	if string(tr[8:]) != magic {
		return nil, fmt.Errorf("%w: %s has bad magic", ErrCorrupt, key)
	}
	n := binary.BigEndian.Uint64(tr[:8])
	if n > maxFooterSize || int64(n) > info.Size-trailerSize {
		return nil, fmt.Errorf("%w: %s footer length %d", ErrCorrupt, key, n)
	}
	fb, err := readRange(ctx, store, key, info.Size-trailerSize-int64(n), int64(n))
	if err != nil {
		return nil, err
	}
	var entries []Entry
	if err := json.Unmarshal(fb, &entries); err != nil {
		return nil, fmt.Errorf("%w: %s footer: %v", ErrCorrupt, key, err)
	}
	return entries, nil
}

// ReadEntry returns the LTX file bytes of one entry.
func ReadEntry(ctx context.Context, store *blob.Store, node string, seq uint64, e Entry) ([]byte, error) {
	return readRange(ctx, store, Key(node, seq), e.Offset, e.Length)
}

// ReadEntryFile is ReadEntry for an entry of any size: it is read into memory
// up to limit bytes, and into a temporary file in dir beyond that. The caller
// closes the file.
func ReadEntryFile(ctx context.Context, store *blob.Store, node string, seq uint64, e Entry, dir string, limit int) (*spool.File, error) {
	rc, err := store.GetRange(ctx, Key(node, seq), e.Offset, e.Length)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	f, err := spool.Collect(dir, limit, rc)
	if err != nil {
		return nil, err
	}
	if f.Size() != e.Length {
		n := f.Size()
		f.Close()
		return nil, fmt.Errorf("%w: %s short range read (%d of %d bytes)", ErrCorrupt, Key(node, seq), n, e.Length)
	}
	return f, nil
}

func readRange(ctx context.Context, store *blob.Store, key string, off, n int64) ([]byte, error) {
	rc, err := store.GetRange(ctx, key, off, n)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != n {
		return nil, fmt.Errorf("%w: %s short range read (%d of %d bytes)", ErrCorrupt, key, len(b), n)
	}
	return b, nil
}

// Scan calls fn for every entry of a node's segments in order, starting at
// segment from, until it reaches a number that doesn't exist. Fences are skipped.
func Scan(ctx context.Context, store *blob.Store, node string, from uint64, fn func(seq uint64, e Entry) error) error {
	for seq := from; ; seq++ {
		entries, err := ReadFooter(ctx, store, node, seq)
		if errors.Is(err, blob.ErrNotFound) {
			return nil
		} else if errors.Is(err, ErrFence) {
			continue
		} else if err != nil {
			return err
		}
		for _, e := range entries {
			if err := fn(seq, e); err != nil {
				return err
			}
		}
	}
}

// ErrMissing is returned by ScanTo for a segment that should exist and doesn't.
var ErrMissing = errors.New("segment: missing")

// ScanTo calls fn for every entry of segments from to to (inclusive), in order,
// skipping fences. Unlike Scan it does not stop at a number that doesn't
// exist: a node writes its segments one after the other, so a number below the
// fence that is missing has expired or been lost, and the segments after it
// must not be silently ignored. It returns ErrMissing (wrapped, with the
// number) for the first one.
func ScanTo(ctx context.Context, store *blob.Store, node string, from, to uint64, fn func(seq uint64, e Entry) error) error {
	for seq := from; seq <= to; seq++ {
		entries, err := ReadFooter(ctx, store, node, seq)
		if errors.Is(err, blob.ErrNotFound) {
			return fmt.Errorf("%w: %s", ErrMissing, Key(node, seq))
		} else if errors.Is(err, ErrFence) {
			continue
		} else if err != nil {
			return err
		}
		for _, e := range entries {
			if err := fn(seq, e); err != nil {
				return err
			}
		}
	}
	return nil
}

// Bounds lists a node's journal and returns the lowest and highest segment
// numbers present (fences included), or 0, 0 for an empty journal. The lowest is
// where a reader starts, since old segments expire (until the lease records
// the range still needed, see docs/internals.md, Node leases).
func Bounds(ctx context.Context, store *blob.Store, node string) (lowest, highest uint64, err error) {
	prefix := Prefix(node)
	for info, err := range store.List(ctx, prefix) {
		if err != nil {
			return 0, 0, err
		}
		var n uint64
		if _, err := fmt.Sscanf(info.Key[len(prefix):], "%d", &n); err != nil || n == 0 {
			continue
		}
		if lowest == 0 || n < lowest {
			lowest = n
		}
		highest = max(highest, n)
	}
	return lowest, highest, nil
}

// NextSeq returns the first free segment number of a node's journal: one more
// than the highest number present (fences included), or 1 for an empty journal.
// A restarted node starts its writer here.
func NextSeq(ctx context.Context, store *blob.Store, node string) (uint64, error) {
	_, highest, err := Bounds(ctx, store, node)
	return highest + 1, err
}

// Fence is what an adopter does to a dead node's journal: starting from the
// latest segment number recorded in the node's lease, it probes forward until
// it finds a free number and creates an empty segment there. If a zombie takes
// a number first, its segment counts and the adopter tries the next one. It
// returns the number of its own fence. Lost responses to its create are
// recognised by blob.Store, so a fence the adopter wrote is never mistaken for
// somebody else's.
func Fence(ctx context.Context, store *blob.Store, node string, from uint64) (uint64, error) {
	for seq := from; ; seq++ {
		_, err := store.Create(ctx, Key(node, seq), bytes.NewReader(nil))
		if err == nil {
			return seq, nil
		}
		if !errors.Is(err, blob.ErrExists) {
			return 0, err
		}
	}
}
