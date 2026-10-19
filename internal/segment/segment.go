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
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/spool"
)

const (
	magic         = "SHLSEG01"
	trailerSize   = 16
	seqKeyDigits  = 20
	maxFooterSize = 1 << 30
	// footerRead is how much of a segment's end ReadFooter reads at once: the
	// trailer and, for all but the largest segments, the footer with it.
	footerRead = 64 << 10
	// scanWindow is how many footers Scan reads at once.
	scanWindow = 64
)

var (
	// ErrFenced means another writer took a segment number: this writer must stop for good.
	ErrFenced = errors.New("segment: writer fenced")
	// ErrClosed is returned by a writer that has stopped being served.
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

// Root is the key prefix of all journals (data).
const Root = "data/journal/"

// Prefix is the key prefix of a node's journal.
func Prefix(node string) string { return Root + node + "/" }

// Key returns the object key of a node's segment.
func Key(node string, seq uint64) string {
	return fmt.Sprintf("%s%0*d", Prefix(node), seqKeyDigits, seq)
}

// Options configure a Writer.
type Options struct {
	StartSeq uint64        // first segment number, 1 if zero
	Retry    time.Duration // first wait before retrying an upload whose outcome is unknown, 25 ms if zero (doubling up to 40 times that)
	Logger   *slog.Logger  // slog.Default() if nil
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

// Writer batches appended LTX files into one segment per Flush. It flushes only
// when asked. It is a service: Serve is its uploader, which writes the segments
// one at a time and in order, so nothing newer is ever written before an older
// segment. Nothing is written until it is served, and once Serve has returned
// the writer is closed for good.
type Writer struct {
	store *blob.Store
	node  string
	opts  Options

	kick    *notify.Kick   // wakes the uploader: a Flush wants what is queued written
	written notify.Counter // entries written so far, in the order they were appended
	done    chan struct{}  // closed once Serve has returned

	mu       sync.Mutex
	pending  []pendingEntry
	appended uint64 // entries accepted so far
	seq      uint64
	stopErr  error         // set once fenced or closed
	fenced   chan struct{} // closed once fenced

	flushes atomic.Int64
}

// NewWriter makes a writer. Nothing is written until it is served and flushed.
func NewWriter(store *blob.Store, node string, opts Options) *Writer {
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
	return &Writer{store: store, node: node, opts: opts, seq: opts.StartSeq, fenced: make(chan struct{}),
		kick: notify.NewKick(), done: make(chan struct{})}
}

// Fenced is closed once the writer found another writer's segment under one of
// its numbers (ErrFenced): it has stopped for good.
func (w *Writer) Fenced() <-chan struct{} { return w.fenced }

// Flushes is the number of segments written so far.
func (w *Writer) Flushes() int64 { return w.flushes.Load() }

// NextSeq is the number the next segment will use.
func (w *Writer) NextSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq
}

// AppendFile queues one LTX file, which may be in a temporary file. The writer
// takes a reference of its own, so the caller still closes its own. It fails
// with ErrFenced or ErrClosed if the writer has stopped. Flush waits until the
// segment holding it is written.
func (w *Writer) AppendFile(shard, generation string, position uint64, ltxFile *spool.File) error {
	hdr, _, err := ltx.PeekHeader(ltxFile.Reader())
	if err != nil {
		return fmt.Errorf("segment: bad ltx file for %s: %w", shard, err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopErr != nil {
		return w.stopErr
	}
	w.pending = append(w.pending, pendingEntry{
		Entry: Entry{Shard: shard, Generation: generation, Position: position, Count: uint64(hdr.MaxTXID-hdr.MinTXID) + 1, Length: ltxFile.Size()},
		body:  ltxFile.Retain(),
	})
	w.appended++
	return nil
}

// Flush asks for the entries queued so far to be written, as one segment if
// none of them is on its way yet, and returns once they are. If ctx ends first,
// Flush returns its error and the uploader goes on: a queued entry is never
// dropped. Only a fence (somebody else's object under a segment's number) or
// the end of Serve fails the entries, and Flush then returns ErrFenced or
// ErrClosed.
func (w *Writer) Flush(ctx context.Context) error {
	w.mu.Lock()
	err, target := w.stopErr, w.appended
	w.mu.Unlock()
	if err != nil {
		return err
	}
	if w.written.Load() >= target {
		return nil
	}
	w.kick.Kick()
	select {
	case <-w.written.Reached(target):
		return nil
	case <-w.done: // fenced or closed
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	if w.written.Load() >= target {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopErr != nil {
		return w.stopErr
	}
	return ErrClosed // set before done is closed
}

func (w *Writer) String() string { return "writer" }

// Serve is the uploader: each wake-up writes whatever is queued as one
// segment. It returns once ctx ends, giving up the segment it is writing, or
// once the writer is fenced. Either way it fails whatever is still queued, and
// the writer is closed for good.
func (w *Writer) Serve(ctx context.Context) error {
	defer close(w.done)
	defer w.close()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-w.kick.C():
		}
		if !w.writeNext(ctx) {
			return nil
		}
	}
}

// writeNext writes what is queued as the next segment, if anything is. A
// segment whose upload fails is retried with the same body and write id until
// the outcome is known, so blob.Store can tell a lost response from somebody
// else's object. It reports whether the writer goes on: false once it is
// fenced or closed.
func (w *Writer) writeNext(ctx context.Context) bool {
	w.mu.Lock()
	pending, seq := w.pending, w.seq
	w.pending = nil
	w.mu.Unlock()
	if len(pending) == 0 {
		return true
	}
	// The segment is built without the lock: it can be a lot of copying, and
	// other shards keep appending meanwhile.
	body, err := w.build(ctx, seq, pending)
	for _, p := range pending {
		p.body.Close() // the segment has its own copy, or the writer is closed
	}
	if err == nil {
		err = w.send(ctx, seq, body, w.store.NewWriteID())
		body.Close()
	}
	switch {
	case err == nil:
	case errors.Is(err, blob.ErrExists):
		// Someone else's object holds our number (blob.Store has already ruled out
		// our own earlier attempts), so a newer writer owns this journal. Fail
		// whatever queued up behind it too.
		w.mu.Lock()
		w.stopErr = ErrFenced
		close(w.fenced)
		w.closePending()
		w.pending = nil
		w.mu.Unlock()
		return false
	default: // closed: Serve fails what is still queued as it returns
		return false
	}
	w.mu.Lock()
	w.seq++
	w.mu.Unlock()
	w.flushes.Add(1)
	w.written.Add(uint64(len(pending)))
	return true
}

// build lays out a segment, trying again (with the same backoff as send) until
// it succeeds or ctx ends: the entries can't be given up.
func (w *Writer) build(ctx context.Context, seq uint64, pending []pendingEntry) (*spool.File, error) {
	backoff := w.opts.Retry
	for {
		body, err := w.encode(pending)
		if err == nil {
			return body, nil
		}
		w.opts.Logger.Error("building a segment failed, will keep trying", "node", w.node, "segment", seq, "err", err)
		if err := sleep(ctx, backoff); err != nil {
			return nil, err
		}
		backoff = min(backoff*2, 40*w.opts.Retry)
	}
}

// send retries one segment's create until the outcome is known: it was
// written (nil), somebody else holds the number (blob.ErrExists) or the writer
// was closed (ErrClosed, once ctx ends).
func (w *Writer) send(ctx context.Context, seq uint64, body *spool.File, id blob.WriteID) error {
	backoff := w.opts.Retry
	for {
		_, err := w.store.CreateWithID(ctx, Key(w.node, seq), body.Reader(), id)
		if err == nil || errors.Is(err, blob.ErrExists) {
			return err
		}
		if ctx.Err() != nil {
			return ErrClosed
		}
		if !blob.Transient(err) {
			// Retrying won't fix this (a bad key, no permission), but the segment
			// can't be given up either: its commits are in nobody else's hands.
			w.opts.Logger.Error("segment upload failed with an error retrying cannot fix, will keep trying",
				"node", w.node, "segment", seq, "err", err)
		}
		if err := sleep(ctx, backoff); err != nil {
			return err
		}
		backoff = min(backoff*2, 40*w.opts.Retry)
	}
}

// sleep waits d, or returns ErrClosed if ctx ends first.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ErrClosed
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

// close fails whatever is still queued with ErrClosed, and accepts nothing more.
// Serve runs it as it returns.
func (w *Writer) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopErr == nil {
		w.stopErr = ErrClosed
	}
	w.closePending()
	w.pending = nil
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
	// The end of the segment, which usually holds the whole footer too.
	end := min(info.Size, footerRead)
	b, err := readRange(ctx, store, key, info.Size-end, end)
	if err != nil {
		return nil, err
	}
	tr := b[len(b)-trailerSize:]
	if string(tr[8:]) != magic {
		return nil, fmt.Errorf("%w: %s has bad magic", ErrCorrupt, key)
	}
	n := binary.BigEndian.Uint64(tr[:8])
	if n > maxFooterSize || int64(n) > info.Size-trailerSize {
		return nil, fmt.Errorf("%w: %s footer length %d", ErrCorrupt, key, n)
	}
	var fb []byte
	if int64(n)+trailerSize <= end {
		fb = b[len(b)-trailerSize-int(n) : len(b)-trailerSize]
	} else if fb, err = readRange(ctx, store, key, info.Size-trailerSize-int64(n), int64(n)); err != nil {
		return nil, err
	}
	var entries []Entry
	if err := json.Unmarshal(fb, &entries); err != nil {
		return nil, fmt.Errorf("%w: %s footer: %v", ErrCorrupt, key, err)
	}
	return entries, nil
}

// ReadEntryFile reads the LTX file of one entry, into memory up to limit bytes
// and into a temporary file in dir beyond that. The caller closes the file.
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

// ErrMissing is returned by Scan for a segment that should exist and doesn't.
var ErrMissing = errors.New("segment: missing")

// Scan calls fn for every entry of a node's segments in order, starting at
// segment from, skipping fences. With to zero it stops at the first number that
// doesn't exist. Otherwise it reads through to (inclusive) and does not stop at
// a number that doesn't exist: a node writes its segments one after the other,
// so a number below the fence that is missing has expired or been lost, and the
// segments after it must not be silently ignored. It returns ErrMissing
// (wrapped, with the number) for the first one.
//
// Footers are read scanWindow at a time, in parallel, and handed to fn in
// order. With to zero, a window may read past the end, which costs a few
// requests that find nothing.
func Scan(ctx context.Context, store *blob.Store, node string, from, to uint64, fn func(seq uint64, e Entry) error) error {
	for start := from; to == 0 || start <= to; start += scanWindow {
		n := uint64(scanWindow)
		if to != 0 {
			n = min(n, to-start+1)
		}
		footers, errs := make([][]Entry, n), make([]error, n)
		var g errgroup.Group
		for i := range footers {
			g.Go(func() error {
				footers[i], errs[i] = ReadFooter(ctx, store, node, start+uint64(i))
				return nil
			})
		}
		g.Wait()
		for i, entries := range footers {
			seq := start + uint64(i)
			if err := errs[i]; errors.Is(err, blob.ErrNotFound) {
				if to == 0 {
					return nil
				}
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
