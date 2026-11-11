package journal

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
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/tempfile"
)

const (
	magic         = "SHLSEG01"
	trailerSize   = 16
	maxFooterSize = 1 << 30
	// footerRead is how much of a segment's end ReadFooter reads at once: the
	// trailer and, for all but the largest segments, the footer with it.
	footerRead = 64 << 10
	// scanWindow is how many footers Scan reads at once.
	scanWindow = 64
)

var (
	// ErrFenced means another writer took a segment number: this writer must stop for good.
	ErrFenced = errors.New("journal: writer fenced")
	// ErrClosed is returned by a writer that has stopped being served.
	ErrClosed = errors.New("journal: writer closed")
	// ErrFence is returned by ReadFooter for an empty segment.
	ErrFence = errors.New("journal: fence")
	// ErrCorrupt is returned for a segment that doesn't parse.
	ErrCorrupt = errors.New("journal: corrupt")
)

// Entry locates one LTX file inside a segment. Position is the shard's own
// position of the entry's first commit and Count the number of commits it
// holds, so the entry covers Position to Position+Count-1. Both are assigned
// by the node, and restore compares them with the manifest's files.
type Entry struct {
	Shard      shard.Name   `json:"shard"`
	Generation shard.Gen    `json:"generation"`
	Position   pos.Position `json:"position"`
	Count      pos.Count    `json:"count"`
	Offset     int64        `json:"offset"`
	Length     int64        `json:"length"`
}

// Last is the position of the entry's last commit.
func (e Entry) Last() pos.Position { return e.Position.Add(e.Count) }

// segName names a segment in an error, as its key does but without the zeros: n1/129.
func segName(store Store, seq pos.Seg) string {
	return fmt.Sprintf("%s/%d", store.Name(), uint64(seq))
}

// WriterOptions configure a Writer.
type WriterOptions struct {
	StartSeq pos.Seg       // first segment number, 1 if zero
	Retry    time.Duration // first wait before retrying an upload whose outcome is unknown, 25 ms if zero (doubling up to 40 times that)
	Logger   *slog.Logger  // slog.Default() if nil
}

// pendingEntry is one LTX file queued for the next segment.
type pendingEntry struct {
	Entry
	body tempfile.File // a range of a log in the entry's held logs
}

// held is the descriptors a run of entries is read through: one dup of each log
// they are in, however many entries each log holds. The logs are the capture's,
// which closes its own when the tail is dropped, so the writer reads through
// descriptors of its own until the segment is written.
type held struct {
	dups map[*tempfile.Log]*tempfile.Log
}

// add returns f read through a dup of its log, made if the entries so far hold
// none of that log.
func (h *held) add(f tempfile.File) (tempfile.File, error) {
	d, ok := h.dups[f.Log()]
	if !ok {
		var err error
		if d, err = f.Log().Dup(); err != nil {
			return tempfile.File{}, err
		}
		if h.dups == nil {
			h.dups = map[*tempfile.Log]*tempfile.Log{}
		}
		h.dups[f.Log()] = d
	}
	return f.In(d), nil
}

// close closes the dups. It is called once.
func (h *held) close() {
	for _, d := range h.dups {
		d.Close()
	}
	h.dups = nil
}

// Writer batches appended LTX files into one segment per Flush. It flushes only
// when asked.
//
// A segment goes to blob storage, where the fence lives.
//
// The body is composed, not copied: it is a reader over the LTX files the
// capture queued and the footer that names them, so the segment's bytes are
// never copied.
//
// It is a service: Serve is the storage uploader, which writes the segments
// one at a time and in order, so nothing newer is ever written before an older
// segment. Nothing is written until it is served, and once Serve has returned the writer is closed for good.
type Writer struct {
	opts    WriterOptions
	storage Store
	kick    *notify.Kick   // wakes the uploader: a Flush wants what is queued written
	written notify.Counter // the entries storage has
	done    chan struct{}  // closed once Serve has returned

	mu       sync.Mutex
	pending  []pendingEntry
	hold     held          // the descriptors the pending entries are read through
	appended uint64        // entries accepted so far
	seq      pos.Seg       // the number of the next segment to write
	stopErr  error         // set once fenced or closed
	fenced   chan struct{} // closed once fenced

	flushes atomic.Int64
}

// NewWriter makes a writer over the journal in blob storage. Nothing is written
// until the writer is served and flushed.
func NewWriter(storage Store, opts WriterOptions) *Writer {
	if opts.Retry <= 0 {
		opts.Retry = 25 * time.Millisecond
	}
	if opts.StartSeq == 0 {
		opts.StartSeq = 1
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Writer{opts: opts, storage: storage, seq: opts.StartSeq, fenced: make(chan struct{}),
		kick: notify.NewKick(), done: make(chan struct{})}
}

// Fenced is closed once the writer found another writer's segment under one of
// its numbers (ErrFenced): it has stopped for good.
func (w *Writer) Fenced() <-chan struct{} { return w.fenced }

// Flushes is the number of segments written so far.
func (w *Writer) Flushes() int64 { return w.flushes.Load() }

// NextSeq is the number the next segment will use.
func (w *Writer) NextSeq() pos.Seg {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.seq
}

// Queued is how many entries have been appended so far. The flush loop
// publishes it after a capture, so a gate knows how many entries a store has
// to have written.
func (w *Writer) Queued() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appended
}

// Written is how many entries are in storage.
func (w *Writer) Written() uint64 { return w.written.Load() }

// Reached is a channel closed once storage has n entries.
func (w *Writer) Reached(n uint64) <-chan struct{} { return w.written.Reached(n) }

// AppendFile queues one LTX file, a range of a log. The writer reads it through
// a dup of the log, one for all the files of that log in the same segment, and
// closes it once the segment is written, so the caller closes its log whenever
// it likes. It fails with ErrFenced or ErrClosed if the writer has stopped, and
// with the dup's error if the log cannot be duplicated. Flush waits until the
// segment holding it is written.
func (w *Writer) AppendFile(name shard.Name, generation shard.Gen, position pos.Position, ltxFile tempfile.File) error {
	hdr, _, err := ltx.PeekHeader(ltxFile.Reader())
	if err != nil {
		return fmt.Errorf("journal: bad ltx file for %s: %w", name, err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopErr != nil {
		return w.stopErr
	}
	body, err := w.hold.add(ltxFile)
	if err != nil {
		return fmt.Errorf("journal: holding the file of %s: %w", name, err)
	}
	w.pending = append(w.pending, pendingEntry{
		Entry: Entry{Shard: name, Generation: generation, Position: position, Count: pos.Count(hdr.MaxTXID-hdr.MinTXID) + 1, Length: ltxFile.Size()},
		body:  body,
	})
	w.appended++
	return nil
}

// Flush asks for the entries queued so far to be written, as one segment if
// none of them is on its way yet, and returns once they are in storage. If
// ctx ends first, Flush returns its error and the uploader goes on: a queued
// entry is never dropped. Only a fence (somebody else's segment under a
// segment's number) or the end of Serve fails the entries, and Flush then
// returns ErrFenced or ErrClosed.
func (w *Writer) Flush(ctx context.Context) error {
	w.mu.Lock()
	err, target := w.stopErr, w.appended
	w.mu.Unlock()
	if err != nil {
		return err
	}
	if w.Written() >= target {
		return nil
	}
	w.kick.Kick()
	select {
	case <-w.Reached(target):
		return nil
	case <-w.done: // fenced or closed
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	if w.Written() >= target {
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

// Serve is the writer's service: the storage uploader, which writes the
// segments one at a time and in order. It returns once ctx ends, giving up the
// segment it is writing, or once the writer is fenced. Either way the writer is
// closed for good.
func (w *Writer) Serve(ctx context.Context) error {
	err := w.serveBlob(ctx)
	w.close()
	close(w.done)
	return err
}

// serveBlob is the storage uploader: each wake-up writes whatever is queued
// as one segment.
func (w *Writer) serveBlob(ctx context.Context) error {
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
// the outcome is known, so the store can tell a lost response from
// somebody else's segment. It reports whether the writer goes on: false once
// it is fenced or closed. Whatever the outcome, the files the segment was read
// through are closed once the upload has returned.
func (w *Writer) writeNext(ctx context.Context) bool {
	w.mu.Lock()
	pending, hold, seq := w.pending, w.hold, w.seq
	w.pending, w.hold = nil, held{}
	w.mu.Unlock()
	if len(pending) == 0 {
		return true
	}
	// The segment is built without the lock: it can be a lot of copying, and
	// other shards keep appending meanwhile.
	err := w.send(ctx, seq, w.compose(pending), w.storage.NewWriteID())
	hold.close()
	switch {
	case err == nil:
	case errors.Is(err, blob.ErrExists):
		// Someone else's segment holds our number (the store has already
		// ruled out our own earlier attempts), so a newer writer owns this
		// journal. Fail whatever queued up behind it too.
		w.stop(ErrFenced)
		return false
	default: // closed: Serve fails what is still queued as it returns
		return false
	}
	w.mu.Lock()
	w.seq++
	w.mu.Unlock()
	w.flushes.Add(1)
	w.written.Add(uint64(len(pending))) // after the files are closed, which the gates wait for
	return true
}

// compose lays out a segment: the LTX files back to back, then the footer. The
// footer is computable without reading anything: each entry's offset is the
// running total of the files' sizes. The result is a reader over the pieces,
// not a copy of them.
func (w *Writer) compose(pending []pendingEntry) *segmentBody {
	files := make([]filePiece, len(pending))
	footer := make([]Entry, len(pending))
	var off int64
	for i, p := range pending {
		files[i] = filePiece{r: p.body.Reader(), size: p.body.Size()}
		p.Offset = off
		off += p.body.Size()
		footer[i] = p.Entry
	}
	fb, _ := json.Marshal(footer) // plain fields: it cannot fail
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(fb)))
	tail := make([]byte, 0, len(n)+len(magic))
	tail = append(tail, n[:]...)
	tail = append(tail, magic...)
	return &segmentBody{files: files, footer: fb, tail: tail, size: off + int64(len(fb)+len(tail))}
}

// send retries one segment's create until the outcome is known: it was
// written (nil), somebody else holds the number (blob.ErrExists) or the writer
// was closed (ErrClosed, once ctx ends).
func (w *Writer) send(ctx context.Context, seq pos.Seg, body *segmentBody, id blob.WriteID) error {
	backoff := w.opts.Retry
	for {
		err := w.storage.CreateWithID(ctx, seq, body, id)
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
				"node", w.storage.Name(), "segment", seq, "err", err)
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

// stop ends the writer: it fails whatever is queued and closes the fence
// channel if the writer was fenced.
func (w *Writer) stop(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err == ErrFenced {
		w.stopErr = ErrFenced
		select {
		case <-w.fenced:
		default:
			close(w.fenced)
		}
	}
	w.closePending()
	w.pending = nil
}

// closePending closes the files of entries that were never written. The caller
// holds w.mu.
func (w *Writer) closePending() {
	w.hold.close()
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
func ReadFooter(ctx context.Context, store Store, seq pos.Seg) ([]Entry, error) {
	b, err := readFooter(ctx, store, seq)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, ErrFence
	}
	entries, err := splitFooter(b, segName(store, seq))
	return entries, err
}

// readFooter reads the end of a segment, big enough to hold the footer whole:
// the end usually holds it, and a footer bigger than the read asks for the
// bigger tail once.
func readFooter(ctx context.Context, store Store, seq pos.Seg) ([]byte, error) {
	b, err := readTail(ctx, store, seq, footerRead)
	if err != nil {
		return nil, err
	}
	if len(b) >= trailerSize {
		if tr := b[len(b)-trailerSize:]; string(tr[8:]) == magic {
			if n := binary.BigEndian.Uint64(tr[:8]); int64(n)+trailerSize > int64(len(b)) && n <= maxFooterSize {
				t, err := readTail(ctx, store, seq, int64(n)+trailerSize)
				if err != nil {
					return nil, err
				}
				if int64(len(t)) >= int64(n)+trailerSize {
					return t, nil
				}
				// Short of what the ending says: splitFooter calls it corrupt.
				b = t
			}
		}
	}
	return b, nil
}

// splitFooter parses a segment's ending b, the tail readFooter gave, into the
// entries it names. A fence is an empty ending and is the caller's error, not
// this one.
func splitFooter(b []byte, name string) ([]Entry, error) {
	if len(b) < trailerSize {
		return nil, fmt.Errorf("%w: %s is %d bytes", ErrCorrupt, name, len(b))
	}
	tr := b[len(b)-trailerSize:]
	if string(tr[8:]) != magic {
		return nil, fmt.Errorf("%w: %s has bad magic", ErrCorrupt, name)
	}
	n := binary.BigEndian.Uint64(tr[:8])
	if n > maxFooterSize {
		return nil, fmt.Errorf("%w: %s footer length %d", ErrCorrupt, name, n)
	}
	if int64(n)+trailerSize > int64(len(b)) {
		return nil, fmt.Errorf("%w: %s footer length %d", ErrCorrupt, name, n)
	}
	fb := b[len(b)-trailerSize-int(n) : len(b)-trailerSize]
	var entries []Entry
	if err := json.Unmarshal(fb, &entries); err != nil {
		return nil, fmt.Errorf("%w: %s footer: %v", ErrCorrupt, name, err)
	}
	return entries, nil
}

// ReadEntryInto appends the LTX file of one entry to log and returns its range.
// The caller owns the log and closes it.
func ReadEntryInto(ctx context.Context, store Store, seq pos.Seg, e Entry, log *tempfile.Log) (tempfile.File, error) {
	rc, err := store.GetRange(ctx, seq, e.Offset, e.Length)
	if err != nil {
		return tempfile.File{}, err
	}
	defer rc.Close()
	a, err := log.Append()
	if err != nil {
		return tempfile.File{}, err
	}
	if _, err := io.Copy(a, rc); err != nil {
		a.Abort()
		return tempfile.File{}, err
	}
	if n := a.Written(); n != e.Length {
		a.Abort()
		return tempfile.File{}, fmt.Errorf("%w: %s short range read (%d of %d bytes)", ErrCorrupt, segName(store, seq), n, e.Length)
	}
	return a.Finish()
}

// readTail reads the last n bytes of the segment, or all of them when the
// segment is shorter. It is how a footer is read: the caller does not know
// where the end is until it asks.
func readTail(ctx context.Context, store Store, seq pos.Seg, n int64) ([]byte, error) {
	rc, err := store.GetRange(ctx, seq, -n, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// ErrMissing is returned by Scan for a segment that should exist and doesn't.
var ErrMissing = errors.New("journal: missing")

// Scan calls fn for every entry of the journal's segments in order, starting
// at segment from, skipping fences. With to zero it stops at the first number
// that doesn't exist. Otherwise it reads through to (inclusive) and does not
// stop at a number that doesn't exist: a node writes its segments one after the
// other, so a number below the fence that is missing has expired or been lost,
// and the segments after it must not be silently ignored. It returns ErrMissing
// (wrapped, with the number) for the first one.
//
// Footers are read scanWindow at a time, in parallel, and handed to fn in
// order. With to zero, a window may read past the end, which costs a few
// requests that find nothing.
func Scan(ctx context.Context, store Store, from, to pos.Seg, fn func(seq pos.Seg, e Entry) error) error {
	for start := from; to == 0 || start <= to; start += scanWindow {
		n := pos.Seg(scanWindow)
		if to != 0 {
			n = min(n, to-start+1)
		}
		footers, errs := make([][]Entry, n), make([]error, n)
		var g errgroup.Group
		for i := range footers {
			g.Go(func() error {
				footers[i], errs[i] = ReadFooter(ctx, store, start+pos.Seg(i))
				return nil
			})
		}
		g.Wait()
		for i, entries := range footers {
			seq := start + pos.Seg(i)
			if err := errs[i]; errors.Is(err, blob.ErrNotFound) {
				if to == 0 {
					return nil
				}
				return fmt.Errorf("%w: %s", ErrMissing, segName(store, seq))
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

// Bounds lists the journal and returns the lowest and highest segment numbers
// present (fences included), or 0, 0 for an empty journal. The lowest is
// where a reader starts, since old segments expire (until the lease records
// the range still needed, see docs/internals.md, Node leases).
func Bounds(ctx context.Context, store Store) (lowest, highest pos.Seg, err error) {
	for seg, err := range store.List(ctx, 1) {
		if err != nil {
			return 0, 0, err
		}
		if lowest == 0 || seg.Seq < lowest {
			lowest = seg.Seq
		}
		highest = max(highest, seg.Seq)
	}
	return lowest, highest, nil
}

// Fence is what an adopter does to a dead node's journal: starting from the
// latest segment number recorded in the node's lease, it probes forward until
// it finds a free number and creates an empty segment there. If a zombie takes
// a number first, its segment counts and the adopter tries the next one. It
// returns the number of its own fence. Lost responses to its create are
// recognised by the store, so a fence the adopter wrote is never mistaken
// for somebody else's.
func Fence(ctx context.Context, store Store, from pos.Seg) (pos.Seg, error) {
	for seq := from; ; seq++ {
		err := store.Create(ctx, seq, bytes.NewReader(nil))
		if err == nil {
			return seq, nil
		}
		if !errors.Is(err, blob.ErrExists) {
			return 0, err
		}
	}
}
