// Package segment writes and reads a node's journal through a journal.Store:
// numbered segments, that each hold every shard's LTX files from one flush
// interval. The format is the LTX files, the footer, the trailer, laid out
// exactly as capture queued them; the store is where they live (the record, or
// one of the copies in front of it later).
//
// Layout of a segment:
//
//	LTX file bytes, exactly as capture queued them, back to back
//	footer: JSON array of Entry
//	8 bytes: footer length, big-endian
//	8 bytes: magic "SHLSEG01"
//
// An empty segment is a fence (see docs/internals.md, Fencing), and lives in
// the record.
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
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/notify"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/spool"
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
	ErrFenced = errors.New("segment: writer fenced")
	// ErrClosed is returned by a writer that has stopped being served.
	ErrClosed = errors.New("segment: writer closed")
	// ErrFence is returned by ReadFooter for an empty segment.
	ErrFence = errors.New("segment: fence")
	// ErrCorrupt is returned for a segment that doesn't parse.
	ErrCorrupt = errors.New("segment: corrupt")
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
func segName(store *journal.Store, seq pos.Seg) string {
	return fmt.Sprintf("%s/%d", store.Name(), uint64(seq))
}

// Store names one of the places a segment is written to. The zero value is no
// store: a writer that isn't asked about a store it doesn't have waits for
// nothing, which is the safe answer (docs/internals.md, Failing loudly, the
// zero-value policy).
type Store int

const (
	// StoreNone is no store: Written counts nothing and Reached is closed
	// already, so nothing waits on it.
	StoreNone Store = iota
	// StoreRecord is the record: blob storage, where the fence is created and
	// where the journal's races are decided.
	StoreRecord
	// StoreLocal is the node's own disk, which holds a copy of the record's
	// segments. It decides nothing.
	StoreLocal

	// storeCount is how many places a store value can name, the array a writer
	// keeps one store behind each.
	storeCount = int(StoreLocal) + 1
)

// String names a store, for the logs.
func (s Store) String() string {
	switch s {
	case StoreNone:
		return "none"
	case StoreRecord:
		return "record"
	case StoreLocal:
		return "local"
	}
	return "unknown"
}

// Valid reports whether s is one of the stores. The zero is a store value, no
// store.
func (s Store) Valid() bool {
	switch s {
	case StoreNone, StoreRecord, StoreLocal:
		return true
	}
	return false
}

// Options configure a Writer.
type Options struct {
	StartSeq pos.Seg       // first segment number, 1 if zero
	Retry    time.Duration // first wait before retrying an upload whose outcome is unknown, 25 ms if zero (doubling up to 40 times that)
	Logger   *slog.Logger  // slog.Default() if nil
	// Local is the node's own disk, which holds a copy of every segment. It
	// may be nil, and then the segments go to the record alone.
	Local *journal.Store
}

// pendingEntry is one LTX file queued for the next segment.
type pendingEntry struct {
	Entry
	body *spool.File // a reference of its own, closed once the segment holds it
}

// store is one place a segment is written to: its journal, the queue of the
// batches waiting for it and the counter of the entries it has written.
type store struct {
	name    string
	journal *journal.Store
	queue   *batchQueue
	written notify.Counter // the entries written to it, in the order they were appended
}

// batch is one segment on its way to the stores: its number, its body and the
// files it is made of. Each store's side of it is marked as it is written, and
// the batch is dropped when both are.
type batch struct {
	seq   pos.Seg
	body  *body
	files []pendingEntry
	count uint64 // the number of entries, which is what the counters count

	recordDone   atomic.Bool // the record has the segment
	localDone    atomic.Bool // the local store's side is finished (written or given up)
	localWritten atomic.Bool // the local store has the segment
	dropped      atomic.Bool // the batch is dropped: its files are closed
}

// closeFiles closes the files the batch is made of, once.
func (b *batch) closeFiles() {
	if b.dropped.CompareAndSwap(false, true) {
		for _, p := range b.files {
			p.body.Close()
		}
	}
}

// Writer batches appended LTX files into one segment per Flush. It flushes only
// when asked.
//
// A segment goes to two places: the record (blob storage), where the fence
// lives, and the node's own disk, which holds a copy of it. Each has its own
// uploader goroutine and its own counter, so a slow record never holds the
// local tier's acknowledgement: the local copy is written beside the record's,
// out of the same body, and the gate that waits for the local store is
// released as soon as the local write is durable.
//
// The body is composed, not spooled: it is a reader over the LTX files the
// capture queued and the footer that names them, so the segment's bytes are
// never copied and no spool holds them.
//
// It is a service: Serve is the record's uploader, which writes the segments
// one at a time and in order, so nothing newer is ever written before an older
// segment, and the local store's uploader, which runs beside it. Nothing is
// written until it is served, and once Serve has returned the writer is closed
// for good.
type Writer struct {
	opts   Options
	stores [storeCount]*store

	kick    *notify.Kick // wakes the uploader: a Flush wants what is queued written
	written [storeCount]notify.Counter
	done    chan struct{} // closed once Serve has returned

	mu       sync.Mutex
	pending  []pendingEntry
	batches  []*batch      // the batches in flight, whose files are open
	appended uint64        // entries accepted so far
	seq      pos.Seg       // the number of the next segment to write
	stopErr  error         // set once fenced or closed
	fenced   chan struct{} // closed once fenced

	flushes atomic.Int64
}

// NewWriter makes a writer over the record, the journal in blob storage. The
// segments go to the node's own disk too when Options.Local names it. Nothing
// is written until the writer is served and flushed.
func NewWriter(record *journal.Store, opts Options) *Writer {
	if opts.Retry <= 0 {
		opts.Retry = 25 * time.Millisecond
	}
	if opts.StartSeq == 0 {
		opts.StartSeq = 1
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	w := &Writer{opts: opts, seq: opts.StartSeq, fenced: make(chan struct{}),
		kick: notify.NewKick(), done: make(chan struct{})}
	w.stores[StoreRecord] = &store{name: "record", journal: record, queue: newBatchQueue()}
	if opts.Local != nil {
		w.stores[StoreLocal] = &store{name: "local", journal: opts.Local, queue: newBatchQueue()}
	}
	return w
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

// Written is how many entries are durable in the named store.
func (w *Writer) Written(s Store) uint64 {
	st := w.stores[s]
	if st == nil {
		return 0
	}
	return st.written.Load()
}

// Reached is a channel closed once the named store has written n entries.
func (w *Writer) Reached(s Store, n uint64) <-chan struct{} {
	st := w.stores[s]
	if st == nil {
		return closedChan
	}
	return st.written.Reached(n)
}

// closedChan is a channel that is closed already, for a store the writer
// doesn't have.
var closedChan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// AppendFile queues one LTX file, which may be in a temporary file. The writer
// takes a reference of its own, so the caller still closes its own. It fails
// with ErrFenced or ErrClosed if the writer has stopped. Flush waits until the
// segment holding it is written.
func (w *Writer) AppendFile(name shard.Name, generation shard.Gen, position pos.Position, ltxFile *spool.File) error {
	hdr, _, err := ltx.PeekHeader(ltxFile.Reader())
	if err != nil {
		return fmt.Errorf("segment: bad ltx file for %s: %w", name, err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopErr != nil {
		return w.stopErr
	}
	w.pending = append(w.pending, pendingEntry{
		Entry: Entry{Shard: name, Generation: generation, Position: position, Count: pos.Count(hdr.MaxTXID-hdr.MinTXID) + 1, Length: ltxFile.Size()},
		body:  ltxFile.Retain(),
	})
	w.appended++
	return nil
}

// Flush asks for the entries queued so far to be written, as one segment if
// none of them is on its way yet, and returns once they are in the record. If
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
	if w.Written(StoreRecord) >= target {
		return nil
	}
	w.kick.Kick()
	select {
	case <-w.Reached(StoreRecord, target):
		return nil
	case <-w.done: // fenced or closed
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	if w.Written(StoreRecord) >= target {
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

// Serve is the writer's service: the record's uploader, which writes the
// segments one at a time and in order, and the local store's uploader, which
// runs beside it. It returns once ctx ends, giving up the segment it is
// writing, or once the writer is fenced. Either way it fails whatever is still
// queued, and the writer is closed for good.
func (w *Writer) Serve(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g, gctx := errgroup.WithContext(ctx)
	if local := w.stores[StoreLocal]; local != nil {
		g.Go(func() error { return w.serveLocal(gctx, local) })
	}
	err := w.serveRecord(gctx)
	cancel() // the local uploader ends with it
	g.Wait()
	w.close()
	close(w.done)
	return err
}

// serveRecord is the record's uploader: each wake-up writes whatever is queued
// as one segment.
func (w *Writer) serveRecord(ctx context.Context) error {
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

// serveLocal is the local store's uploader: it writes the batches the record's
// uploader made, one at a time and in order. It is the writer's second
// goroutine, and the local tier's acknowledgement waits for it and never for
// the record's.
func (w *Writer) serveLocal(ctx context.Context, local *store) error {
	for {
		b := local.queue.pop(ctx)
		if b == nil {
			return nil
		}
		w.writeLocal(ctx, local, b)
	}
}

// writeNext writes what is queued as the next segment, if anything is. A
// segment whose upload fails is retried with the same body and write id until
// the outcome is known, so journal.Store can tell a lost response from
// somebody else's segment. It reports whether the writer goes on: false once
// it is fenced or closed.
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
	if err != nil {
		// The build gives up only when the context ends, so the entries are
		// lost with the writer.
		w.closeFiles(pending)
		w.stop(ErrClosed)
		return false
	}
	b := &batch{seq: seq, body: body, files: pending, count: uint64(len(pending))}
	w.mu.Lock()
	w.batches = append(w.batches, b)
	w.mu.Unlock()
	// The local copy is written beside the record's, out of the same body. The
	// queue is unbounded, so a slow disk never holds the record's write.
	if local := w.stores[StoreLocal]; local != nil {
		local.queue.push(b)
	} else {
		b.localDone.Store(true) // there is no local side to wait for
	}
	err = w.send(ctx, seq, body, w.stores[StoreRecord].journal.NewWriteID())
	switch {
	case err == nil:
	case errors.Is(err, blob.ErrExists):
		// Someone else's segment holds our number (journal.Store has already
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
	w.finish(b, StoreRecord, ctx)
	return true
}

// build lays out a segment, trying again (with the same backoff as send) until
// it succeeds or ctx ends: the entries can't be given up.
func (w *Writer) build(ctx context.Context, seq pos.Seg, pending []pendingEntry) (*body, error) {
	backoff := w.opts.Retry
	for {
		body, err := w.compose(pending)
		if err == nil {
			return body, nil
		}
		w.opts.Logger.Error("building a segment failed, will keep trying", "node", w.stores[StoreRecord].journal.Name(), "segment", seq, "err", err)
		if err := sleep(ctx, backoff); err != nil {
			return nil, err
		}
		backoff = min(backoff*2, 40*w.opts.Retry)
	}
}

// compose lays out a segment: the LTX files back to back, then the footer. The
// footer is computable without reading anything: each entry's offset is the
// running total of the files' sizes. The result is a reader over the pieces,
// not a copy of them.
func (w *Writer) compose(pending []pendingEntry) (*body, error) {
	files := make([]filePiece, len(pending))
	footer := make([]Entry, len(pending))
	var off int64
	for i, p := range pending {
		files[i] = filePiece{r: p.body.Reader(), size: p.body.Size()}
		p.Offset = off
		off += p.body.Size()
		footer[i] = p.Entry
	}
	fb, err := json.Marshal(footer)
	if err != nil {
		return nil, err
	}
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(fb)))
	tail := make([]byte, 0, len(n)+len(magic))
	tail = append(tail, n[:]...)
	tail = append(tail, magic...)
	return &body{files: files, footer: fb, tail: tail, size: off + int64(len(fb)+len(tail))}, nil
}

// writeLocal writes one batch to the node's own disk. A failure is the local
// store's problem alone: the tier is empty and the gate falls back to the
// record, so the writer gives up on the local store rather than hold the node
// up.
func (w *Writer) writeLocal(ctx context.Context, local *store, b *batch) {
	id := local.journal.NewWriteID()
	backoff := w.opts.Retry
	for attempt := 0; attempt < localAttempts; attempt++ {
		err := local.journal.CreateWithID(ctx, b.seq, b.body, id)
		if err == nil {
			b.localWritten.Store(true)
			w.finish(b, StoreLocal, ctx)
			return
		}
		if ctx.Err() != nil {
			break
		}
		if !blob.Transient(err) {
			w.opts.Logger.Error("the local store refused a segment, the local tier falls back to the record",
				"segment", b.seq, "err", err)
			break
		}
		if err := sleep(ctx, backoff); err != nil {
			break
		}
		backoff = min(backoff*2, 40*w.opts.Retry)
	}
	if !b.localWritten.Load() && ctx.Err() == nil {
		w.opts.Logger.Error("the local store could not take a segment, the local tier falls back to the record",
			"segment", b.seq)
	}
	w.finish(b, StoreLocal, ctx)
}

// localAttempts is how many times the local store is asked to take a segment
// before the tier is given up on. The record is never given up on: its copy is
// the only one that matters.
const localAttempts = 5

// finish marks one store's side of a batch as done and counts what it has
// written. When both sides are done, the local copy goes (the record has the
// segment) and the files the batch is made of are closed.
func (w *Writer) finish(b *batch, s Store, ctx context.Context) {
	switch s {
	case StoreRecord:
		b.recordDone.Store(true)
	case StoreLocal:
		b.localDone.Store(true)
	case StoreNone:
		panic("segment: no store to finish a batch in")
	}
	both := b.recordDone.Load() && b.localDone.Load()
	if both && b.dropped.CompareAndSwap(false, true) {
		// The files are read by both stores, so they stay open until both are
		// done with the batch.
		for _, p := range b.files {
			p.body.Close()
		}
	}
	// The counter: the gates wait for it, so it comes after the files are
	// closed and before the local copy is dropped.
	switch s {
	case StoreRecord:
		w.stores[StoreRecord].written.Add(b.count)
	case StoreLocal:
		w.stores[StoreLocal].written.Add(b.count)
	case StoreNone:
		panic("segment: no store to count a batch in")
	}
	if !both {
		return
	}
	if local := w.stores[StoreLocal]; local != nil && b.localWritten.Load() {
		// The local copy goes once the record has the segment: while the
		// record's write is in flight, the local copy is the only one.
		if err := local.journal.Delete(ctx, b.seq); err != nil && ctx.Err() == nil {
			w.opts.Logger.Warn("could not drop the local copy of a segment", "segment", b.seq, "err", err)
		}
	}
	w.forget(b)
}

// forget drops a batch from the writer's list of the ones in flight.
func (w *Writer) forget(b *batch) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, x := range w.batches {
		if x == b {
			w.batches = append(w.batches[:i], w.batches[i+1:]...)
			return
		}
	}
}

// send retries one segment's create until the outcome is known: it was
// written (nil), somebody else holds the number (blob.ErrExists) or the writer
// was closed (ErrClosed, once ctx ends).
func (w *Writer) send(ctx context.Context, seq pos.Seg, body *body, id blob.WriteID) error {
	backoff := w.opts.Retry
	for {
		err := w.stores[StoreRecord].journal.CreateWithID(ctx, seq, body, id)
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
				"node", w.stores[StoreRecord].journal.Name(), "segment", seq, "err", err)
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

// closeFiles closes the files of entries that were never written.
func (w *Writer) closeFiles(pending []pendingEntry) {
	for _, p := range pending {
		p.body.Close()
	}
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
	// The batches in flight: their files are closed. Their local copies stay:
	// the record may or may not have them, and the next start lands the tail.
	for _, b := range w.batches {
		b.closeFiles()
	}
	w.batches = nil
}

// ReadFooter returns the entries of a segment. A fence returns ErrFence.
func ReadFooter(ctx context.Context, store *journal.Store, seq pos.Seg) ([]Entry, error) {
	b, err := readFooter(ctx, store, seq)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, ErrFence
	}
	entries, _, err := splitFooter(b, segName(store, seq))
	return entries, err
}

// readFooter reads the end of a segment, big enough to hold the footer whole:
// the end usually holds it, and a footer bigger than the read asks for the
// bigger tail once.
func readFooter(ctx context.Context, store *journal.Store, seq pos.Seg) ([]byte, error) {
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
// entries it names and the footer's own bytes, which a caller holds the
// streamed ending against. A fence is an empty ending and is the caller's
// error, not this one.
func splitFooter(b []byte, name string) ([]Entry, []byte, error) {
	if len(b) < trailerSize {
		return nil, nil, fmt.Errorf("%w: %s is %d bytes", ErrCorrupt, name, len(b))
	}
	tr := b[len(b)-trailerSize:]
	if string(tr[8:]) != magic {
		return nil, nil, fmt.Errorf("%w: %s has bad magic", ErrCorrupt, name)
	}
	n := binary.BigEndian.Uint64(tr[:8])
	if n > maxFooterSize {
		return nil, nil, fmt.Errorf("%w: %s footer length %d", ErrCorrupt, name, n)
	}
	if int64(n)+trailerSize > int64(len(b)) {
		return nil, nil, fmt.Errorf("%w: %s footer length %d", ErrCorrupt, name, n)
	}
	fb := b[len(b)-trailerSize-int(n) : len(b)-trailerSize]
	var entries []Entry
	if err := json.Unmarshal(fb, &entries); err != nil {
		return nil, nil, fmt.Errorf("%w: %s footer: %v", ErrCorrupt, name, err)
	}
	return entries, fb, nil
}

// ReadEntryFile reads the LTX file of one entry, into memory up to limit bytes
// and into a temporary file in dir beyond that. The caller closes the file.
func ReadEntryFile(ctx context.Context, store *journal.Store, seq pos.Seg, e Entry, dir string, limit int) (*spool.File, error) {
	rc, err := store.GetRange(ctx, seq, e.Offset, e.Length)
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
		return nil, fmt.Errorf("%w: %s short range read (%d of %d bytes)", ErrCorrupt, segName(store, seq), n, e.Length)
	}
	return f, nil
}

// readTail reads the last n bytes of the segment, or all of them when the
// segment is shorter. It is how a footer is read: the caller does not know
// where the end is until it asks.
func readTail(ctx context.Context, store *journal.Store, seq pos.Seg, n int64) ([]byte, error) {
	rc, err := store.GetRange(ctx, seq, -n, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// ErrMissing is returned by Scan for a segment that should exist and doesn't.
var ErrMissing = errors.New("segment: missing")

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
func Scan(ctx context.Context, store *journal.Store, from, to pos.Seg, fn func(seq pos.Seg, e Entry) error) error {
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
func Bounds(ctx context.Context, store *journal.Store) (lowest, highest pos.Seg, err error) {
	for seq, err := range store.List(ctx, 1) {
		if err != nil {
			return 0, 0, err
		}
		if lowest == 0 || seq < lowest {
			lowest = seq
		}
		highest = max(highest, seq)
	}
	return lowest, highest, nil
}

// Fence is what an adopter does to a dead node's journal: starting from the
// latest segment number recorded in the node's lease, it probes forward until
// it finds a free number and creates an empty segment there. If a zombie takes
// a number first, its segment counts and the adopter tries the next one. It
// returns the number of its own fence. Lost responses to its create are
// recognised by journal.Store, so a fence the adopter wrote is never mistaken
// for somebody else's.
func Fence(ctx context.Context, store *journal.Store, from pos.Seg) (pos.Seg, error) {
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
