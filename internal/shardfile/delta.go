package shardfile

import (
	"bufio"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/kill"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/remindex"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// DefaultMaxFiles is how many files a manifest may list before the next
// snapshot rebases the shard instead of adding a delta.
const DefaultMaxFiles = 8

// DefaultDeltaAge is how old the oldest tail file gets before a delta is
// written, if the tail has not passed DefaultDeltaSize before then. The age
// bounds how long an adopter replays a dead node's journal, and how long
// segments must be kept.
const DefaultDeltaAge = 15 * time.Minute

// DefaultDeltaSize is how many bytes of tail a shard may hold before a delta is
// written, whatever its age: it bounds the memory the tails take.
const DefaultDeltaSize = 256 << 10

// Hooks let tests stop between steps. A returned error aborts the operation
// at that point, as a crash would.
type Hooks struct {
	// At is called at the kill points of a delta and a rebase (nil in production).
	At              func(kill.Point)
	AfterFileWrite  func() error // delta or base written, manifest not yet updated
	AfterRebaseSwap func() error // rebase's manifest update done, superseded files not yet deleted
	// BeforeUpdate is called once the tail is taken, before the file is written
	// and the manifest updated, with the position the files will reach. An error
	// from it fails the operation with the manifest as it was. It returns the
	// manifest's next_reminder for files that reach end, in Unix minutes (0 for
	// none), which the update records with its entry in the reminder index
	// (docs/internals.md, Reminder index). Without the hook the manifest keeps its
	// own.
	BeforeUpdate func(ctx context.Context, end pos.Position) (nextReminder remindex.Minute, err error)
}

func (hk Hooks) at(p kill.Point) {
	if hk.At != nil {
		hk.At(p)
	}
}

// reminder calls the BeforeUpdate hook, if there is one, for files that reach end,
// and records its answer in next with the entry that comes first.
func (h *Handle) reminder(ctx context.Context, hk Hooks, end pos.Position, next *Manifest) error {
	if hk.BeforeUpdate == nil {
		return nil
	}
	reminder, err := hk.BeforeUpdate(ctx, end)
	if err != nil {
		return fmt.Errorf("before the manifest update: %w", err)
	}
	return h.setReminder(ctx, next, reminder)
}

// Policy says when a snapshot is written, and when it is a rebase and not a delta.
type Policy struct {
	MaxFiles int   // how many files the manifest may list before a rebase, DefaultMaxFiles if zero
	MaxBytes int64 // how many bytes of tail make a delta due whatever their age, DefaultDeltaSize if zero
}

func (p Policy) withDefaults() Policy {
	if p.MaxFiles <= 0 {
		p.MaxFiles = DefaultMaxFiles
	}
	if p.MaxBytes <= 0 {
		p.MaxBytes = DefaultDeltaSize
	}
	return p
}

// RebaseDue reports whether the shard's next snapshot should be a rebase: it
// has more than MaxFiles files. The limit is what bounds how many files a
// restore reads, so it is the whole rule.
func (h *Handle) RebaseDue(p Policy) bool {
	return len(h.Manifest.Files) > p.withDefaults().MaxFiles
}

// source is one of the files being merged, positioned at its current page.
type source struct {
	dec  *ltx.Decoder
	buf  []byte
	pgno uint32
	idx  int // position in the list of files: the later, the newer
	// pre and post are the file's own checksums: the checksum of the database
	// before it and the one after it.
	pre, post ltx.Checksum
}

// next reads the file's next page into buf, and reports false at its end,
// where it verifies the file's checksum.
func (s *source) next() (bool, error) {
	var ph ltx.PageHeader
	if err := s.dec.DecodePage(&ph, s.buf); errors.Is(err, io.EOF) {
		if err := s.dec.Close(); err != nil {
			return false, err
		}
		s.post = s.dec.Trailer().PostApplyChecksum
		return false, nil
	} else if err != nil {
		return false, err
	}
	s.pgno = ph.Pgno
	return true, nil
}

// pageHeap orders sources by the page they are at, and for the same page the
// newest file first.
type pageHeap []*source

func (h pageHeap) Len() int { return len(h) }
func (h pageHeap) Less(i, j int) bool {
	if h[i].pgno != h[j].pgno {
		return h[i].pgno < h[j].pgno
	}
	return h[i].idx > h[j].idx
}
func (h pageHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *pageHeap) Push(x any)   { *h = append(*h, x.(*source)) }
func (h *pageHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// reencode merges captured tail files into one LTX file covering the shard's
// positions rg, keeping the latest version of each page. The result has the
// shard's own positions in its header.
//
// The files are checked to be a chain: each one's pre-apply checksum is the
// one before's post-apply checksum. A break says a file does not follow the
// one before it, which is a bug in what was written, and fails the delta as a
// gap in the positions does.
//
// The delta carries the checksums of the chain: the first file's before and
// the last file's after. A file without checksums fails the delta.
func reencode(w io.Writer, files []capture.TailFile, rg Range) error {
	return decodeFile(func() error { return merge(w, files, rg) })
}

func merge(w io.Writer, files []capture.TailFile, rg Range) error {
	if len(files) == 0 {
		return errors.New("no files to encode")
	}
	srcs := make([]*source, len(files))
	var pageSize capture.PageSize
	var pages capture.Pages
	var ts int64
	for i, f := range files {
		dec := ltx.NewDecoder(bufio.NewReaderSize(f.Body.Reader(), 64<<10))
		if err := dec.DecodeHeader(); err != nil {
			return err
		}
		hdr := dec.Header()
		if pageSize != 0 && hdr.PageSize != uint32(pageSize) {
			return fmt.Errorf("page size changed from %d to %d", pageSize, hdr.PageSize)
		}
		pageSize, pages, ts = capture.PageSize(hdr.PageSize), capture.Pages(hdr.Commit), hdr.Timestamp
		if hdr.NoChecksum() {
			return fmt.Errorf("the file at position %d has no checksums", f.Position)
		}
		srcs[i] = &source{dec: dec, buf: make([]byte, hdr.PageSize), idx: i, pre: hdr.PreApplyChecksum}
	}
	enc, err := ltx.NewEncoder(w)
	if err != nil {
		return err
	}
	hdr := ltx.Header{
		Version:   ltx.Version,
		PageSize:  uint32(pageSize),
		Commit:    uint32(pages),
		MinTXID:   ltx.TXID(rg.Min),
		MaxTXID:   ltx.TXID(rg.Max),
		Timestamp: ts,
	}
	// The checksum of the database before the delta. A snapshot has no database
	// before it, and says so with no checksum.
	hdr.PreApplyChecksum = srcs[0].pre
	if hdr.MinTXID == 1 && hdr.PreApplyChecksum != 0 {
		return fmt.Errorf("the file at position 1 has a checksum before it, which a snapshot cannot have")
	}
	if err := enc.EncodeHeader(hdr); err != nil {
		return err
	}
	var h pageHeap
	for _, s := range srcs {
		if ok, err := s.next(); err != nil {
			return err
		} else if ok {
			h = append(h, s)
		}
	}
	heap.Init(&h)
	for h.Len() > 0 {
		top := h[0]
		pgno := top.pgno
		// The newest file's page is written, then every file that is at this page moves on.
		// Pages past the end of the final database are gone.
		if pgno <= uint32(pages) {
			if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, top.buf); err != nil {
				return err
			}
		}
		for h.Len() > 0 && h[0].pgno == pgno {
			s := h[0]
			if ok, err := s.next(); err != nil {
				return err
			} else if ok {
				heap.Fix(&h, 0)
			} else {
				heap.Pop(&h)
			}
		}
	}
	// Every file has been read to its end, so every trailer has been read.
	for i := 1; i < len(srcs); i++ {
		if got, want := srcs[i].pre, srcs[i-1].post; got != want {
			return fmt.Errorf("the file at position %d does not follow the one before it: the checksum before it is %s, the one before says %s",
				files[i].Position, got, want)
		}
	}
	// The checksum of the database after the delta: the last file's.
	enc.SetPostApplyChecksum(srcs[len(srcs)-1].post)
	return enc.Close()
}

// putFile writes an LTX file with Create. The name carries the writer's
// epoch, so nobody else's file can be there: blob.Store recognises a lost
// response to this very write, and any other "exists" is returned as it is.
//
// A retry of the same file in this process (after a failed manifest update,
// say) is the same write: the handle keeps the write id until the manifest
// has adopted the file, so blob.Store can tell the earlier attempt's object
// from somebody else's without comparing bytes.
func (h *Handle) putFile(ctx context.Context, f File, body blob.Body) error {
	key := FileKey(h.Name, h.Manifest.Generation, f)
	id, ok := h.writes[key]
	if !ok {
		id = h.F.Blob.NewWriteID()
		if h.writes == nil {
			h.writes = map[string]blob.WriteID{}
		}
		h.writes[key] = id
	}
	_, err := h.F.Blob.CreateWithID(ctx, key, body, id)
	return err
}

// WriteDelta merges the shard's tail into one LTX file, adds it to the
// manifest and drops the tail. It reads only captured files, never the
// SQLite file. The delta's range continues from the manifest's last position.
// It does nothing if the tail is empty.
func (h *Handle) WriteDelta(ctx context.Context, sh *capture.Shard, hooks Hooks) error {
	tail, err := sh.Tail()
	if err != nil {
		return fmt.Errorf("delta for %s: %w", h.Name, err)
	}
	defer tail.Close()
	if len(tail.Files) == 0 {
		return nil
	}
	if err := h.putTail(ctx, tail.Files, hooks, nil); err != nil {
		return err
	}
	sh.DropTail(tail.Files[len(tail.Files)-1].Last())
	return nil
}

// putTail writes tail (captured files in position order) as one delta and
// adds it to the manifest. edit, if set, changes the manifest in the same
// compare-and-swap.
func (h *Handle) putTail(ctx context.Context, tail []capture.TailFile, hooks Hooks, edit func(*Manifest)) error {
	if err := h.settle(ctx); err != nil {
		return err
	}
	r := Range{tail[0].Position, tail[len(tail)-1].Last()}
	file := File{r, h.F.Blob.Epoch()}
	if slices.Contains(h.Manifest.Files, file) {
		return nil // an earlier call's update landed, though its answer was lost
	}
	if prev := h.Manifest.LastPosition(); r.Min != prev+1 {
		return fmt.Errorf("delta %s for %s does not continue from position %d", r, h.Name, prev)
	}
	for i := 1; i < len(tail); i++ {
		if tail[i].Position != tail[i-1].Last()+1 {
			return fmt.Errorf("tail of %s has a gap before position %d", h.Name, tail[i].Position)
		}
	}
	log, delta, err := h.F.build(func(w io.Writer) error { return reencode(w, tail, r) })
	if err != nil {
		return fmt.Errorf("delta for %s: %w", h.Name, err)
	}
	defer log.Close()
	// The reminder and its entry come before the file, so a failed entry leaves no
	// file behind that a retry with a new handle (an adoption's) would find.
	next := h.Manifest.clone()
	next.Files = append(next.Files, file)
	if edit != nil {
		edit(next)
	}
	if err := h.reminder(ctx, hooks, r.Max, next); err != nil {
		return err
	}
	if err := h.putFile(ctx, file, delta.Reader()); err != nil {
		return err
	}
	hooks.at(kill.DeltaBeforeManifest)
	if hooks.AfterFileWrite != nil {
		if err := hooks.AfterFileWrite(); err != nil {
			return err
		}
	}
	if err := h.update(ctx, "delta", next); err != nil {
		return err
	}
	delete(h.writes, FileKey(h.Name, h.Manifest.Generation, file))
	return nil
}

// Snapshotted says what MaybeSnapshot wrote. The zero value is Nothing, which
// is the safe answer: nothing was written and nothing needs waiting for
// (docs/internals.md, Failing loudly, the zero-value policy).
type Snapshotted int

const (
	Nothing Snapshotted = iota
	WroteDelta
	WroteRebase
)

// String names what was written, for the logs and the errors.
func (s Snapshotted) String() string {
	switch s {
	case Nothing:
		return "nothing"
	case WroteDelta:
		return "delta"
	case WroteRebase:
		return "rebase"
	}
	return "unknown"
}

// Valid reports whether s is one of the outcomes. The zero is one of them.
func (s Snapshotted) Valid() bool {
	switch s {
	case Nothing, WroteDelta, WroteRebase:
		return true
	}
	return false
}

// MaybeSnapshot is the snapshot of an active shard (docs/internals.md, Node journals
// and snapshots): a delta when the oldest tail file is older than maxAge or the
// tail holds at least p.MaxBytes, whichever comes first, or a rebase instead if
// one is due (RebaseDue). A shard that is due for a rebase and has no tail is
// rebased too if that leaves it with fewer files; one that is not due and has
// a small, young tail is left alone.
func (h *Handle) MaybeSnapshot(ctx context.Context, sh *capture.Shard, maxAge time.Duration, p Policy, hooks Hooks) (Snapshotted, error) {
	tailN, oldest := sh.TailInfo()
	due := h.RebaseDue(p)
	switch {
	case tailN == 0:
		if due && len(h.Manifest.Files) > 1 {
			return h.rebase(ctx, sh, hooks)
		}
	case time.Since(oldest) >= maxAge || sh.TailBytes() >= p.withDefaults().MaxBytes:
		if due {
			return h.rebase(ctx, sh, hooks)
		}
		return WroteDelta, h.WriteDelta(ctx, sh, hooks)
	}
	return Nothing, nil
}

func (h *Handle) rebase(ctx context.Context, sh *capture.Shard, hooks Hooks) (Snapshotted, error) {
	did, err := h.Rebase(ctx, sh, hooks)
	if did {
		return WroteRebase, err
	}
	return Nothing, err
}

// Rebase replaces the manifest's files with one base: a full snapshot of the
// shard from capture (capture.Shard.Snapshot). The app keeps writing meanwhile.
//
// The snapshot covers the shard's positions up to the one capture says it is
// at (see capture.Shard.Snapshot), so the base is 1-{position}, and the commits
// after it are the tail, still in the node's logs and journal. The manifest
// is pointed at the base alone by compare-and-swap before the old files are
// deleted, as a crash in between leaves the old files in use and the new base
// unreferenced. It reports whether it wrote a base: there is nothing to do for a
// shard that has no commits, or whose only file already covers everything.
func (h *Handle) Rebase(ctx context.Context, sh *capture.Shard, hooks Hooks) (bool, error) {
	if err := h.settle(ctx); err != nil {
		return false, err
	}
	// The snapshot goes to a file of its own: it is the whole database.
	var position pos.Position
	log, snap, err := h.F.build(func(w io.Writer) (err error) {
		position, err = h.snapshotTo(ctx, sh, w)
		return err
	})
	if err != nil {
		return false, fmt.Errorf("rebase %s: %w", h.Name, err)
	}
	defer log.Close()
	if position == 0 {
		return false, nil // nothing has been written
	}
	if last := h.Manifest.LastPosition(); position < last {
		return false, fmt.Errorf("rebase %s: snapshot at position %d is behind the manifest's %d", h.Name, position, last)
	}
	base := File{Range{1, position}, h.F.Blob.Epoch()}
	if len(h.Manifest.Files) == 1 && h.Manifest.Files[0].Max == position {
		return false, nil // already one file that covers it
	}
	next := h.Manifest.clone()
	next.Files = []File{base}
	if err := h.reminder(ctx, hooks, position, next); err != nil {
		return false, err
	}
	if err := h.putFile(ctx, base, snap.Reader()); err != nil {
		return false, err
	}
	hooks.at(kill.RebaseBeforeManifest)
	if hooks.AfterFileWrite != nil {
		if err := hooks.AfterFileWrite(); err != nil {
			return false, err
		}
	}
	// The files the base supersedes: h.update swaps h.Manifest to the base
	// alone, so the old list is taken here, before the swap.
	superseded := h.Manifest.Files
	if err := h.update(ctx, "rebase", next); err != nil {
		return false, err
	}
	delete(h.writes, FileKey(h.Name, h.Manifest.Generation, base))
	sh.DropTail(position)
	hooks.at(kill.RebaseAfterSwap)
	if hooks.AfterRebaseSwap != nil {
		if err := hooks.AfterRebaseSwap(); err != nil {
			return true, err
		}
	}
	// Superseded files are garbage now. A failure here only leaves garbage behind,
	// and a delete that finds nothing is done.
	var firstErr error
	for _, f := range superseded {
		if f == base {
			continue
		}
		err := h.F.Blob.Delete(ctx, FileKey(h.Name, h.Manifest.Generation, f), "")
		if err != nil && !errors.Is(err, blob.ErrNotFound) && firstErr == nil {
			firstErr = err
		}
	}
	return true, firstErr
}

// snapshotTo writes a snapshot of the shard to w as an LTX file covering
// positions 1 to the position it returns. The snapshot reader is closed before
// this returns, so the node's WAL restart is held off no longer than the copy.
//
// The base carries the checksum of the database it holds, which is the
// snapshot's own: it is the first file of the shard, and the files that follow
// it are checked against it.
func (h *Handle) snapshotTo(ctx context.Context, sh *capture.Shard, w io.Writer) (position pos.Position, err error) {
	position, rc, err := sh.Snapshot(ctx)
	if err != nil {
		return 0, err
	}
	defer rc.Close() // the WAL may be restarted again once the reader is closed
	if position == 0 {
		return 0, nil
	}
	// The snapshot is decoded and encoded again rather than passed through:
	// its whole file is read, and its header rebuilt, before anything is
	// uploaded.
	if err := reencodeSnapshot(w, rc, position); err != nil {
		return 0, err
	}
	return position, nil
}

// reencodeSnapshot decodes the snapshot in r and encodes it again to w, with
// the shard's own positions in the header and the snapshot's checksum in the
// trailer.
func reencodeSnapshot(w io.Writer, r io.Reader, position pos.Position) error {
	return decodeFile(func() error { return encodeSnapshot(w, r, position) })
}

func encodeSnapshot(w io.Writer, r io.Reader, position pos.Position) error {
	dec := ltx.NewDecoder(bufio.NewReaderSize(r, 1<<20))
	if err := dec.DecodeHeader(); err != nil {
		return err
	}
	hdr := dec.Header()
	if hdr.NoChecksum() {
		return errors.New("the snapshot has no checksums")
	}
	enc, err := ltx.NewEncoder(w)
	if err != nil {
		return err
	}
	out := ltx.Header{
		Version:   ltx.Version,
		PageSize:  hdr.PageSize,
		Commit:    hdr.Commit,
		MinTXID:   1,
		MaxTXID:   ltx.TXID(position),
		Timestamp: hdr.Timestamp,
	}
	if err := enc.EncodeHeader(out); err != nil {
		return err
	}
	buf := make([]byte, hdr.PageSize)
	for {
		var ph ltx.PageHeader
		if err := dec.DecodePage(&ph, buf); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		if err := enc.EncodePage(ph, buf); err != nil {
			return err
		}
	}
	if err := dec.Close(); err != nil {
		return err
	}
	// The checksum of the database the snapshot holds.
	enc.SetPostApplyChecksum(dec.Trailer().PostApplyChecksum)
	return enc.Close()
}

// getFile downloads one of the shard's LTX files to a temporary file.
func (h *Handle) getFile(ctx context.Context, f File) (*tempfile.Log, error) {
	rc, _, err := h.F.Blob.Get(ctx, FileKey(h.Name, h.Manifest.Generation, f))
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	dir, err := h.F.dir()
	if err != nil {
		return nil, err
	}
	return dir.Collect(rc)
}
