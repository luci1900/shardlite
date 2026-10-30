package segment

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// Land copies segment seq out of src, a copy of the journal, into the record's
// store, checked as the bytes leave. The restart runs it on its previous
// life's local rows (docs/internals.md, Takeover: Restart). It is the one
// path bytes take from a store into the system of record, so a copy it reads
// back could be anything a broken store or disk handed out, where a segment
// composed from the capture is verified by construction: its frames were
// checksummed where they were captured and its footer is computed over its
// own offsets. So the check is the formats' own, not a new one.
//
// The footer comes first, for only it knows where each file's bytes are. The
// whole segment then streams past once, every file decoded as it flows, and
// the record's create takes the write id of the write these bytes are a
// retry of. A segment the check fails lands nowhere, and nothing goes above
// it in the record.
func Land(ctx context.Context, record, src *journal.Store, seq pos.Seg, id blob.WriteID, dir *tempfile.Dir) error {
	name := segName(src, seq)
	head, err := src.Head(ctx, seq)
	if err != nil {
		return fmt.Errorf("head %s: %w", name, err)
	}
	if head.Size == 0 {
		// A fence is never landed: it holds nothing of the shard's tail.
		return fmt.Errorf("%w: %s is empty", ErrCorrupt, name)
	}
	tail, err := readFooter(ctx, src, seq)
	if err != nil {
		return err
	}
	entries, _, err := splitFooter(tail, name)
	if err != nil {
		return err
	}
	end, err := checkLayout(entries, head.Size, name)
	if err != nil {
		return err
	}
	// The bytes after the files are the footer and its trailer, bounded: a
	// footer that names less than the segment holds cannot pass the checks
	// below, and without a bound a fake one could ask for gigabytes.
	if rest := head.Size - end; rest <= trailerSize || maxFooterSize+trailerSize < rest {
		return fmt.Errorf("%w: %s holds %d bytes after its files", ErrCorrupt, name, rest)
	}
	log, err := dir.Create()
	if err != nil {
		return fmt.Errorf("land %s: %w", name, err)
	}
	defer log.Close()
	sw, err := log.Append()
	if err != nil {
		return fmt.Errorf("land %s: %w", name, err)
	}
	rc, err := src.Get(ctx, seq)
	if err != nil {
		sw.Abort()
		return fmt.Errorf("read %s: %w", name, err)
	}
	defer rc.Close()
	if err := checkStream(ctx, sw, rc, entries, head.Size, end, name); err != nil {
		sw.Abort()
		return err
	}
	f, err := sw.Finish()
	if err != nil {
		return fmt.Errorf("land %s: %w", name, err)
	}
	if err := record.CreateWithID(ctx, seq, landBody{r: f.Reader()}, id); err != nil {
		return fmt.Errorf("put %s: %w", name, err)
	}
	return nil
}

// landBody is the checked segment as the record's create reads it: bytes that
// passed the landing's checks, replayed as many times as the create retries.
type landBody struct {
	r *io.SectionReader
}

func (b landBody) Size() int64 { return b.r.Size() }

func (b landBody) ReadAt(p []byte, off int64) (int, error) { return b.r.ReadAt(p, off) }

// checkLayout says the entries lay out the segment they were read from: at
// least one, each holding bytes and positions, the files back to back from
// the segment's first byte, the last of them ending below its size. It
// returns where they end. A footer that doesn't would read whatever bytes
// the store hands out back as a segment, so the landing refuses before
// anything is put.
func checkLayout(entries []Entry, size int64, name string) (int64, error) {
	if len(entries) == 0 {
		return 0, fmt.Errorf("%w: %s has no files", ErrCorrupt, name)
	}
	var off int64
	for _, e := range entries {
		if e.Length <= 0 || e.Count == 0 {
			return 0, fmt.Errorf("%w: %s's entry for %s holds no bytes", ErrCorrupt, name, e.Shard)
		}
		if e.Offset != off {
			return 0, fmt.Errorf("%w: %s's files for %s start at %d, want %d", ErrCorrupt, name, e.Shard, e.Offset, off)
		}
		if e.Offset+e.Length > size {
			return 0, fmt.Errorf("%w: %s holds %d bytes, its footer says its files end at %d", ErrCorrupt, name, size, e.Offset+e.Length)
		}
		off = e.Offset + e.Length
	}
	return off, nil
}

// checkStream reads the segment once, past a tee into sw, checking each file
// as it flows and the bytes after them. The checked bytes are in sw's log,
// so the record's create retries from there and never rereads the store.
//
// A file is decoded whole before it is trusted: the decoder's Close verifies
// the file's own checksum, its header must name the range its footer entry
// says, and its page size must be one SQLite takes. The files of one shard
// carry the chain from one to the next: the checksum of the database before
// a file is the one the file before it left, so a shard's files in the
// segment are one history. Snapshots and files without checksums say neither
// side, which the applier reads the same way.
func checkStream(ctx context.Context, sw *tempfile.Appender, r io.Reader, entries []Entry, size, end int64, name string) error {
	tee := io.TeeReader(r, sw)
	after := map[shard.Name]ltx.Checksum{} // each shard's after-checksum, with the files it came from
	withFiles := map[shard.Name]bool{}     // ... and whether any of them had checksums
	for _, e := range entries {
		// The entry's bytes and no more: the reader ends them where the footer
		// does, and one left over is a footer that doesn't hold its own.
		lr := &io.LimitedReader{R: tee, N: e.Length}
		dec := ltx.NewDecoder(bufio.NewReaderSize(lr, 64<<10))
		if err := dec.DecodeHeader(); err != nil {
			return fmt.Errorf("%w: %s's files for %s: %v", ErrCorrupt, name, e.Shard, err)
		}
		hdr := dec.Header()
		said := !hdr.NoChecksum() && !hdr.IsSnapshot()
		switch {
		case hdr.PageSize < 512 || hdr.PageSize > 65536 || hdr.PageSize&(hdr.PageSize-1) != 0:
			return fmt.Errorf("%w: %s's files for %s have a page size of %d", ErrCorrupt, name, e.Shard, hdr.PageSize)
		case hdr.MinTXID != ltx.TXID(e.Position) || hdr.MaxTXID != ltx.TXID(e.Last()):
			// The file is the one the footer names: the positions it covers are
			// the positions, and any other range would not continue the shard.
			return fmt.Errorf("%w: %s's entry for %s says its file covers positions %d to %d, the file says %d to %d",
				ErrCorrupt, name, e.Shard, e.Position, e.Last(), hdr.MinTXID, hdr.MaxTXID)
		case said && withFiles[e.Shard] && hdr.PreApplyChecksum != after[e.Shard]:
			return fmt.Errorf("%w: %s's files for %s: the database's checksum before the file is %s, the one before it left %s",
				ErrCorrupt, name, e.Shard, hdr.PreApplyChecksum, after[e.Shard])
		}
		buf := make([]byte, hdr.PageSize)
		for {
			var ph ltx.PageHeader
			if err := dec.DecodePage(&ph, buf); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return fmt.Errorf("%w: %s's files for %s: %v", ErrCorrupt, name, e.Shard, err)
			}
		}
		// Close reads the trailer and verifies the file's checksum.
		if err := dec.Close(); err != nil {
			return fmt.Errorf("%w: %s's files for %s: %v", ErrCorrupt, name, e.Shard, err)
		}
		if lr.N != 0 {
			return fmt.Errorf("%w: %s's files for %s end %d bytes short of what the footer says", ErrCorrupt, name, e.Shard, lr.N)
		}
		after[e.Shard] = dec.Trailer().PostApplyChecksum
		withFiles[e.Shard] = said
	}

	// The bytes after the files are the footer and its trailer. They were read
	// once at the end of the segment for the entries, and here again as they
	// stream past, where they must parse to the same entries, the same trailer
	// and nothing more or less.
	region := make([]byte, size-end)
	if _, err := io.ReadFull(tee, region); err != nil {
		return fmt.Errorf("%w: %s's end: %v", ErrCorrupt, name, err)
	}
	tr := region[len(region)-trailerSize:]
	if string(tr[8:]) != magic {
		return fmt.Errorf("%w: %s has bad magic", ErrCorrupt, name)
	}
	if n := binary.BigEndian.Uint64(tr[:8]); int64(n)+trailerSize != int64(len(region)) {
		return fmt.Errorf("%w: %s says its footer is %d bytes, which is not the %d it sits on", ErrCorrupt, name, n, len(region)-trailerSize)
	}
	var streamed []Entry
	if err := json.Unmarshal(region[:len(region)-trailerSize], &streamed); err != nil {
		return fmt.Errorf("%w: %s footer: %v", ErrCorrupt, name, err)
	}
	if !slices.Equal(streamed, entries) {
		return fmt.Errorf("%w: %s's footer does not read the same twice", ErrCorrupt, name)
	}
	if got, want := sw.Written(), size; got != want {
		return fmt.Errorf("%w: %s streamed %d of %d bytes", ErrCorrupt, name, got, want)
	}
	return nil
}
