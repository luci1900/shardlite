package shardfile

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
)

// DefaultMaxFiles is how many files a manifest may list before they are merged into one base.
const DefaultMaxFiles = 8

// DefaultDeltaAge is how old the oldest tail file gets before a delta is written.
const DefaultDeltaAge = 3 * time.Minute

// Hooks let tests stop between steps. A returned error aborts the operation
// at that point, as a crash would.
type Hooks struct {
	AfterFileWrite func() error // delta or base written, manifest not yet updated
	AfterMergeSwap func() error // merge's manifest update done, superseded files not yet deleted
}

// compact merges LTX files that already carry the shard's positions (manifest
// files) into one, keeping the latest version of each page.
func compact(inputs [][]byte) ([]byte, error) {
	rs := make([]io.Reader, len(inputs))
	for i, b := range inputs {
		rs[i] = bytes.NewReader(b)
	}
	var out bytes.Buffer
	c, err := ltx.NewCompactor(&out, rs)
	if err != nil {
		return nil, err
	}
	c.HeaderFlags = ltx.HeaderFlagNoChecksum
	if err := c.Compact(context.Background()); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// reencode merges captured tail files into one LTX file covering the shard's
// positions rg, keeping the latest version of each page. The files carry
// Litestream's commit numbers, which restart in every activation, so the
// LTX compactor (which wants contiguous numbers) can't be used. The result
// has the shard's own positions in its header.
func reencode(files []capture.TailFile, rg Range) ([]byte, error) {
	pages := map[uint32][]byte{}
	var pageSize, commit uint32
	var ts int64
	for _, f := range files {
		dec := ltx.NewDecoder(bytes.NewReader(f.Data))
		if err := dec.DecodeHeader(); err != nil {
			return nil, err
		}
		hdr := dec.Header()
		if pageSize != 0 && hdr.PageSize != pageSize {
			return nil, fmt.Errorf("page size changed from %d to %d", pageSize, hdr.PageSize)
		}
		pageSize, commit, ts = hdr.PageSize, hdr.Commit, hdr.Timestamp
		for {
			var ph ltx.PageHeader
			buf := make([]byte, hdr.PageSize)
			if err := dec.DecodePage(&ph, buf); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return nil, err
			}
			pages[ph.Pgno] = buf
		}
		if err := dec.Close(); err != nil { // verifies the file's checksum
			return nil, err
		}
	}
	var out bytes.Buffer
	enc, err := ltx.NewEncoder(&out)
	if err != nil {
		return nil, err
	}
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagNoChecksum,
		PageSize:  pageSize,
		Commit:    commit,
		MinTXID:   ltx.TXID(rg.Min),
		MaxTXID:   ltx.TXID(rg.Max),
		Timestamp: ts,
	}); err != nil {
		return nil, err
	}
	pgnos := make([]uint32, 0, len(pages))
	for pgno := range pages {
		if pgno <= commit { // pages past the end of the final database are gone
			pgnos = append(pgnos, pgno)
		}
	}
	slices.Sort(pgnos)
	for _, pgno := range pgnos {
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, pages[pgno]); err != nil {
			return nil, err
		}
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// putFile writes an LTX file with Create. The name carries the writer's
// epoch, so nobody else's file can be there: blob.Store recognises a lost
// response to this very write, and any other "exists" is returned as it is.
//
// A retry of the same file in this process (after a failed manifest update,
// say) is the same write: the handle keeps the write id until the manifest
// has adopted the file, so blob.Store can tell the earlier attempt's object
// from somebody else's without comparing bytes.
func (h *Handle) putFile(ctx context.Context, f File, data []byte) error {
	key := FileKey(h.Name, h.Manifest.Generation, f)
	id, ok := h.writes[key]
	if !ok {
		id = h.F.Blob.NewWriteID()
		if h.writes == nil {
			h.writes = map[string]blob.WriteID{}
		}
		h.writes[key] = id
	}
	_, err := h.F.Blob.CreateWithID(ctx, key, bytes.NewReader(data), id)
	return err
}

// WriteDelta merges the shard's tail into one LTX file, adds it to the
// manifest and drops the tail. It reads only captured files, never the
// SQLite file. The delta's range continues from the manifest's last position,
// whatever Litestream's numbers were. It does nothing if the tail is empty.
func (h *Handle) WriteDelta(ctx context.Context, sh *capture.Shard, hooks Hooks) error {
	tail := sh.Tail()
	if len(tail) == 0 {
		return nil
	}
	if err := h.putTail(ctx, tail, hooks, nil); err != nil {
		return err
	}
	sh.DropTail(tail[len(tail)-1].Last())
	return nil
}

// putTail writes tail (captured files in position order) as one delta and
// adds it to the manifest. edit, if set, changes the manifest in the same
// compare-and-swap.
func (h *Handle) putTail(ctx context.Context, tail []capture.TailFile, hooks Hooks, edit func(*Manifest)) error {
	r := Range{tail[0].Position, tail[len(tail)-1].Last()}
	if prev := h.Manifest.LastPosition(); r.Min != prev+1 {
		return fmt.Errorf("delta %s for %s does not continue from position %d", r, h.Name, prev)
	}
	for i := 1; i < len(tail); i++ {
		if tail[i].Position != tail[i-1].Last()+1 {
			return fmt.Errorf("tail of %s has a gap before position %d", h.Name, tail[i].Position)
		}
	}
	data, err := reencode(tail, r)
	if err != nil {
		return fmt.Errorf("delta for %s: %w", h.Name, err)
	}
	file := File{r, h.F.Blob.Epoch()}
	if err := h.putFile(ctx, file, data); err != nil {
		return err
	}
	if hooks.AfterFileWrite != nil {
		if err := hooks.AfterFileWrite(); err != nil {
			return err
		}
	}
	next := h.Manifest.clone()
	next.Files = append(next.Files, file.String())
	if edit != nil {
		edit(next)
	}
	if err := h.update(ctx, next); err != nil {
		return err
	}
	delete(h.writes, FileKey(h.Name, h.Manifest.Generation, file))
	return nil
}

// MaybeWriteDelta writes a delta if the oldest tail file is older than maxAge.
func (h *Handle) MaybeWriteDelta(ctx context.Context, sh *capture.Shard, maxAge time.Duration, hooks Hooks) (bool, error) {
	tail := sh.Tail()
	if len(tail) == 0 || time.Since(tail[0].At) < maxAge {
		return false, nil
	}
	return true, h.WriteDelta(ctx, sh, hooks)
}

// getFile downloads one of the shard's LTX files.
func (h *Handle) getFile(ctx context.Context, f File) ([]byte, error) {
	rc, _, err := h.F.Blob.Get(ctx, FileKey(h.Name, h.Manifest.Generation, f))
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// MergeIfNeeded merges all the manifest's files into a new base when it
// lists more than maxFiles. It reports whether it merged.
func (h *Handle) MergeIfNeeded(ctx context.Context, maxFiles int, hooks Hooks) (bool, error) {
	if len(h.Manifest.Files) <= maxFiles {
		return false, nil
	}
	return true, h.Merge(ctx, hooks)
}

// Merge replaces the manifest's files with one base covering all of them.
func (h *Handle) Merge(ctx context.Context, hooks Hooks) error {
	files, err := h.Manifest.List()
	if err != nil {
		return err
	}
	if len(files) < 2 {
		return nil
	}
	inputs := make([][]byte, len(files))
	for i, f := range files {
		if inputs[i], err = h.getFile(ctx, f); err != nil {
			return fmt.Errorf("merge %s: %w", h.Name, err)
		}
	}
	data, err := compact(inputs)
	if err != nil {
		return fmt.Errorf("merge %s: %w", h.Name, err)
	}
	base := File{Range{1, files[len(files)-1].Max}, h.F.Blob.Epoch()}
	if err := h.putFile(ctx, base, data); err != nil {
		return err
	}
	if hooks.AfterFileWrite != nil {
		if err := hooks.AfterFileWrite(); err != nil {
			return err
		}
	}
	next := h.Manifest.clone()
	next.Files = []string{base.String()}
	if err := h.update(ctx, next); err != nil {
		return err
	}
	delete(h.writes, FileKey(h.Name, h.Manifest.Generation, base))
	if hooks.AfterMergeSwap != nil {
		if err := hooks.AfterMergeSwap(); err != nil {
			return err
		}
	}
	// Superseded files are garbage now. A failure here only leaves garbage behind,
	// and a delete that finds nothing is done.
	var firstErr error
	for _, f := range files {
		if f == base {
			continue
		}
		err := h.F.Blob.Delete(ctx, FileKey(h.Name, h.Manifest.Generation, f), "")
		if err != nil && !errors.Is(err, blob.ErrNotFound) && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
