// Package shardfile holds what shardlite stores per shard in blob storage:
// manifests, base and delta LTX files, merging and restoring.
package shardfile

import (
	"errors"
	"fmt"
	"hash"
	"io"
	"os"

	"github.com/superfly/ltx"

	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/pos"
)

// ErrBadFile means a file of a shard is not the one the shard's files should
// be: it fails its checksums, or says another range. The shard cannot be
// restored from it and is not served.
var ErrBadFile = errors.New("shardfile: the file is not the one the shard holds")

// State describes a database after a chain of LTX files has been applied.
type State struct {
	Position pos.Position // the shard's position after the last commit applied
	PageSize capture.PageSize
	Pages    capture.Pages // size in pages
}

// Applier writes a chain of LTX files onto a database file, verifying each
// file's checksums and that the files follow one another.
//
// It keeps the CRC of every page it writes, so it can check the checksum of
// the database before and after each file: the file says what the database
// should be before it and after it, and a difference means the file is not the
// one that was written.
type Applier struct {
	f      *os.File
	state  State
	crcs   *capture.Checksums
	hasher hash.Hash64
}

// NewApplier starts from an empty file at path (truncating it).
func NewApplier(path string) (*Applier, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &Applier{f: f, crcs: capture.NewChecksums(), hasher: ltx.NewHasher()}, nil
}

// State returns where the chain has got to.
func (a *Applier) State() State { return a.state }

// Checksums is the CRC of each page of the database the chain built, and the
// running checksum of the whole of it. Capture continues from them, so an
// activation reads no pages.
func (a *Applier) Checksums() *capture.Checksums { return a.crcs.Clone() }

// Apply applies one LTX file that covers the shard's positions rg. The
// numbers inside the file are not used: the caller's range is what the file
// is taken to cover, and the file's own header is checked against it. The
// first file must start at position 1.
//
// A file written before the checksums existed has none, and is applied
// unchecked: the CRCs of the pages it writes are kept all the same, so the
// first file with checksums is checked from there on.
func (a *Applier) Apply(r io.Reader, rg Range) error {
	err := decodeFile(func() error { return a.apply(r, rg) })
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBadFile, err)
	}
	return nil
}

func (a *Applier) apply(r io.Reader, rg Range) error {
	dec := ltx.NewDecoder(r)
	if err := dec.DecodeHeader(); err != nil {
		return fmt.Errorf("decode header: %w", err)
	}
	hdr := dec.Header()
	// The file's own range is checked against the caller's: the header carries
	// the positions the file covers, and a file that says another is not the
	// file the caller asked for.
	if hdr.MinTXID != ltx.TXID(rg.Min) || hdr.MaxTXID != ltx.TXID(rg.Max) {
		return fmt.Errorf("file %s covers positions %d-%d, want %s", rg, hdr.MinTXID, hdr.MaxTXID, rg)
	}
	if a.state.Position == 0 {
		if rg.Min != 1 {
			return fmt.Errorf("first file starts at position %d, want 1", rg.Min)
		}
		a.state.PageSize = capture.PageSize(hdr.PageSize)
	} else {
		if capture.PageSize(hdr.PageSize) != a.state.PageSize {
			return fmt.Errorf("page size changed from %d to %d", a.state.PageSize, hdr.PageSize)
		}
		if rg.Min != a.state.Position+1 {
			return fmt.Errorf("file covering positions %s does not follow position %d", rg, a.state.Position)
		}
	}
	// The checksum of the database before this file. A snapshot has no database
	// before it, and says so with no checksum.
	if !hdr.NoChecksum() && !hdr.IsSnapshot() && hdr.PreApplyChecksum != a.crcs.Checksum() {
		return fmt.Errorf("file %s: the database's checksum is %s before it, the file says %s",
			rg, a.crcs.Checksum(), hdr.PreApplyChecksum)
	}
	buf := make([]byte, hdr.PageSize)
	for {
		var ph ltx.PageHeader
		if err := dec.DecodePage(&ph, buf); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("decode page: %w", err)
		}
		if ph.Pgno > hdr.Commit {
			continue
		}
		if _, err := a.f.WriteAt(buf, int64(ph.Pgno-1)*int64(hdr.PageSize)); err != nil {
			return err
		}
		a.crcs.Set(capture.Pgno(ph.Pgno), ltx.ChecksumPageWithHasher(a.hasher, ph.Pgno, buf))
	}
	// Close reads the trailer and verifies the file checksum.
	if err := dec.Close(); err != nil {
		return fmt.Errorf("verify file %s: %w", rg, err)
	}
	if err := a.f.Truncate(int64(hdr.Commit) * int64(hdr.PageSize)); err != nil {
		return err
	}
	a.crcs.Truncate(capture.Pages(hdr.Commit))
	// The checksum of the database after this file.
	if !hdr.NoChecksum() {
		if got, want := a.crcs.Checksum(), dec.Trailer().PostApplyChecksum; got != want {
			return fmt.Errorf("file %s: the database's checksum is %s after it, the file says %s", rg, got, want)
		}
	}
	a.state.Position, a.state.Pages = rg.Max, capture.Pages(hdr.Commit)
	return nil
}

// Finish syncs and closes the file.
func (a *Applier) Finish() (State, error) {
	err := a.f.Sync()
	if cerr := a.f.Close(); err == nil {
		err = cerr
	}
	return a.state, err
}

// Abort closes the file without syncing.
func (a *Applier) Abort() { _ = a.f.Close() }

// decodeFile runs fn, turning a panic in the LTX decoder into an ordinary error.
// The library panics on a file cut short at the end of its page block, and a
// restore that takes the node down with it is worse than a restore that fails.
func decodeFile(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("the LTX file is malformed: %v", r)
		}
	}()
	return fn()
}
