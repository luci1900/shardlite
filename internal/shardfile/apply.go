// Package shardfile holds what shardlite stores per shard in blob storage:
// manifests, base and delta LTX files, merging and restoring.
package shardfile

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/superfly/ltx"
)

// State describes a database after a chain of LTX files has been applied.
type State struct {
	Position uint64 // the shard's position after the last commit applied
	PageSize uint32
	Commit   uint32 // size in pages
}

// Applier writes a chain of LTX files onto a database file, verifying each
// file's checksums and that the files follow one another.
type Applier struct {
	f     *os.File
	state State
}

// NewApplier starts from an empty file at path (truncating it).
func NewApplier(path string) (*Applier, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &Applier{f: f}, nil
}

// State returns where the chain has got to.
func (a *Applier) State() State { return a.state }

// Apply applies one LTX file that covers the shard's positions rg. The
// numbers inside the file are not used: files from the journal carry Litestream's
// commit numbers, which only mean something within one activation. The first
// file must start at position 1.
func (a *Applier) Apply(r io.Reader, rg Range) error {
	dec := ltx.NewDecoder(r)
	if err := dec.DecodeHeader(); err != nil {
		return fmt.Errorf("decode header: %w", err)
	}
	hdr := dec.Header()
	if a.state.Position == 0 {
		if rg.Min != 1 {
			return fmt.Errorf("first file starts at position %d, want 1", rg.Min)
		}
		a.state.PageSize = hdr.PageSize
	} else {
		if hdr.PageSize != a.state.PageSize {
			return fmt.Errorf("page size changed from %d to %d", a.state.PageSize, hdr.PageSize)
		}
		if rg.Min != a.state.Position+1 {
			return fmt.Errorf("file covering positions %s does not follow position %d", rg, a.state.Position)
		}
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
	}
	// Close reads the trailer and verifies the file checksum.
	if err := dec.Close(); err != nil {
		return fmt.Errorf("verify file %s: %w", rg, err)
	}
	if err := a.f.Truncate(int64(hdr.Commit) * int64(hdr.PageSize)); err != nil {
		return err
	}
	a.state.Position, a.state.Commit = rg.Max, hdr.Commit
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
