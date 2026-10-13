package shardfile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/luci1900/shardlite/internal/spool"
)

// DefaultSpoolLimit is how large a delta gets before it is built in a
// temporary file instead of in memory (see package spool).
const DefaultSpoolLimit = spool.DefaultLimit

// newSpool starts collecting bytes in memory up to limit and in a temporary
// file in the data directory beyond it (never the scratch directory, which is
// tmpfs and so memory too). A limit of zero or less is always a file.
func (f *Files) newSpool(limit int) (*spool.Writer, error) {
	if f.TempDir == "" {
		return nil, errors.New("shardfile: no temporary directory set (Files.TempDir)")
	}
	return spool.NewWriter(f.TempDir, limit), nil
}

// download copies an object to a new temporary file, whatever its size.
func download(f *Files, rc io.Reader) (*spool.File, error) {
	w, err := f.newSpool(0)
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(w, rc); err != nil {
		w.Abort()
		return nil, err
	}
	return w.Finish()
}

// ClearTemp removes everything in the temporary directory: files left by a
// process that crashed. Nothing else may be using the directory.
func (f *Files) ClearTemp() error {
	if f.TempDir == "" {
		return nil
	}
	entries, err := os.ReadDir(f.TempDir)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(f.TempDir, 0o755)
	} else if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(f.TempDir, e.Name())); err != nil {
			return fmt.Errorf("clearing %s: %w", f.TempDir, err)
		}
	}
	return nil
}
