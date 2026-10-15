package shardfile

import (
	"errors"

	"github.com/luci1900/shardlite/internal/spool"
)

// tempDir is where files too large for memory go (never the scratch directory,
// which is tmpfs and so memory too).
func (f *Files) tempDir() (string, error) {
	if f.TempDir == "" {
		return "", errors.New("shardfile: no temporary directory set (Files.TempDir)")
	}
	return f.TempDir, nil
}

// newSpool starts collecting bytes in memory up to limit and in a temporary
// file in the data directory beyond it. A limit of zero or less is always a file.
func (f *Files) newSpool(limit int) (*spool.Writer, error) {
	dir, err := f.tempDir()
	if err != nil {
		return nil, err
	}
	return spool.NewWriter(dir, limit), nil
}
