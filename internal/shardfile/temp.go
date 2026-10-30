package shardfile

import (
	"errors"
	"io"

	"github.com/luci1900/shardlite/internal/tempfile"
)

// dir is the temporary directory.
func (f *Files) dir() (*tempfile.Dir, error) {
	if f.Temp == nil {
		return nil, errors.New("shardfile: no temporary directory set (Files.Temp)")
	}
	return f.Temp, nil
}

// build writes a file with fill into a log of its own and returns the log and
// the file's range in it. The caller closes the log.
func (f *Files) build(fill func(io.Writer) error) (*tempfile.Log, tempfile.File, error) {
	dir, err := f.dir()
	if err != nil {
		return nil, tempfile.File{}, err
	}
	log, err := dir.Create()
	if err != nil {
		return nil, tempfile.File{}, err
	}
	a, err := log.Append()
	if err != nil {
		log.Close()
		return nil, tempfile.File{}, err
	}
	if err := fill(a); err != nil {
		a.Abort()
		log.Close()
		return nil, tempfile.File{}, err
	}
	file, err := a.Finish()
	if err != nil {
		log.Close()
		return nil, tempfile.File{}, err
	}
	return log, file, nil
}
