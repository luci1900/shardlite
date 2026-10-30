//go:build !linux

package tempfile

import "os"

// createUnnamed makes a file in dir whose name is removed at once.
func createUnnamed(dir string) (*os.File, error) { return createRemoved(dir) }
