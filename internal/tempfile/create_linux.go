package tempfile

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// createUnnamed makes a file in dir that never has a name where the file system
// can (O_TMPFILE), and otherwise one whose name is removed at once.
func createUnnamed(dir string) (*os.File, error) {
	fd, err := unix.Open(dir, unix.O_TMPFILE|unix.O_RDWR|unix.O_CLOEXEC, 0o600)
	if err == nil {
		return os.NewFile(uintptr(fd), dir+"/(unnamed)"), nil
	}
	if !errors.Is(err, unix.EOPNOTSUPP) && !errors.Is(err, unix.EISDIR) && !errors.Is(err, unix.EINVAL) {
		return nil, err
	}
	return createRemoved(dir)
}
