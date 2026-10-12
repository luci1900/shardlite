//go:build !darwin

package blob

import (
	"errors"

	"golang.org/x/sys/unix"
)

// isNoAttr reports whether err means the file has no such extended attribute.
func isNoAttr(err error) bool { return errors.Is(err, unix.ENODATA) }
