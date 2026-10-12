package node

import (
	"fmt"
	"strings"
)

// Filesystem magic numbers (statfs f_type on Linux, see statfs(2)).
const (
	magicNFS   = 0x6969
	magicSMB   = 0x517B
	magicCIFS  = 0xFF534D42
	magicSMB2  = 0xFE534D42
	magicFUSE  = 0x65735546
	magicTmpfs = 0x01021994
	magicRamfs = 0x858458F6
)

// linuxFSName names the filesystems the node cares about from statfs's f_type,
// and returns "" for any other.
func linuxFSName(magic int64) string {
	switch uint32(magic) {
	case magicNFS:
		return "nfs"
	case magicSMB, magicCIFS, magicSMB2:
		return "smb"
	case magicFUSE:
		return "fuse"
	case magicTmpfs:
		return "tmpfs"
	case magicRamfs:
		return "ramfs"
	}
	return ""
}

// macFSName normalises f_fstypename on macOS to the same names.
func macFSName(typename string) string {
	t := strings.ToLower(typename)
	switch {
	case t == "nfs":
		return "nfs"
	case t == "smbfs":
		return "smb"
	case t == "macfuse", t == "osxfuse", strings.HasPrefix(t, "fuse"):
		return "fuse"
	case t == "webdav", t == "afpfs":
		return "network"
	}
	return t
}

// networkFS reports whether a filesystem name is one SQLite's locking and
// shared memory can't be trusted on: NFS, SMB, FUSE (and the other network
// filesystems macOS names). flock and the -shm mapping need a local disk.
func networkFS(name string) bool {
	switch name {
	case "nfs", "smb", "fuse", "network":
		return true
	}
	return false
}

// fsTypeFunc names the filesystem a path is on (see fstype_linux.go and
// fstype_darwin.go). Tests inject their own.
type fsTypeFunc func(path string) (string, error)

// refuseNetworkFS returns an error naming the setting if path is on a network filesystem.
func refuseNetworkFS(fsType fsTypeFunc, path, what string) error {
	name, err := fsType(path)
	if err != nil {
		return fmt.Errorf("checking the filesystem of %s (%s): %w", what, path, err)
	}
	if networkFS(name) {
		return fmt.Errorf("%s (%s) is on a %s filesystem: flock and SQLite's shared memory need a local disk, so shardlite refuses to start (use a local disk or, in Docker, a named volume rather than a bind mount)", what, path, name)
	}
	return nil
}
