//go:build linux

package node

import "golang.org/x/sys/unix"

// filesystemOf names the filesystem path is on (see linuxFSName), "" for one the node doesn't care about.
func filesystemOf(path string) (string, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return "", err
	}
	return linuxFSName(int64(st.Type)), nil
}
