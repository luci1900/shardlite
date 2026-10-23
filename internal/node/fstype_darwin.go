//go:build darwin

package node

import "golang.org/x/sys/unix"

// filesystemOf names the filesystem path is on (see macFSName).
func filesystemOf(path string) (string, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return "", err
	}
	return macFSName(unix.ByteSliceToString(st.Fstypename[:])), nil
}
