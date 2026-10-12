package capture

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
)

// SQLite's deadman switch byte in the -shm file: every connection to a WAL
// database holds a shared lock on it while open ((22+SQLITE_SHM_NLOCK)*4 +
// SQLITE_SHM_NLOCK, SQLite's UNIX_SHM_DMS). SQLite itself uses F_GETLK on it
// to tell whether it is the first connection.
const dmsByte = 128

// ProbeOnce reports whether a process other than this one has the database
// open. It asks with F_GETLK for a write lock on the deadman switch byte,
// which neither takes a lock nor races a connection that is opening.
//
// fcntl locks belong to a process, and closing any descriptor on a file drops
// all of that process's locks on it. So ProbeOnce opens and closes a
// descriptor on the -shm file, and must only be called when this process has
// nothing else open on the shard (see Shard.Release): it then has no locks of
// its own on the file for the close to drop, the app's locks are never
// affected, and this process's own connections can't be mistaken for another
// process's. A missing -shm file means no connection has used the database in
// WAL mode.
func ProbeOnce(dbPath string) (bool, error) {
	held, _, err := ProbeOwner(dbPath)
	return held, err
}

// ProbeOwner is ProbeOnce that also returns the pid the kernel reports for
// the lock holder, as seen from this process's pid namespace (0 if the holder
// isn't visible from it, which is the case across containers).
func ProbeOwner(dbPath string) (held bool, pid int, err error) {
	f, err := os.OpenFile(dbPath+"-shm", os.O_RDWR, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return false, 0, nil
	} else if err != nil {
		return false, 0, err
	}
	defer f.Close()
	lk := syscall.Flock_t{Type: syscall.F_WRLCK, Whence: 0, Start: dmsByte, Len: 1}
	if err := syscall.FcntlFlock(f.Fd(), syscall.F_GETLK, &lk); err != nil {
		return false, 0, err
	}
	return lk.Type != syscall.F_UNLCK, int(lk.Pid), nil
}
