package capture

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
)

// WALSidecars is the pair of files beside a shard's database: the write-ahead
// log (-wal) and the shared wal-index (-shm). They live and die together
// (docs/internals.md): a writing connection creates both, capture's
// connections keep both across their closes (persistWAL, so a release can
// trust the wal-index and the next activation can fold the log), and whoever
// makes the database file complete on its own removes both.
//
// The halves are closed on purpose. This is the only place that spells their
// names, so nothing outside it can remove or look for one half where the pair
// is meant: use Remove or Any. Reading one half alone is fine, and goes
// through the method that says why. The wal-index is derived state, rebuilt
// from the log, so a lone stale one means nothing.
type WALSidecars struct {
	db  string
	wal string
	shm string
}

// WALSidecarsOf returns the pair beside dbPath.
func WALSidecarsOf(dbPath string) WALSidecars {
	return WALSidecars{db: dbPath, wal: dbPath + walSuffix, shm: dbPath + shmSuffix}
}

// The pair's filename suffixes, the only form in which they are spelled
// anywhere besides the two lines above.
const (
	walSuffix = "-wal"
	shmSuffix = "-shm"
)

// WALSidecarSuffixes returns the pair's filename suffixes, for a caller that
// classifies the files in a directory (which belong beside some database)
// rather than the files beside one database it knows.
func WALSidecarSuffixes() []string { return []string{walSuffix, shmSuffix} }

// Any reports whether either half is there: somebody has written to the
// database, or a connection to it is open.
func (s WALSidecars) Any() bool {
	_, werr := os.Stat(s.wal)
	_, serr := os.Stat(s.shm)
	return werr == nil || serr == nil
}

// Remove removes the pair as one, whichever halves are there.
func (s WALSidecars) Remove() error {
	for _, p := range []string{s.wal, s.shm} {
		if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

// Paths returns the pair, for listing a database's local files among others
// (a cleanup that also takes the database itself, or a rollback journal).
func (s WALSidecars) Paths() []string { return []string{s.wal, s.shm} }

// Held reports whether a process other than this one has the database open.
// It asks with F_GETLK for a write lock on the wal-index's deadman switch
// byte, which neither takes a lock nor races a connection that is opening.
//
// fcntl locks belong to a process, and closing any descriptor on a file drops
// all of that process's locks on it. So Held opens and closes a descriptor on
// the wal-index, and must only be called when this process has nothing else
// open on the shard (see Shard.Release): it then has no locks of its own for
// the close to drop, the app's locks are never affected, and this process's
// own connections can't be mistaken for another process's. A missing
// wal-index means no connection has used the database in WAL mode.
func (s WALSidecars) Held() (bool, error) {
	f, err := os.OpenFile(s.shm, os.O_RDWR, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	defer f.Close()
	lk := syscall.Flock_t{Type: syscall.F_WRLCK, Whence: 0, Start: dmsByte, Len: 1}
	if err := syscall.FcntlFlock(f.Fd(), syscall.F_GETLK, &lk); err != nil {
		return false, err
	}
	return lk.Type != syscall.F_UNLCK, nil
}

// SQLite's deadman switch byte in the wal-index file: every connection to a
// WAL database holds a shared lock on it while open ((22+SQLITE_SHM_NLOCK)*4 +
// SQLITE_SHM_NLOCK, SQLite's UNIX_SHM_DMS). SQLite itself uses F_GETLK on it
// to tell whether it is the first connection.
const dmsByte = 128

// openWAL opens the log for reading. A connection creates it, so it is there
// once the app has written.
func (s WALSidecars) openWAL() (*os.File, error) {
	return os.Open(s.wal)
}

// openOrCreate opens the wal-index, creating it if it is missing. SQLite
// reuses one that exists, so one made here has to be the one SQLite would
// have made: with the database file's permissions and owner. The mode is set
// after the file is created so the node's umask can't narrow it, as it would
// for a database that another user shares through its group.
//
// The descriptor must be closed at a time when this process holds no locks on
// the shard (before capture opens it), because closing it drops them.
func (s WALSidecars) openOrCreate() (*os.File, error) {
	fi, err := os.Stat(s.db)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(s.shm, os.O_RDWR|os.O_CREATE|os.O_EXCL, fi.Mode().Perm())
	if errors.Is(err, fs.ErrExist) {
		return os.OpenFile(s.shm, os.O_RDWR, 0)
	} else if err != nil {
		return nil, err
	}
	if err := f.Chmod(fi.Mode().Perm()); err != nil {
		f.Close()
		return nil, err
	}
	// Changing the owner only works for a process that may, as with SQLite.
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		_ = f.Chown(int(st.Uid), int(st.Gid))
	}
	return f, nil
}
