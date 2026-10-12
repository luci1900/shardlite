package blob

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"
)

// Op names a Local operation for the fault hook.
type Op string

const (
	OpCreate   Op = "create"
	OpUpdate   Op = "update"
	OpGet      Op = "get"
	OpGetRange Op = "get-range"
	OpHead     Op = "head"
	OpDelete   Op = "delete"
	OpList     Op = "list"
)

// Hook is called before (after == false) and after (after == true) every
// operation. An error from the before call fails the call with ErrNotApplied
// (nothing happened). An error from the after call means the operation
// happened but the caller sees a failure, like a lost response: ErrUnknown. A
// hook may also block to simulate slow storage.
type Hook func(ctx context.Context, op Op, key string, after bool) error

// writeAttr is the extended attribute that holds an object's ETag (the MD5 of
// its contents) and write id.
const writeAttr = "user.shardlite.write"

// Local is a Backend backed by one directory. Each object is a file. Its ETag
// (the MD5 of its contents, as on S3) and write id are stored in an extended
// attribute of the file, set on the temporary file before it is renamed into
// place, so contents and attribute appear together. Head and List read the
// attribute and never an object's contents. An object that lost its attribute
// (a backup tool dropped it) gets it back on first read.
type Local struct {
	// NoSync skips every fsync. Set it before the first call. Tests that kill
	// processes but not the kernel can use it: writes in the page cache survive
	// a SIGKILL, and on macOS each fsync is an F_FULLFSYNC.
	NoSync bool

	dir  string
	hook atomic.Pointer[Hook]
	tmpN atomic.Uint64

	contentReads atomic.Int64 // opens of an object's contents, for tests
	beforeRename func()       // test hook: called after the temporary file is complete, before the rename
}

const (
	lockDir = ".locks"
	tmpDir  = ".tmp"
)

// NewLocal returns a store rooted at dir, creating it if needed. It fails if
// the filesystem doesn't support user extended attributes.
func NewLocal(dir string) (*Local, error) {
	for _, d := range []string{dir, filepath.Join(dir, lockDir), filepath.Join(dir, tmpDir)} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	if err := checkXattr(filepath.Join(dir, tmpDir), unix.Fsetxattr); err != nil {
		return nil, err
	}
	return &Local{dir: dir}, nil
}

// checkXattr sets and reads the write attribute on a file in dir.
func checkXattr(dir string, set func(fd int, attr string, data []byte, flags int) error) error {
	f, err := os.CreateTemp(dir, "xattr-check-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err := set(int(f.Fd()), writeAttr, []byte("check"), 0); err != nil {
		return fmt.Errorf("blob: the filesystem of %s does not support user extended attributes (%v): the local backend needs ext4, XFS, btrfs, ZFS or APFS", filepath.Dir(dir), err)
	}
	got, err := fgetxattr(int(f.Fd()))
	if err != nil || string(got) != "check" {
		return fmt.Errorf("blob: the filesystem of %s does not keep user extended attributes (read back %q: %v)", filepath.Dir(dir), got, err)
	}
	return nil
}

func fgetxattr(fd int) ([]byte, error) {
	return growXattr(func(b []byte) (int, error) { return unix.Fgetxattr(fd, writeAttr, b) })
}

func lgetxattr(path string) ([]byte, error) {
	return growXattr(func(b []byte) (int, error) { return unix.Lgetxattr(path, writeAttr, b) })
}

func growXattr(get func([]byte) (int, error)) ([]byte, error) {
	buf := make([]byte, 128)
	for {
		n, err := get(buf)
		if errors.Is(err, unix.ERANGE) {
			buf = make([]byte, 2*len(buf))
			continue
		}
		if err != nil {
			return nil, err
		}
		return buf[:n], nil
	}
}

// attr is what an object's extended attribute holds: the hex MD5 of its
// contents (the ETag) and the write id.
type attr struct {
	etag ETag
	id   WriteID
}

// encodeAttr is the attribute value: "md5hex node:epoch:n", or just "md5hex "
// for an object without a write id. The MD5 is fixed length, so the value is
// split on the first space.
func encodeAttr(etag ETag, id WriteID) []byte {
	if id.IsZero() {
		return []byte(string(etag) + " ")
	}
	return []byte(string(etag) + " " + id.Node + ":" + strconv.FormatUint(id.Epoch, 10) + ":" + strconv.FormatUint(id.N, 10))
}

func decodeAttr(b []byte) (attr, error) {
	etag, rest, ok := strings.Cut(string(b), " ")
	if !ok || etag == "" {
		return attr{}, fmt.Errorf("blob: malformed write attribute %q", b)
	}
	a := attr{etag: ETag(etag)}
	if rest == "" {
		return a, nil
	}
	i := strings.LastIndexByte(rest, ':')
	j := strings.LastIndexByte(rest[:max(i, 0)], ':')
	if i < 0 || j < 0 {
		return attr{}, fmt.Errorf("blob: malformed write attribute %q", b)
	}
	epoch, err1 := strconv.ParseUint(rest[j+1:i], 10, 64)
	n, err2 := strconv.ParseUint(rest[i+1:], 10, 64)
	if err1 != nil || err2 != nil {
		return attr{}, fmt.Errorf("blob: malformed write attribute %q", b)
	}
	a.id = WriteID{Node: rest[:j], Epoch: epoch, N: n}
	return a, nil
}

// attrOf reads the attribute of the object at path p, repairing it if it is
// missing. locked says the caller holds the key's flock.
func (l *Local) attrOf(key, p string, locked bool) (attr, error) {
	b, err := lgetxattr(p)
	switch {
	case err == nil:
		return decodeAttr(b)
	case errors.Is(err, fs.ErrNotExist):
		return attr{}, ErrNotFound
	case isNoAttr(err):
		return l.repair(key, p, locked)
	}
	return attr{}, err
}

// repair gives an object that has lost its attribute (a backup tool dropped
// it) the MD5 of its contents and a zero write id.
func (l *Local) repair(key, p string, locked bool) (attr, error) {
	if !locked {
		unlock, err := l.lock(key)
		if err != nil {
			return attr{}, err
		}
		defer unlock()
	}
	if b, err := lgetxattr(p); err == nil { // repaired meanwhile
		return decodeAttr(b)
	}
	f, err := os.Open(p)
	if errors.Is(err, fs.ErrNotExist) {
		return attr{}, ErrNotFound
	} else if err != nil {
		return attr{}, err
	}
	defer f.Close()
	l.contentReads.Add(1)
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return attr{}, err
	}
	a := attr{etag: ETag(hex.EncodeToString(h.Sum(nil)))}
	if err := unix.Fsetxattr(int(f.Fd()), writeAttr, encodeAttr(a.etag, a.id), 0); err != nil {
		return attr{}, fmt.Errorf("blob: restoring the write attribute of %s: %w", p, err)
	}
	return a, nil
}

// SetHook installs a fault hook (nil removes it).
func (l *Local) SetHook(h Hook) {
	if h == nil {
		l.hook.Store(nil)
		return
	}
	l.hook.Store(&h)
}

func (l *Local) call(ctx context.Context, op Op, key string, after bool) error {
	if h := l.hook.Load(); h != nil {
		return (*h)(ctx, op, key, after)
	}
	return nil
}

// run wraps fn with the hook calls and maps hook failures onto outcomes.
func (l *Local) run(ctx context.Context, op Op, key string, fn func() error) error {
	if err := l.call(ctx, op, key, false); err != nil {
		return fmt.Errorf("%w: %v", ErrNotApplied, err)
	}
	if err := fn(); err != nil {
		return err
	}
	if err := l.call(ctx, op, key, true); err != nil {
		return fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	return nil
}

func (l *Local) path(key string) (string, error) {
	if key == "" || strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/") {
		return "", fmt.Errorf("blob: invalid key %q", key)
	}
	for _, part := range strings.Split(key, "/") {
		if part == "" || part == "." || part == ".." || strings.HasPrefix(part, ".") {
			return "", fmt.Errorf("blob: invalid key %q", key)
		}
	}
	return filepath.Join(l.dir, filepath.FromSlash(key)), nil
}

// lock takes an exclusive flock on the key's lock file, which makes
// compare-and-swap atomic across processes.
func (l *Local) lock(key string) (unlock func(), err error) {
	sum := sha256.Sum256([]byte(key))
	f, err := os.OpenFile(filepath.Join(l.dir, lockDir, hex.EncodeToString(sum[:])), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return func() { _ = f.Close() }, nil // closing releases the lock
}

// read returns an object's contents and the attribute of the same inode, and
// repairs a missing attribute first.
func (l *Local) read(key, p string) ([]byte, attr, os.FileInfo, error) {
	for {
		f, err := os.Open(p)
		if errors.Is(err, fs.ErrNotExist) {
			return nil, attr{}, nil, ErrNotFound
		} else if err != nil {
			return nil, attr{}, nil, err
		}
		l.contentReads.Add(1)
		fi, err := f.Stat()
		var raw []byte
		if err == nil {
			raw, err = fgetxattr(int(f.Fd()))
		}
		if isNoAttr(err) {
			f.Close()
			if _, err := l.repair(key, p, false); err != nil {
				return nil, attr{}, nil, err
			}
			continue
		}
		if err != nil {
			f.Close()
			return nil, attr{}, nil, err
		}
		a, err := decodeAttr(raw)
		if err != nil {
			f.Close()
			return nil, attr{}, nil, err
		}
		b, err := io.ReadAll(f)
		f.Close()
		return b, a, fi, err
	}
}

// write puts body at p through a temporary file that is fsynced and renamed.
// The MD5 of the contents (the ETag) is computed as they stream in, and with
// the write id it is an attribute of the temporary file, so the rename moves
// contents and attribute together. Failures before the object is renamed into
// place are ErrNotApplied. After that the object has changed, so they are
// ErrUnknown.
func (l *Local) write(p string, body io.Reader, id WriteID) (ETag, error) {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return "", fmt.Errorf("%w: %v", ErrNotApplied, err)
	}
	tmp := filepath.Join(l.dir, tmpDir, fmt.Sprintf("%d-%d", os.Getpid(), l.tmpN.Add(1)))
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrNotApplied, err)
	}
	h := md5.New()
	_, err = io.Copy(io.MultiWriter(f, h), body)
	etag := ETag(hex.EncodeToString(h.Sum(nil)))
	if err == nil {
		err = unix.Fsetxattr(int(f.Fd()), writeAttr, encodeAttr(etag, id), 0)
	}
	if err == nil && !l.NoSync { // one fsync covers the contents and the attribute
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return "", fmt.Errorf("%w: %v", ErrNotApplied, err)
	}
	if l.beforeRename != nil {
		l.beforeRename()
	}
	if err := os.Rename(tmp, p); err != nil {
		os.Remove(tmp)
		return "", fmt.Errorf("%w: %v", ErrNotApplied, err)
	}
	if d, err := os.Open(filepath.Dir(p)); err == nil && !l.NoSync {
		_ = d.Sync()
		d.Close()
	} else if err == nil {
		d.Close()
	}
	return etag, nil
}

func (l *Local) Create(ctx context.Context, key string, body io.Reader, id WriteID) (etag ETag, err error) {
	p, err := l.path(key)
	if err != nil {
		return "", err
	}
	err = l.run(ctx, OpCreate, key, func() error {
		unlock, err := l.lock(key)
		if err != nil {
			return err
		}
		defer unlock()
		if _, err := os.Stat(p); err == nil {
			return ErrExists
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		etag, err = l.write(p, body, id)
		return err
	})
	return etag, err
}

func (l *Local) Update(ctx context.Context, key string, body io.Reader, expected ETag, id WriteID) (etag ETag, err error) {
	p, err := l.path(key)
	if err != nil {
		return "", err
	}
	err = l.run(ctx, OpUpdate, key, func() error {
		unlock, err := l.lock(key)
		if err != nil {
			return err
		}
		defer unlock()
		cur, err := l.attrOf(key, p, true)
		if err != nil {
			return err
		}
		if cur.etag != expected {
			return ErrConflict
		}
		etag, err = l.write(p, body, id)
		return err
	})
	return etag, err
}

func (l *Local) Get(ctx context.Context, key string) (rc io.ReadCloser, etag ETag, err error) {
	p, err := l.path(key)
	if err != nil {
		return nil, "", err
	}
	err = l.run(ctx, OpGet, key, func() error {
		b, a, _, err := l.read(key, p)
		if err != nil {
			return err
		}
		rc, etag = io.NopCloser(bytes.NewReader(b)), a.etag
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	return rc, etag, nil
}

func (l *Local) GetRange(ctx context.Context, key string, offset, length int64) (rc io.ReadCloser, err error) {
	p, err := l.path(key)
	if err != nil {
		return nil, err
	}
	err = l.run(ctx, OpGetRange, key, func() error {
		f, err := os.Open(p)
		if errors.Is(err, fs.ErrNotExist) {
			return ErrNotFound
		} else if err != nil {
			return err
		}
		defer f.Close()
		l.contentReads.Add(1)
		fi, err := f.Stat()
		if err != nil {
			return err
		}
		if offset < 0 || offset > fi.Size() {
			return fmt.Errorf("blob: range offset %d out of bounds (size %d)", offset, fi.Size())
		}
		if length <= 0 || offset+length > fi.Size() {
			length = fi.Size() - offset
		}
		b := make([]byte, length)
		if _, err := f.ReadAt(b, offset); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		rc = io.NopCloser(bytes.NewReader(b))
		return nil
	})
	return rc, err
}

func (l *Local) Head(ctx context.Context, key string) (info ObjectInfo, err error) {
	p, err := l.path(key)
	if err != nil {
		return info, err
	}
	err = l.run(ctx, OpHead, key, func() error {
		fi, err := os.Lstat(p)
		if errors.Is(err, fs.ErrNotExist) {
			return ErrNotFound
		} else if err != nil {
			return err
		}
		a, err := l.attrOf(key, p, false)
		if err != nil {
			return err
		}
		info = ObjectInfo{Key: key, Size: fi.Size(), ETag: a.etag, LastModified: fi.ModTime(), WriteID: a.id}
		return nil
	})
	return info, err
}

func (l *Local) Delete(ctx context.Context, key string, expected ETag) error {
	p, err := l.path(key)
	if err != nil {
		return err
	}
	return l.run(ctx, OpDelete, key, func() error {
		unlock, err := l.lock(key)
		if err != nil {
			return err
		}
		defer unlock()
		cur, err := l.attrOf(key, p, true)
		if err != nil {
			return err
		}
		if expected != "" && cur.etag != expected {
			return ErrConflict
		}
		if err := os.Remove(p); err != nil {
			return fmt.Errorf("%w: %v", ErrNotApplied, err)
		}
		return nil
	})
}

func (l *Local) List(ctx context.Context, prefix string) iter.Seq2[ObjectInfo, error] {
	return func(yield func(ObjectInfo, error) bool) {
		var infos []ObjectInfo
		err := l.run(ctx, OpList, prefix, func() error {
			return filepath.WalkDir(l.dir, func(p string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				rel, _ := filepath.Rel(l.dir, p)
				if d.IsDir() {
					if rel == lockDir || rel == tmpDir {
						return filepath.SkipDir
					}
					return nil
				}
				key := filepath.ToSlash(rel)
				if !strings.HasPrefix(key, prefix) {
					return nil
				}
				fi, err := d.Info()
				if err != nil {
					if errors.Is(err, fs.ErrNotExist) {
						return nil
					}
					return err
				}
				infos = append(infos, ObjectInfo{Key: key, Size: fi.Size(), LastModified: fi.ModTime()})
				return nil
			})
		})
		if err != nil {
			yield(ObjectInfo{}, err)
			return
		}
		slices.SortFunc(infos, func(a, b ObjectInfo) int { return strings.Compare(a.Key, b.Key) })
		for _, info := range infos {
			a, err := l.attrOf(info.Key, filepath.Join(l.dir, filepath.FromSlash(info.Key)), false)
			if errors.Is(err, ErrNotFound) {
				continue // deleted since the walk
			} else if err != nil {
				yield(ObjectInfo{}, err)
				return
			}
			info.ETag, info.WriteID = a.etag, a.id
			if !yield(info, nil) {
				return
			}
		}
	}
}

var _ Backend = (*Local)(nil)
