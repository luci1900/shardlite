package blob

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"iter"
	"slices"
	"strings"
	"sync"
	"time"
)

// Memory is a Backend that keeps objects in a map under one mutex, so every
// operation is atomic. Tests use it in place of S3: it behaves as the S3
// backend does, and the conformance suite runs against both.
//
//   - ETags are the hex MD5 of the contents, so identical bytes keep their ETag.
//   - A conditional update or delete of a missing key is ErrNotFound (AWS
//     answers 404 NoSuchKey to If-Match on a missing key). RustFS answers 412
//     there, which the callers treat the same way.
//   - An unconditional delete of a missing key succeeds.
//   - Listings carry no write ids, as on S3.
//
// Contents are copied on the way in and out. Share one instance between
// everything that should see the same bucket, and keep it across a simulated
// crash the way a bucket outlives a node.
type Memory struct {
	mu      sync.Mutex
	objects map[string]memObject
	now     func() time.Time
}

type memObject struct {
	data     []byte
	etag     ETag
	id       WriteID
	modified time.Time
}

// MemoryOption configures a Memory backend.
type MemoryOption func(*Memory)

// WithClock sets the clock that stamps LastModified (time.Now by default), so
// tests can age objects. The function is called with the backend's lock held.
func WithClock(now func() time.Time) MemoryOption {
	return func(m *Memory) { m.now = now }
}

// NewMemory returns an empty in-memory backend.
func NewMemory(opts ...MemoryOption) *Memory {
	m := &Memory{objects: map[string]memObject{}, now: time.Now}
	for _, o := range opts {
		o(m)
	}
	return m
}

func memETag(b []byte) ETag {
	sum := md5.Sum(b)
	return ETag(hex.EncodeToString(sum[:]))
}

func checkKey(key string) error {
	if key == "" {
		return fmt.Errorf("blob: invalid key %q", key)
	}
	return nil
}

func (m *Memory) put(key string, body io.ReadSeeker, id WriteID) (ETag, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", fmt.Errorf("%w: reading the body: %v", ErrNotApplied, err)
	}
	o := memObject{data: data, etag: memETag(data), id: id, modified: m.now()}
	m.objects[key] = o
	return o.etag, nil
}

// ended reports a context that has ended before a write: nothing has changed.
// Once the operation has started it has happened, whatever the context does.
func ended(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: %w", ErrNotApplied, context.Cause(ctx))
	}
	return nil
}

func (m *Memory) Create(ctx context.Context, key string, body io.ReadSeeker, id WriteID) (ETag, error) {
	if err := checkKey(key); err != nil {
		return "", err
	}
	if err := ended(ctx); err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objects[key]; ok {
		return "", ErrExists
	}
	return m.put(key, body, id)
}

func (m *Memory) Update(ctx context.Context, key string, body io.ReadSeeker, expected ETag, id WriteID) (ETag, error) {
	if err := checkKey(key); err != nil {
		return "", err
	}
	if err := ended(ctx); err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.objects[key]
	if !ok {
		return "", ErrNotFound
	}
	if cur.etag != expected {
		return "", ErrConflict
	}
	return m.put(key, body, id)
}

func (m *Memory) Get(ctx context.Context, key string) (io.ReadCloser, ETag, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objects[key]
	if !ok {
		return nil, "", ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(slices.Clone(o.data))), o.etag, nil
}

// GetRange reads length bytes from offset (to the end of the object if length
// is not positive or reaches past it). A negative offset asks for the last
// -offset bytes. An offset past the end is a plain error.
func (m *Memory) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	size := int64(len(o.data))
	if offset < 0 {
		if length != 0 {
			return nil, fmt.Errorf("blob: a range from the end takes no length (got %d)", length)
		}
		// A suffix range: the last -offset bytes, or all of them.
		start := max(size+offset, 0)
		return io.NopCloser(bytes.NewReader(slices.Clone(o.data[start:]))), nil
	}
	if offset > size {
		return nil, fmt.Errorf("blob: range offset %d out of bounds (size %d)", offset, size)
	}
	if length <= 0 || offset+length > size {
		length = size - offset
	}
	return io.NopCloser(bytes.NewReader(slices.Clone(o.data[offset : offset+length]))), nil
}

func (m *Memory) Head(ctx context.Context, key string) (ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objects[key]
	if !ok {
		return ObjectInfo{}, ErrNotFound
	}
	return ObjectInfo{Key: key, Size: int64(len(o.data)), ETag: o.etag, LastModified: o.modified, WriteID: o.id}, nil
}

// Delete removes the object. With expected set it is conditional: a missing key
// is ErrNotFound and a changed object ErrConflict. Without it, a missing key is
// success.
func (m *Memory) Delete(ctx context.Context, key string, expected ETag) error {
	if err := ended(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objects[key]
	if expected != "" {
		if !ok {
			return ErrNotFound
		}
		if o.etag != expected {
			return ErrConflict
		}
	}
	delete(m.objects, key)
	return nil
}

func (m *Memory) List(ctx context.Context, prefix, after string) iter.Seq2[ObjectInfo, error] {
	return func(yield func(ObjectInfo, error) bool) {
		m.mu.Lock()
		var infos []ObjectInfo
		for k, o := range m.objects {
			if strings.HasPrefix(k, prefix) && k > after {
				infos = append(infos, ObjectInfo{Key: k, Size: int64(len(o.data)), ETag: o.etag, LastModified: o.modified})
			}
		}
		m.mu.Unlock()
		slices.SortFunc(infos, func(a, b ObjectInfo) int { return strings.Compare(a.Key, b.Key) })
		for _, info := range infos {
			if !yield(info, nil) {
				return
			}
		}
	}
}

var _ Backend = (*Memory)(nil)
