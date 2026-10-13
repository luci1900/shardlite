package blob

import (
	"context"
	"fmt"
	"io"
	"iter"
	"sync/atomic"
)

// Op names a backend operation for the fault hook.
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
// hook may also block to simulate slow storage. For OpList the key is the
// prefix.
type Hook func(ctx context.Context, op Op, key string, after bool) error

// Hooked is a Backend with a fault hook in front of another one.
type Hooked struct {
	b    Backend
	hook atomic.Pointer[Hook]
}

// WithHook wraps b so a test can inject faults with SetHook. It works over any
// backend. Faults below the backend (dropped responses, refused connections)
// belong in the S3 backend's HTTP transport instead.
func WithHook(b Backend) *Hooked { return &Hooked{b: b} }

// SetHook installs a fault hook (nil removes it).
func (h *Hooked) SetHook(fn Hook) {
	if fn == nil {
		h.hook.Store(nil)
		return
	}
	h.hook.Store(&fn)
}

func (h *Hooked) call(ctx context.Context, op Op, key string, after bool) error {
	if fn := h.hook.Load(); fn != nil {
		return (*fn)(ctx, op, key, after)
	}
	return nil
}

// before and afterErr map hook failures onto outcomes.
func (h *Hooked) before(ctx context.Context, op Op, key string) error {
	if err := h.call(ctx, op, key, false); err != nil {
		return fmt.Errorf("%w: %v", ErrNotApplied, err)
	}
	return nil
}

func (h *Hooked) after(ctx context.Context, op Op, key string) error {
	if err := h.call(ctx, op, key, true); err != nil {
		return fmt.Errorf("%w: %v", ErrUnknown, err)
	}
	return nil
}

func (h *Hooked) Create(ctx context.Context, key string, body io.ReadSeeker, id WriteID) (ETag, error) {
	if err := h.before(ctx, OpCreate, key); err != nil {
		return "", err
	}
	etag, err := h.b.Create(ctx, key, body, id)
	if err != nil {
		return "", err
	}
	if err := h.after(ctx, OpCreate, key); err != nil {
		return "", err
	}
	return etag, nil
}

func (h *Hooked) Update(ctx context.Context, key string, body io.ReadSeeker, expected ETag, id WriteID) (ETag, error) {
	if err := h.before(ctx, OpUpdate, key); err != nil {
		return "", err
	}
	etag, err := h.b.Update(ctx, key, body, expected, id)
	if err != nil {
		return "", err
	}
	if err := h.after(ctx, OpUpdate, key); err != nil {
		return "", err
	}
	return etag, nil
}

func (h *Hooked) Get(ctx context.Context, key string) (io.ReadCloser, ETag, error) {
	if err := h.before(ctx, OpGet, key); err != nil {
		return nil, "", err
	}
	rc, etag, err := h.b.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	if err := h.after(ctx, OpGet, key); err != nil {
		rc.Close()
		return nil, "", err
	}
	return rc, etag, nil
}

func (h *Hooked) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if err := h.before(ctx, OpGetRange, key); err != nil {
		return nil, err
	}
	rc, err := h.b.GetRange(ctx, key, offset, length)
	if err != nil {
		return nil, err
	}
	if err := h.after(ctx, OpGetRange, key); err != nil {
		rc.Close()
		return nil, err
	}
	return rc, nil
}

func (h *Hooked) Head(ctx context.Context, key string) (ObjectInfo, error) {
	if err := h.before(ctx, OpHead, key); err != nil {
		return ObjectInfo{}, err
	}
	info, err := h.b.Head(ctx, key)
	if err != nil {
		return ObjectInfo{}, err
	}
	if err := h.after(ctx, OpHead, key); err != nil {
		return ObjectInfo{}, err
	}
	return info, nil
}

func (h *Hooked) Delete(ctx context.Context, key string, expected ETag) error {
	if err := h.before(ctx, OpDelete, key); err != nil {
		return err
	}
	if err := h.b.Delete(ctx, key, expected); err != nil {
		return err
	}
	return h.after(ctx, OpDelete, key)
}

// List collects the whole listing before the after call, so a lost response
// loses all of it.
func (h *Hooked) List(ctx context.Context, prefix string) iter.Seq2[ObjectInfo, error] {
	return func(yield func(ObjectInfo, error) bool) {
		if err := h.before(ctx, OpList, prefix); err != nil {
			yield(ObjectInfo{}, err)
			return
		}
		var infos []ObjectInfo
		for info, err := range h.b.List(ctx, prefix) {
			if err != nil {
				yield(ObjectInfo{}, err)
				return
			}
			infos = append(infos, info)
		}
		if err := h.after(ctx, OpList, prefix); err != nil {
			yield(ObjectInfo{}, err)
			return
		}
		for _, info := range infos {
			if !yield(info, nil) {
				return
			}
		}
	}
}

var _ Backend = (*Hooked)(nil)
