package blob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"sync/atomic"
	"time"
)

// Options tune a Store's retries.
type Options struct {
	Attempts int           // attempts per call, 4 if zero
	Backoff  time.Duration // wait before the first retry, doubling each time; 25 ms if zero
}

// Store is the one type every caller writes through. It tags each write with
// a write id (its node, epoch and a counter), retries failures that may have
// been transient, and after an unknown outcome reads the object's write id
// back to tell its own earlier attempt from somebody else's write.
//
// Callers therefore see success, a conflict with someone else, or an error
// that persisted through every attempt.
type Store struct {
	b     Backend
	node  string
	epoch uint64
	opts  Options
	n     atomic.Uint64
}

// NewStore wraps a backend. node and epoch go into every write id.
func NewStore(b Backend, node string, epoch uint64, opts Options) *Store {
	if opts.Attempts <= 0 {
		opts.Attempts = 4
	}
	if opts.Backoff <= 0 {
		opts.Backoff = 25 * time.Millisecond
	}
	return &Store{b: b, node: node, epoch: epoch, opts: opts}
}

// Epoch is the epoch that goes into this store's write ids.
func (s *Store) Epoch() uint64 { return s.epoch }

// NewWriteID returns an id for CreateWithID.
func (s *Store) NewWriteID() WriteID {
	return WriteID{Node: s.node, Epoch: s.epoch, N: s.n.Add(1)}
}

func (s *Store) wait(ctx context.Context, attempt int) error {
	d := s.opts.Backoff << attempt
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Transient reports whether err is an outcome that retrying may fix: the
// request certainly did nothing (ErrNotApplied) or may have done something
// (ErrUnknown). Any other failure (a bad key, no permission) won't go away.
func Transient(err error) bool { return retryable(err) }

func retryable(err error) bool { return errors.Is(err, ErrNotApplied) || errors.Is(err, ErrUnknown) }

// write runs a Create or Update with retries. try makes one attempt with the
// write id. conflict is the outcome that means "somebody got there first".
// ambiguous says an earlier call with the same id already had an unknown outcome.
func (s *Store) write(ctx context.Context, key string, conflict error, id WriteID, ambiguous bool, try func(id WriteID) (ETag, error)) (ETag, error) {
	var last error
	for attempt := 0; attempt < s.opts.Attempts; attempt++ {
		if attempt > 0 {
			if err := s.wait(ctx, attempt-1); err != nil {
				return "", err
			}
		}
		etag, err := try(id)
		switch {
		case err == nil:
			return etag, nil
		case errors.Is(err, conflict) && ambiguous:
			// An earlier attempt may have been ours. Its write id says.
			etag, owned, oerr := s.ownedBy(ctx, key, id)
			if oerr != nil {
				// Couldn't read it back: still unknown, not somebody else's.
				last = fmt.Errorf("%w: reading back %s: %v", ErrUnknown, key, oerr)
				continue
			}
			if owned {
				return etag, nil
			}
			return "", err
		case errors.Is(err, ErrUnknown):
			ambiguous = true
			last = err
		case errors.Is(err, ErrNotApplied):
			last = err
		default:
			return "", err
		}
	}
	// Out of attempts. If the last word was "unknown", look once more.
	if ambiguous {
		if etag, owned, err := s.ownedBy(ctx, key, id); err == nil && owned {
			return etag, nil
		}
	}
	return "", last
}

// ownedBy reports whether the object at key was written with id. An error
// means it couldn't be read, which says nothing about who wrote it.
func (s *Store) ownedBy(ctx context.Context, key string, id WriteID) (ETag, bool, error) {
	info, err := s.Head(ctx, key)
	if err != nil {
		return "", false, err
	}
	return info.ETag, info.WriteID == id, nil
}

// Create writes the object only if the key doesn't exist. ErrExists means
// somebody else's object is there.
func (s *Store) Create(ctx context.Context, key string, body io.Reader) (ETag, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	return s.write(ctx, key, ErrExists, s.NewWriteID(), false, func(id WriteID) (ETag, error) {
		return s.b.Create(ctx, key, bytes.NewReader(data), id)
	})
}

// CreateWithID is Create for a caller that keeps retrying one write after
// Create has given up: every call is a retry of the write named by id (from
// NewWriteID), so "exists" is checked against that id whatever happened
// before. The body must be the same each time.
func (s *Store) CreateWithID(ctx context.Context, key string, body io.Reader, id WriteID) (ETag, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	return s.write(ctx, key, ErrExists, id, true, func(id WriteID) (ETag, error) {
		return s.b.Create(ctx, key, bytes.NewReader(data), id)
	})
}

// Update replaces the object only if its ETag is expected. ErrConflict means
// somebody else changed it.
func (s *Store) Update(ctx context.Context, key string, body io.Reader, expected ETag) (ETag, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	return s.write(ctx, key, ErrConflict, s.NewWriteID(), false, func(id WriteID) (ETag, error) {
		return s.b.Update(ctx, key, bytes.NewReader(data), expected, id)
	})
}

// Delete removes the object. With expected set it is a conditional delete
// (ErrConflict if somebody changed the object); without it, whatever the version.
// Finding nothing counts as done only after an earlier attempt had an unknown
// outcome (it may have been ours). Otherwise the object was never there, or
// somebody else deleted it, and a backend that tells (the local one does, S3
// doesn't, unconditionally) gives ErrNotFound, which callers accept as done.
func (s *Store) Delete(ctx context.Context, key string, expected ETag) error {
	var last error
	ambiguous := false
	for attempt := 0; attempt < s.opts.Attempts; attempt++ {
		if attempt > 0 {
			if err := s.wait(ctx, attempt-1); err != nil {
				return err
			}
		}
		err := s.b.Delete(ctx, key, expected)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, ErrNotFound) && ambiguous:
			return nil
		case retryable(err):
			ambiguous = ambiguous || errors.Is(err, ErrUnknown)
			last = err
		default:
			return err
		}
	}
	return last
}

// read retries a read-only call.
func read[T any](ctx context.Context, s *Store, call func() (T, error)) (T, error) {
	var zero T
	var last error
	for attempt := 0; attempt < s.opts.Attempts; attempt++ {
		if attempt > 0 {
			if err := s.wait(ctx, attempt-1); err != nil {
				return zero, err
			}
		}
		v, err := call()
		if err == nil {
			return v, nil
		}
		if !retryable(err) {
			return zero, err
		}
		last = err
	}
	return zero, last
}

func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, ETag, error) {
	type r struct {
		rc   io.ReadCloser
		etag ETag
	}
	v, err := read(ctx, s, func() (r, error) {
		rc, etag, err := s.b.Get(ctx, key)
		return r{rc, etag}, err
	})
	return v.rc, v.etag, err
}

func (s *Store) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	return read(ctx, s, func() (io.ReadCloser, error) { return s.b.GetRange(ctx, key, offset, length) })
}

func (s *Store) Head(ctx context.Context, key string) (ObjectInfo, error) {
	return read(ctx, s, func() (ObjectInfo, error) { return s.b.Head(ctx, key) })
}

// List returns objects by key prefix. A failure while listing is not retried
// mid-way (the caller sees it); one before the first result is.
func (s *Store) List(ctx context.Context, prefix string) iter.Seq2[ObjectInfo, error] {
	return func(yield func(ObjectInfo, error) bool) {
		for attempt := 0; ; attempt++ {
			started := false
			failed := false
			for info, err := range s.b.List(ctx, prefix) {
				if err != nil && !started && retryable(err) && attempt+1 < s.opts.Attempts {
					failed = true
					break
				}
				started = true
				if !yield(info, err) {
					return
				}
				if err != nil {
					return
				}
			}
			if !failed {
				return
			}
			if err := s.wait(ctx, attempt); err != nil {
				yield(ObjectInfo{}, fmt.Errorf("list %s: %w", prefix, err))
				return
			}
		}
	}
}
