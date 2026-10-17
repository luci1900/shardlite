package blob

import (
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
	// Settle bounds the read-back that tells whether a write of unknown outcome
	// landed. It runs on a context detached from the caller's, so a caller that
	// has given up still learns. 5 s if zero.
	Settle time.Duration
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
	if opts.Settle <= 0 {
		opts.Settle = 5 * time.Second
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
func Transient(err error) bool { return errors.Is(err, ErrNotApplied) || errors.Is(err, ErrUnknown) }

// settleCtx is the context for reading back after an unknown outcome: the
// caller's values, not its cancellation, and a time limit of its own.
func (s *Store) settleCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), s.opts.Settle)
}

// unknown is the error for an outcome nobody could learn: ErrUnknown, with the
// reason (and the caller's context error, if that is why it stopped).
//
// The cause is only text: a read-back that failed with ErrNotApplied must not
// make the outcome look like nothing happened. A context error in it stays
// findable, as for every error the store returns. (The rest of its chain,
// the SDK's error included, is left out for the same reason.)
func unknown(cause error, why string) error {
	switch {
	case cause == nil:
		return fmt.Errorf("%w: %s", ErrUnknown, why)
	case errors.Is(cause, context.DeadlineExceeded):
		return fmt.Errorf("%w: %s: %v: %w", ErrUnknown, why, cause, context.DeadlineExceeded)
	case errors.Is(cause, context.Canceled):
		return fmt.Errorf("%w: %s: %v: %w", ErrUnknown, why, cause, context.Canceled)
	}
	return fmt.Errorf("%w: %s: %v", ErrUnknown, why, cause)
}

// write runs a Create or Update with retries. try makes one attempt with the
// write id. conflict is the outcome that means "somebody got there first".
// ambiguous says an earlier call with the same id already had an unknown outcome.
//
// An unknown outcome is never lost: if the caller's context ends first, the
// store still reads back (on a context of its own) and returns success if the
// write landed, and otherwise ErrUnknown together with the context's error.
func (s *Store) write(ctx context.Context, key string, conflict error, id WriteID, ambiguous bool, try func(id WriteID) (ETag, error)) (ETag, error) {
	var last error
	// settle reads the object back on its own context: whether it carries our id.
	settle := func() (etag ETag, owned bool, err error) {
		sctx, cancel := s.settleCtx(ctx)
		defer cancel()
		return s.ownedBy(sctx, key, id)
	}
	for attempt := 0; attempt < s.opts.Attempts; attempt++ {
		if attempt > 0 {
			if err := s.wait(ctx, attempt-1); err != nil {
				if !ambiguous {
					if last != nil {
						return "", fmt.Errorf("%w: %w", last, err)
					}
					return "", err
				}
				if etag, owned, oerr := settle(); oerr == nil && owned {
					return etag, nil
				}
				return "", unknown(err, "the context ended after an attempt of unknown outcome")
			}
		}
		etag, err := try(id)
		switch {
		case err == nil:
			return etag, nil
		case errors.Is(err, conflict) && ambiguous:
			// An earlier attempt may have been ours. Its write id says.
			etag, owned, oerr := settle()
			if oerr != nil {
				// Couldn't read it back: still unknown, not somebody else's.
				last = unknown(oerr, "reading back "+key)
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
			// After an attempt of unknown outcome, this one not happening says
			// nothing about that one: the outcome stays unknown.
			if !ambiguous {
				last = err
			}
		default:
			return "", err
		}
	}
	// Out of attempts. If any attempt's outcome is unknown, look once more, and
	// otherwise it stays unknown, whatever the later attempts said.
	if ambiguous {
		if etag, owned, err := settle(); err == nil && owned {
			return etag, nil
		}
		if !errors.Is(last, ErrUnknown) {
			last = unknown(nil, "an earlier attempt's outcome is unknown")
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
//
// The whole body is sent from its start on every attempt, through a reader of
// that attempt's own, so a retry never needs the bytes in memory: a section of
// a file, or a reader over a small slice, are both fine.
func (s *Store) Create(ctx context.Context, key string, body Body) (ETag, error) {
	return s.write(ctx, key, ErrExists, s.NewWriteID(), false, attempts(body, func(b io.ReadSeeker, id WriteID) (ETag, error) {
		return s.b.Create(ctx, key, b, id)
	}))
}

// CreateWithID is Create for a caller that keeps retrying one write after
// Create has given up: every call is a retry of the write named by id (from
// NewWriteID), so "exists" is checked against that id whatever happened
// before. The body must be the same each time.
func (s *Store) CreateWithID(ctx context.Context, key string, body Body, id WriteID) (ETag, error) {
	return s.write(ctx, key, ErrExists, id, true, attempts(body, func(b io.ReadSeeker, id WriteID) (ETag, error) {
		return s.b.Create(ctx, key, b, id)
	}))
}

// Update replaces the object only if its ETag is expected. ErrConflict means
// somebody else changed it.
func (s *Store) Update(ctx context.Context, key string, body Body, expected ETag) (ETag, error) {
	return s.write(ctx, key, ErrConflict, s.NewWriteID(), false, attempts(body, func(b io.ReadSeeker, id WriteID) (ETag, error) {
		return s.b.Update(ctx, key, b, expected, id)
	}))
}

// attempts makes the single attempt a write retries. Each attempt reads the
// body through a reader of its own, from the start: the HTTP transport may
// still be reading an earlier attempt's body after that attempt has returned
// (when the server answered early, say), so attempts can't share an offset.
func attempts(body Body, try func(io.ReadSeeker, WriteID) (ETag, error)) func(WriteID) (ETag, error) {
	return func(id WriteID) (ETag, error) {
		return try(io.NewSectionReader(body, 0, body.Size()), id)
	}
}

// Delete removes the object. With expected set it is a conditional delete
// (ErrConflict if somebody changed the object); without it, whatever the version.
// Finding nothing counts as done only after an earlier attempt had an unknown
// outcome (it may have been ours). Otherwise the object was never there, or
// somebody else deleted it, and a backend that tells (the local one does, S3
// doesn't, unconditionally) gives ErrNotFound, which callers accept as done.
// The same goes for a conflict: some stores answer 412 to a conditional delete
// of a missing key, so after an unknown outcome the object itself says whether
// the delete landed.
func (s *Store) Delete(ctx context.Context, key string, expected ETag) error {
	var last error
	ambiguous := false
	// settle looks at the object on a context of its own after an unknown outcome:
	// gone means done, and an object that is still the expected one means the
	// delete did not happen (yet), or was refused if cause is the conflict.
	settle := func(cause error) error {
		sctx, cancel := s.settleCtx(ctx)
		defer cancel()
		info, err := s.Head(sctx, key)
		switch {
		case errors.Is(err, ErrNotFound):
			return nil
		case err != nil:
			return unknown(errors.Join(err, cause), "reading back "+key)
		case errors.Is(cause, ErrConflict):
			return cause // still there: somebody changed it, as the store said
		case expected != "" && info.ETag != expected:
			return ErrConflict // changed since: the delete can no longer apply
		}
		return unknown(cause, "the delete may still land")
	}
	for attempt := 0; attempt < s.opts.Attempts; attempt++ {
		if attempt > 0 {
			if err := s.wait(ctx, attempt-1); err != nil {
				if !ambiguous {
					if last != nil {
						return fmt.Errorf("%w: %w", last, err)
					}
					return err
				}
				return settle(err)
			}
		}
		err := s.b.Delete(ctx, key, expected)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, ErrNotFound) && ambiguous:
			return nil
		case errors.Is(err, ErrConflict) && ambiguous:
			// An earlier attempt may have landed, and some stores answer a
			// conditional delete of a missing key with a failed precondition.
			return settle(err)
		case Transient(err):
			// After an attempt of unknown outcome, one that didn't happen says
			// nothing about it: the earlier error stays the one to report.
			if !ambiguous || errors.Is(err, ErrUnknown) {
				last = err
			}
			ambiguous = ambiguous || errors.Is(err, ErrUnknown)
		default:
			return err
		}
	}
	if ambiguous {
		return settle(last)
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
		if !Transient(err) {
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
				if err != nil && !started && Transient(err) && attempt+1 < s.opts.Attempts {
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
