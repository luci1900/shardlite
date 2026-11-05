package journal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/pos"
)

// Options tune a Store's retries. They match the blob store's, and the node
// sets them alike from its configuration.
type Options blob.Options

// Store is the one type every journal caller writes through. Its segments live
// in whatever Backend the node's journal does, blob storage included; the same
// write-id and retry handling applies to all of them.
//
// Like blob.Store, it tags each create with a write id (its writer's node,
// epoch and a counter) and, when a create's outcome is not known whether it
// landed, reads the number back to tell its own earlier attempt from somebody
// else's segment. Callers see success, blob.ErrExists (somebody else's segment
// holds the number: a fence, or a newer writer's), or an outcome that stayed
// unknown. A segment is created once and never modified, so a lost response
// costs a retry of the same body and id, never a fence.
type Store struct {
	b     Backend
	node  cluster.Node // whose writes went in with these write ids
	epoch pos.Epoch
	opts  Options
	n     atomic.Uint64
}

// NewStore wraps a backend. node and epoch go into every write id this store
// mints, so the writer they name is the node at that epoch.
func NewStore(b Backend, node cluster.Node, epoch pos.Epoch, opts Options) *Store {
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

// Name is the node whose journal this is, for error messages. It is not
// necessarily the writer the write ids name: an adopter writes into another
// node's journal.
func (s *Store) Name() cluster.Node { return s.b.Name() }

// Epoch is the epoch that goes into this store's write ids.
func (s *Store) Epoch() pos.Epoch { return s.epoch }

// NewWriteID returns an id for CreateWithID.
func (s *Store) NewWriteID() blob.WriteID {
	return blob.WriteID{Node: s.node, Epoch: s.epoch, N: s.n.Add(1)}
}

// maxBackoff caps the longest wait between attempts.
const maxBackoff = time.Second

// wait waits before a retry for a random time up to the attempt's backoff (full
// jitter, as AWS advises for SlowDown), so that callers that failed together
// don't retry together.
func (s *Store) wait(ctx context.Context, attempt int) error {
	d := maxBackoff
	if attempt < 30 && s.opts.Backoff < maxBackoff>>attempt {
		d = s.opts.Backoff << attempt
	}
	t := time.NewTimer(rand.N(d))
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Transient reports whether err is an outcome that retrying may fix.
func Transient(err error) bool { return blob.Transient(err) }

// settleCtx is the context for reading back after an unknown outcome: the
// caller's values, not its cancellation, and a time limit of its own.
func (s *Store) settleCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), s.opts.Settle)
}

// unknown is the error for an outcome nobody could learn, as blob.Store's is.
func unknown(cause error, why string) error {
	switch {
	case cause == nil:
		return fmt.Errorf("%w: %s", blob.ErrUnknown, why)
	case errors.Is(cause, context.DeadlineExceeded):
		return fmt.Errorf("%w: %s: %v: %w", blob.ErrUnknown, why, cause, context.DeadlineExceeded)
	case errors.Is(cause, context.Canceled):
		return fmt.Errorf("%w: %s: %v: %w", blob.ErrUnknown, why, cause, context.Canceled)
	}
	return fmt.Errorf("%w: %s: %v", blob.ErrUnknown, why, cause)
}

// create runs one create with blob.Store's retries: the same body and write id
// every attempt, and a read-back that settles an attempt of unknown outcome
// (which may still land: the outcome it reports is journal.ErrUnknown then).
func (s *Store) create(ctx context.Context, seq pos.Seg, body blob.Body, id blob.WriteID, ambiguous bool) error {
	var last error
	settle := func() (owned bool, err error) {
		sctx, cancel := s.settleCtx(ctx)
		defer cancel()
		info, err := s.b.Head(sctx, seq)
		if err != nil {
			return false, err
		}
		return info.WriteID == id, nil
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
				if owned, oerr := settle(); oerr == nil && owned {
					return nil
				}
				return unknown(err, "the context ended after an attempt of unknown outcome")
			}
		}
		err := s.b.Create(ctx, seq, io.NewSectionReader(body, 0, body.Size()), id)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, blob.ErrExists) && ambiguous:
			// An earlier attempt may have been ours. Its write id says.
			owned, oerr := settle()
			if oerr != nil {
				last = unknown(oerr, "reading back the segment "+seqName(seq))
				continue
			}
			if owned {
				return nil
			}
			return err
		case errors.Is(err, blob.ErrUnknown):
			ambiguous = true
			last = err
		case errors.Is(err, blob.ErrNotApplied):
			// After an attempt of unknown outcome, this one not happening says
			// nothing about that one: the outcome stays unknown.
			if !ambiguous {
				last = err
			}
		default:
			return err
		}
	}
	// Out of attempts. If any attempt's outcome is unknown, look once more, and
	// otherwise it stays unknown, whatever the later attempts said.
	if ambiguous {
		if owned, err := settle(); err == nil && owned {
			return nil
		}
		if !errors.Is(last, blob.ErrUnknown) {
			last = unknown(nil, "an earlier attempt's outcome is unknown")
		}
	}
	return last
}

// Create writes a segment once. blob.ErrExists means somebody else's is under
// the number (a fence this caller didn't write, or another writer's segment):
// for a write that may be retried, CreateWithID.
func (s *Store) Create(ctx context.Context, seq pos.Seg, body blob.Body) error {
	return s.create(ctx, seq, body, s.NewWriteID(), false)
}

// CreateWithID is Create for a caller that keeps retrying one write after it
// has given up: every call is a retry of the write named by id (from
// NewWriteID), so "exists" is checked against that id whatever happened before.
// The body must be the same each time.
func (s *Store) CreateWithID(ctx context.Context, seq pos.Seg, body blob.Body, id blob.WriteID) error {
	return s.create(ctx, seq, body, id, true)
}

// seqName names a segment in an error, the way the blob key would but
// without the zeros: n1/129.
func seqName(seq pos.Seg) string { return fmt.Sprintf("%d", uint64(seq)) }

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

// Get returns the whole segment, read back.
func (s *Store) Get(ctx context.Context, seq pos.Seg) (io.ReadCloser, error) {
	return read(ctx, s, func() (io.ReadCloser, error) { return s.b.Get(ctx, seq) })
}

// GetRange reads a range of the segment, read back. A negative offset asks
// for the last -offset bytes, and takes no length.
func (s *Store) GetRange(ctx context.Context, seq pos.Seg, offset, length int64) (io.ReadCloser, error) {
	if offset < 0 && length != 0 {
		return nil, fmt.Errorf("journal: a range from the end takes no length (got %d)", length)
	}
	return read(ctx, s, func() (io.ReadCloser, error) { return s.b.GetRange(ctx, seq, offset, length) })
}

// Head says the segment's size (0 is a fence) and write id.
func (s *Store) Head(ctx context.Context, seq pos.Seg) (SegmentInfo, error) {
	return read(ctx, s, func() (SegmentInfo, error) { return s.b.Head(ctx, seq) })
}

// Delete removes a segment. A missing one is done, and an attempt of unknown
// outcome is retried: deleting twice is deleting once.
func (s *Store) Delete(ctx context.Context, seq pos.Seg) error {
	var last error
	for attempt := 0; attempt < s.opts.Attempts; attempt++ {
		if attempt > 0 {
			if err := s.wait(ctx, attempt-1); err != nil {
				if last != nil {
					return fmt.Errorf("%w: %w", last, err)
				}
				return err
			}
		}
		err := s.b.Delete(ctx, seq)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, blob.ErrNotFound):
			return nil
		case errors.Is(err, blob.ErrUnknown):
			// It may have been applied. A repeated delete of a segment that is
			// still there deletes it; one that is gone is ErrNotFound, done.
			last = err
		case errors.Is(err, blob.ErrNotApplied):
			if last == nil {
				last = err
			}
		default:
			return err
		}
	}
	return last
}

// List returns the journal's segment numbers from from (inclusive) on, in
// number order. A failure while listing is not retried mid-way (the caller
// sees it); one before the first number is.
func (s *Store) List(ctx context.Context, from pos.Seg) iter.Seq2[pos.Seg, error] {
	if from < 1 {
		from = 1
	}
	return func(yield func(pos.Seg, error) bool) {
		for attempt := 0; ; attempt++ {
			started := false
			failed := false
			for seq, err := range s.b.List(ctx, from) {
				if err != nil && !started && Transient(err) && attempt+1 < s.opts.Attempts {
					failed = true
					break
				}
				started = true
				if !yield(seq, err) {
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
				yield(0, err)
				return
			}
		}
	}
}
