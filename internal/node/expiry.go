package node

import (
	"context"
	"log/slog"
	"time"

	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/pos"
)

// maxExpireEvery is the longest wait between two passes.
const maxExpireEvery = 10 * time.Minute

// expiry is the node's own journal expiry (docs/internals.md, Cleaning the
// journal): every tenth of age (at most every ten minutes) until ctx ends it
// lists the journal and deletes the non-empty segments written more than age
// ago. Empty segments (fences) are never deleted, since a zombie paused for
// longer than age could otherwise write into a freed number. It works through
// the journal's List and Delete alone, so it is the same on every journal
// backend.
//
// Segments are never rewritten, so a listing that says a segment is old is
// enough: there is nothing to look at again before deleting it.
//
// It starts only once Run does, so the node's old segments have been adopted
// before any of them can expire. age must be positive. Run runs it as one of the
// node's services.
//
// An adopter sweeps a dead node's journal with it too, once (expire), with
// an age of zero (every non-empty segment) and below set to its fence: segments from the fence on may be
// the node's next life's, written since the adoption finished.
type expiry struct {
	journal journal.Store
	age     time.Duration
	below pos.Seg // only segments numbered below this, if set
	log   *slog.Logger
}

func (*expiry) String() string { return "journal expiry" }

func (s *expiry) Serve(ctx context.Context) error {
	defer serving(s.log, s.String())()
	t := time.NewTicker(min(s.age/10, maxExpireEvery))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if _, err := s.expire(ctx, time.Now()); err != nil && ctx.Err() == nil {
				s.log.Warn("expiring the node journal failed, trying again later", "err", err)
			}
		}
	}
}

// expire deletes the node's non-empty segments that are older than the age at
// now, and returns how many it deleted. It stops at the first failure: the next
// pass starts again.
func (s *expiry) expire(ctx context.Context, now time.Time) (deleted int, err error) {
	var old []pos.Seg
	for seg, err := range s.journal.List(ctx, 1) {
		if err != nil {
			return deleted, err
		}
		if s.below > 0 && seg.Seq >= s.below {
			break // the listing is in number order
		}
		if seg.Size > 0 && (s.age == 0 || now.Sub(seg.Written) > s.age) {
			old = append(old, seg.Seq)
		}
	}
	for _, seq := range old {
		if err := s.journal.Delete(ctx, seq); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}
