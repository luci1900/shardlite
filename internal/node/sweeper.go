package node

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/supervisor"
)

// maxSweepEvery is the longest wait between two sweeps.
const maxSweepEvery = 10 * time.Minute

// sweeper is the node's own journal sweeper (docs/internals.md, Cleaning the
// journal): every tenth of age (at most every ten minutes) until ctx ends it
// lists journal/{node}/ and deletes the non-empty segments last modified more
// than age ago. Empty segments (fences) are never deleted, since a zombie
// paused for longer than age could otherwise write into a freed number. It
// works through the blob store's List and Delete alone, so it is the same on
// every backend.
//
// Segments are never rewritten, so a listing that says a segment is old is
// enough: there is nothing to look at again before deleting it.
//
// It starts only once Run does, so the node's old segments have been adopted
// before any of them can expire. age must be positive. Run runs it as one of the
// node's services.
type sweeper struct {
	store *blob.Store
	node  string
	age   time.Duration
	log   *slog.Logger
}

func (*sweeper) String() string { return "journal sweeper" }

func (s *sweeper) Serve(ctx context.Context) error {
	defer serving(s.log, s.String())()
	t := time.NewTicker(min(s.age/10, maxSweepEvery))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return supervisor.NoRestart(nil)
		case <-t.C:
			if _, err := s.sweepJournal(ctx, time.Now()); err != nil && ctx.Err() == nil {
				s.log.Warn("sweeping the node journal failed, trying again later", "err", err)
			}
		}
	}
}

// sweepJournal deletes the node's non-empty segments that are older than the age at
// now, and returns how many it deleted. It stops at the first failure: the next
// sweep starts again.
func (s *sweeper) sweepJournal(ctx context.Context, now time.Time) (deleted int, err error) {
	var old []string
	for info, err := range s.store.List(ctx, segment.Prefix(s.node)) {
		if err != nil {
			return deleted, err
		}
		if info.Size > 0 && now.Sub(info.LastModified) > s.age {
			old = append(old, info.Key)
		}
	}
	for _, key := range old {
		if err := s.store.Delete(ctx, key, ""); err != nil && !errors.Is(err, blob.ErrNotFound) {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}
