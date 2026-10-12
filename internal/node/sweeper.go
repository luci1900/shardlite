package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/segment"
)

// maxSweepEvery is the longest wait between two sweeps.
const maxSweepEvery = 10 * time.Minute

// StartJournalSweeper starts the node's own journal sweeper (design.md, Cleaning
// the journal): every tenth of age (at most every ten minutes) it lists
// journal/{node}/ and deletes the non-empty segments last modified more than age
// ago. Empty segments (fences) are never deleted, since a zombie paused for
// longer than age could otherwise write into a freed number. It works through
// the blob store's List and Delete alone, so it is the same on every backend.
//
// Segments are never rewritten, so a listing that says a segment is old is
// enough: there is nothing to look at again before deleting it.
//
// Start it after Start has returned, so the node's old segments have been
// adopted before any of them can expire. It runs until Close, which joins it.
// It can be started once.
func (n *Node) StartJournalSweeper(age time.Duration) error {
	if age <= 0 {
		return fmt.Errorf("node: journal expiry is %v, want a positive duration", age)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sweepCancel != nil {
		return fmt.Errorf("node: the journal sweeper is already running")
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.sweepCancel = cancel
	every := min(age/10, maxSweepEvery)
	n.sweepGroup.Go(func() error {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				if _, err := n.sweepJournal(ctx, age, time.Now()); err != nil && ctx.Err() == nil {
					n.Logger.Warn("sweeping the node journal failed, trying again later", "err", err)
				}
			}
		}
	})
	return nil
}

// sweepJournal deletes the node's non-empty segments that are older than age at
// now, and returns how many it deleted. It stops at the first failure: the next
// sweep starts again.
func (n *Node) sweepJournal(ctx context.Context, age time.Duration, now time.Time) (deleted int, err error) {
	var old []string
	for info, err := range n.Store.List(ctx, segment.Prefix(n.Name)) {
		if err != nil {
			return deleted, err
		}
		if info.Size > 0 && now.Sub(info.LastModified) > age {
			old = append(old, info.Key)
		}
	}
	for _, key := range old {
		if err := n.Store.Delete(ctx, key, ""); err != nil && !errors.Is(err, blob.ErrNotFound) {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}
