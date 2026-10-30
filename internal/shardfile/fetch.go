package shardfile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/segment"
)

// fetchTimeout is how long a fetch waits for the peer before it gives up on
// what it holds: a peer that does not answer within it counts as dead, and
// the adoption carries on, the window it held lost (the ensemble's documented
// bound, docs/guide.md, Durability). The right value is still open.
const fetchTimeout = 5 * time.Second

// FetchTail copies into the record's journal what the node's peer holds
// above the run the record landed, before the fence goes above it (docs/
// internals.md, Takeover). The peer holds segments the record never landed,
// and a fence written first would strand them below it, unreachable though
// the cluster still holds them. The copy is a key-to-key copy between two
// stores with the same layout, segment.Land's checked path, and it reuses the
// write id the peer stored, so the record sees the owner's own attempt and
// its lost-response machinery treats it as one.
//
// The run lands in order from the first number above the record's landed run,
// and stops at the first number the peer does not hold: a gap in the record below a
// landed segment would fail the adoption after it, and the commits past a gap
// are lost whatever the fence does. A number the record already holds is
// skipped, so a fetch repeated after a half-done one costs heads and copies
// nothing twice. The fence then goes above what landed, which is why the
// caller runs this before ReadJournal.
//
// Errors come back for the caller to log and carry on: the fetch reports what
// it could not learn, and the adoption's own steps decide what that cost.
func (f *Files) FetchTail(ctx context.Context, node cluster.Node, from, to pos.Seg, peer *journal.Store, logger *slog.Logger) (int, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if peer == nil || to < 1 {
		return 0, nil
	}
	from = max(from, 1)
	record := f.Journal(node)
	// Where the record's landed run ends: the highest number it holds below
	// the fence's range. The record's numbers are contiguous up to it, so a
	// number at or below it needs no copy.
	landed := from - 1
	for seq, err := range record.List(ctx, from) {
		if err != nil {
			return 0, fmt.Errorf("fetch %s: listing the record: %w", node, err)
		}
		if seq >= to {
			break
		}
		landed = max(landed, seq)
	}
	// The peer answers within the timeout or counts as dead.
	mctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	copied := 0
	for seq := landed + 1; seq < to; seq++ {
		id, err := peer.Head(mctx, seq)
		if err != nil {
			if copied > 0 {
				logger.Warn("the peer's run ends before the dead writer's range, what is past the gap is lost",
					"node", node, "segment", seq, "err", err)
			}
			if errors.Is(err, blob.ErrNotFound) {
				break // the run ends here
			}
			return copied, fmt.Errorf("fetch %s: asking the peer for %d: %w", node, seq, err)
		}
		if id.Size == 0 {
			// The peer never holds a fence (the fence is created in the
			// record), so an empty segment is a peer gone wrong: the run
			// ends, and nothing of any shard's history is in it.
			logger.Error("the peer holds an empty segment, the fetch stops in front of it", "node", node, "segment", seq)
			break
		}
		if err := segment.Land(mctx, record, peer, seq, id.WriteID, f.Temp); err != nil {
			return copied, fmt.Errorf("fetch %s: landing %d: %w", node, seq, err)
		}
		copied++
	}
	if copied > 0 {
		logger.Info("fetched the peer's tail into the record", "node", node, "segments", copied, "from", landed+1)
	}
	return copied, nil
}
