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

// fetchTimeout is how long a fetch waits for the mirrors before it gives up on
// what they hold: a mirror that does not answer within it counts as dead, and
// the adoption carries on, the window it held lost (the ensemble's documented
// bound, docs/guide.md, Durability). The right value is still open.
const fetchTimeout = 5 * time.Second

// FetchTail copies into the record's journal what the node's mirrors hold
// above the run the record landed, before the fence goes above it (docs/
// internals.md, Takeover). A mirror holds segments the record never landed,
// and a fence written first would strand them below it, unreachable though
// the cluster still holds them. The copy is a key-to-key copy between two
// stores with the same layout, segment.Land's checked path, and it reuses the
// write id the mirror stored, so the record sees the owner's own attempt and
// its lost-response machinery treats it as one.
//
// The run lands in order from the first number above the record's landed run,
// and stops at the first number no mirror holds: a gap in the record below a
// landed segment would fail the adoption after it, and the commits past a gap
// are lost whatever the fence does. A number the record already holds is
// skipped, so a fetch repeated after a half-done one costs heads and copies
// nothing twice. The fence then goes above what landed, which is why the
// caller runs this before ReadJournal.
//
// Errors come back for the caller to log and carry on: the fetch reports what
// it could not learn, and the adoption's own steps decide what that cost.
func (f *Files) FetchTail(ctx context.Context, node cluster.Node, from, to pos.Seg, mirrors []*journal.Store, logger *slog.Logger) (int, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if len(mirrors) == 0 || to < 1 {
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
	// The mirrors answer within the timeout or count as dead.
	mctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	copied := 0
	for seq := landed + 1; seq < to; seq++ {
		src, id, err := holder(mctx, mirrors, seq)
		if err != nil {
			return copied, fmt.Errorf("fetch %s: asking the mirrors for %d: %w", node, seq, err)
		}
		if src == nil {
			if copied > 0 {
				logger.Warn("the mirrors' run ends before the dead writer's range, what is past the gap is lost",
					"node", node, "segment", seq)
			}
			break
		}
		if id.Size == 0 {
			// A mirror never holds a fence (the fence is created in the
			// record), so an empty segment is a mirror gone wrong: the run
			// ends, and nothing of any shard's history is in it.
			logger.Error("a mirror holds an empty segment, the fetch stops in front of it", "node", node, "segment", seq)
			break
		}
		if err := segment.Land(mctx, record, src, seq, id.WriteID, f.TempDir, f.spoolLimit()); err != nil {
			return copied, fmt.Errorf("fetch %s: landing %d: %w", node, seq, err)
		}
		copied++
	}
	if copied > 0 {
		logger.Info("fetched the mirrors' tail into the record", "node", node, "segments", copied, "from", landed+1)
	}
	return copied, nil
}

// holder asks each mirror in turn which of them holds seq, and returns the
// first that does, with its head (nil, zero, nil when none does). A mirror
// that fails to answer is not holding it: the caller's timeout has already
// bounded the wait, and the error says what was lost.
func holder(ctx context.Context, mirrors []*journal.Store, seq pos.Seg) (*journal.Store, journal.SegmentInfo, error) {
	var last error
	for _, m := range mirrors {
		info, err := m.Head(ctx, seq)
		switch {
		case err == nil:
			return m, info, nil
		case errors.Is(err, blob.ErrNotFound):
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			return nil, journal.SegmentInfo{}, err
		default:
			last = err
		}
	}
	return nil, journal.SegmentInfo{}, last
}
