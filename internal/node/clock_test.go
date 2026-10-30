package node

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/kind"
)

// feedClock gives the watch one sample a second from t0+from for n seconds,
// whose store's time is the node's plus extra: a store whose clock steps shows
// as a change of extra.
func feedClock(w *coord.ClockWatch, t0 time.Time, from, n int, extra time.Duration) {
	for i := from; i < from+n; i++ {
		start := t0.Add(time.Duration(i) * time.Second)
		w.Sink(blob.ClockSample{Store: start.Add(extra).Truncate(time.Second), Start: start, End: start.Add(10 * time.Millisecond)}, nil)
	}
}

// While the watch reports that the clocks disagree, `none` and `peer` answers fail
// with a retry hint and `storage` answers do not, and they recover with the watch.
func TestClockDisagreementFailsNoneAndPeer(t *testing.T) {
	t.Parallel()
	r := newUnstartedMRig(t, nil)
	r.clock = coord.NewClockWatch(nil)
	r.startNode()
	r.startShards(nil)
	none, peer, storage := r.acquire("test.slow/1"), r.acquire("test.vault/1"), r.acquire("test.wallet/1")
	defer none.Release()
	defer peer.Release()
	defer storage.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t0 := time.Now()
	feedClock(r.clock, t0, 0, 10, 0)
	if err := r.m.Gate(ctx, none, kind.SyncNone); err != nil {
		t.Fatalf("a steady clock failed a none answer: %v", err)
	}

	feedClock(r.clock, t0, 10, 2, 30*time.Second) // the store's clock steps
	for _, tier := range []kind.Sync{kind.SyncNone, kind.SyncPeer} {
		var un *unavailable
		err := r.m.Gate(ctx, none, tier)
		if !errors.As(err, &un) || un.retry <= 0 || !strings.Contains(err.Error(), "clock") {
			t.Fatalf("a %v answer under a disagreeing clock: %v, want a 503 with a retry hint that names the clock", tier, err)
		}
	}
	if err := r.m.Gate(ctx, storage, kind.SyncStorage); err != nil {
		t.Fatalf("a storage answer failed on the clock: %v", err)
	}

	feedClock(r.clock, t0, 12, 8, 30*time.Second) // steady at the new offset for a while
	if err := r.m.Gate(ctx, none, kind.SyncNone); err != nil {
		t.Fatalf("a none answer did not recover: %v", err)
	}
	// (A shard with nothing new to commit needs no peer.)
	if err := r.m.Gate(ctx, peer, kind.SyncPeer); err != nil {
		t.Fatalf("a peer answer did not recover: %v", err)
	}
}
