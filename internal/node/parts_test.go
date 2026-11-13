package node

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/syncs/service"
	"github.com/luci1900/shardlite/internal/test/harness"
)

// Tests of the parts of the shardset on their own, each with what it depends on
// passed in: a node on the in-memory store, and no shardset.

// serveJournal serves n's writer and keeper, as Run's journal stage does, until
// stop is called or the test ends.
func serveJournal(t *testing.T, n *Node) (stop func()) {
	stop = service.Background(slog.Default(), n.Writer, n.Keeper, n.Confirmer)
	t.Cleanup(stop)
	return stop
}

// The watchdog's period is on the Clock: it looks at the shards only when the
// clock moves.
func TestWatchdogRunsOnTheClock(t *testing.T) {
	t.Parallel()
	clk := harness.NewFakeClock()
	st := &Stats{}
	tab := newTable(slog.New(slog.DiscardHandler))
	w := &watchdog{tab: tab, clock: clk, every: time.Second, alertAge: time.Minute, log: slog.New(slog.DiscardHandler), stats: st}
	ctx, cancel := context.WithCancel(context.Background())
	var g errgroup.Group
	g.Go(func() error { w.Serve(ctx); return nil })
	defer func() { cancel(); g.Wait() }()
	time.Sleep(50 * time.Millisecond)
	if n := st.Watchdog.Load(); n != 0 {
		t.Fatalf("%d passes with the clock standing still", n)
	}
	advanceUntil(t, clk, time.Second, "a pass of the watchdog", func() bool { return st.Watchdog.Load() >= 1 })
}
