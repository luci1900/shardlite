package node

import (
	"context"
	"log/slog"
	"time"
)

// tailAlertAge is how old a tail gets before it raises the alert: three times
// the age at which a delta is written, so a healthy shard never raises it.
func tailAlertAge(deltaAge time.Duration) time.Duration { return 3 * deltaAge }

// JournalExpiry is how long the node keeps its journal segments before its
// journal expiry deletes them: 24 hours, or four times the tail alert age if that is
// longer, so the alert comes in time to act.
func JournalExpiry(deltaAge time.Duration) time.Duration {
	return max(24*time.Hour, 4*tailAlertAge(deltaAge))
}

// tailAlertEvery is how often a shard repeats the tail-age alert.
const tailAlertEvery = time.Minute

// watchdog is the part of the Catalog that raises the tail-age alert, on its own
// timer, apart from the shards' loops, since a shard whose delta keeps failing
// (or hangs, holding its loop) must still raise it. Its period is on the Clock,
// and the ages it compares are real time.
type watchdog struct {
	tab      *table
	clock    Clock
	every    time.Duration
	alertAge time.Duration
	log      *slog.Logger
	stats    *Stats
}

func (w *watchdog) String() string { return "watchdog" }

// Serve is the watchdog's loop, as a service.
func (w *watchdog) Serve(ctx context.Context) error {
	tick, stop := w.clock.NewTicker(w.every)
	defer stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick:
		}
		w.tab.shards(w.check, entryActive, entryBusy)
		w.stats.Watchdog.Inc()
	}
}

// check raises an alert (an error-level log line) if the shard's oldest tail
// file is older than the tail alert age: its commits are only in journal
// segments, which expire, and deltas should have replaced them long ago.
func (w *watchdog) check(a *shard) {
	sh := a.cap.load()
	if sh == nil {
		return
	}
	n, oldest := sh.TailInfo()
	if n == 0 {
		return
	}
	age := time.Since(oldest)
	if age < w.alertAge || time.Since(a.tail.alertedAt) < tailAlertEvery {
		return
	}
	a.tail.alertedAt = time.Now()
	w.stats.TailAlerts.Add(1)
	w.log.Error("a shard's tail is too old, its deltas are not being written", "shard", a.Name, "tail_age", age, "alert_age", w.alertAge)
}
