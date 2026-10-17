package node

import "time"

// Clock is the time the shards use for reminders (their timers and due times),
// wake hints (the minute of a key, which keys are due), the age of rows, the
// retry backoff of messages, each shard's idle and delta timers with the times
// they compare (a shard's last use and hold-off), and the periods of the cache
// loop, the watchdog and the wake scan. Tests move it. Flush cycles, the ages of
// tails (which decide whether a delta is due, and the tail alert), the short
// pauses between a commit's writes, and shutdown use real time.
type Clock interface {
	Now() time.Time
	// NewTimer sends the time on the channel once, d from now. The function
	// stops it and reports whether it had not fired.
	NewTimer(d time.Duration) (c <-chan time.Time, stop func() bool)
	// NewTicker sends the time on the channel every d (dropping a tick if the
	// receiver is behind) until the function is called.
	NewTicker(d time.Duration) (c <-chan time.Time, stop func())
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(d time.Duration) (<-chan time.Time, func() bool) {
	t := time.NewTimer(d)
	return t.C, t.Stop
}

func (realClock) NewTicker(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTicker(d)
	return t.C, t.Stop
}

func (m *Catalog) now() time.Time { return m.opts.Clock.Now() }
