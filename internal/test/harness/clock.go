package harness

import (
	"sync"
	"time"
)

// FakeClock is a clock for tests that move time: nothing that waits on it (a
// timer, a ticker, a backoff) fires until Advance moves it past. It has the
// methods of a node's Clock.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*fakeTimer]bool
}

type fakeTimer struct {
	at    time.Time
	every time.Duration // a ticker's period, 0 for a timer
	f     func()
}

// NewFakeClock returns a clock set to a few seconds past a minute boundary.
func NewFakeClock() *FakeClock {
	return &FakeClock{now: time.Now().Truncate(time.Minute).Add(10 * time.Second), timers: map[*fakeTimer]bool{}}
}

// Now returns the clock's time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) add(d, every time.Duration, f func()) (*fakeTimer, bool) {
	c.mu.Lock()
	t := &fakeTimer{at: c.now.Add(d), every: every, f: f}
	c.timers[t] = true
	c.mu.Unlock()
	return t, d <= 0
}

func (c *FakeClock) stopper(t *fakeTimer) func() bool {
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		was := c.timers[t]
		delete(c.timers, t)
		return was
	}
}

// NewTimer returns a channel that gets the time once the clock has moved d
// past now, and a stop function that reports whether it stopped the timer
// before it fired.
func (c *FakeClock) NewTimer(d time.Duration) (<-chan time.Time, func() bool) {
	ch := make(chan time.Time, 1)
	t, due := c.add(d, 0, func() { ch <- c.Now() })
	if due {
		c.fire(t)
	}
	return ch, c.stopper(t)
}

// NewTicker returns a channel that gets the time each time the clock has moved
// d past the last tick, and a function that stops it.
func (c *FakeClock) NewTicker(d time.Duration) (<-chan time.Time, func()) {
	ch := make(chan time.Time, 1)
	t, _ := c.add(d, d, func() {
		select {
		case ch <- c.Now():
		default:
		}
	})
	stop := c.stopper(t)
	return ch, func() { stop() }
}

func (c *FakeClock) fire(t *fakeTimer) {
	c.mu.Lock()
	was := c.timers[t]
	if t.every > 0 {
		t.at = c.now.Add(t.every) // a ticker skips the periods it was behind for
	} else {
		delete(c.timers, t)
	}
	c.mu.Unlock()
	if was {
		t.f()
	}
}

// Advance moves the clock forward by d and fires every timer and ticker that
// has come due.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var due []*fakeTimer
	for t := range c.timers {
		if !t.at.After(c.now) {
			due = append(due, t)
		}
	}
	c.mu.Unlock()
	for _, t := range due {
		c.fire(t)
	}
}
