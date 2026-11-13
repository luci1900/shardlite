package apptest

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"
)

// Log is a list that goroutines add to and a test waits on: WaitFor wakes on
// each Add instead of polling, as Eventually does. The zero value is an empty
// log, ready to use.
type Log[T any] struct {
	mu      sync.Mutex
	items   []T
	changed chan struct{} // closed and replaced on each Add
}

// Add appends v and wakes the waiters.
func (l *Log[T]) Add(v T) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.items = append(l.items, v)
	if l.changed != nil {
		close(l.changed)
		l.changed = nil
	}
}

// All is a copy of the items so far.
func (l *Log[T]) All() []T {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.items)
}

// Len is how many items there are.
func (l *Log[T]) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.items)
}

// WaitFor blocks until cond holds for the items (checked now and after each
// Add) and returns them, or fails the test, naming what it waited for, if
// timeout passes first. cond must not call the log.
func (l *Log[T]) WaitFor(tb testing.TB, timeout time.Duration, what string, cond func([]T) bool) []T {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		l.mu.Lock()
		if cond(l.items) {
			got := slices.Clone(l.items)
			l.mu.Unlock()
			return got
		}
		if l.changed == nil {
			l.changed = make(chan struct{})
		}
		ch := l.changed
		l.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			tb.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
	}
}

// WaitLen waits until the log has at least n items.
func (l *Log[T]) WaitLen(tb testing.TB, timeout time.Duration, what string, n int) []T {
	tb.Helper()
	return l.WaitFor(tb, timeout, what, func(items []T) bool { return len(items) >= n })
}
