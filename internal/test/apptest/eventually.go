package apptest

import (
	"testing"
	"time"
)

// Timeout is the usual deadline for Eventually: it is for failures only, and a
// slow machine under -race takes a while.
const Timeout = 30 * time.Second

// Eventually polls cond every few milliseconds until it is true, and fails the
// test, naming what it waited for, if timeout passes first. It is how tests
// wait for something to happen (a delivery, a flush, a deactivation) without a
// fixed sleep.
func Eventually(tb testing.TB, timeout time.Duration, what string, cond func() bool) {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			tb.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
