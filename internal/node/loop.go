package node

import (
	"context"
	"errors"
)

// errLoopEnded is what do returns once the loop it sends to has ended.
var errLoopEnded = errors.New("node: the loop has ended")

// do sends fn to a loop and waits for the loop to run it, the way Syncthing's
// folder runner takes requests (doInSync): the loop receives from queue and
// calls what it receives, one at a time, between its own jobs. do gives up when
// ctx ends or once the loop has ended (ended is closed), so a caller never waits
// for a loop that is gone. A loop that ran fn just before it ended still
// answers.
func do[T any](ctx context.Context, queue chan<- func(), ended <-chan struct{}, fn func() T) (T, error) {
	var zero T
	res := make(chan T, 1)
	select {
	case queue <- func() { res <- fn() }:
	case <-ended:
		return zero, errLoopEnded
	case <-ctx.Done():
		return zero, context.Cause(ctx)
	}
	select {
	case v := <-res:
		return v, nil
	case <-ended:
		select {
		case v := <-res:
			return v, nil
		default:
			return zero, errLoopEnded
		}
	case <-ctx.Done():
		return zero, context.Cause(ctx)
	}
}

// acquireSem takes a place in sem, a channel used as a semaphore, or reports
// false if ctx ends first.
func acquireSem(ctx context.Context, sem chan struct{}) bool {
	select {
	case sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}
