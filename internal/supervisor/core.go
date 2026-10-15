// Package supervisor has what the node's suture supervisors are built with: the
// error types that tell suture what to do with a service that returned, the
// Spec every supervisor uses, and Background, which serves services outside
// Run's tree (docs/concurrency.md).
//
// It is adapted from Syncthing's lib/svcutil (Copyright (C) 2016 The Syncthing
// Authors), which is under the Mozilla Public License 2.0
// (https://mozilla.org/MPL/2.0/).
package supervisor

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/thejerf/suture/v4"
)

// FatalError is an error after which the node must keep its lease: the node can
// no longer write, so it must not give the lease up.
type FatalError struct{ Err error }

// Fatal wraps err (which must not be nil) as a fatal error. An error that is
// already fatal is returned as it is.
func Fatal(err error) error {
	if IsFatal(err) {
		return err
	}
	return &FatalError{Err: err}
}

// IsFatal reports whether err, or an error it wraps, is fatal.
func IsFatal(err error) bool {
	_, ok := errors.AsType[*FatalError](err)
	return ok
}

func (e *FatalError) Error() string { return e.Err.Error() }
func (e *FatalError) Unwrap() error { return e.Err }

// NoRestart wraps err (which may be nil) so that suture removes the service that
// returned it instead of restarting it.
func NoRestart(err error) error {
	if err == nil {
		return suture.ErrDoNotRestart
	}
	return &noRestartError{err}
}

type noRestartError struct{ err error }

func (e *noRestartError) Error() string      { return e.err.Error() }
func (e *noRestartError) Unwrap() error      { return e.err }
func (*noRestartError) Is(target error) bool { return target == suture.ErrDoNotRestart }

// Spec is what every supervisor of the node is made with: panics pass through
// (so a panic crashes the process), a stopping supervisor waits up to timeout
// for its services, and suture's events go to log.
func Spec(log *slog.Logger, timeout time.Duration) suture.Spec {
	return suture.Spec{
		EventHook:         eventHook(log),
		Timeout:           timeout,
		PassThroughPanics: true,
	}
}

// eventHook logs suture's events. A service that did not stop in time is an
// error. Restarts and backoff should not happen, since no service of the node
// returns an error suture restarts on, so they are warnings.
func eventHook(log *slog.Logger) suture.EventHook {
	return func(ev suture.Event) {
		m := ev.Map()
		l := log.With("supervisor", m["supervisor_name"], "service", m["service_name"])
		switch e := ev.(type) {
		case suture.EventStopTimeout:
			l.Error("a service did not stop in time")
		case suture.EventServicePanic:
			l.Error("a service panicked", "panic", e.PanicMsg)
		case suture.EventServiceTerminate:
			l.Warn("a service ended and is restarted", "err", e.Err)
		default:
			l.Warn("suture event", "event", ev.String())
		}
	}
}

// Background serves services on a supervisor of its own until stop is called,
// which waits for them to end. It is for services that run outside Run's tree
// (the test node, and tests). stop may be called again.
func Background(log *slog.Logger, name string, services ...suture.Service) (stop func()) {
	sup := suture.New(name, Spec(log, time.Minute))
	for _, s := range services {
		sup.Add(s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := sup.ServeBackground(ctx)
	return sync.OnceFunc(func() {
		cancel()
		<-done
	})
}
