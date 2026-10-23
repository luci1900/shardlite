// Package service runs the node's long-lived parts: a Group is started and
// stopped by its owner, which joins every service in it (docs/internals.md, Concurrency).
// Nothing restarts. A service that returns before its group is stopped has
// failed, and the owner learns it from Done.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// Service is a long-lived part of the node with a name of its own: Serve runs
// until its context ends, and String is its name in logs.
type Service interface {
	Serve(ctx context.Context) error
	String() string
}

// Group runs services that its owner starts together and stops together.
type Group struct {
	log    *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
	eg     errgroup.Group
	svcs   []*running
	ended  chan struct{} // closed when the first service returns
	once   sync.Once
}

type running struct {
	name string
	done chan struct{} // closed once serve has returned, after err is set
	err  error
}

// New makes a group whose services run on a context derived from ctx.
func New(ctx context.Context, log *slog.Logger) *Group {
	ctx, cancel := context.WithCancel(ctx)
	return &Group{log: log, ctx: ctx, cancel: cancel, ended: make(chan struct{})}
}

// Go starts serve as the service called name. Only the group's owner calls it,
// and never after Stop.
func (g *Group) Go(name string, serve func(context.Context) error) {
	r := &running{name: name, done: make(chan struct{})}
	g.svcs = append(g.svcs, r)
	g.eg.Go(func() error {
		defer close(r.done)
		defer g.once.Do(func() { close(g.ended) })
		r.err = serve(g.ctx)
		return nil
	})
}

// Add starts svc under its own name.
func (g *Group) Add(svc Service) { g.Go(svc.String(), svc.Serve) }

// Background runs svcs in a group of their own, for tests and tools that serve
// parts of the node outside Run, and returns what stops it (once, waiting up to
// a minute).
func Background(log *slog.Logger, svcs ...Service) (stop func()) {
	g := New(context.Background(), log)
	for _, s := range svcs {
		g.Add(s)
	}
	return sync.OnceFunc(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		g.Stop(ctx)
	})
}

// Done is closed once any service has returned: before Stop, a failure.
func (g *Group) Done() <-chan struct{} { return g.ended }

// Stop cancels the services and waits for them until ctx ends, as
// http.Server.Shutdown does. It returns the errors they failed with (an error
// that only says their context ended is a clean stop), and ctx's error if it
// ended first. A service still running then is logged by name and left behind,
// so a stop never hangs on one.
func (g *Group) Stop(ctx context.Context) error {
	g.cancel()
	var errs []error
	stuck := false
	for _, r := range g.svcs {
		select {
		case <-r.done:
		case <-ctx.Done():
		}
		select {
		case <-r.done:
			if r.err != nil && !errors.Is(r.err, context.Canceled) {
				errs = append(errs, fmt.Errorf("%s: %w", r.name, r.err))
			}
		default:
			g.log.Error("a service did not stop in time", "service", r.name)
			stuck = true
		}
	}
	if stuck {
		errs = append(errs, context.Cause(ctx))
	}
	return errors.Join(errs...)
}
