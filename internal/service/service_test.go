package service

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

type named struct{ serve func(context.Context) error }

func (n named) Serve(ctx context.Context) error { return n.serve(ctx) }
func (named) String() string                    { return "named" }

func untilStopped(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// Services that end when asked stop cleanly, whichever way they were started.
func TestStopIsClean(t *testing.T) {
	t.Parallel()
	g := New(context.Background(), slog.New(slog.DiscardHandler))
	g.Go("plain", untilStopped)
	g.Add(named{untilStopped})
	if err := g.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	select {
	case <-g.Done():
	default:
		t.Fatal("Done is open after every service returned")
	}
}

// A service that returns before Stop closes Done, and Stop reports its error
// under its name.
func TestFailureBeforeStop(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	g := New(context.Background(), slog.New(slog.DiscardHandler))
	g.Go("steady", untilStopped)
	g.Add(named{func(context.Context) error { return boom }})
	<-g.Done()
	err := g.Stop(context.Background())
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "named: boom") {
		t.Fatalf("stop: %v, want the failure under its name", err)
	}
}

// A service that doesn't stop is logged by name and left behind once Stop's
// context has ended, which Stop returns, and the services that did stop are
// still joined.
func TestServiceThatWontStop(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		g := New(context.Background(), slog.New(slog.NewTextHandler(&logs, nil)))
		release := make(chan struct{})
		g.Go("stuck", func(context.Context) error {
			<-release
			return nil
		})
		g.Go("steady", untilStopped)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		start := time.Now()
		if err := g.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("stop: %v, want the deadline", err)
		}
		if waited := time.Since(start); waited != time.Second {
			t.Fatalf("stop waited %v, want the deadline", waited)
		}
		if out := logs.String(); !strings.Contains(out, "service=stuck") || strings.Contains(out, "service=steady") {
			t.Fatalf("log %q, want only the stuck service", out)
		}
		close(release) // so the bubble can end
	})
}
