package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thejerf/suture/v4"
)

func TestFatalWraps(t *testing.T) {
	t.Parallel()
	base := errors.New("lease lost")
	err := Fatal(base)
	if !errors.Is(err, base) || !IsFatal(err) {
		t.Fatalf("Fatal(%v) = %v: want the error itself and fatal", base, err)
	}
	if again := Fatal(fmt.Errorf("keeper: %w", err)); !IsFatal(again) || again.Error() != "keeper: lease lost" {
		t.Fatalf("Fatal of a fatal error = %v", again)
	}
	if IsFatal(base) || errors.Is(err, suture.ErrDoNotRestart) {
		t.Fatal("a plain error is fatal, or a fatal one is also no-restart")
	}
	nr := NoRestart(base)
	if !errors.Is(nr, suture.ErrDoNotRestart) || !errors.Is(nr, base) || IsFatal(nr) {
		t.Fatalf("NoRestart(%v) = %v", base, nr)
	}
	if NoRestart(nil) != suture.ErrDoNotRestart {
		t.Fatal("NoRestart(nil) is not ErrDoNotRestart")
	}
}

// service is a function as a service, for the tests.
type service func(ctx context.Context) error

func (service) String() string                    { return "test" }
func (s service) Serve(ctx context.Context) error { return s(ctx) }

// A service that returns NoRestart ends, and suture does not run it again,
// whether or not it carries an error.
func TestNoRestartIsNotRestarted(t *testing.T) {
	t.Parallel()
	sup := suture.New("root", Spec(slog.New(slog.DiscardHandler), time.Second))
	var runs atomic.Int32
	done := make(chan struct{}, 2)
	sup.Add(service(func(ctx context.Context) error { runs.Add(1); done <- struct{}{}; return NoRestart(nil) }))
	sup.Add(service(func(ctx context.Context) error {
		runs.Add(1)
		done <- struct{}{}
		return NoRestart(errors.New("failed"))
	}))
	ctx, cancel := context.WithCancel(context.Background())
	errc := sup.ServeBackground(ctx)
	<-done
	<-done
	time.Sleep(100 * time.Millisecond) // a restart would have happened by now
	cancel()
	<-errc
	if n := runs.Load(); n != 2 {
		t.Fatalf("%d runs, want one each", n)
	}
}
