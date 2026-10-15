package notify

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestKickCoalesces(t *testing.T) {
	t.Parallel()
	k := NewKick()
	select {
	case <-k.C():
		t.Fatal("a wake-up is pending before any Kick")
	default:
	}
	for range 5 {
		k.Kick()
	}
	<-k.C()
	select {
	case <-k.C():
		t.Fatal("five Kicks were more than one wake-up")
	default:
	}
	k.Kick()
	select {
	case <-k.C():
	case <-time.After(5 * time.Second):
		t.Fatal("a Kick after a receive was lost")
	}
}

func TestKickWakesAWaiter(t *testing.T) {
	t.Parallel()
	k := NewKick()
	woke := make(chan struct{})
	go func() {
		<-k.C()
		close(woke)
	}()
	k.Kick()
	select {
	case <-woke:
	case <-time.After(5 * time.Second):
		t.Fatal("the waiter did not wake")
	}
}

func TestCounterReached(t *testing.T) {
	t.Parallel()
	var c Counter
	select {
	case <-c.Reached(0):
	default:
		t.Fatal("0 is reached at the start")
	}
	two, three := c.Reached(2), c.Reached(3)
	if c.Inc() != 1 {
		t.Fatal("Inc")
	}
	select {
	case <-two:
		t.Fatal("2 reached at 1")
	default:
	}
	if c.Add(1) != 2 {
		t.Fatal("Add")
	}
	select {
	case <-two:
	default:
		t.Fatal("2 not reached at 2")
	}
	select {
	case <-three:
		t.Fatal("3 reached at 2")
	default:
	}
	c.Add(5)
	select {
	case <-three:
	default:
		t.Fatal("3 not reached at 7")
	}
	if c.Load() != 7 {
		t.Fatalf("Load = %d", c.Load())
	}
	select {
	case <-c.Reached(7):
	default:
		t.Fatal("a value already reached must return a closed channel")
	}
}

func TestCounterWait(t *testing.T) {
	t.Parallel()
	var c Counter
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			if err := c.Wait(context.Background(), 100); err != nil {
				t.Error(err)
			}
		})
	}
	for range 100 {
		c.Inc()
	}
	wg.Wait()
}

func TestCounterWaitEndsWithContext(t *testing.T) {
	t.Parallel()
	var c Counter
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- c.Wait(ctx, 1) }()
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not end with its context")
	}
}
