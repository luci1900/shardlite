package apptest

import (
	"testing"
	"time"
)

func TestLogWaitForWakesOnAdd(t *testing.T) {
	t.Parallel()
	var l Log[int]
	go func() {
		for i := range 5 {
			l.Add(i)
		}
	}()
	got := l.WaitFor(t, 5*time.Second, "five items", func(items []int) bool { return len(items) == 5 })
	for i, v := range got {
		if v != i {
			t.Fatalf("items = %v", got)
		}
	}
}

func TestLogWaitForSeesExistingItems(t *testing.T) {
	t.Parallel()
	var l Log[string]
	l.Add("a")
	l.Add("b")
	if got := l.WaitLen(t, time.Second, "two items", 2); len(got) != 2 {
		t.Fatalf("got %v", got)
	}
	if l.Len() != 2 || len(l.All()) != 2 {
		t.Fatal("Len or All")
	}
}

func TestLogAllIsACopy(t *testing.T) {
	t.Parallel()
	var l Log[int]
	l.Add(1)
	all := l.All()
	all[0] = 9
	if l.All()[0] != 1 {
		t.Fatal("All shares the log's storage")
	}
}

func TestLogWaitForFailsAfterTimeout(t *testing.T) {
	t.Parallel()
	var l Log[int]
	ft := &fakeTB{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { recover() }() // Fatalf stops the goroutine
		l.WaitFor(ft, 20*time.Millisecond, "nothing", func([]int) bool { return false })
	}()
	<-done
	if !ft.failed {
		t.Fatal("WaitFor did not fail the test on timeout")
	}
}

// fakeTB records Fatalf instead of failing the real test, and stops the caller
// the way FailNow does.
type fakeTB struct {
	testing.TB
	failed bool
}

func (f *fakeTB) Helper()               {}
func (f *fakeTB) Fatalf(string, ...any) { f.failed = true; panic("fatal") }
