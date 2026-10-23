package main

import (
	"bytes"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseCPU(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]time.Duration{
		"0:00.00\n":   0,
		"  1:02.50":   62*time.Second + 500*time.Millisecond,
		"00:00:07":    7 * time.Second,
		"01:02:03":    time.Hour + 2*time.Minute + 3*time.Second,
		"2-00:00:01":  48*time.Hour + time.Second,
		"12:34:56.78": 12*time.Hour + 34*time.Minute + 56*time.Second + 780*time.Millisecond,
	} {
		got, err := parseCPU(in)
		if err != nil || got != want {
			t.Errorf("parseCPU(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	if _, err := parseCPU("nonsense"); err == nil {
		t.Error("nonsense parsed")
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// helper process: TESTWATCH_HELPER=exit3 exits 3, =sleep sits idle, =trickle
// sits idle with a ticker running.
func TestMain(m *testing.M) {
	switch os.Getenv("TESTWATCH_HELPER") {
	case "exit3":
		os.Exit(3)
	case "sleep":
		time.Sleep(time.Minute)
		os.Exit(0)
	case "trickle":
		for range time.Tick(20 * time.Millisecond) { // stuck, but a timer still fires
		}
	}
	os.Exit(m.Run())
}

func TestRunPassesExitStatus(t *testing.T) {
	exe, _ := os.Executable()
	t.Setenv("TESTWATCH_HELPER", "exit3")
	if got := run(exe, nil, os.Stdout, os.Stderr, time.Minute, 10*time.Millisecond, time.Second); got != 3 {
		t.Fatalf("exit code %d, want 3", got)
	}
}

// An idle binary is sent SIGQUIT, and the runtime's goroutine dump comes out
// on its stderr, after testwatch's own line saying why.
func TestRunKillsAnIdleBinary(t *testing.T) {
	exe, _ := os.Executable()
	t.Setenv("TESTWATCH_HELPER", "sleep")
	start := time.Now()
	var stderr lockedBuffer // written by testwatch and by exec's copy of the binary's stderr
	got := run(exe, nil, os.Stdout, &stderr, 200*time.Millisecond, 50*time.Millisecond, time.Second)
	if got == 0 {
		t.Fatal("an idle binary was not reported as failed")
	}
	if time.Since(start) > 20*time.Second {
		t.Fatalf("took %v to give up", time.Since(start))
	}
	out := stderr.String()
	i, j := strings.Index(out, "testwatch: "), strings.Index(out, "SIGQUIT: quit")
	if i < 0 || j < i || !strings.Contains(out[j:], "goroutine 1 ") || !strings.Contains(out, "time.Sleep") {
		t.Fatalf("no goroutine dump after testwatch's line: %q", out)
	}
}

// A binary that is stuck but still wakes for a timer uses a little CPU: that
// is idle too.
func TestRunKillsABinaryThatOnlyTicks(t *testing.T) {
	exe, _ := os.Executable()
	t.Setenv("TESTWATCH_HELPER", "trickle")
	start := time.Now()
	got := run(exe, nil, os.Stdout, os.Stderr, time.Second, 50*time.Millisecond, time.Second)
	if got == 0 {
		t.Fatal("a ticking binary was not reported as failed")
	}
	if time.Since(start) > 20*time.Second {
		t.Fatalf("took %v to give up", time.Since(start))
	}
}
