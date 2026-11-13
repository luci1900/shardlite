package apptest_test

import (
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/test/apptest"
)

func TestProcWaitForFindsOutput(t *testing.T) {
	t.Parallel()
	p, err := apptest.StartProc("echo", "sh", nil, nil, "-c", "echo listening 127.0.0.1:1; exec sleep 60")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Kill)
	m, err := p.WaitFor(`listening (\S+)`, apptest.Timeout)
	if err != nil || m[1] != "127.0.0.1:1" {
		t.Fatalf("got %q, %v", m, err)
	}
}

// A process that never says it listens fails the wait at the deadline.
func TestProcWaitForFailsWhenNeverReady(t *testing.T) {
	t.Parallel()
	p, err := apptest.StartProc("silent", "sleep", nil, nil, "60")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Kill)
	if _, err := p.WaitFor(`listening`, 50*time.Millisecond); err == nil || !strings.Contains(err.Error(), "no \"listening\"") {
		t.Fatalf("a silent process passed the wait: %v", err)
	}
}

// A process that ends without saying it listens fails the wait at once, not
// at the deadline.
func TestProcWaitForFailsWhenItExits(t *testing.T) {
	t.Parallel()
	p, err := apptest.StartProc("short", "sh", nil, nil, "-c", "echo bye")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Kill)
	_, err = p.WaitFor(`listening`, apptest.Timeout)
	if err == nil || !strings.Contains(err.Error(), "exited") || !strings.Contains(err.Error(), "bye") {
		t.Fatalf("an exited process passed the wait: %v", err)
	}
}
