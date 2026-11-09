// Package shardlitetest tests an app built on package shardlite against a real
// node. Start runs the shardlite binary on its in-memory store
// (SHARDLITE_BLOB=memory://), with the app's handler behind it, so a test sees
// everything the node does (the declaration, the hop limit, the gates, real
// wakes) and no copy of it has to be kept in step. A test sends a client
// request to the node's public address with Shardlite-Target, makes a call
// through the app's Node and waits for a wake after setting a reminder that is
// due.
//
// The binary is the path in SHARDLITE_BIN, else shardlite on PATH. Without one
// Start fails the test, with how to build it.
//
// Like package shardlite it imports only the standard library, and the test
// brings the driver through the Config's Open.
package shardlitetest

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	shardlite "github.com/luci1900/shardlite/sdk/go"
)

// How long Start waits for the node to log that it is up and then to answer
// ready, and how long the node has to stop on SIGTERM before it is killed.
const (
	upTimeout    = 30 * time.Second
	readyTimeout = 20 * time.Second
	stopTimeout  = 15 * time.Second
)

// Node is a running node and the app behind it.
type Node struct {
	// URL is the node's public base URL, where a client's requests go.
	URL string
	// Control is the host:port of the node's control API, which App calls.
	Control string
	// App is the app's Node, built from the Config and pointed at the node.
	App *shardlite.Node

	out *lockedBuf
}

// Output is what the node has written so far (its log, on stderr).
func (n *Node) Output() string { return n.out.String() }

// Start runs a node for t, which stops it when the test ends and logs its
// output if the test failed. It builds the app's Node from cfg with Addr set
// to the node's control address, serves app(node) on a local port as the app,
// and returns once the node's public ready route answers 200.
//
// The node keeps everything in memory, so it is one node with nothing left
// when it stops. Tests that share a node by sharing the returned value should
// use shard ids of their own.
func Start(t testing.TB, cfg shardlite.Config, app func(*shardlite.Node) http.Handler) *Node {
	t.Helper()
	bin, err := binary()
	if err != nil {
		t.Fatal(err)
	}
	public, control := freeAddr(t), freeAddr(t)
	cfg.Addr = control
	node, err := shardlite.New(cfg)
	if err != nil {
		t.Fatalf("shardlitetest: %v", err)
	}
	srv := httptest.NewServer(app(node))
	t.Cleanup(srv.Close)

	out := &lockedBuf{}
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"SHARDLITE_BLOB=memory://",
		"SHARDLITE_NODE=shardlitetest",
		"SHARDLITE_DATA="+filepath.Join(t.TempDir(), "data"),
		"SHARDLITE_APP="+srv.URL,
		"SHARDLITE_LISTEN_PUBLIC="+public,
		"SHARDLITE_LISTEN_CONTROL="+control,
		"SHARDLITE_LISTEN_PEER=127.0.0.1:0",
		"SHARDLITE_FLUSH=50ms",
	)
	cmd.Stdout, cmd.Stderr = out, out
	if err := cmd.Start(); err != nil {
		t.Fatalf("shardlitetest: starting %s: %v", bin, err)
	}
	done := make(chan struct{})
	var exit error
	go func() { exit = cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(stopTimeout):
			cmd.Process.Kill()
			<-done
			t.Errorf("shardlitetest: the node did not stop on SIGTERM within %v", stopTimeout)
		}
		if t.Failed() {
			t.Logf("the node's output:\n%s", out.String())
		}
	})

	n := &Node{URL: "http://" + public, Control: control, App: node, out: out}
	waitFor(t, upTimeout, "the node to log that it is up", func() bool {
		select {
		case <-done:
			t.Fatalf("shardlitetest: the node exited (%v) before it was up:\n%s", exit, out.String())
		default:
		}
		return strings.Contains(out.String(), "shardlite is up")
	})
	waitFor(t, readyTimeout, "the node to answer ready", func() bool {
		res, err := http.Get(n.URL + shardlite.ReadyPath)
		if err != nil {
			return false
		}
		res.Body.Close()
		return res.StatusCode == http.StatusOK
	})
	return n
}

// binary is the node's binary: SHARDLITE_BIN, else shardlite on PATH.
func binary() (string, error) {
	const howTo = `build it with "go build -o <dir>/shardlite ./cmd/shardlite" from the shardlite repository and set SHARDLITE_BIN=<dir>/shardlite (or put <dir> on PATH)`
	if bin := os.Getenv("SHARDLITE_BIN"); bin != "" {
		if _, err := os.Stat(bin); err != nil {
			return "", fmt.Errorf("shardlitetest: SHARDLITE_BIN is %q, which cannot be read (%v): %s", bin, err, howTo)
		}
		return bin, nil
	}
	bin, err := exec.LookPath("shardlite")
	if err != nil {
		return "", fmt.Errorf("shardlitetest: no shardlite binary: SHARDLITE_BIN is not set and shardlite is not on PATH: %s", howTo)
	}
	return bin, nil
}

// freeAddr is a loopback address nothing listens on at the moment.
func freeAddr(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("shardlitetest: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

// waitFor polls cond until it holds, and fails the test after timeout.
func waitFor(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("shardlitetest: timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// lockedBuf is a buffer the node's output is written to while the test reads
// it.
type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}
