package node

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
)

// Stopping the servers ends what is left of their requests and joins them: a
// WebSocket, which Shutdown alone would leave open, and a request still running
// after the grace period. A request that finishes within the grace period gets
// its answer. Once the servers have stopped, no request holds a shard.
func TestServerShutdownEndsAndJoinsRequests(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("slow/ws")
	var started, ended apptest.Log[string]
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		started.Add(q.URL.Path)
		defer ended.Add(q.URL.Path)
		switch q.URL.Path {
		case "/ws":
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			defer conn.Close()
			rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
			rw.Flush()
			sc := bufio.NewScanner(rw)
			for sc.Scan() {
				rw.WriteString("echo " + sc.Text() + "\n")
				rw.Flush()
			}
		case "/slow":
			<-q.Context().Done() // until the node gives up on it
		case "/finish":
			time.Sleep(100 * time.Millisecond) // within the grace period
			io.WriteString(w, "done")
		}
	})
	refs := r.refs

	conn, err := net.Dial("tcp", strings.TrimPrefix(r.pub, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET /ws?shardlite-target=slow/ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
	br := bufio.NewReader(conn)
	if resp, err := http.ReadResponse(br, nil); err != nil || resp.StatusCode != 101 {
		t.Fatalf("upgrade: %v %v", resp, err)
	}
	fmt.Fprintf(conn, "one\n")
	if line, err := br.ReadString('\n'); err != nil || line != "echo one\n" {
		t.Fatalf("%q %v", line, err)
	}

	type answer struct {
		status int
		body   string
		err    error
	}
	call := func(base, path, target string) chan answer {
		ch := make(chan answer, 1)
		go func() {
			q, _ := http.NewRequest("GET", base+path, nil)
			q.Header.Set("Shardlite-Target", target)
			resp, err := r.client.Do(q)
			if err != nil {
				ch <- answer{err: err}
				return
			}
			defer resp.Body.Close()
			b, err := io.ReadAll(resp.Body)
			ch <- answer{resp.StatusCode, string(b), err}
		}()
		return ch
	}
	r.create("slow/finish")
	slow := call(r.ctl, "/slow", "slow/call") // through the control server
	finish := call(r.pub, "/finish", "slow/finish")
	started.WaitFor(t, 10*time.Second, "the requests to reach the app", func(p []string) bool {
		return slices.Contains(p, "/ws") && slices.Contains(p, "/slow") && slices.Contains(p, "/finish")
	})
	for _, target := range []string{"slow/ws", "slow/call", "slow/finish"} {
		if n := refs(target); n < 1 {
			t.Fatalf("%s: %d references before the servers stop", target, n)
		}
	}

	r.stop()
	// Joined: nothing is left holding a shard the moment the servers have stopped.
	for _, target := range []string{"slow/ws", "slow/call", "slow/finish"} {
		if n := refs(target); n != 0 {
			t.Fatalf("%s: %d references after the servers stopped", target, n)
		}
	}
	if a := <-finish; a.err != nil || a.status != 200 || a.body != "done" {
		t.Fatalf("a request that finished within the grace period: %+v", a)
	}
	if a := <-slow; a.err == nil && a.status == 200 {
		t.Fatalf("a request still running after the grace period was not ended: %+v", a)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := br.ReadString('\n'); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("the WebSocket is still open: %v", err)
	}
	ended.WaitFor(t, 10*time.Second, "the app's handlers to end", func(p []string) bool {
		return slices.Contains(p, "/ws") && slices.Contains(p, "/slow")
	})
}

// acceptCounter counts the connections a listener has accepted.
type acceptCounter struct {
	net.Listener
	n atomic.Int32
}

func (l *acceptCounter) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.n.Add(1)
	}
	return c, err
}

// serveOn starts an httpServer with a long grace on two listeners (a server
// may have several) and returns its listeners, a stop function that cancels
// it and the channel Serve's result arrives on.
func serveOn(t *testing.T, h http.Handler) ([]*acceptCounter, context.CancelFunc, <-chan error) {
	t.Helper()
	var lns []*acceptCounter
	var raw []net.Listener
	for range 2 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		c := &acceptCounter{Listener: ln}
		lns, raw = append(lns, c), append(raw, c)
	}
	srv := &httpServer{name: "test server", log: slog.New(slog.DiscardHandler), lns: raw, handler: h, grace: time.Minute}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(cancel)
	return lns, cancel, done
}

// A connection that has sent nothing does not hold up a stop: Shutdown alone
// would wait for it until it is 5 seconds old.
func TestServerStopDoesNotWaitForSilentConnections(t *testing.T) {
	t.Parallel()
	lns, cancel, done := serveOn(t, http.NotFoundHandler())
	var conns []net.Conn
	for _, ln := range lns {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close() // open until after the assertion
		conns = append(conns, c)
	}
	apptest.Eventually(t, 10*time.Second, "both connections to be accepted", func() bool {
		return lns[0].n.Load() == 1 && lns[1].n.Load() == 1
	})
	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2*time.Second + silentGrace):
		t.Fatal("the stop is still waiting for connections that sent nothing")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("the stop took %v with silent connections open, want about %v", d, silentGrace)
	}
	for _, c := range conns {
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := c.Read(make([]byte, 1)); err != io.EOF && !errors.Is(err, syscall.ECONNRESET) {
			t.Fatalf("a silent connection was not closed: %v", err)
		}
	}
}

// Closing the silent connections leaves a request in flight alone, which
// finishes within the grace period.
func TestServerStopLetsARequestInFlightFinish(t *testing.T) {
	t.Parallel()
	entered, release := make(chan struct{}), make(chan struct{})
	lns, cancel, done := serveOn(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		io.WriteString(w, "done")
	}))
	silent, err := net.Dial("tcp", lns[1].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	answer := make(chan string, 1)
	go func() {
		res, err := http.Get("http://" + lns[0].Addr().String())
		if err != nil {
			answer <- err.Error()
			return
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		answer <- string(b)
	}()
	<-entered
	cancel()
	// The silent connection is closed while the request is still running.
	silent.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, err := silent.Read(make([]byte, 1)); err != io.EOF && !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("the silent connection was not closed: %v", err)
	}
	select {
	case err := <-done:
		t.Fatalf("Serve returned (%v) with a request still in flight", err)
	default:
	}
	close(release)
	if got := <-answer; got != "done" {
		t.Fatalf("the request in flight answered %q", got)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
