package node

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/segment"
)

// cyclesUntil runs flush cycles until ch yields a value (or the test times
// out), for tests with manualFlush that must let a gate through.
func cyclesUntil[T any](r *srig, ch <-chan T) T {
	r.t.Helper()
	deadline := time.After(20 * time.Second)
	for {
		select {
		case v := <-ch:
			return v
		case <-deadline:
			r.t.Fatal("timed out waiting for the call to get through the gate")
		case <-time.After(20 * time.Millisecond):
			r.cycle()
		}
	}
}

// commitTo has the app commit one row to the (activated, then released) shard.
func (r *srig) commitTo(target string) {
	r.t.Helper()
	ref := r.acquire(target)
	r.commit(ref.Path())
	ref.Release()
}

// closeInApp has the app close its connection to the shard, so it can deactivate.
func (r *srig) closeInApp(target string) {
	r.t.Helper()
	ref := r.acquire(target)
	path := ref.Path()
	ref.Release()
	r.omu.Lock()
	defer r.omu.Unlock()
	if r.opened[path] {
		r.app.Must(r.t, "close", path)
		delete(r.opened, path)
	}
}

type seenChunk struct {
	data string
	rows int // of the caller, in blob storage, when the callee saw it
}

// calleeReadingBody is an app that, for a call, reports every chunk of the
// request body it gets together with how many of the caller's rows were in blob
// storage at that moment, and answers when the body ends.
func (r *srig) calleeReadingBody(caller string, seen chan<- seenChunk, started chan<- struct{}) {
	var once sync.Once
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.Header.Get("Shardlite-Caller") != caller {
			return
		}
		once.Do(func() { close(started) })
		buf := make([]byte, 64)
		for {
			n, err := q.Body.Read(buf)
			if n > 0 {
				seen <- seenChunk{string(buf[:n]), r.rows(caller)}
			}
			if err != nil {
				break
			}
		}
		seen <- seenChunk{"EOF", r.rows(caller)}
		io.WriteString(w, "done")
	})
}

func TestGateOnStreamingCallBody(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, ctype string
		h2          bool
	}{{"http1", "application/octet-stream", false}, {"grpc client stream over h2c", "application/grpc", true}} {
		t.Run(tc.name, func(t *testing.T) {
			r := newSRigWith(t, manualFlush)
			seen := make(chan seenChunk, 16)
			started := make(chan struct{})
			r.calleeReadingBody("wallet/st1", seen, started)

			commits := 1
			r.commitTo("wallet/st1")
			pr, pw := io.Pipe()
			req, _ := http.NewRequest("POST", r.ctl+"/stream", pr)
			req.Header.Set("Shardlite-Target", "slow/callee")
			req.Header.Set("Shardlite-Caller", "wallet/st1")
			req.Header.Set("Content-Type", tc.ctype)
			client := r.client
			if tc.h2 {
				client = r.h2c
			}
			type result struct {
				resp *http.Response
				body string
				err  error
			}
			res := make(chan result, 1)
			go func() {
				resp, err := client.Do(req)
				if err != nil {
					res <- result{err: err}
					return
				}
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				res <- result{resp, string(b), nil}
			}()
			// The headers wait for the first commit.
			cyclesUntil(r, started)
			if got := r.rows("wallet/st1"); got != 1 {
				t.Fatalf("the callee got the call with %d rows in blob storage", got)
			}

			for _, chunk := range []string{"first", "second"} {
				r.commitTo("wallet/st1")
				commits++
				if _, err := pw.Write([]byte(chunk)); err != nil {
					t.Fatal(err)
				}
				c := cyclesUntil(r, seen)
				if c.data != chunk || c.rows != commits {
					t.Fatalf("chunk %q reached the callee with %d of the caller's %d rows in blob storage (saw %q)", chunk, c.rows, commits, c.data)
				}
			}
			// The end of the body (which carries trailers) is gated as well.
			r.commitTo("wallet/st1")
			commits++
			pw.Close()
			c := cyclesUntil(r, seen)
			if c.data != "EOF" || c.rows != commits {
				t.Fatalf("the end of the body reached the callee with %d of %d rows in blob storage (%q)", c.rows, commits, c.data)
			}
			if got := cyclesUntil(r, res); got.err != nil || got.resp.StatusCode != 200 || got.body != "done" {
				t.Fatalf("%v %v %q", got.err, got.resp, got.body)
			}
			r.closeInApp("wallet/st1")
		})
	}
}

// What a calling shard sends over an upgraded connection is gated on the caller
// for as long as the connection lasts.
func TestGateOnUpgradedCall(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	seen := make(chan seenChunk, 16)
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.Header.Get("Shardlite-Caller") != "wallet/up1" {
			return
		}
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		rw.Flush()
		sc := bufio.NewScanner(rw)
		for sc.Scan() {
			seen <- seenChunk{sc.Text(), r.rows("wallet/up1")}
		}
	})
	r.commitTo("wallet/up1")
	conn, err := net.Dial("tcp", strings.TrimPrefix(r.ctl, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET /ws HTTP/1.1\r\nHost: x\r\nShardlite-Target: slow/up\r\nShardlite-Caller: wallet/up1\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
	br := bufio.NewReader(conn)
	type resp struct {
		r   *http.Response
		err error
	}
	got := make(chan resp, 1)
	go func() { rs, err := http.ReadResponse(br, nil); got <- resp{rs, err} }()
	if rs := cyclesUntil(r, got); rs.err != nil || rs.r.StatusCode != 101 {
		t.Fatalf("%v %v", rs.r, rs.err)
	}
	commits := 1
	for _, m := range []string{"m1", "m2", "m3"} {
		r.commitTo("wallet/up1")
		commits++
		fmt.Fprintf(conn, "%s\n", m)
		c := cyclesUntil(r, seen)
		if c.data != m || c.rows != commits {
			t.Fatalf("%q reached the callee with %d of the caller's %d rows in blob storage (saw %q)", m, c.rows, commits, c.data)
		}
	}
	// A caller that is not gated (its kind has sync off) is not held by the upgrade.
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		rw.Flush()
		line, _ := bufio.NewReader(rw).ReadString('\n')
		seen <- seenChunk{strings.TrimSpace(line), 0}
	})
	r.commitTo("users/up2")
	r.withoutCycle(func() {
		conn2, err := net.Dial("tcp", strings.TrimPrefix(r.ctl, "http://"))
		if err != nil {
			t.Error(err)
			return
		}
		defer conn2.Close()
		fmt.Fprintf(conn2, "GET /ws HTTP/1.1\r\nHost: x\r\nShardlite-Target: slow/up\r\nShardlite-Caller: users/up2\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		if rs, err := http.ReadResponse(bufio.NewReader(conn2), nil); err != nil || rs.StatusCode != 101 {
			t.Errorf("%v %v", rs, err)
			return
		}
		fmt.Fprintf(conn2, "hello\n")
		select {
		case c := <-seen:
			if c.data != "hello" {
				t.Errorf("saw %q", c.data)
			}
		case <-time.After(20 * time.Second):
			t.Error("the callee never saw the message")
		}
	})
}

// A caller whose kind has sync off, a call that says Shardlite-Sync: off, a
// caller that isn't active here and a call from the app are not held, however
// much they send. The flush counter is what shows it.
func TestCallsThatAreNotGated(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	var calls atomic.Int64
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.Header.Get("Shardlite-Caller") == "" {
			return
		}
		calls.Add(1)
		io.Copy(io.Discard, q.Body)
		io.WriteString(w, "ok")
	})
	r.commitTo("users/ng1")  // sync off
	r.commitTo("wallet/ng2") // sync on, but the call says off
	do := func(caller string, hdr map[string]string) {
		h := map[string]string{"Shardlite-Target": "slow/ng", "Shardlite-Caller": caller}
		for k, v := range hdr {
			h[k] = v
		}
		pr, pw := io.Pipe()
		go func() {
			for range 3 {
				pw.Write([]byte("chunk")) // returns once the node has read it
			}
			pw.Close()
		}()
		req, _ := http.NewRequest("POST", r.ctl+"/x", pr)
		for k, v := range h {
			req.Header.Set(k, v)
		}
		resp, err := r.client.Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("%s: %d", caller, resp.StatusCode)
		}
	}
	r.withoutCycle(func() {
		do("users/ng1", nil)
		do("wallet/ng2", map[string]string{"Shardlite-Sync": "off"})
		do("wallet/neverseen", nil) // not active on this node
		do("app", nil)
	})
	if calls.Load() != 4 {
		t.Fatalf("%d calls reached the callee", calls.Load())
	}
	// ...and a sync-off caller can ask for the gate on the call.
	done := make(chan struct{})
	go func() { do("users/ng1", map[string]string{"Shardlite-Sync": "on"}); close(done) }()
	cyclesUntil(r, done)
}

// The calling shard stays active for as long as a streaming call lasts, even
// past its idle timeout, and can deactivate once the call ends.
func TestCallerStaysActiveForTheCall(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	r := newSRigWith(t, withClock(clk)) // wallet's idle timeout is 300 ms
	seen := make(chan seenChunk, 16)
	started := make(chan struct{})
	r.calleeReadingBody("wallet/ia1", seen, started)
	r.commitTo("wallet/ia1")
	r.closeInApp("wallet/ia1")

	pr, pw := io.Pipe()
	req, _ := http.NewRequest("POST", r.ctl+"/stream", pr)
	req.Header.Set("Shardlite-Target", "slow/callee")
	req.Header.Set("Shardlite-Caller", "wallet/ia1")
	done := make(chan error, 1)
	go func() {
		resp, err := r.client.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		done <- err
	}()
	<-started
	a := r.shard("wallet/ia1")
	for range 4 { // four idle timeouts, with a look of the idle timer in each
		looks := a.looks.idle.Load()
		clk.Advance(300 * time.Millisecond)
		waitCounter(t, "a look of the idle timer", &a.looks.idle, looks+1)
	}
	if !r.m.IsActive("wallet/ia1") {
		t.Fatal("the caller was deactivated during its streaming call")
	}
	pw.Write([]byte("late"))
	if c := <-seen; c.data != "late" {
		t.Fatalf("saw %q", c.data)
	}
	pw.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	advanceUntil(t, clk, 100*time.Millisecond, "the caller to deactivate once the call ended", r.inactive("wallet/ia1"))
}

// --- hops and deadlines ---

// A client's own Shardlite-Hops and Shardlite-Deadline never reach the app, and
// neither header gets out in a response.
func TestHopsAndDeadlineAreTheNodes(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	var last atomic.Pointer[http.Request]
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		last.Store(q.Clone(context.Background()))
		w.Header().Set("Shardlite-Hops", "9")
		w.Header().Set("Shardlite-Deadline", "9")
	})
	resp, _ := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "slow/dd1", "Shardlite-Hops": "20", "Shardlite-Deadline": "1"}, "")
	q := last.Load()
	if resp.StatusCode != 200 || q.Header.Get("Shardlite-Hops") != "0" || q.Header.Get("Shardlite-Deadline") != "" {
		t.Fatalf("a client request reached the app with hops %q and deadline %q", q.Header.Get("Shardlite-Hops"), q.Header.Get("Shardlite-Deadline"))
	}
	if resp.Header.Get("Shardlite-Hops") != "" || resp.Header.Get("Shardlite-Deadline") != "" {
		t.Fatalf("the app's headers got out: %v", resp.Header)
	}
	// A shardless request is sent with hops 0 too.
	r.req("GET", r.pub, "/news", map[string]string{"Shardlite-Hops": "20"}, "")
	if q := last.Load(); q.Header.Get("Shardlite-Hops") != "0" || q.Header.Get("Shardlite-Deadline") != "" {
		t.Fatalf("shardless: %v", q.Header)
	}
	// A call is one deeper than the call it says it is serving, and starts at 0 when it says nothing.
	r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "slow/dd1", "Shardlite-Hops": "4"}, "")
	if d := last.Load().Header.Get("Shardlite-Hops"); d != "5" {
		t.Fatalf("hops of a call from hops 4: %q", d)
	}
	r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "slow/dd1"}, "")
	if d := last.Load().Header.Get("Shardlite-Hops"); d != "0" {
		t.Fatalf("hops of a call with none: %q", d)
	}
	if resp, _ := r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "slow/dd1", "Shardlite-Hops": "x"}, ""); resp.StatusCode != 400 {
		t.Fatalf("bad hops: %d", resp.StatusCode)
	}
	if resp, _ := r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "slow/dd1", "Shardlite-Deadline": "soon"}, ""); resp.StatusCode != 400 {
		t.Fatalf("bad deadline: %d", resp.StatusCode)
	}
	// A call's deadline reaches the app, smaller than it was.
	r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "slow/dd1", "Shardlite-Deadline": "5000"}, "")
	if ms, err := strconv.Atoi(last.Load().Header.Get("Shardlite-Deadline")); err != nil || ms <= 0 || ms > 5000 {
		t.Fatalf("deadline %q", last.Load().Header.Get("Shardlite-Deadline"))
	}
}

// Shards calling each other in a loop stop at the limit with 508, and the node logs it.
func TestCallLoopStopsAtTheHopLimit(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	var logs bytes.Buffer
	var lmu sync.Mutex
	r.srv.log = slog.New(slog.NewTextHandler(writerFunc(func(p []byte) (int, error) { lmu.Lock(); defer lmu.Unlock(); return logs.Write(p) }), nil))

	var invocations, maxSeen atomic.Int64
	var pass atomic.Bool
	pass.Store(true)
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if !strings.HasPrefix(q.URL.Path, "/ping") {
			return
		}
		invocations.Add(1)
		d, _ := strconv.Atoi(q.Header.Get("Shardlite-Hops"))
		maxSeen.Store(max(maxSeen.Load(), int64(d)))
		self := q.Header.Get("Shardlite-Target")
		other := map[string]string{"slow/la": "slow/lb", "slow/lb": "slow/la"}[self]
		next := "/ping"
		if q.URL.Query().Has("left") { // the bounded variant counts down
			n, _ := strconv.Atoi(q.URL.Query().Get("left"))
			if n == 0 {
				return
			}
			next += "?left=" + strconv.Itoa(n-1)
		}
		c, _ := http.NewRequest("POST", r.ctl+next, nil)
		c.Header.Set("Shardlite-Target", other)
		c.Header.Set("Shardlite-Caller", self)
		if pass.Load() {
			c.Header.Set("Shardlite-Hops", q.Header.Get("Shardlite-Hops"))
		}
		resp, err := r.client.Do(c)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
	})
	resp, _ := r.req("POST", r.pub, "/ping", map[string]string{"Shardlite-Target": "slow/la"}, "")
	if resp.StatusCode != 508 {
		t.Fatalf("a call loop ended with %d, want 508", resp.StatusCode)
	}
	// Hops 0 (the client's request) to 32 ran; the call at 33 was refused.
	if invocations.Load() != DefaultMaxHops+1 || maxSeen.Load() != DefaultMaxHops {
		t.Fatalf("the app ran %d times, deepest %d", invocations.Load(), maxSeen.Load())
	}
	lmu.Lock()
	logged := logs.String()
	lmu.Unlock()
	if !strings.Contains(logged, "chained too far") || !strings.Contains(logged, "target=slow/") || !strings.Contains(logged, "caller=slow/") {
		t.Fatalf("the refusal was not logged with the caller and target:\n%s", logged)
	}
	// gRPC callers get RESOURCE_EXHAUSTED.
	g, _ := r.req("POST", r.ctl, "/svc/M", map[string]string{"Shardlite-Target": "slow/la", "Shardlite-Hops": strconv.Itoa(DefaultMaxHops), "Content-Type": "application/grpc"}, "")
	if g.Header.Get("Grpc-Status") != "8" {
		t.Fatalf("gRPC: %v", g.Header)
	}

	// An app that doesn't pass the header on starts again at 0 on every hop.
	pass.Store(false)
	invocations.Store(0)
	maxSeen.Store(0)
	resp, _ = r.req("POST", r.pub, "/ping?left=60", map[string]string{"Shardlite-Target": "slow/la"}, "")
	if resp.StatusCode != 200 || invocations.Load() != 61 || maxSeen.Load() != 0 {
		t.Fatalf("without passing it on: status %d, %d invocations, deepest %d", resp.StatusCode, invocations.Load(), maxSeen.Load())
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// holdJournal makes every journal segment upload wait until the returned function
// is called (or the test ends): a flush that never completes, however slow the
// machine running the test is.
func (r *srig) holdJournal() (release func()) {
	gate := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	r.t.Cleanup(release)
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if !after && op == blob.OpCreate && strings.HasPrefix(key, segment.Root) {
			select {
			case <-gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	return release
}

// slowJournal makes every journal segment upload take d (a slow flush).
func (r *srig) slowJournal(d time.Duration) {
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if !after && op == blob.OpCreate && strings.HasPrefix(key, segment.Root) {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
}

// A call with a short deadline to a shard whose gate is held fails with 504, and
// the app saw the time left. The flush never completes
// (the journal upload is held), so only the deadline can end the call, and the
// test doesn't depend on how fast the machine is: the shard is active before the
// call, so the deadline can't run out during activation.
func TestDeadlineStopsAHeldGate(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	ref, err := r.m.Acquire(context.Background(), "wallet/dl1")
	if err != nil {
		t.Fatal(err)
	}
	defer ref.Release()
	r.holdJournal()
	var got atomic.Value
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		got.Store(q.Header.Get("Shardlite-Deadline"))
		r.commit(q.Header.Get("Shardlite-Path")) // the response now waits for a flush
		io.WriteString(w, "ok")
	})
	resp, _ := r.req("POST", r.ctl, "/x", map[string]string{"Shardlite-Target": "wallet/dl1", "Shardlite-Deadline": "500"}, "")
	if resp.StatusCode != 504 {
		t.Fatalf("status %d, want 504", resp.StatusCode)
	}
	seen, _ := got.Load().(string)
	// The time left, rounded up to a millisecond: never more than was sent (how much
	// less depends on how long the node took, which the test doesn't control).
	ms, err := strconv.Atoi(seen)
	if err != nil || ms <= 0 || ms > 500 {
		t.Fatalf("the app saw Shardlite-Deadline %q, want at most the 500 sent", seen)
	}
	// gRPC callers get DEADLINE_EXCEEDED.
	resp, _ = r.req("POST", r.ctl, "/svc/M", map[string]string{"Shardlite-Target": "wallet/dl1", "Shardlite-Deadline": "300", "Content-Type": "application/grpc"}, "")
	if resp.Header.Get("Grpc-Status") != "4" {
		t.Fatalf("gRPC: %v", resp.Header)
	}
	// A deadline that has passed is refused before anything runs.
	before := r.m.Stats.Activations.Load()
	resp, _ = r.req("POST", r.ctl, "/x", map[string]string{"Shardlite-Target": "wallet/dl2", "Shardlite-Deadline": "0"}, "")
	if resp.StatusCode != 504 || r.m.Stats.Activations.Load() != before {
		t.Fatalf("expired deadline: %d, activations %d", resp.StatusCode, r.m.Stats.Activations.Load()-before)
	}
}

// grpc-timeout on a gRPC client request becomes Shardlite-Deadline for the app
// (and the app's grpc-timeout is the time left).
func TestGRPCTimeoutBecomesTheDeadline(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	var dl, gt atomic.Value
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		dl.Store(q.Header.Get("Shardlite-Deadline"))
		gt.Store(q.Header.Get("Grpc-Timeout"))
		io.Copy(io.Discard, q.Body)
		w.Header().Set("Content-Type", "application/grpc")
	})
	q, _ := http.NewRequest("POST", r.pub+"/svc/Method", strings.NewReader("x"))
	q.Header.Set("Content-Type", "application/grpc")
	q.Header.Set("Grpc-Timeout", "5S")
	q.Header.Set("Shardlite-Target", "slow/gt1")
	resp, err := r.h2c.Do(q)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	ms, err := strconv.Atoi(dl.Load().(string))
	if err != nil || ms < 3000 || ms > 5000 {
		t.Fatalf("Shardlite-Deadline %q for grpc-timeout 5S", dl.Load())
	}
	if g := gt.Load().(string); !strings.HasSuffix(g, "m") {
		t.Fatalf("the app's grpc-timeout is %q, want the time left in milliseconds", g)
	}
	// Plain HTTP clients have no deadline.
	r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "slow/gt1", "Grpc-Timeout": "5S"}, "")
	if dl.Load().(string) != "" {
		t.Fatalf("a non-gRPC request got a deadline: %q", dl.Load())
	}
	// A bad one is refused.
	q, _ = http.NewRequest("POST", r.pub+"/svc/Method", strings.NewReader("x"))
	q.Header.Set("Content-Type", "application/grpc")
	q.Header.Set("Grpc-Timeout", "soon")
	q.Header.Set("Shardlite-Target", "slow/gt1")
	resp, err = r.h2c.Do(q)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.Header.Get("Grpc-Status") != "3" {
		t.Fatalf("bad grpc-timeout: %v", resp.Header)
	}
}
