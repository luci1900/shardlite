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

	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/test/blobtest"
	"github.com/luci1900/shardlite/internal/test/harness"
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
			req.Header.Set(hdrContext, ctxOf("wallet/st1", 0))
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

// A call from a shard can't be upgraded, whatever its kind's sync setting: what
// it sent would need gating for as long as the connection lasts. A call from the
// app with no shard behind it can.
func TestUpgradedCallFromAShardIsRefused(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		rw.Flush()
	})
	for _, tc := range []struct {
		caller string
		want   int
	}{{"wallet/up1", http.StatusBadRequest}, {"users/up2", http.StatusBadRequest}, {"", http.StatusSwitchingProtocols}} {
		conn, err := net.Dial("tcp", strings.TrimPrefix(r.ctl, "http://"))
		if err != nil {
			t.Fatal(err)
		}
		caller := ""
		if tc.caller != "" {
			caller = hdrContext + ": " + ctxOf(tc.caller, 0) + "\r\n"
		}
		fmt.Fprintf(conn, "GET /ws HTTP/1.1\r\nHost: x\r\nShardlite-Target: slow/up\r\n%sUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n", caller)
		rs, err := http.ReadResponse(bufio.NewReader(conn), nil)
		conn.Close()
		if err != nil || rs.StatusCode != tc.want {
			t.Fatalf("caller %q: %v %v, want %d", tc.caller, rs, err, tc.want)
		}
	}
}

// A caller activated here whose commits must be durable first is held however
// much it sends; a caller that isn't active on this node and a call from the
// app are not. The flush counter is what shows it.
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
	r.commitTo("users/ng1")  // the none tier: the call waits for nothing
	r.commitTo("wallet/ng2") // storage's, and the header can name storage again
	do := func(caller string, hdr map[string]string) {
		h := map[string]string{"Shardlite-Target": "slow/ng"}
		if caller != "app" {
			h[hdrContext] = ctxOf(caller, 0)
		}
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
		do("wallet/neverseen", nil) // not active on this node
		do("app", nil)
	})
	if calls.Load() != 2 {
		t.Fatalf("%d calls reached the callee", calls.Load())
	}
	r.withoutCycle(func() {
		do("users/ng1", nil)
		do("wallet/ng2", map[string]string{"Shardlite-Sync": "none"})
	})
	if calls.Load() != 4 {
		t.Fatalf("%d calls reached the callee", calls.Load())
	}
	// ...and a none-tier caller can ask for storage on the call.
	done := make(chan struct{})
	go func() { do("users/ng1", map[string]string{"Shardlite-Sync": "storage"}); close(done) }()
	cyclesUntil(r, done)
}

// A call that names a tier that doesn't exist is refused before it leaves the
// node. The caller is active here, so the node would have had to decide what
// to wait for, and copying the kind's tier could quietly give less than what
// the app said.
func TestCallWithAnUnknownTierIsRefused(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	var calls atomic.Int64
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.Header.Get("Shardlite-Caller") != "" {
			calls.Add(1)
			io.WriteString(w, "ok")
		}
	})
	r.commitTo("users/ng1")
	type result struct{ status int }
	ch := make(chan result, 1)
	go func() {
		req, _ := http.NewRequest("POST", r.ctl+"/x", strings.NewReader("chunk"))
		req.Header.Set("Shardlite-Target", "slow/ng")
		req.Header.Set(hdrContext, ctxOf("users/ng1", 0))
		req.Header.Set("Shardlite-Sync", "maybe")
		resp, err := r.client.Do(req)
		if err != nil {
			t.Error(err)
			ch <- result{0}
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		ch <- result{resp.StatusCode}
	}()
	// A refusal answers at once; a gate would need the cycles this runs.
	if got := cyclesUntil(r, ch); got.status != http.StatusBadRequest {
		t.Fatalf("status %d, want %d", got.status, http.StatusBadRequest)
	}
	if calls.Load() != 0 {
		t.Fatal("the call reached the callee")
	}
}

// The calling shard stays active for as long as a streaming call lasts, even
// past its idle timeout, and can deactivate once the call ends.
func TestCallerStaysActiveForTheCall(t *testing.T) {
	t.Parallel()
	clk := harness.NewFakeClock()
	r := newSRigWith(t, withClock(clk)) // wallet's idle timeout is 300 ms
	seen := make(chan seenChunk, 16)
	started := make(chan struct{})
	r.calleeReadingBody("wallet/ia1", seen, started)
	r.commitTo("wallet/ia1")
	r.closeInApp("wallet/ia1")

	pr, pw := io.Pipe()
	req, _ := http.NewRequest("POST", r.ctl+"/stream", pr)
	req.Header.Set("Shardlite-Target", "slow/callee")
	req.Header.Set(hdrContext, ctxOf("wallet/ia1", 0))
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
	if !r.m.IsActive(shardName("wallet/ia1")) {
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

// --- hops ---

// A client's own Shardlite-Hops and Shardlite-Context never reach the app, which
// gets no Shardlite-Hops at all, and a Shardlite-Deadline (which the node no
// longer reads) is stripped like any other Shardlite-* header, from a client and
// from a call. None of them gets out in a response.
func TestHopsAreTheNodes(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("slow/dd1")
	var last atomic.Pointer[http.Request]
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		last.Store(q.Clone(context.Background()))
		w.Header().Set("Shardlite-Hops", "9")
		w.Header().Set(hdrContext, ctxOf("slow/dd1", 9))
		w.Header().Set("Shardlite-Deadline", "9")
	})
	resp, _ := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "slow/dd1", "Shardlite-Hops": "20", hdrContext: ctxOf("slow/dd1", 20), "Shardlite-Deadline": "1"}, "")
	q := last.Load()
	if resp.StatusCode != 200 || q.Header.Get(hdrContext) != ctxOf("slow/dd1", 0) || q.Header.Get("Shardlite-Hops") != "" || q.Header.Get("Shardlite-Deadline") != "" {
		t.Fatalf("a client request reached the app with %v", q.Header)
	}
	if resp.Header.Get("Shardlite-Hops") != "" || resp.Header.Get(hdrContext) != "" || resp.Header.Get("Shardlite-Deadline") != "" {
		t.Fatalf("the app's headers got out: %v", resp.Header)
	}
	// A shardless request has no context, whatever the client sent.
	r.req("GET", r.pub, "/news", map[string]string{"Shardlite-Hops": "20", hdrContext: ctxOf("slow/dd1", 20)}, "")
	if q := last.Load(); q.Header.Get(hdrContext) != "" || q.Header.Get("Shardlite-Hops") != "" || q.Header.Get("Shardlite-Deadline") != "" {
		t.Fatalf("shardless: %v", q.Header)
	}
	// A call is one deeper than the request its context says it is serving, and a call with none is from the app, at hop 1.
	r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "slow/dd1", hdrContext: ctxOf("users/dd2", 4)}, "")
	if c := last.Load().Header.Get(hdrContext); c != ctxOf("slow/dd1", 5) {
		t.Fatalf("context of a call from hops 4: %q", c)
	}
	r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "slow/dd1", "Shardlite-Hops": "4"}, "")
	if h := last.Load().Header; h.Get(hdrContext) != ctxOf("slow/dd1", 1) || h.Get("Shardlite-Hops") != "" || h.Get("Shardlite-Caller") != "app" {
		t.Fatalf("a call with no context, with a Shardlite-Hops of its own: %v", h)
	}
	// A Shardlite-Deadline on a call is stripped, not read: a value that isn't a number is no error.
	if resp, _ := r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": "slow/dd1", "Shardlite-Deadline": "soon"}, ""); resp.StatusCode != 200 || last.Load().Header.Get("Shardlite-Deadline") != "" {
		t.Fatalf("a call with a deadline: %d %q", resp.StatusCode, last.Load().Header.Get("Shardlite-Deadline"))
	}
}

// A call's context names its caller, with hops one more than the context's. A
// context that doesn't parse or lacks a key is a 400, and a key the node doesn't
// know is ignored.
func TestCallContext(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("slow/cc1")
	var last atomic.Pointer[http.Request]
	r.app_(func(w http.ResponseWriter, q *http.Request) { last.Store(q.Clone(context.Background())) })
	call := func(ctx string) *http.Response {
		h := map[string]string{"Shardlite-Target": "slow/cc1"}
		if ctx != "" {
			h[hdrContext] = ctx
		}
		resp, _ := r.req("GET", r.ctl, "/x", h, "")
		return resp
	}
	if resp := call(ctxOf("users/cc2", 2)); resp.StatusCode != 200 || last.Load().Header.Get("Shardlite-Caller") != "users/cc2" || last.Load().Header.Get(hdrContext) != ctxOf("slow/cc1", 3) {
		t.Fatalf("a call with a context: %d %v", resp.StatusCode, last.Load().Header)
	}
	if resp := call(""); resp.StatusCode != 200 || last.Load().Header.Get("Shardlite-Caller") != "app" || last.Load().Header.Get(hdrContext) != ctxOf("slow/cc1", 1) {
		t.Fatalf("a call without one: %d %v", resp.StatusCode, last.Load().Header)
	}
	if resp := call(`shard="users/cc2", hops=2, from="elsewhere", extra`); resp.StatusCode != 200 || last.Load().Header.Get("Shardlite-Caller") != "users/cc2" || last.Load().Header.Get(hdrContext) != ctxOf("slow/cc1", 3) {
		t.Fatalf("a call with a key the node doesn't know: %d %v", resp.StatusCode, last.Load().Header)
	}
	last.Store(nil)
	for _, bad := range []string{`shard="users/cc2"`, `hops=1`, `shard=, hops=1`, `shard="users", hops=1`, `shard="users/cc2", hops="1"`, `shard="users/cc2", hops=-1`, `users/cc2`} {
		resp := call(bad)
		if resp.StatusCode != 400 {
			t.Errorf("context %q: %d, want 400", bad, resp.StatusCode)
		}
		wantReason(t, resp, reasonBadRequest)
	}
	if last.Load() != nil {
		t.Fatal("a call with a bad context reached the app")
	}
}

// Shards calling each other in a loop stop at the limit with 508, and the node logs it.
func TestCallLoopStopsAtTheHopLimit(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("slow/la")
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
		c, _, err := parseContext(q.Header)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		maxSeen.Store(max(maxSeen.Load(), c.hops))
		self := q.Header.Get("Shardlite-Opened")
		other := map[string]string{"slow/la": "slow/lb", "slow/lb": "slow/la"}[self]
		next := "/ping"
		if q.URL.Query().Has("left") { // the bounded variant counts down
			n, _ := strconv.Atoi(q.URL.Query().Get("left"))
			if n == 0 {
				return
			}
			next += "?left=" + strconv.Itoa(n-1)
		}
		call, _ := http.NewRequest("POST", r.ctl+next, nil)
		call.Header.Set("Shardlite-Target", other)
		if pass.Load() {
			call.Header.Set(hdrContext, q.Header.Get(hdrContext))
		}
		resp, err := r.client.Do(call)
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
	g, _ := r.req("POST", r.ctl, "/svc/M", map[string]string{"Shardlite-Target": "slow/la", hdrContext: ctxOf("slow/lb", DefaultMaxHops), "Content-Type": "application/grpc"}, "")
	if g.Header.Get("Grpc-Status") != "8" {
		t.Fatalf("gRPC: %v", g.Header)
	}

	// An app that doesn't pass the context on makes calls from the app, at hop 1 each time.
	pass.Store(false)
	invocations.Store(0)
	maxSeen.Store(0)
	resp, _ = r.req("POST", r.pub, "/ping?left=60", map[string]string{"Shardlite-Target": "slow/la"}, "")
	if resp.StatusCode != 200 || invocations.Load() != 61 || maxSeen.Load() != 1 {
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
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if !after && op == blobtest.OpCreate && strings.HasPrefix(key, journal.Root) {
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
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if !after && op == blobtest.OpCreate && strings.HasPrefix(key, journal.Root) {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
}

// A gRPC client's grpc-timeout reaches the app as the client sent it: the client
// enforces its own deadline.
func TestGRPCTimeoutPassesThrough(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("slow/gt1")
	var gt atomic.Value
	r.app_(func(w http.ResponseWriter, q *http.Request) {
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
	if gt.Load().(string) != "5S" {
		t.Fatalf("the app saw grpc-timeout %q", gt.Load())
	}
}
