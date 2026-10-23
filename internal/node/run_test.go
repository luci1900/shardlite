package node

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// nodeGoroutines lists the goroutines running code of the node's packages (not
// the test's own, which are in _test.go files).
func nodeGoroutines() []string {
	buf := make([]byte, 1<<22)
	for { // the dump is cut at the buffer's size: grow it until it all fits
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	var out []string
	for i, g := range strings.Split(string(buf), "\n\n") {
		if i == 0 || strings.Contains(g, "_test.go") {
			continue // the calling goroutine, and the test's helpers
		}
		for _, pkg := range []string{"internal/node.", "internal/capture.", "internal/journal.", "internal/segment.", "internal/coord.", "internal/shardfile.", "internal/blob."} {
			if strings.Contains(g, "luci1900/shardlite/"+pkg) {
				out = append(out, g)
				break
			}
		}
	}
	return out
}

// goroutineID is the number in a goroutine's stack header.
func goroutineID(g string) string {
	f := strings.Fields(g)
	if len(f) < 2 {
		return g
	}
	return f[1]
}

// startedSince lists the goroutines of now that were not running in before: other
// tests' goroutines that end meanwhile do not count, and neither do any that
// were already running.
func startedSince(before, now []string) []string {
	seen := map[string]bool{}
	for _, g := range before {
		seen[goroutineID(g)] = true
	}
	var out []string
	for _, g := range now {
		if !seen[goroutineID(g)] {
			out = append(out, g)
		}
	}
	return out
}

// runKinds is what the test's app declares: long idle timeouts, so the shards
// the test activates stay active while it counts goroutines, and sync off for
// users, so a flush is not what the writes wait for.
const runKinds = `{"app": "test", "kinds": {"users": {"public": true, "sync": "local", "idle_timeout": "1h"}, "wallet": {"public": true, "idle_timeout": "1h"}}}`

// Run returns only after every goroutine it started has ended: the loops, the
// servers, the ready poll, the flush cycle's syncs, the segment writer and
// the lease keeper.
func TestRunLeavesNoGoroutines(t *testing.T) {
	app := apptest.Start(t)
	var mu sync.Mutex
	opened := map[string]bool{}
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == appReadyPath {
			io.WriteString(w, runKinds)
			return
		}
		path := q.Header.Get("Shardlite-Path")
		mu.Lock()
		if !opened[path] {
			app.Must(t, "open", path, path)
			opened[path] = true
		}
		app.Must(t, "insert", path, "1", "64")
		mu.Unlock()
		io.WriteString(w, "ok")
	}))
	defer fake.Close()

	dir := t.TempDir()
	vars := map[string]string{
		"SHARDLITE_NODE": "run-test", "SHARDLITE_BLOB": "s3://unused",
		"SHARDLITE_DATA": filepath.Join(dir, "data"),
		"SHARDLITE_APP":  fake.URL, "SHARDLITE_LISTEN_PUBLIC": "127.0.0.1:0", "SHARDLITE_LISTEN_CONTROL": "127.0.0.1:0", "SHARDLITE_LISTEN_PEER": "127.0.0.1:0",
		"SHARDLITE_FLUSH": "50ms", "SHARDLITE_LISTEN_PPROF": "127.0.0.1:0",
	}
	env, err := ParseEnv(func(k string) string { return vars[k] })
	if err != nil {
		t.Fatal(err)
	}
	before := nodeGoroutines()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr := make(chan [2]string, 1)
	ran := make(chan error, 1)
	go func() {
		ran <- Run(ctx, env, slog.New(slog.DiscardHandler), func(public, control, peer string) { addr <- [2]string{public, control} }, WithBackend(blob.NewMemory()))
	}()
	var public, control string
	select {
	case a := <-addr:
		public, control = a[0], a[1]
	case err := <-ran:
		t.Fatalf("Run ended early: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("the node did not come up")
	}
	eventually(t, "the app to be seen as healthy", func() bool {
		resp, err := http.Get("http://" + public + "/_shardlite/ready")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == 200
	})
	// Shards of both kinds with writes, created by calls: lazy creation, capture, syncs, gated responses.
	for i := range 5 {
		for _, kind := range []string{"test.users", "test.wallet"} {
			q, _ := http.NewRequest("POST", "http://"+control+"/write", nil)
			q.Header.Set("Shardlite-Target", fmt.Sprintf("%s/%d", kind, i))
			resp, err := http.DefaultClient.Do(q)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatalf("status %d", resp.StatusCode)
			}
		}
	}
	if n := len(startedSince(before, nodeGoroutines())); n == 0 {
		t.Fatal("no new node goroutines while running: the test is not looking at anything")
	}

	cancel()
	select {
	case err := <-ran:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return")
	}
	// No waiting: Run's return is the join.
	if left := startedSince(before, nodeGoroutines()); len(left) > 0 {
		t.Fatalf("%d node goroutines left after Run returned:\n%s", len(left), strings.Join(left, "\n\n"))
	}
}

// A listener that can't be bound fails Run before the shards are built: Run
// closes the listeners it has and stops the journal it started.
func TestRunFailsWhenAListenerCannotBeBound(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	public, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	vars := map[string]string{
		"SHARDLITE_NODE": "bind-test", "SHARDLITE_BLOB": "s3://unused",
		"SHARDLITE_DATA": filepath.Join(dir, "data"),
		"SHARDLITE_APP":  "http://127.0.0.1:1", "SHARDLITE_LISTEN_PUBLIC": "127.0.0.1:0", "SHARDLITE_LISTEN_CONTROL": taken.Addr().String(), "SHARDLITE_LISTEN_PEER": "127.0.0.1:0",
	}
	env, err := ParseEnv(func(k string) string { return vars[k] })
	if err != nil {
		t.Fatal(err)
	}
	before := nodeGoroutines()

	err = Run(context.Background(), env, slog.New(slog.DiscardHandler), nil, WithBackend(blob.NewMemory()), withPublicListener(public))
	if err == nil {
		t.Fatal("Run started with its control address taken")
	}
	if _, aerr := public.Accept(); !errors.Is(aerr, net.ErrClosed) {
		t.Fatalf("the public listener is still open (Accept: %v)", aerr)
	}
	if left := startedSince(before, nodeGoroutines()); len(left) > 0 {
		t.Fatalf("%d node goroutines left after Run returned:\n%s", len(left), strings.Join(left, "\n\n"))
	}
}

// runLog is a slog.Handler that keeps the messages logged at Warn and above, and
// Run's services starting and stopping (logged at Debug) as "start X" and
// "stop X".
type runLog struct{ warned, events apptest.Log[string] }

func (l *runLog) Enabled(context.Context, slog.Level) bool { return true }
func (l *runLog) Handle(_ context.Context, r slog.Record) error {
	var service string
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "service" {
			service = a.Value.String()
		}
		return true
	})
	switch r.Message {
	case "service started":
		l.events.Add("start " + service)
	case "service stopped":
		l.events.Add("stop " + service)
	default:
		if r.Level >= slog.LevelWarn {
			l.warned.Add(r.Message)
		}
	}
	return nil
}
func (l *runLog) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *runLog) WithGroup(string) slog.Handler      { return l }

// Run's shutdown ends and joins the servers' requests before it lets go of the
// shards: a WebSocket on the public server and a call on the control server
// still running after the grace period both end, their shards are let go and
// the lease is given up.
func TestRunJoinsRequestsBeforeLettingGo(t *testing.T) {
	t.Parallel()
	var started apptest.Log[string]
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == appReadyPath {
			io.WriteString(w, `{"app": "test", "kinds": {"users": {"public": true, "sync": "local", "idle_timeout": "1h"}}}`)
			return
		}
		switch q.URL.Path {
		case "/ws":
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			defer conn.Close()
			rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
			rw.Flush()
			started.Add(q.URL.Path)
			io.Copy(io.Discard, rw) // until the node closes it
		case "/slow":
			started.Add(q.URL.Path)
			<-q.Context().Done()
		}
	}))
	defer fake.Close()
	dir := t.TempDir()
	vars := map[string]string{
		"SHARDLITE_NODE": "run-join-test", "SHARDLITE_BLOB": "s3://unused",
		"SHARDLITE_DATA": filepath.Join(dir, "data"),
		"SHARDLITE_APP":  fake.URL, "SHARDLITE_LISTEN_PUBLIC": "127.0.0.1:0", "SHARDLITE_LISTEN_CONTROL": "127.0.0.1:0", "SHARDLITE_LISTEN_PEER": "127.0.0.1:0",
		"SHARDLITE_FLUSH": "50ms",
	}
	env, err := ParseEnv(func(k string) string { return vars[k] })
	if err != nil {
		t.Fatal(err)
	}
	mem := blob.NewMemory()
	var log runLog

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type addrs struct{ public, control string }
	up := make(chan addrs, 1)
	ran := make(chan error, 1)
	go func() {
		ran <- Run(ctx, env, slog.New(&log), func(public, control, peer string) { up <- addrs{public, control} }, WithBackend(mem), withGrace(200*time.Millisecond))
	}()
	var a addrs
	select {
	case a = <-up:
	case err := <-ran:
		t.Fatalf("Run ended early: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("the node did not come up")
	}
	// The first requests wait for the poll to land the declaration.
	eventually(t, "the app's declaration", func() bool {
		resp, err := http.Get("http://" + a.public + readyPath)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == 200
	})

	// A call creates the shard the client connects to.
	q, _ := http.NewRequest("GET", "http://"+a.control+"/", nil)
	q.Header.Set("Shardlite-Target", "test.users/ws")
	if resp, err := http.DefaultClient.Do(q); err != nil || resp.StatusCode != 200 {
		t.Fatalf("creating users/ws: %v %v", resp, err)
	} else {
		resp.Body.Close()
	}
	conn, err := net.Dial("tcp", a.public)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET /ws?shardlite-target=test.users/ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
	br := bufio.NewReader(conn)
	if resp, err := http.ReadResponse(br, nil); err != nil || resp.StatusCode != 101 {
		t.Fatalf("upgrade: %v %v", resp, err)
	}
	slow := make(chan error, 1)
	go func() {
		q, _ := http.NewRequest("GET", "http://"+a.control+"/slow", nil)
		q.Header.Set("Shardlite-Target", "test.users/call")
		resp, err := http.DefaultClient.Do(q)
		if err == nil {
			_, err = io.ReadAll(resp.Body)
			resp.Body.Close()
		}
		slow <- err
	}()
	started.WaitFor(t, 10*time.Second, "the requests to reach the app", func(p []string) bool {
		return slices.Contains(p, "/ws") && slices.Contains(p, "/slow")
	})

	cancel()
	select {
	case err := <-ran:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return")
	}
	// A shard still held when Run let go of the shards would have been left
	// active, with the lease kept.
	for _, m := range log.warned.All() {
		if strings.Contains(m, "not deactivated") || strings.Contains(m, "releasing the lease") || strings.Contains(m, "still running") {
			t.Fatalf("Run logged %q", m)
		}
	}
	<-slow // ended, with whatever answer the node had time to give
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := br.ReadString('\n'); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("the WebSocket is still open: %v", err)
	}
}

// runRig is Run on the in-memory store in front of a fake app that answers
// everything with 200.
type runRig struct {
	t       *testing.T
	env     Env
	mem     *blob.Memory
	hooked  *blobtest.Hooked // mem, as Run sees it
	log     runLog
	cancel  context.CancelFunc
	ran     chan error
	public  string
	control string
	peer    string
}

func startRun(t *testing.T, name string, vars map[string]string, opts ...RunOption) *runRig {
	t.Helper()
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == appReadyPath {
			io.WriteString(w, runKinds)
		}
	}))
	t.Cleanup(fake.Close)
	dir := t.TempDir()
	all := map[string]string{
		"SHARDLITE_NODE": name, "SHARDLITE_BLOB": "s3://unused",
		"SHARDLITE_DATA": filepath.Join(dir, "data"),
		"SHARDLITE_APP":  fake.URL, "SHARDLITE_LISTEN_PUBLIC": "127.0.0.1:0", "SHARDLITE_LISTEN_CONTROL": "127.0.0.1:0", "SHARDLITE_LISTEN_PEER": "127.0.0.1:0",
		"SHARDLITE_FLUSH": "50ms",
	}
	for k, v := range vars {
		all[k] = v
	}
	env, err := ParseEnv(func(k string) string { return all[k] })
	if err != nil {
		t.Fatal(err)
	}
	r := &runRig{t: t, env: env, mem: blob.NewMemory(), ran: make(chan error, 1)}
	r.hooked = blobtest.WithHook(r.mem)
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	up := make(chan [3]string, 1)
	opts = append([]RunOption{WithBackend(r.hooked), withGrace(200 * time.Millisecond)}, opts...)
	go func() {
		r.ran <- Run(ctx, env, slog.New(&r.log), func(public, control, peer string) { up <- [3]string{public, control, peer} }, opts...)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.ran:
		case <-time.After(30 * time.Second):
			t.Error("Run did not return")
		}
	})
	select {
	case a := <-up:
		r.public, r.control, r.peer = a[0], a[1], a[2]
	case err := <-r.ran:
		r.ran <- err
		t.Fatalf("Run ended early: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("the node did not come up")
	}
	// The kinds come from the app's declaration now, not a file read at
	// startup: the first requests wait for the poll to land.
	eventually(t, "the app's declaration", func() bool {
		resp, err := http.Get("http://" + r.public + readyPath)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == 200
	})
	return r
}

// wait is what Run returned, once it has.
func (r *runRig) wait() error {
	r.t.Helper()
	select {
	case err := <-r.ran:
		r.ran <- err // for the cleanup
		return err
	case <-time.After(30 * time.Second):
		r.t.Fatal("Run did not return")
		return nil
	}
}

// lease is the node's lease as it is in blob storage.
func (r *runRig) lease() *coord.NodeLease {
	r.t.Helper()
	l, err := coord.NewRegistry(blob.WithPrefix(r.mem, r.env.Prefix), r.env.Name, coord.RegistryOptions{}).Get(context.Background(), r.env.Name)
	if err != nil {
		r.t.Fatal(err)
	}
	return l
}

// in fails unless the events happened in the order of the groups: every event
// of a group after every event of the group before it.
func (r *runRig) in(groups ...[]string) {
	r.t.Helper()
	got := r.log.events.All()
	for i := 1; i < len(groups); i++ {
		for _, a := range groups[i-1] {
			for _, b := range groups[i] {
				ia, ib := slices.Index(got, a), slices.Index(got, b)
				if ia < 0 || ib < 0 || ia > ib {
					r.t.Fatalf("want %q before %q, events: %v", a, b, got)
				}
			}
		}
	}
}

// stopOrder is the order of Run's shutdown: the public server, the control and
// peer servers and the shards. The journal (the writer and the keeper) stops
// last, and has no line of its own.
var stopOrder = [][]string{{"stop public server"}, {"stop control server", "stop peer server"}, {"stop catalog"}}

// Run starts every service at once, with everything built before, and stops
// them one supervisor at a time: the services that depend on nothing (the ready poll,
// the journal expiry and pprof) stop with the public server,
// first. A shard being let go of holds up the shards' stop.
func TestRunStartsAndStopsInOrder(t *testing.T) {
	t.Parallel()
	r := startRun(t, "run-order-test", map[string]string{"SHARDLITE_LISTEN_PPROF": "127.0.0.1:0"})
	all := []string{"public server", "control server", "peer server", "ready poll", "catalog", "journal expiry", "pprof"}
	r.log.events.WaitFor(t, 5*time.Second, "every service to start", func(e []string) bool {
		for _, p := range all {
			if !slices.Contains(e, "start "+p) {
				return false
			}
		}
		return true
	})
	resp, err := http.Get("http://" + r.control + "/x?shardlite-target=test.users/o1") // a call, which creates the shard
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	release := make(chan struct{})
	r.hooked.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if after || op == blobtest.OpGet || op == blobtest.OpList || !strings.HasSuffix(key, shardfile.ManifestKey(shardName("test.users/o1"))) {
			return nil
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})
	independent := []string{"stop ready poll", "stop journal expiry", "stop pprof"}

	r.cancel()
	r.log.events.WaitFor(t, 10*time.Second, "the services that depend on nothing to stop", func(e []string) bool {
		for _, p := range independent {
			if !slices.Contains(e, p) {
				return false
			}
		}
		return true
	})
	close(release)
	if err := r.wait(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	r.in(stopOrder...)
	r.in(independent, []string{"stop catalog"})
	if l := r.lease(); !l.Released {
		t.Fatalf("the lease was not released: %+v", l)
	}
}

// A lost lease stops the node at once: Run returns the lease error and the
// lease is left as it is. The supervisors still stop in order (no shard is let go of
// with a server still running), and nothing is let go of.
func TestRunStopsAtOnceWhenTheLeaseIsLost(t *testing.T) {
	t.Parallel()
	r := startRun(t, "run-lost-test", map[string]string{"SHARDLITE_LEASE_TTL": "1500ms"})
	// Another life of the node takes the lease. The keeper's next renewal finds it.
	other, err := coord.NewRegistry(blob.WithPrefix(r.mem, r.env.Prefix), r.env.Name, coord.RegistryOptions{TTL: time.Hour}).Acquire(context.Background(), coord.NodeInfo{Name: r.env.Name})
	if err != nil {
		t.Fatal(err)
	}
	err = r.wait()
	if !errors.Is(err, coord.ErrLeaseLost) {
		t.Fatalf("Run returned %v, want a fatal lease error", err)
	}
	r.in(stopOrder...)
	if l := r.lease(); l.Released || l.Epoch != other.Epoch {
		t.Fatalf("the lease is %+v, want the other life's (epoch %d) as it took it", l, other.Epoch)
	}
}

// A fatal error during the shutdown is still returned: the lease is lost while
// a shard is being let go of, and Run returns the lease error and keeps the
// lease.
func TestRunFatalErrorDuringShutdown(t *testing.T) {
	t.Parallel()
	r := startRun(t, "run-fatal-stop-test", map[string]string{"SHARDLITE_LEASE_TTL": "1500ms"})
	resp, err := http.Get("http://" + r.control + "/x?shardlite-target=test.users/fs1") // a call, which creates the shard
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	r.hooked.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if after || op == blobtest.OpGet || op == blobtest.OpList || !strings.HasSuffix(key, shardfile.ManifestKey(shardName("test.users/fs1"))) {
			return nil
		}
		once.Do(func() { close(entered) })
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})
	r.cancel()
	select {
	case <-entered: // letting go of users/fs1
	case <-time.After(10 * time.Second):
		t.Fatalf("the shard was not let go of: %v", r.log.events.All())
	}
	other, err := coord.NewRegistry(blob.WithPrefix(r.mem, r.env.Prefix), r.env.Name, coord.RegistryOptions{TTL: time.Hour}).Acquire(context.Background(), coord.NodeInfo{Name: r.env.Name})
	if err != nil {
		t.Fatal(err)
	}
	r.log.warned.WaitFor(t, 10*time.Second, "the keeper to find the lease lost", func(w []string) bool {
		return slices.Contains(w, "node lease is no longer valid, the node must stop")
	})
	close(release)
	err = r.wait()
	if !errors.Is(err, coord.ErrLeaseLost) {
		t.Fatalf("Run returned %v, want a fatal lease error", err)
	}
	r.in(stopOrder...)
	if l := r.lease(); l.Released || l.Epoch != other.Epoch {
		t.Fatalf("the lease is %+v, want the other life's (epoch %d) as it took it", l, other.Epoch)
	}
	for _, m := range r.log.warned.All() {
		if strings.Contains(m, "still running") {
			t.Fatalf("Run logged %q", m)
		}
	}
}

// brokenListener is a listener whose Accept fails once broken is closed.
type brokenListener struct {
	net.Listener
	broken chan struct{}
}

var errListenerBroke = errors.New("listener broke")

func (l *brokenListener) Accept() (net.Conn, error) {
	accepted := make(chan struct{})
	defer close(accepted)
	go func() { // the test's own: it ends with Accept
		select {
		case <-l.broken:
			l.Listener.Close()
		case <-accepted:
		}
	}()
	c, err := l.Listener.Accept()
	select {
	case <-l.broken:
		if c != nil {
			c.Close()
		}
		return nil, errListenerBroke
	default:
	}
	return c, err
}

// Any other failure stops the node gracefully: a public listener that fails
// ends the public server, and Run stops the rest in order, gives up the lease
// (every shard was let go) and returns the failure.
func TestRunStopsGracefullyWhenAPartFails(t *testing.T) {
	t.Parallel()
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := &brokenListener{Listener: inner, broken: make(chan struct{})}
	r := startRun(t, "run-fail-test", nil, withPublicListener(ln))
	close(ln.broken)
	err = r.wait()
	if !errors.Is(err, errListenerBroke) {
		t.Fatalf("Run returned %v, want the listener's error", err)
	}
	r.in(stopOrder...)
	if l := r.lease(); !l.Released {
		t.Fatalf("the lease was not released: %+v", l)
	}
}

// The peer listener answers who the node is over h2c, and nothing else. The
// lease publishes its address, SHARDLITE_ADVERTISE_PEER if set.
func TestPeerListenerSaysWhoTheNodeIs(t *testing.T) {
	t.Parallel()
	r := startRun(t, "peer-test", map[string]string{"SHARDLITE_ADVERTISE_PEER": "peer-test.internal:2281"})
	if got := r.lease().Address; got != "peer-test.internal:2281" {
		t.Fatalf("the lease's address is %q", got)
	}
	protos := new(http.Protocols)
	protos.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: &http.Transport{Protocols: protos}}
	defer client.CloseIdleConnections()
	resp, err := client.Get("http://" + r.peer + "/_shardlite/node")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got NodeInfoReply
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if resp.ProtoMajor != 2 || got.Name != "peer-test" || got.Epoch != r.lease().Epoch {
		t.Fatalf("proto %d, reply %+v, lease %+v", resp.ProtoMajor, got, r.lease())
	}
	for _, path := range []string{"/", "/_shardlite/ready", "/x?shardlite-target=test.users/o1"} {
		resp, err := client.Get("http://" + r.peer + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s answered %d, want 404", path, resp.StatusCode)
		}
	}
	// Not even a delete a node forwards: there is no such request.
	req, err := http.NewRequest(http.MethodDelete, "http://"+r.peer+"/_shardlite/shards/users/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Shardlite-Origin", "delete")
	req.Header.Set("Shardlite-Target", "test.users/x")
	if resp, err := client.Do(req); err != nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("a delete on the peer listener: %v, %v", resp, err)
	} else {
		resp.Body.Close()
	}
}

// Without SHARDLITE_ADVERTISE_PEER the node publishes the peer listener's address
// unless it is a wildcard, which other nodes can't use.
func TestPeerAddressRule(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if got, err := peerAddress("", ln); err != nil || got != ln.Addr().String() {
		t.Fatalf("got %q, %v", got, err)
	}
	if got, err := peerAddress("h:1", ln); err != nil || got != "h:1" {
		t.Fatalf("got %q, %v", got, err)
	}
	wild, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer wild.Close()
	if _, err := peerAddress("", wild); err == nil || !strings.Contains(err.Error(), "SHARDLITE_ADVERTISE_PEER") {
		t.Fatalf("a wildcard was accepted: %v", err)
	}
}
