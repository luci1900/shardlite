package node

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/luci1900/shardlite/internal/apptest"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/lease"
)

// slowFlush makes the flush interval long enough for a test to tell a request
// that waited for a flush from one that did not.
func slowFlush(o *ShardsetOptions) { o.Flush.Interval = 300 * time.Millisecond }

// manualFlush stops the flush loop from ever ticking, so the test decides when
// a cycle runs (r.cycle) and what a request waited for is read off the cycle
// counter, not the clock. Idle shards stay active too: a deactivation waits for a
// cycle, which only the test would run.
func manualFlush(o *ShardsetOptions) {
	o.Flush.Interval, o.Flush.NoEarly, o.Lifecycle.NoIdle = time.Hour, true, true
}

func (r *srig) cycle() { r.m.flush.cycle(context.Background()) }

// withoutCycle runs do and requires it to return without any cycle having run:
// a response that must not wait for a flush.
func (r *srig) withoutCycle(do func()) {
	r.t.Helper()
	before := r.m.Flusher().Completed()
	done := make(chan struct{})
	go func() { do(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		r.t.Fatal("the request did not return, and no flush cycle ran")
	}
	if got := r.m.Flusher().Completed(); got != before {
		r.t.Fatalf("%d cycles ran", got-before)
	}
}

// withCycles runs do and requires it not to return until a cycle has run; it
// then runs cycles until do returns: a response that must wait for a flush.
func (r *srig) withCycles(do func()) {
	r.t.Helper()
	done := make(chan struct{})
	go func() { do(); close(done) }()
	for ran := false; ; ran = true {
		select {
		case <-done:
			if !ran {
				r.t.Fatal("the request returned before any flush cycle ran")
			}
			return
		case <-time.After(20 * time.Millisecond):
		}
		r.cycle()
	}
}

func TestGateOnResponses(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	r.create("wallet/g1", "users/g2", "users/g3")
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		r.commit(q.Header.Get("Shardlite-File"))
		if v := q.URL.Query().Get("sync"); v != "" {
			w.Header().Set("Shardlite-Sync", v)
		}
		io.WriteString(w, "done")
	})

	// A storage-tier kind: when the client has the response, the write is in blob storage.
	var resp *http.Response
	r.withCycles(func() {
		resp, _ = r.req("POST", r.pub, "/w", map[string]string{"Shardlite-Target": "wallet/g1"}, "")
	})
	if resp.StatusCode != 200 || r.rows("wallet/g1") != 1 {
		t.Fatalf("status %d, rows in blob storage: %d", resp.StatusCode, r.rows("wallet/g1"))
	}
	// A none-tier kind: the answer waits for nothing, so no cycle has run.
	r.withoutCycle(func() {
		resp, _ = r.req("POST", r.pub, "/w", map[string]string{"Shardlite-Target": "users/g2"}, "")
	})
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	// Shardlite-Sync: storage on a none kind waits for blob storage.
	r.withCycles(func() {
		r.req("POST", r.pub, "/w?sync=storage", map[string]string{"Shardlite-Target": "users/g2"}, "")
	})
	if r.rows("users/g2") != 2 {
		t.Fatalf("sync storage by header: %d rows in blob storage", r.rows("users/g2"))
	}
	// The shard is created before the answer, whatever the tier.
	r.withoutCycle(func() {
		r.req("POST", r.pub, "/w", map[string]string{"Shardlite-Target": "users/g3"}, "")
	})
	if _, err := r.manifest("users/g3"); err != nil {
		t.Fatalf("the response to the first write came before the shard existed: %v", err)
	}

	// The header never reaches the client.
	r.withCycles(func() {
		resp, _ = r.req("POST", r.pub, "/w?sync=storage", map[string]string{"Shardlite-Target": "users/g2"}, "")
	})
	if resp.Header.Get("Shardlite-Sync") != "" {
		t.Fatal("Shardlite-Sync reached the client")
	}

	// A tier name that doesn't exist is refused: copying the kind's tier could
	// quietly give less than what the app said.
	resp, _ = r.req("POST", r.pub, "/w?sync=maybe", map[string]string{"Shardlite-Target": "users/g2"}, "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("Shardlite-Sync: maybe answered %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	// So is the tier that was dropped.
	resp, _ = r.req("POST", r.pub, "/w?sync=peer", map[string]string{"Shardlite-Target": "users/g2"}, "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("Shardlite-Sync: peer answered %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

// The none tier: the answer waits for nothing, not for a cycle and not for
// storage. The write to storage is held back here, and no cycle runs, so an
// answer that waited for either would never come.
func TestNoneTierWaitsForNothing(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	r.create("users/none")
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		r.commit(q.Header.Get("Shardlite-File"))
		io.WriteString(w, "done")
	})
	release := make(chan struct{})
	var freed sync.Once
	letGo := func() { freed.Do(func() { close(release) }) }
	t.Cleanup(letGo)
	r.local.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op == blobtest.OpCreate && strings.HasPrefix(key, journal.Root) {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	r.withoutCycle(func() {
		resp, _ := r.req("POST", r.pub, "/w", map[string]string{"Shardlite-Target": "users/none"}, "")
		if resp.StatusCode != 200 {
			t.Errorf("status %d", resp.StatusCode)
		}
	})
	if rows := r.rows("users/none"); rows != 0 {
		t.Fatalf("storage had %d rows: the write was not held back", rows)
	}
	// The commit still reaches storage once a cycle runs.
	letGo()
	r.cycle()
	apptest.Eventually(r.t, apptest.Timeout, "storage to hold the commit", func() bool { return r.rows("users/none") == 1 })
}

func TestGateOnStreams(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, slowFlush)
	r.create("wallet/sse")
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 1; i <= 3; i++ {
			r.commit(q.Header.Get("Shardlite-File"))
			fmt.Fprintf(w, "data: %d\n\n", i)
			w.(http.Flusher).Flush()
		}
	})
	q, _ := http.NewRequest("GET", r.pub+"/events", nil)
	q.Header.Set("Shardlite-Target", "wallet/sse")
	resp, err := r.client.Do(q)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	for i := 1; i <= 3; i++ {
		line, err := br.ReadString('\n')
		if err != nil || line != fmt.Sprintf("data: %d\n", i) {
			t.Fatalf("%q %v", line, err)
		}
		// Chunk i left the app after commit i. It must not reach the client before commit i is durable.
		if got := r.rows("wallet/sse"); got < i {
			t.Fatalf("event %d reached the client with %d rows in blob storage", i, got)
		}
		br.ReadString('\n')
	}
}

func TestGateOnUpgradedConnections(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, slowFlush)
	r.create("wallet/ws")
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		path := q.Header.Get("Shardlite-File")
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
		rw.Flush()
		sc := bufio.NewScanner(rw)
		for sc.Scan() {
			r.commit(path) // a write on each message, then the echo
			rw.WriteString("echo " + sc.Text() + "\n")
			rw.Flush()
		}
	})
	conn, err := net.Dial("tcp", strings.TrimPrefix(r.pub, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET /ws HTTP/1.1\r\nHost: x\r\nShardlite-Target: wallet/ws\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n")
	br := bufio.NewReader(conn)
	if resp, err := http.ReadResponse(br, nil); err != nil || resp.StatusCode != 101 {
		t.Fatalf("%v %v", resp, err)
	}
	for i := 1; i <= 3; i++ {
		fmt.Fprintf(conn, "m%d\n", i)
		line, err := br.ReadString('\n')
		if err != nil || line != fmt.Sprintf("echo m%d\n", i) {
			t.Fatalf("%q %v", line, err)
		}
		if got := r.rows("wallet/ws"); got < i {
			t.Fatalf("echo %d reached the client with %d rows in blob storage", i, got)
		}
	}
}

// A call to another shard is bytes leaving the caller: the caller's node holds it
// until what the caller committed is durable, if the caller's kind is on the `storage` tier.
func TestGateOnCalls(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	r.create("wallet/call", "users/call", "wallet/call2")
	var seen atomic.Int64 // rows of the caller in blob storage when the callee got the call
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		switch q.URL.Path {
		case "/callee":
			caller := q.Header.Get("Shardlite-Caller")
			seen.Store(int64(r.rows(caller)))
		default: // the caller commits, then calls another shard through the control API
			r.commit(q.Header.Get("Shardlite-File"))
			c, _ := http.NewRequest("POST", r.ctl+"/callee", nil)
			c.Header.Set("Shardlite-Target", "slow/callee")
			c.Header.Set(hdrContext, q.Header.Get(hdrContext))
			if v := q.URL.Query().Get("callsync"); v != "" {
				c.Header.Set("Shardlite-Sync", v)
			}
			resp, err := r.client.Do(c)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			resp.Body.Close()
		}
		if v := q.URL.Query().Get("sync"); v != "" {
			w.Header().Set("Shardlite-Sync", v)
		}
		io.WriteString(w, "ok")
	})
	// A storage-tier caller: its response is held too, but the callee must have seen the commit.
	r.withCycles(func() {
		r.req("POST", r.pub, "/go?sync=storage", map[string]string{"Shardlite-Target": "wallet/call"}, "")
	})
	if seen.Load() != 1 {
		t.Fatalf("the callee got the call when blob storage had %d of the caller's rows", seen.Load())
	}
	// A none-tier caller: the call waits for nothing, and the header can ask
	// for storage.
	seen.Store(0)
	r.withCycles(func() {
		r.req("POST", r.pub, "/go?callsync=storage", map[string]string{"Shardlite-Target": "users/call"}, "")
	})
	if seen.Load() != 1 {
		t.Fatalf("Shardlite-Sync: storage on a call: %d rows", seen.Load())
	}
}

func TestSyncEndpointAndLeaseLoss(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, slowFlush)
	r.create("users/s1", "wallet/s2")
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		r.commit(q.Header.Get("Shardlite-File"))
	})
	// A write to a sync-off shard, then POST /_shardlite/sync: durable on return.
	r.req("POST", r.pub, "/w", map[string]string{"Shardlite-Target": "users/s1"}, "")
	if resp, _ := r.req("POST", r.ctl, "/_shardlite/sync", nil, ""); resp.StatusCode != 204 {
		t.Fatalf("sync: %d", resp.StatusCode)
	}
	if r.rows("users/s1") != 1 {
		t.Fatal("sync returned before the commit was in blob storage")
	}

	// Another life of the node takes the lease: this one is fenced out at its next renewal.
	other := lease.NewRegistry(r.local, "n1", lease.RegistryOptions{TTL: time.Hour})
	if _, err := other.Acquire(context.Background(), lease.NodeInfo{Name: "n1"}); err != nil {
		t.Fatal(err)
	}
	apptest.Eventually(t, apptest.Timeout, "the lease to be lost", func() bool { return r.n.Keeper.Check() != nil })
	if resp, _ := r.req("POST", r.ctl, "/_shardlite/sync", nil, ""); resp.StatusCode != 503 {
		t.Fatalf("sync after the lease was lost: %d", resp.StatusCode)
	} else {
		wantReason(t, resp, reasonLeaseLost)
	}
	if resp, _ := r.req("POST", r.pub, "/w", map[string]string{"Shardlite-Target": "wallet/s2"}, ""); resp.StatusCode != 503 {
		t.Fatalf("request after the lease was lost: %d", resp.StatusCode)
	} else {
		wantReason(t, resp, reasonLeaseLost)
	}
	if resp, _ := r.req("GET", r.pub, "/_shardlite/ready", nil, ""); resp.StatusCode != 503 {
		t.Fatalf("ready after the lease was lost: %d", resp.StatusCode)
	} else {
		wantReason(t, resp, reasonNotReady)
	}
	_ = errors.New
}

func TestCreationOverHTTP(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		path := q.Header.Get("Shardlite-File")
		if q.URL.Path == "/write" {
			r.app.Must(t, "open", path, path) // creates its tables
			r.app.Must(t, "insert", path, "1", "64")
		} else {
			r.app.Must(t, "openro", path, path) // opens without writing
		}
		r.app.Must(t, "close", path) // as the app contract asks, so the shard can deactivate
		io.WriteString(w, "ok")
	})
	// A call creates the shard at activation: a read finds it created.
	if resp, _ := r.req("GET", r.ctl, "/read", map[string]string{"Shardlite-Target": "users/read"}, ""); resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	if _, err := r.manifest("users/read"); err != nil {
		t.Fatalf("no manifest for an active shard: %v", err)
	}
	if resp, _ := r.req("POST", r.ctl, "/write", map[string]string{"Shardlite-Target": "users/write"}, ""); resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
	// Once idle, the shard that was only read is deleted and the written one is kept.
	apptest.Eventually(t, apptest.Timeout, "idle deactivation", func() bool { return len(r.m.Active()) == 0 })
	if _, err := r.manifest("users/read"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("a shard that was only read was kept: %v", err)
	}
	if _, err := r.manifest("users/write"); err != nil {
		t.Fatalf("a written shard was deleted: %v", err)
	}
}

// Once the node's lease is past its deadline no gate lets output through, sync
// on or off and with no new frames to wait for: another node may own the shard
// by now. A response is refused with 503, and a stream is cut at its next chunk.
func TestGatesCheckTheLease(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("wallet/l1", "slow/l2", "slow/l3")
	next := make(chan struct{})
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		io.WriteString(w, "data: 1\n\n")
		if q.URL.Path != "/events" {
			return
		}
		w.(http.Flusher).Flush()
		select {
		case <-next:
		case <-q.Context().Done():
			return
		}
		io.WriteString(w, "data: 2\n\n")
	})
	targets := []string{"wallet/l1", "slow/l2"} // `storage` and `none`
	for _, target := range targets {
		if resp, _ := r.req("GET", r.pub, "/", map[string]string{"Shardlite-Target": target}, ""); resp.StatusCode != 200 {
			t.Fatalf("%s: %d", target, resp.StatusCode)
		}
		apptest.Eventually(t, apptest.Timeout, "the shard to be captured", func() bool { return !r.shard(target).cap.hasNewFrames() })
	}
	q, _ := http.NewRequest("GET", r.pub+"/events", nil)
	q.Header.Set("Shardlite-Target", "slow/l3")
	stream, err := r.client.Do(q)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Body.Close()
	br := bufio.NewReader(stream.Body)
	if line, err := br.ReadString('\n'); err != nil || line != "data: 1\n" {
		t.Fatalf("%q %v", line, err)
	}

	r.leaseAhead.Store(int64(2 * time.Hour))
	for _, target := range targets {
		if resp, body := r.req("GET", r.pub, "/", map[string]string{"Shardlite-Target": target}, ""); resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
			t.Fatalf("%s with the lease past its deadline: %d %q", target, resp.StatusCode, body)
		}
	}
	close(next)
	if rest, err := io.ReadAll(br); err == nil || strings.Contains(string(rest), "data: 2") {
		t.Fatalf("the stream went on with the lease past its deadline: %q %v", rest, err)
	}
}
