package node

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
)

// The durable send of docs/guide.md, Reminders, across two nodes. In the
// transaction that causes it, the sender inserts a reminder due now and a row of
// its own under the same key naming the target, the path and the payload. Its
// wake handler selects the due reminders, makes each call with Idempotency-Key
// set to the key and deletes both rows once the call answered 2xx (leaving them
// and answering 503 when a call fails, so the node wakes it again with backoff).
// Its calls carry the context of the wake, so the receiver's node reads the
// sender as the caller and the call is gated on the sender's commits (the call
// is made on n0, which the sender lives on).
// The receiver records (Shardlite-Caller, Idempotency-Key) in a table of its own
// in the same transaction as its work, and skips work it has seen.
//
// Here the call fails once before the receiver did anything, and the sender
// then answers 503 after a call that succeeded, so the call is made three times
// and the receiver does its work once.
func TestDurableSendBetweenNodes(t *testing.T) {
	c := newClusterWith(t, 2, func(o *ShardsetOptions) {
		o.Reminders.Backoff, o.Reminders.MaxBackoff = 20*time.Millisecond, 50*time.Millisecond
	})
	n0, n1 := c.nodes[0], c.nodes[1]
	app := apptest.Start(t)
	paths := make(chan string, 2)

	var wakes, receiverCalls atomic.Int32
	var answeredAfterCall atomic.Bool
	var mu sync.Mutex
	var seen []http.Header // the receiver's requests for /work

	n0.serve(func(w http.ResponseWriter, q *http.Request) {
		path := q.Header.Get("Shardlite-File")
		if q.URL.Path != remindersPath {
			paths <- path
			return
		}
		// The sender's wake handler.
		wakes.Add(1)
		wake := q.Header.Get(hdrContext) // the wake's context, passed on to each call
		out, err := app.Do("query", "q", path, `SELECT group_concat(r.key || '|' || s.target || '|' || s.path || '|' || s.body, ';') FROM _shardlite_reminders r JOIN sends s ON s.key = r.key WHERE r.due <= unixepoch()`)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		out, _ = strconv.Unquote(out)
		for _, row := range strings.Split(out, ";") {
			f := strings.Split(row, "|")
			if len(f) != 4 {
				continue
			}
			key, target, cpath, body := f[0], f[1], f[2], f[3]
			call, _ := http.NewRequest("POST", "http://"+n0.Control+cpath, strings.NewReader(body))
			call.Header.Set("Shardlite-Target", target)
			call.Header.Set(hdrContext, wake)
			call.Header.Set("Idempotency-Key", key)
			resp, err := clusterClient.Do(call)
			if err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			if answeredAfterCall.CompareAndSwap(false, true) {
				w.WriteHeader(http.StatusServiceUnavailable) // the sender failed after the call succeeded
				return
			}
			if _, err := app.Do("exec", path, `BEGIN; DELETE FROM sends WHERE key = '`+key+`'; DELETE FROM _shardlite_reminders WHERE key = '`+key+`'; COMMIT;`); err != nil {
				t.Error(err)
			}
		}
	})
	n1.serve(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path != "/work" {
			paths <- q.Header.Get("Shardlite-File")
			return
		}
		mu.Lock()
		seen = append(seen, q.Header.Clone())
		mu.Unlock()
		if receiverCalls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable) // the receiver failed before doing anything
			return
		}
		b, _ := io.ReadAll(q.Body)
		path := q.Header.Get("Shardlite-File")
		// The receiver's work and its record of the call, in one transaction.
		_, err := app.Do("exec", path, `BEGIN; INSERT OR IGNORE INTO seen (caller, key) VALUES ('`+q.Header.Get("Shardlite-Caller")+`', '`+q.Header.Get("Idempotency-Key")+`'); INSERT INTO work (payload) SELECT '`+string(b)+`' WHERE changes() > 0; COMMIT;`)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	n0.create("users/s")
	ask(t, n0, "users/s")
	sender := <-paths
	n1.create("users/r")
	ask(t, n1, "users/r")
	receiver := <-paths
	for _, p := range []string{sender, receiver} {
		app.Must(t, "open", p, p)
		defer app.Must(t, "close", p)
	}
	app.Must(t, "exec", receiver, `CREATE TABLE seen (caller TEXT, key TEXT, PRIMARY KEY (caller, key)); CREATE TABLE work (payload TEXT)`)
	app.Must(t, "exec", sender, `CREATE TABLE sends (key TEXT PRIMARY KEY, target TEXT, path TEXT, body TEXT)`)
	app.Must(t, "exec", sender, `BEGIN; INSERT INTO sends VALUES ('send:1', 'users/r', '/work', 'hello'); INSERT INTO _shardlite_reminders (key, due) VALUES ('send:1', unixepoch()); COMMIT;`)

	apptest.Eventually(t, 30*time.Second, "the reminder to be handled", func() bool {
		return app.Must(t, "query", "q", sender, `SELECT count(*) FROM _shardlite_reminders`) == `"0"`
	})
	if n := receiverCalls.Load(); n != 3 {
		t.Fatalf("the receiver was called %d times, want 3 (one failed, one did the work, one was a repeat)", n)
	}
	if n := wakes.Load(); n != 3 {
		t.Fatalf("the sender was woken %d times, want 3 (the call failed, the sender failed after it, the call was repeated)", n)
	}
	if got := app.Must(t, "query", "q", receiver, `SELECT count(*) FROM work`); got != `"1"` {
		t.Fatalf("the receiver did its work %s times, want once", got)
	}
	if got := app.Must(t, "query", "q", sender, `SELECT count(*) FROM sends`); got != `"0"` {
		t.Fatalf("%s sends left", got)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, h := range seen {
		if h.Get("Shardlite-Caller") != "users/s" || h.Get("Idempotency-Key") != "send:1" {
			t.Fatalf("the receiver saw %v", h)
		}
	}
}

// Shardlite-Inbox: true from a client (on the public listener, forwarded or
// not), from the app (on the control listener) or from a peer is an ordinary
// header: the request is served as any other would be, and nothing is put in a
// table.
func TestInboxHeaderIsOrdinaryBetweenNodes(t *testing.T) {
	c := newCluster(t, 2)
	n0, n1 := c.nodes[0], c.nodes[1]
	var mu sync.Mutex
	var seen []http.Header
	n0.serve(func(w http.ResponseWriter, q *http.Request) {
		mu.Lock()
		seen = append(seen, q.Header.Clone())
		mu.Unlock()
		io.WriteString(w, "served")
	})
	n0.create("users/h")
	ask(t, n0, "users/h") // n0 owns it
	for _, via := range []struct{ name, base string }{
		{"public, local", "http://" + n0.Public},
		{"public, forwarded", "http://" + n1.Public},
		{"control", "http://" + n0.Control},
	} {
		mu.Lock()
		seen = nil
		mu.Unlock()
		resp, body := send(t, "POST", via.base+"/hello", map[string]string{"Shardlite-Target": "users/h", "Shardlite-Inbox": "true"}, "body")
		if resp.StatusCode != http.StatusOK || body != "served" {
			t.Fatalf("%s: answered %d %q, want the app's 200", via.name, resp.StatusCode, body)
		}
		mu.Lock()
		if len(seen) != 1 || seen[0].Get("Shardlite-Inbox") != "" {
			t.Fatalf("%s: the app saw %v", via.name, seen)
		}
		mu.Unlock()
	}
	// On the peer listener the header changes nothing: a request that says where
	// it came from is served as a forward, and one that doesn't is refused as it
	// would be without the header.
	for _, tc := range []struct {
		origin string
		status int
	}{{"call", http.StatusOK}, {"", http.StatusNotFound}} {
		mu.Lock()
		seen = nil
		mu.Unlock()
		hdr := map[string]string{"Shardlite-Target": "users/h", hdrContext: ctxOf("users/x", 0), "Shardlite-Inbox": "true"}
		if tc.origin != "" {
			hdr["Shardlite-Origin"] = tc.origin
		}
		resp, _ := send(t, "POST", "http://"+n0.Peer+"/hello", hdr, "body")
		if resp.StatusCode != tc.status {
			t.Fatalf("peer listener, origin %q: %d, want %d", tc.origin, resp.StatusCode, tc.status)
		}
		mu.Lock()
		for _, h := range seen {
			if h.Get("Shardlite-Inbox") != "" {
				t.Fatalf("the app saw %v", h)
			}
		}
		mu.Unlock()
	}
}
