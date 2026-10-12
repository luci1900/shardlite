package node

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
)

// nodeGoroutines lists the goroutines running code of the node's packages (not
// the test's own, which are in _test.go files).
func nodeGoroutines() []string {
	buf := make([]byte, 1<<22)
	buf = buf[:runtime.Stack(buf, true)]
	var out []string
	for i, g := range strings.Split(string(buf), "\n\n") {
		if i == 0 || strings.Contains(g, "_test.go") {
			continue // the calling goroutine, and the test's helpers
		}
		for _, pkg := range []string{"internal/node.", "internal/capture.", "internal/segment.", "internal/coord.", "internal/shardfile.", "internal/blob."} {
			if strings.Contains(g, "luci1900/shardlite/"+pkg) {
				out = append(out, g)
				break
			}
		}
	}
	return out
}

// Run returns only after every goroutine it started has ended: the loops, the
// servers, the health poll, the flush cycle's syncs, the segment writer and
// the lease keeper.
func TestRunLeavesNoGoroutines(t *testing.T) {
	app := apptest.Start(t)
	var mu sync.Mutex
	opened := map[string]bool{}
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.URL.Path == "/health" {
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
	kinds := filepath.Join(dir, "kinds.json")
	if err := os.WriteFile(kinds, []byte(`{"kinds": {"users": {"public": true, "sync": "off", "idle_timeout": "1h"}, "wallet": {"public": true, "idle_timeout": "1h"}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	vars := map[string]string{
		"SHARDLITE_NODE": "run-test", "SHARDLITE_BLOB": filepath.Join(dir, "blobs"), "SHARDLITE_KINDS": kinds,
		"SHARDLITE_DATA": filepath.Join(dir, "data"), "SHARDLITE_META": filepath.Join(dir, "meta"),
		"SHARDLITE_APP": fake.URL, "SHARDLITE_LISTEN": "127.0.0.1:0", "SHARDLITE_CONTROL": "127.0.0.1:0",
		"SHARDLITE_FLUSH": "50ms", "SHARDLITE_PPROF": "127.0.0.1:0",
	}
	env, err := ParseEnv(func(k string) string { return vars[k] })
	if err != nil {
		t.Fatal(err)
	}
	before := len(nodeGoroutines())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr := make(chan string, 1)
	ran := make(chan error, 1)
	go func() {
		ran <- Run(ctx, env, slog.New(slog.DiscardHandler), func(public, control string) { addr <- public })
	}()
	var public string
	select {
	case public = <-addr:
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
	// Shards of both kinds with writes: lazy creation, capture, syncs, gated responses.
	for i := range 5 {
		for _, kind := range []string{"users", "wallet"} {
			q, _ := http.NewRequest("POST", "http://"+public+"/write", nil)
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
	if n := len(nodeGoroutines()); n <= before {
		t.Fatalf("%d node goroutines while running, %d before: the test is not looking at anything", n, before)
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
	left := nodeGoroutines()
	if len(left) > before {
		t.Fatalf("%d node goroutines left after Run returned (%d before it started):\n%s", len(left), before, strings.Join(left, "\n\n"))
	}
}
