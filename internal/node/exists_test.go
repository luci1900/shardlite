package node

import (
	"bytes"
	"errors"
	"github.com/luci1900/shardlite/internal/apptest"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// shardWrites counts the calls that would change a shard in blob storage.
func (r *mrig) shardWrites() int {
	n := 0
	for _, op := range []string{"create", "update", "delete"} {
		n += r.calls(op, shardfile.ManifestPrefix) + r.calls(op, shardfile.FilesPrefix)
	}
	return n
}

// A client reaches only shards that exist: for one that doesn't, the node reads
// its manifest once and answers as for an undeclared kind, without writing
// anything or asking the app. A call creates the shard, and the client then
// reaches it.
func TestClientsReachOnlyShardsThatExist(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	var served atomic.Int64
	h := http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) { served.Add(1); io.WriteString(w, "served") })
	r.handler.Store(&h)
	target := map[string]string{"Shardlite-Target": "slow/e1"}

	gets, writes := r.calls("get", shardfile.ManifestPrefix), r.shardWrites()
	resp, body := r.req("GET", r.pub, "/x", target, "")
	_, undeclared := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "slow2/e1"}, "")
	if resp.StatusCode != http.StatusNotFound || strings.Replace(body, `"slow"`, `"slow2"`, 1) != undeclared {
		t.Fatalf("a client's request for a missing shard: %d %q, an undeclared kind: %q", resp.StatusCode, body, undeclared)
	}
	if got := r.calls("get", shardfile.ManifestPrefix) - gets; got != 1 {
		t.Fatalf("%d manifest reads", got)
	}
	if got := r.shardWrites() - writes; got != 0 || served.Load() != 0 || r.m.IsActive(shardName("slow/e1")) {
		t.Fatalf("%d writes, %d requests to the app, active %v", got, served.Load(), r.m.IsActive(shardName("slow/e1")))
	}
	if _, err := r.manifest("slow/e1"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest: %v", err)
	}

	// gRPC clients get NOT_FOUND.
	resp, _ = r.req("POST", r.pub, "/svc/Method", map[string]string{"Shardlite-Target": "slow/e1", "Content-Type": "application/grpc"}, "")
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Grpc-Status") != "5" {
		t.Fatalf("gRPC: %d grpc-status %q", resp.StatusCode, resp.Header.Get("Grpc-Status"))
	}

	// A call that asks only for an existing shard doesn't create it either.
	ifExists := map[string]string{"Shardlite-Target": "slow/e1", "Shardlite-If-Exists": "true"}
	if resp, _ := r.req("GET", r.ctl, "/x", ifExists, ""); resp.StatusCode != http.StatusNotFound || served.Load() != 0 {
		t.Fatalf("a call with Shardlite-If-Exists: %d, %d requests to the app", resp.StatusCode, served.Load())
	}
	if _, err := r.manifest("slow/e1"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("manifest after the call with Shardlite-If-Exists: %v", err)
	}

	// A call creates it, and then the client and the same call reach it.
	if resp, body := r.req("GET", r.ctl, "/x", target, ""); resp.StatusCode != http.StatusOK || body != "served" {
		t.Fatalf("a call: %d %q", resp.StatusCode, body)
	}
	if _, err := r.manifest("slow/e1"); err != nil {
		t.Fatalf("no manifest after the call: %v", err)
	}
	for name, base := range map[string]string{"the client": r.pub, "a call with Shardlite-If-Exists": r.ctl} {
		hdr := target
		if base == r.ctl {
			hdr = ifExists
		}
		if resp, body := r.req("GET", base, "/x", hdr, ""); resp.StatusCode != http.StatusOK || body != "served" {
			t.Fatalf("%s after the call: %d %q", name, resp.StatusCode, body)
		}
	}
}

// A client's request forwarded from another node is held to the client rule by
// the node it reaches.
func TestForwardedClientRequestDoesNotCreate(t *testing.T) {
	c := newCluster(t, 1)
	nd := c.nodes[0]
	h2c := new(http.Protocols)
	h2c.SetUnencryptedHTTP2(true)
	client := &http.Client{Transport: &http.Transport{Protocols: h2c}}
	for _, tc := range []struct {
		origin string
		want   int
	}{{originClient, http.StatusNotFound}, {originCall, http.StatusOK}} {
		req, _ := http.NewRequest(http.MethodGet, "http://"+nd.Peer+"/x", nil)
		req.Header.Set(hdrTarget, "users/fwd-"+tc.origin)
		req.Header.Set(hdrOrigin, tc.origin)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Fatalf("forwarded with origin %s: %d, want %d", tc.origin, resp.StatusCode, tc.want)
		}
	}
}

// A shard that goes away (marked for deletion, or deleted for holding nothing)
// answers clients with 404 rather than coming back empty. The first shard of a
// kind deleted for holding nothing is logged at info, with the rule, and later
// ones at debug. A marked one is never logged at info.
func TestClientsGet404ForDeletedShards(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	var lmu sync.Mutex
	logger := slog.New(slog.NewTextHandler(writerFunc(func(p []byte) (int, error) { lmu.Lock(); defer lmu.Unlock(); return logs.Write(p) }), &slog.HandlerOptions{Level: slog.LevelDebug}))
	r := newSRigWith(t, func(o *ShardsetOptions) { o.Logger = logger })
	logged := func(level, shard string) bool {
		lmu.Lock()
		defer lmu.Unlock()
		for _, l := range strings.Split(logs.String(), "\n") {
			if strings.Contains(l, "level="+level) && strings.Contains(l, "deleted at deactivation") && strings.Contains(l, "shard="+shard) {
				return true
			}
		}
		return false
	}
	gone := func(target string) {
		t.Helper()
		apptest.Eventually(t, apptest.Timeout, target+" deleted", func() bool {
			_, err := r.manifest(target)
			return errors.Is(err, blob.ErrNotFound) && !r.m.IsActive(shardName(target))
		})
		if resp, _ := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": target}, ""); resp.StatusCode != http.StatusNotFound {
			t.Fatalf("a client after %s was deleted: %d", target, resp.StatusCode)
		}
	}

	// Marked for deletion.
	ref := r.acquire("users/d1")
	r.write(ref, "a")
	r.app.Must(t, "exec", "a", "CREATE TABLE _shardlite_delete (x)")
	r.app.Must(t, "close", "a")
	ref.Release()
	gone("users/d1")
	if logged("INFO", "users/d1") || !logged("DEBUG", "users/d1") {
		t.Fatalf("the marked shard's deletion was logged wrongly:\n%s", logs.String())
	}

	// Created by a call that writes nothing, twice.
	for _, target := range []string{"users/d2", "users/d3"} {
		if resp, _ := r.req("GET", r.ctl, "/x", map[string]string{"Shardlite-Target": target}, ""); resp.StatusCode != http.StatusNoContent {
			t.Fatalf("a call to %s: %d", target, resp.StatusCode)
		}
		gone(target)
	}
	if !logged("INFO", "users/d2") || logged("INFO", "users/d3") || !logged("DEBUG", "users/d3") {
		t.Fatalf("the empty shards' deletions were logged wrongly:\n%s", logs.String())
	}
}
