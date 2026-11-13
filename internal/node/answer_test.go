package node

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/test/apptest"
)

// gRPC: h2c to the app, trailers passed through, streaming without buffering.
func TestServerGRPC(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("slow/rpc")
	var proto atomic.Int32
	// The app holds back its second frame until the test has the first. A node
	// that buffers the response never delivers it, and the safety timer lets go.
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	var buffered atomic.Bool
	safety := time.AfterFunc(10*time.Second, func() { buffered.Store(true); release() })
	defer safety.Stop()
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		proto.Store(int32(q.ProtoMajor))
		if q.Header.Get("Te") != "trailers" || q.Header.Get("Shardlite-Opened") != "slow/rpc" {
			http.Error(w, "bad", 400)
			return
		}
		io.Copy(io.Discard, q.Body)
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("Trailer", "Grpc-Status, Grpc-Message, Shardlite-Trailer")
		w.Write([]byte("frame1"))
		w.(http.Flusher).Flush()
		<-gate
		w.Write([]byte("frame2"))
		w.Header().Set("Grpc-Status", "0")
		w.Header().Set("Grpc-Message", "fine")
		w.Header().Set("Shardlite-Trailer", "x")
	})
	q, _ := http.NewRequest("POST", r.pub+"/svc/Method", strings.NewReader("request"))
	q.Header.Set("Content-Type", "application/grpc")
	q.Header.Set("Te", "trailers")
	q.Header.Set("Shardlite-Target", "slow/rpc")
	resp, err := r.h2c.Do(q) // the client reaches the node over h2c too
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("client protocol %d", resp.ProtoMajor)
	}
	buf := make([]byte, 6)
	if _, err := io.ReadFull(resp.Body, buf); err != nil || string(buf) != "frame1" {
		t.Fatalf("%q %v", buf, err)
	}
	if buffered.Load() {
		t.Fatal("the first frame came only when the app finished: buffered")
	}
	release()
	rest, _ := io.ReadAll(resp.Body)
	if string(rest) != "frame2" {
		t.Fatalf("rest %q", rest)
	}
	if resp.Trailer.Get("Grpc-Status") != "0" || resp.Trailer.Get("Grpc-Message") != "fine" || resp.Trailer.Get("Shardlite-Trailer") != "" {
		t.Fatalf("trailers: %v", resp.Trailer)
	}
	if proto.Load() != 2 {
		t.Fatalf("the app was reached over HTTP/%d, want h2c", proto.Load())
	}

	// An error the node makes, over h2c: a gRPC status, not an HTTP error.
	q, _ = http.NewRequest("POST", r.pub+"/svc/Method", strings.NewReader(""))
	q.Header.Set("Content-Type", "application/grpc")
	q.Header.Set("Shardlite-Target", "typo/1")
	resp2, err := r.h2c.Do(q)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != 200 || resp2.Header.Get("Grpc-Status") != "5" {
		t.Fatalf("%d %v %v", resp2.StatusCode, resp2.Header, resp2.Trailer)
	}
	wantReason(t, resp2, reasonUnknownShard) // a response header beside grpc-status, as HTTP/2 carries it
}

// A request the app never had (the dial to it failed) is a 502 with the default
// retry hint on every listener, so the client or the forwarding peer can try
// again.
func TestServerAppDown(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("slow/d1")
	peer := httptest.NewServer(r.srv.PeerHandler())
	t.Cleanup(peer.Close)
	r.fake.Close()
	for _, tc := range []struct {
		name, base string
		hdr        map[string]string
	}{
		{"public", r.pub, map[string]string{"Shardlite-Target": "slow/d1"}},
		{"control", r.ctl, map[string]string{"Shardlite-Target": "slow/d1"}},
		{"peer", peer.URL, map[string]string{"Shardlite-Target": "slow/d1", "Shardlite-Origin": originClient}},
	} {
		resp, _ := r.req("GET", tc.base, "/x", tc.hdr, "")
		if resp.StatusCode != 502 || resp.Header.Get("Retry-After") != "1" {
			t.Fatalf("app down, %s: %d, Retry-After %q", tc.name, resp.StatusCode, resp.Header.Get("Retry-After"))
		}
		wantReason(t, resp, reasonAppUnreachable)
	}
	if resp, _ := r.req("POST", r.pub, "/x", map[string]string{"Shardlite-Target": "slow/d1", "Content-Type": "application/grpc"}, ""); resp.Header.Get("Grpc-Status") != "14" {
		t.Fatalf("app down, grpc: %v", resp.Header)
	} else {
		wantReason(t, resp, reasonAppUnreachable)
	}
	apptest.Eventually(t, apptest.Timeout, "not ready", func() bool { ok, _ := r.srv.Ready(); return !ok })
	if resp, _ := r.req("GET", r.pub, "/_shardlite/ready", nil, ""); resp.StatusCode != 503 {
		t.Fatalf("ready with the app down: %d", resp.StatusCode)
	}
}

// A request the app had and that then failed is a 502 without a retry hint,
// since the app may have acted on it.
func TestServerAppFailsAfterTheRequest(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.create("slow/d1")
	peer := httptest.NewServer(r.srv.PeerHandler())
	t.Cleanup(peer.Close)
	var got atomic.Int32
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		got.Add(1)
		panic(http.ErrAbortHandler) // the app took the request, then closed the connection with no answer
	})
	for i, tc := range []struct {
		name, base string
		hdr        map[string]string
	}{
		{"public", r.pub, map[string]string{"Shardlite-Target": "slow/d1"}},
		{"control", r.ctl, map[string]string{"Shardlite-Target": "slow/d1"}},
		{"peer", peer.URL, map[string]string{"Shardlite-Target": "slow/d1", "Shardlite-Origin": originClient}},
	} {
		resp, _ := r.req("POST", tc.base, "/x", tc.hdr, "body")
		if resp.StatusCode != 502 || resp.Header.Get("Retry-After") != "" {
			t.Fatalf("app failing after the request, %s: %d, Retry-After %q", tc.name, resp.StatusCode, resp.Header.Get("Retry-After"))
		}
		wantReason(t, resp, reasonAppFailed)
		if n := int(got.Load()); n != i+1 {
			t.Fatalf("after %s the app had %d requests, want %d", tc.name, n, i+1)
		}
	}
}
