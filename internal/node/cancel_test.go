package node

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
)

// refs is how many holds a shard has (-1 if it is not active).
func (r *srig) refs(target string) int {
	r.m.mu.Lock()
	defer r.m.mu.Unlock()
	if a := r.m.active[target]; a != nil {
		return a.refs
	}
	return -1
}

// A client that goes away while its response is held at the gate: the node
// stops waiting, cancels the request to the app and lets go of the shard.
func TestClientDisconnectWhileHeldAtTheGate(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.slowJournal(5 * time.Second) // every flush takes 5 s
	var appCancelled, appAnswered atomic.Bool
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		r.commit(q.Header.Get("Shardlite-Path"))
		w.WriteHeader(200)
		w.(http.Flusher).Flush() // the response starts, and now waits at the gate
		appAnswered.Store(true)
		<-q.Context().Done()
		appCancelled.Store(true)
	})
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "POST", r.pub+"/w", nil)
	req.Header.Set("Shardlite-Target", "wallet/cx1")
	done := make(chan error, 1)
	go func() {
		resp, err := r.client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		done <- err
	}()
	eventually(t, "the request to be held at the gate", func() bool {
		return r.refs("wallet/cx1") == 1 && r.m.flush.Mark() > r.m.flush.Completed() && appAnswered.Load()
	})
	start := time.Now()
	cancel()
	<-done
	eventually(t, "the request to the app to be cancelled", appCancelled.Load)
	eventually(t, "the shard to be released", func() bool { return r.refs("wallet/cx1") == 0 })
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("cancelling took %v: the node waited for the flush", d)
	}
}

// A caller that gives up on a call held at the callee's gate: the callee's
// request is cancelled too and nothing stays held.
func TestCallerCancelsAtTheCalleesGate(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	r.slowJournal(5 * time.Second)
	var calleeCancelled, calleeEntered atomic.Bool
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.Header.Get("Shardlite-Caller") == "" {
			return
		}
		calleeEntered.Store(true)
		r.commit(q.Header.Get("Shardlite-Path")) // the callee is a wallet: its response is gated
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-q.Context().Done()
		calleeCancelled.Store(true)
	})
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "POST", r.ctl+"/x", nil)
	req.Header.Set("Shardlite-Target", "wallet/cx2")
	req.Header.Set("Shardlite-Caller", "app")
	done := make(chan error, 1)
	go func() {
		resp, err := r.client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		done <- err
	}()
	eventually(t, "the call to be held at the callee's gate", func() bool { return r.refs("wallet/cx2") == 1 && r.m.flush.Mark() > r.m.flush.Completed() })
	// Cancel only once the request has reached the app: a cancel before that
	// means the app never sees it, and there is nothing for the node to cancel.
	eventually(t, "the callee's request to reach the app", calleeEntered.Load)
	cancel()
	<-done
	eventually(t, "the callee's request to be cancelled", calleeCancelled.Load)
	eventually(t, "the callee to be released", func() bool { return r.refs("wallet/cx2") == 0 })
}

// ...and held at the caller's gate, before the callee is activated or even asked.
func TestCallerCancelsAtItsOwnGate(t *testing.T) {
	t.Parallel()
	r := newSRigWith(t, manualFlush)
	var reached atomic.Int32
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.Header.Get("Shardlite-Caller") != "" {
			reached.Add(1)
		}
	})
	r.commitTo("wallet/cx3") // sync on, and no flush cycle will ever run
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "POST", r.ctl+"/x", nil)
	req.Header.Set("Shardlite-Target", "slow/cx3callee")
	req.Header.Set("Shardlite-Caller", "wallet/cx3")
	done := make(chan error, 1)
	go func() {
		resp, err := r.client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		done <- err
	}()
	eventually(t, "the call to be held at the caller's gate", func() bool { return r.refs("wallet/cx3") == 1 })
	cancel()
	<-done
	eventually(t, "the caller to be released", func() bool { return r.refs("wallet/cx3") == 0 })
	if reached.Load() != 0 || r.m.IsActive("slow/cx3callee") {
		t.Fatalf("the callee was reached (%d) or activated (%v) by a call that was cancelled", reached.Load(), r.m.IsActive("slow/cx3callee"))
	}
}

// ...and while the callee is being activated: the call stops waiting, the
// activation finishes for whoever asks next, and the shard is not held.
func TestCallerCancelsDuringActivation(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	var reached atomic.Int32
	r.app_(func(w http.ResponseWriter, q *http.Request) {
		if q.Header.Get("Shardlite-Caller") != "" {
			reached.Add(1)
		}
	})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	r.local.SetHook(func(ctx context.Context, op blob.Op, key string, after bool) error {
		if !after && op == blob.OpCreate && strings.HasPrefix(key, "manifests/") {
			once.Do(func() { close(entered) })
			<-release // a slow activation
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "POST", r.ctl+"/x", nil)
	req.Header.Set("Shardlite-Target", "slow/cx4")
	req.Header.Set("Shardlite-Caller", "app")
	done := make(chan error, 1)
	go func() {
		resp, err := r.client.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		done <- err
	}()
	<-entered
	cancel()
	<-done
	if r.m.IsActive("slow/cx4") {
		t.Fatal("the activation did not take as long as the hook made it")
	}
	close(release)
	// The activation is not the caller's to cancel (others may be waiting for it): it ends.
	eventually(t, "the activation to finish", func() bool { return r.m.IsActive("slow/cx4") })
	if r.refs("slow/cx4") != 0 {
		t.Fatalf("the cancelled call holds the shard: %d", r.refs("slow/cx4"))
	}
	if reached.Load() != 0 {
		t.Fatal("the app was called by a cancelled call")
	}
}

// A streaming call that is cancelled stops being gated, and releases the caller
// and the callee.
//
// Over HTTP/2 (gRPC streams) the node learns of the cancellation at once, from
// the stream's reset. Over HTTP/1.1 net/http only notices a closed connection
// when something reads the request body, and the node is not reading while it
// holds a chunk at the gate: it finds out when the gate opens (the next flush
// cycle, 250 ms by default) and the next read fails.
func TestCancelledStreamingCall(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		h2    bool
		cycle bool // HTTP/1.1: a flush cycle has to run before the node notices
	}{{"h2c (gRPC)", true, false}, {"HTTP/1.1", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			r := newSRigWith(t, manualFlush)
			seen := make(chan seenChunk, 4)
			started := make(chan struct{})
			var calleeErr atomic.Value
			done := make(chan struct{})
			r.app_(func(w http.ResponseWriter, q *http.Request) {
				if q.Header.Get("Shardlite-Caller") != "wallet/cx5" {
					return
				}
				close(started)
				buf := make([]byte, 16)
				for {
					n, err := q.Body.Read(buf)
					if n > 0 {
						seen <- seenChunk{string(buf[:n]), 0}
					}
					if err != nil {
						calleeErr.Store(err.Error())
						break
					}
				}
				close(done)
			})
			r.commitTo("wallet/cx5")
			pr, pw := io.Pipe()
			ctx, cancel := context.WithCancel(context.Background())
			req, _ := http.NewRequestWithContext(ctx, "POST", r.ctl+"/stream", pr)
			req.Header.Set("Shardlite-Target", "slow/cx5callee")
			req.Header.Set("Shardlite-Caller", "wallet/cx5")
			client := r.client
			if tc.h2 {
				client = r.h2c
				req.Header.Set("Content-Type", "application/grpc")
			}
			cerr := make(chan error, 1)
			go func() {
				resp, err := client.Do(req)
				if err == nil {
					resp.Body.Close()
				}
				cerr <- err
			}()
			cyclesUntil(r, started)
			r.commitTo("wallet/cx5")
			pw.Write([]byte("held")) // waits for a flush cycle that has not run
			select {                 // no flush cycle runs, so the chunk stays at the gate
			case c := <-seen:
				t.Fatalf("%q reached the callee before the caller's commit was durable", c.data)
			case <-time.After(300 * time.Millisecond):
			}
			cancel()
			pr.CloseWithError(context.Canceled) // the client's writer for the body ends with its call
			<-cerr
			if tc.cycle {
				select {
				case <-done:
					t.Fatal("HTTP/1.1 noticed the cancellation without a flush cycle: the test's premise changed, update the comment")
				case <-time.After(300 * time.Millisecond):
				}
				r.cycle()
			}
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("the callee is still reading a cancelled call")
			}
			if e, _ := calleeErr.Load().(string); e == "EOF" {
				t.Fatalf("the callee saw a clean end of the body: %q", e)
			}
			eventually(t, "the caller and the callee to be released", func() bool { return r.refs("wallet/cx5") == 0 && r.refs("slow/cx5callee") == 0 })
			// Over HTTP/2 nothing the cancelled call held goes on to the callee. Over
			// HTTP/1.1 the chunk held at the gate is let through by the cycle that
			// makes its commit durable, and then the callee's read fails.
			select {
			case c := <-seen:
				if !tc.cycle {
					t.Fatalf("%q reached the callee after the call was cancelled", c.data)
				}
			default:
			}
		})
	}
}
