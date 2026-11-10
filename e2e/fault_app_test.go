//go:build fault

package e2e

import (
	"maps"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
)

var longClient = &http.Client{Timeout: 2 * time.Minute}

// sameAsApp checks that blob storage holds what the app has in the shard: the
// restored shard and the app's own view of it have the same rows.
func (r *faultRig) sameAsApp(label string) {
	r.t.Helper()
	st, body, err := doRequest(longClient, "GET", "http://"+r.s.pub+at(faultWallet, "/tokens"), faultWallet, "")
	if err != nil || st != 200 {
		r.t.Fatalf("%s: the app's tokens: %d %v", label, st, err)
	}
	want := map[string]bool{}
	for _, tok := range strings.Split(body, "\n") {
		if tok != "" {
			want[tok] = true
		}
	}
	// The response was held for a flush, so everything the app has is in storage.
	have, ok, err := r.v.tokens(faultWallet)
	if err != nil || !ok {
		r.t.Fatalf("%s: restore: ok=%v %v", label, ok, err)
	}
	if !maps.Equal(have, want) {
		r.t.Fatalf("%s: the restored shard has %d tokens, the app has %d", label, len(have), len(want))
	}
}

// post sends a request to the shard and fails the test unless it succeeds.
func (r *faultRig) post(route string) string {
	r.t.Helper()
	st, body, err := doRequest(longClient, "POST", "http://"+r.s.pub+at(faultWallet, route), faultWallet, "")
	if err != nil || st/100 != 2 {
		r.t.Fatalf("POST %s: %d %q %v", route, st, body, err)
	}
	return body
}

// An app that checkpoints by itself while it writes (the WAL-reset hazard, Litestream
// issue #1540's area): what reaches blob storage is what the app has.
func TestFaultAppCheckpointsItself(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"PASSIVE", "FULL", "RESTART", "TRUNCATE"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			r := newFaultRig(t, "SHARDLITE_FLUSH=10ms")
			r.mustCommit(faultWallet)
			start := time.Now()
			res := r.post("/checkpoints?n=30&mode=" + mode)
			t.Logf("%s checkpoint in a loop of 30: %s in %v", mode, strings.TrimSpace(res), time.Since(start).Round(time.Millisecond))
			r.sameAsApp("after the checkpoints")
			r.verify("after the checkpoints")
		})
	}
}

// An app that closes and reopens its connection all the time.
func TestFaultAppReconnectsConstantly(t *testing.T) {
	t.Parallel()
	r := newFaultRig(t, "SHARDLITE_FLUSH=10ms")
	r.mustCommit(faultWallet)
	r.post("/churn?n=300")
	r.sameAsApp("after the churn")
	r.verify("after the churn")
}

// retryCommit writes until the node acknowledges, for faults that fail a write.
func (r *faultRig) retryCommit() {
	r.t.Helper()
	var err error
	for range 200 {
		if err = r.commit(faultWallet); err == nil {
			return
		}
	}
	r.t.Fatalf("the write was never acknowledged, last error: %v", err)
}

// faults runs the node with a fault setting for a while: writes fail and are
// retried, acknowledged commits are never lost, and the node goes on.
func (r *faultRig) faults(label string) {
	r.t.Helper()
	// At least 40 writes, and on until a fault has been injected (a temporary
	// file is created rarely enough that a few writes can pass without one).
	for i := 0; i < 40 || (i < 400 && !strings.Contains(r.s.node.out.String(), "fault: injected")); i++ {
		r.retryCommit()
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case <-r.s.node.done:
		r.t.Fatalf("%s: the node died:\n%s", label, r.s.node.out.String())
	default:
	}
	r.t.Logf("%s: %d writes attempted, %d acknowledged", label, r.n, len(r.acked))
	if !strings.Contains(r.s.node.out.String(), "fault: injected") {
		r.t.Fatalf("%s: the node logged no injected fault, so none was tested:\n%s", label, r.s.node.out.String())
	}
	if r.reportedLeaks {
		apptest.Eventually(r.t, apptest.Timeout, "the node to report a failed delete", func() bool {
			return strings.Contains(r.s.node.out.String(), "could not delete an LTX file")
		})
	}
	r.verify("with " + label)
	// A node without the fault goes on from what the faulty one left.
	r.base = append([]string(nil), faultEnv...)
	r.restart()
	r.mustCommit(faultWallet)
	r.verify("after " + label)
}

// Failing temporary files: a tail's log that cannot be created, a write to it that
// fails, a descriptor that cannot be duplicated. Writes fail and are retried, and
// deltas fail and are retried by the node's own loop.
func TestFaultTemporaryFiles(t *testing.T) {
	t.Parallel()
	for _, op := range []string{"create", "append", "dup"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			newFaultRig(t, "SHARDLITE_FAULT_TEMP="+op+":300").faults(op + " faults") // 30% of them
		})
	}
}

// Failing and slow blob storage: writes fail and are retried.
func TestFaultStorage(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"fail", "lost"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			newFaultRig(t, "SHARDLITE_FAULT_STORAGE="+mode+":300:30ms").faults(mode + " faults") // 30% of segment PUTs, up to 30 ms slow
		})
	}
}

// Failing LTX file uploads: a delta or a rebase's base that fails or whose response is
// lost is retried by the node's own loop, and nothing acknowledged is lost. A file
// whose manifest update then loses leaks, which the rig's walk bounds by the kills.
func TestFaultFiles(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"fail", "lost"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			newFaultRig(t, "SHARDLITE_FAULT_STORAGE="+mode+":300:30ms:files").faults(mode + " file uploads") // 30% of the creates
		})
	}
}

// Failing LTX file deletes: every delete of a superseded file fails, or lands and
// loses its response, and nothing acknowledged is lost. A failed delete reports the
// file it leaves, which the rig wants in the node's log. A lost one is retried, finds
// the file gone and counts as done, so it leaves nothing.
func TestFaultFileDeletes(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"fail", "lost"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			r := newFaultRig(t, "SHARDLITE_FAULT_STORAGE="+mode+":1000:0s:file-deletes") // every delete
			r.reportedLeaks = mode == "fail"
			r.faults(mode + " file deletes")
		})
	}
}
