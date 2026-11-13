//go:build fault

package e2e

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/test/apptest"
	"github.com/luci1900/shardlite/internal/test/fault"
)

// A node killed while it stops loses nothing. The node is told to stop with SIGTERM, drains
// and starts letting go of its shards, and dies right after the first one (fault.StopLetGo): by
// then that shard is released and the others are still owned by the node, whose lease is not
// released. Every write a `storage` request acknowledged is in blob storage, and every shard is
// served by the other node once it has adopted the dead one (docs/internals.md, Takeover and
// Shutdown).
func TestFaultKillDuringStop(t *testing.T) {
	t.Parallel()
	const shards = 6
	fire := filepath.Join(t.TempDir(), "fire")
	n1 := newStack(t, faultEnv[0], "SHARDLITE_FAULT_KILL_AT="+string(fault.StopLetGo), "SHARDLITE_FAULT_KILL_WHEN="+fire)
	n2 := newStackOn(t, n1.store, faultEnv[0], "SHARDLITE_NODE=n2")
	v := newVerifier(t, n1)
	client := &http.Client{Timeout: 10 * time.Second}

	names := make([]string, shards)
	for i := range names {
		names[i] = fmt.Sprintf("wallet/d%d", i)
	}
	n1.create(t, names...)

	acked := map[string]map[string]bool{} // shard → tokens that must survive
	add := func(s *stack, shard, tok string) bool {
		st, _, err := doRequest(client, "POST", "http://"+s.pub+at(shard, "/add"), shard, tok)
		if err != nil || st != 200 {
			return false
		}
		if acked[shard] == nil {
			acked[shard] = map[string]bool{}
		}
		acked[shard][tok] = true
		return true
	}
	owner := func(shard string) string {
		m, _, err := v.files.GetManifest(t.Context(), shardName(shard))
		if err != nil {
			t.Fatal(err)
		}
		return string(m.Owner)
	}
	for _, shard := range names {
		for i := range 5 {
			if tok := fmt.Sprintf("%s-a%d", shard, i); !add(n1, shard, tok) {
				t.Fatalf("writing %s to %s", tok, shard)
			}
		}
		if got := owner(shard); got != "n1" {
			t.Fatalf("%s is owned by %q before the stop, want n1", shard, got)
		}
	}

	// The stop: the point is armed, then the node is told to stop and dies when it has let go
	// of its first shard.
	if err := os.WriteFile(fire, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	n1.node.Signal(syscall.SIGTERM)
	select {
	case <-n1.node.Done:
	case <-time.After(60 * time.Second):
		t.Fatalf("the node did not die at %s:\n%s", fault.StopLetGo, n1.node.Output())
	}
	if !killed(n1.node.Err) {
		t.Fatalf("the node stopped by itself (%v) and was not killed at %s:\n%s", n1.node.Err, fault.StopLetGo, n1.node.Output())
	}
	if out := n1.node.Output(); !strings.Contains(out, "shutting down") {
		t.Fatalf("the node was killed before it began to stop:\n%s", out)
	}

	// The node died with the lease held, part of the way through letting go: the first shard
	// was released and the rest are not, and nothing acknowledged is missing.
	released := 0
	for _, shard := range names {
		if owner(shard) == "" {
			released++
		}
	}
	t.Logf("%d of %d shards released when the node died", released, shards)
	if released == 0 {
		t.Fatalf("no shard was released when the node died, so the kill was not during the stop:\n%s", n1.node.Output())
	}
	check := func(label string) {
		t.Helper()
		for shard, toks := range acked {
			have, ok, err := v.tokens(shard)
			if err != nil || !ok {
				t.Fatalf("%s: restoring %s: %v (durable: %v)", label, shard, err, ok)
			}
			for tok := range toks {
				if !have[tok] {
					t.Fatalf("%s: %q was acknowledged but is missing from %s (%d tokens in it, %d acknowledged)", label, tok, shard, len(have), len(toks))
				}
			}
		}
	}
	check("after the kill")

	// The other node serves every shard, the released ones at once and the rest once it has
	// adopted the dead node, and a write to each is acknowledged.
	for _, shard := range names {
		apptest.Eventually(t, 60*time.Second, shard+" to be served by n2", func() bool {
			return add(n2, shard, shard+"-b0")
		})
		if got := owner(shard); got != "n2" {
			t.Fatalf("%s is owned by %q after adoption, want n2", shard, got)
		}
		st, body, err := doRequest(client, "GET", "http://"+n2.pub+at(shard, "/tokens"), shard, "")
		if err != nil || st != 200 {
			t.Fatalf("n2's tokens of %s: %d %v", shard, st, err)
		}
		seen := map[string]bool{}
		for _, tok := range strings.Split(body, "\n") {
			seen[tok] = true
		}
		for tok := range acked[shard] {
			if !seen[tok] {
				t.Fatalf("%q was acknowledged but n2 does not serve it from %s", tok, shard)
			}
		}
	}
	check("after the adoption")
}

// killed reports whether a process ended by SIGKILL, which is how the fault build's kill points
// end it.
func killed(err error) bool {
	var ee *exec.ExitError
	if !errors.As(err, &ee) {
		return false
	}
	ws, ok := ee.Sys().(syscall.WaitStatus)
	return ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL
}
