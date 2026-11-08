//go:build fault

package e2e

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// Peer headers between the nodes that are dropped or garbled make placement
// worse and nothing else: a call from a shard still reaches its target and the
// write is durable, and a value that does not read is ignored (docs/internals.md,
// Routing and forwarding).
func TestFaultPeerHeaders(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"garble", "drop"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			fault := "SHARDLITE_FAULT_PEER_HEADER=" + mode + ":500" // half the values
			n1 := newStack(t, faultEnv[0], fault)
			n2 := newStackOn(t, n1.store, faultEnv[0], fault, "SHARDLITE_NODE=n2")
			v := newVerifier(t, n1)
			client := &http.Client{Timeout: 20 * time.Second}
			n1.create(t, "wallet/from-a")
			n2.create(t, "wallet/from-b")

			// transfer has the shard from call the shard to, which does not exist yet, so the
			// call creates it on the node placement picks. The write is durable when it is
			// answered. It returns the node that owns to.
			transfer := func(s *stack, from, to, tok string) string {
				t.Helper()
				st, body, err := doRequest(client, "POST", "http://"+s.pub+at(from, "/transfer")+"?to="+to+"&token="+tok, from, "")
				if err != nil || st != 200 {
					t.Fatalf("transfer %s from %s to %s: %d %q %v", tok, from, to, st, body, err)
				}
				if !v.has(to, "credit:"+tok) {
					t.Fatalf("%s has no credit:%s in blob storage", to, tok)
				}
				m, _, err := v.files.GetManifest(context.Background(), shardName(to))
				if err != nil {
					t.Fatal(err)
				}
				return string(m.Owner)
			}

			// Until the lease scan lists the other node there is nowhere to place anything
			// but here, so keep calling until a shard lands on n2.
			deadline := time.Now().Add(90 * time.Second)
			for i := 0; ; i++ {
				if owner := transfer(n1, "wallet/from-a", fmt.Sprintf("wallet/warm-%d", i), fmt.Sprintf("w%d", i)); owner == "n2" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("no call from a shard on n1 was placed on n2 in %d tries:\n%s", i+1, n1.node.out.String())
				}
				time.Sleep(200 * time.Millisecond)
			}
			// Both nodes now call new shards, and every call is answered and durable.
			for i := range 40 {
				s, from := n1, "wallet/from-a"
				if i%2 == 1 {
					s, from = n2, "wallet/from-b"
				}
				transfer(s, from, fmt.Sprintf("wallet/to-%d", i), fmt.Sprintf("t%d", i))
			}
			for _, s := range []*stack{n1, n2} {
				out := s.node.out.String()
				if !strings.Contains(out, "fault: injected peer header") {
					t.Errorf("a node logged no injected fault, so none was tested:\n%s", out)
				}
			}
		})
	}
}
