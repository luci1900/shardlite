package e2e

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/s3test"
)

// add posts a token to a shard and returns the status, the body and the retry hint.
func (s *stack) add(shard, token string) (int, string, string) {
	q, _ := http.NewRequest("POST", "http://"+s.pub+at(shard, "/add"), strings.NewReader(token))
	q.Header.Set("Shardlite-Target", shard)
	resp, err := http.DefaultClient.Do(q)
	if err != nil {
		return 0, err.Error(), ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header.Get("Retry-After")
}

// The store's clock as the node sees it is moved by a proxy. An offset within
// the lease TTL only warns, a step in it only warns too and `none` answers go
// on, and an offset past the TTL stops the node from starting.
func TestSkewedClockOnlyWarns(t *testing.T) {
	t.Parallel()
	st := s3test.Setup(t)
	skew := s3test.StartSkew(t, st.Endpoint)
	skew.Set(3 * time.Second) // within the 5s lease TTL, past the second that warns
	s := newStack(t, "SHARDLITE_S3_ENDPOINT="+skew.URL)
	// (The stack has a prefix of its own on the same store the proxy fronts.)
	s.create(t, "users/u1", "wallet/w1")

	// Within the limit nothing changes, but the operator is told.
	s.node.waitFor(t, "more than a second from the store's", 20*time.Second)
	for _, shard := range []string{"users/u1", "wallet/w1"} {
		if code, body, _ := s.add(shard, "before-"+shard); code != 200 {
			t.Fatalf("%s with a constant offset: %d %q", shard, code, body)
		}
	}

	// A step: every answer goes on (the node warned already and says nothing more).
	skew.Set(40 * time.Second)
	for _, shard := range []string{"users/u1", "wallet/w1"} {
		if code, body, _ := s.add(shard, "during-"+shard); code != 200 {
			t.Fatalf("%s under a step: %d %q", shard, code, body)
		}
	}

	// Past the TTL the node does not start.
	s.node.kill()
	skew.Set(10 * time.Minute)
	p := startProc(t, "shardlite", s.nodeBin, s.nodeEnv())
	select {
	case <-p.done:
	case <-time.After(30 * time.Second):
		t.Fatalf("the node started with a clock 10 minutes off:\n%s", p.out.String())
	}
	if out := p.out.String(); !strings.Contains(out, "lease TTL") {
		t.Fatalf("the refusal does not say why:\n%s", out)
	}
}
