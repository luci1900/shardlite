package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/s3test"
)

// clockReport is the clock object of the control API's node reply.
type clockReport struct {
	Samples  int  `json:"samples"`
	Warning  bool `json:"warning"`
	Unsteady bool `json:"unsteady"`
}

func (s *stack) nodeClock(t testing.TB) clockReport {
	t.Helper()
	resp, err := http.Get("http://" + s.ctl + "/_shardlite/node")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var reply struct{ Clock *clockReport }
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil || reply.Clock == nil {
		t.Fatalf("the node's reply has no clock: %v", err)
	}
	return *reply.Clock
}

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

// The store's clock as the node sees it is moved by a proxy. A constant offset
// within the lease TTL only warns. A step makes `none` answers fail with a retry
// hint while `storage` answers go on, and they recover once the offset has been
// steady for a while. An offset past the TTL stops the node from starting.
func TestSkewedClockFailsNoneAnswersAndNotStorage(t *testing.T) {
	t.Parallel()
	st := s3test.Setup(t)
	skew := s3test.StartSkew(t, st.Endpoint)
	skew.Set(3 * time.Second) // within the 5s lease TTL, past the second that warns
	s := newStack(t, "SHARDLITE_S3_ENDPOINT="+skew.URL)
	// (The stack has a prefix of its own on the same store the proxy fronts.)
	s.create(t, "e2e.users/u1", "e2e.wallet/w1")

	// Within the limit nothing changes, but the operator is told.
	apptest.Eventually(t, 20*time.Second, "the node to warn of the offset", func() bool {
		c := s.nodeClock(t)
		return c.Samples > 0 && c.Warning
	})
	if c := s.nodeClock(t); c.Unsteady {
		t.Fatalf("a constant offset is unsteady: %+v", c)
	}
	for _, shard := range []string{"e2e.users/u1", "e2e.wallet/w1"} {
		if code, body, _ := s.add(shard, "before-"+shard); code != 200 {
			t.Fatalf("%s with a constant offset: %d %q", shard, code, body)
		}
	}

	// A step: the node's none answers fail, its storage answers do not.
	skew.Set(40 * time.Second)
	apptest.Eventually(t, 20*time.Second, "a none answer to fail on the clock", func() bool {
		code, body, retry := s.add("e2e.users/u1", "during")
		if code == 200 {
			return false
		}
		if code != http.StatusServiceUnavailable || retry == "" || !strings.Contains(body, "clock") {
			t.Fatalf("a none answer under a step: %d %q (Retry-After %q)", code, body, retry)
		}
		return true
	})
	if code, body, _ := s.add("e2e.wallet/w1", "during"); code != 200 {
		t.Fatalf("a storage answer failed on the clock: %d %q", code, body)
	}
	if !s.nodeClock(t).Unsteady {
		t.Fatal("the node does not report the step")
	}

	// It recovers once the samples since show a steady offset.
	apptest.Eventually(t, 60*time.Second, "none answers to recover", func() bool {
		code, _, _ := s.add("e2e.users/u1", "after")
		return code == 200
	})
	if s.nodeClock(t).Unsteady {
		t.Fatal("the node answers but still reports the step")
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
