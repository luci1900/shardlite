package shardlite_test

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	shardlite "github.com/luci1900/shardlite/sdk/go"
)

// known has an entry for every Reason, so the linter's mapliteral check
// fails when a constant is added without one. ReasonOther is the only false.
var known = map[shardlite.Reason]bool{
	shardlite.ReasonBadRequest:       true,
	shardlite.ReasonUnknownShard:     true,
	shardlite.ReasonNotActive:        true,
	shardlite.ReasonNoEndpoint:       true,
	shardlite.ReasonHops:             true,
	shardlite.ReasonAppUnreachable:   true,
	shardlite.ReasonAppFailed:        true,
	shardlite.ReasonOwnerUnreachable: true,
	shardlite.ReasonOwnerFailed:      true,
	shardlite.ReasonMoved:            true,
	shardlite.ReasonDraining:         true,
	shardlite.ReasonLeaseLost:        true,
	shardlite.ReasonBadFile:          true,
	shardlite.ReasonSyncUnmet:        true,
	shardlite.ReasonNotReady:         true,
	shardlite.ReasonTimeout:          true,
	shardlite.ReasonInternal:         true,
	shardlite.ReasonOther:            false,
}

// The Reason constants are the fixture's reasons, which the node's own
// tests pin to the node. So the SDK has no constant the node lacks.
func TestReasonsAreTheFixtures(t *testing.T) {
	t.Parallel()
	b, err := os.ReadFile(filepath.Join("..", "testdata", "conformance.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fix struct {
		Reasons []string `json:"reasons"`
	}
	if err := json.Unmarshal(b, &fix); err != nil {
		t.Fatal(err)
	}
	var got []string
	for r, ok := range known {
		if ok {
			got = append(got, string(r))
		}
	}
	slices.Sort(got)
	want := slices.Sorted(slices.Values(fix.Reasons))
	if !slices.Equal(got, want) {
		t.Errorf("the SDK has the reasons %v, the fixture %v", got, want)
	}
}

// answering is a Config whose app is answered by respond, in place of a node.
func answering(respond func(*http.Request) *http.Response) shardlite.Config {
	cfg := testConfig()
	cfg.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		res := respond(r)
		res.Request = r
		return res, nil
	})}
	return cfg
}

func reply(status int, body string, header http.Header) *http.Response {
	if header == nil {
		header = http.Header{}
	}
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(body))}
}

func TestNodeErrorOfACallTheNodeRefused(t *testing.T) {
	app := newNode(t, answering(func(r *http.Request) *http.Response {
		return reply(http.StatusNotFound, "the kind \"ghost\" is not declared\n", http.Header{"Shardlite-Error": {"unknown-shard"}})
	}))
	ghost := shardlite.Kind{Name: "ghost"}
	res, err := app.CallAsApp(t.Context(), ghost.Shard("1"), http.MethodGet, "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	nerr, ok := shardlite.NodeError(res)
	if !ok {
		t.Fatal("NodeError did not see the node's error")
	}
	if nerr.Status != http.StatusNotFound || nerr.Reason != shardlite.ReasonUnknownShard || nerr.RawReason != "unknown-shard" || !strings.Contains(nerr.Message, "ghost") {
		t.Errorf("error = %+v", nerr)
	}
	if d, ok := nerr.RetryAfter(); ok {
		t.Errorf("RetryAfter = %v, but the node gave none", d)
	}
	if msg := nerr.Error(); !strings.Contains(msg, "404") || !strings.Contains(msg, "unknown-shard") || !strings.Contains(msg, "ghost") {
		t.Errorf("Error() = %q, want the status, the reason and the message", msg)
	}
}

func TestNodeErrorLeavesAnAppsErrorAlone(t *testing.T) {
	app := newNode(t, answering(func(*http.Request) *http.Response {
		return reply(http.StatusConflict, "no such guild member\n", nil)
	}))
	res, err := app.CallAsApp(t.Context(), guild.Shard("7"), http.MethodGet, "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if nerr, ok := shardlite.NodeError(res); ok || nerr != nil {
		t.Fatalf("NodeError took the app's %d for the node's: %+v", res.StatusCode, nerr)
	}
	// It did not touch the body, which the app reads as it likes.
	if body, _ := io.ReadAll(res.Body); strings.TrimSpace(string(body)) != "no such guild member" {
		t.Errorf("body = %q", body)
	}
}

func TestNodeErrorWithAReasonTheSDKDoesNotKnow(t *testing.T) {
	res := reply(http.StatusServiceUnavailable, "the quota is out\n", http.Header{"Shardlite-Error": {"quota"}})
	nerr, ok := shardlite.NodeError(res)
	if !ok {
		t.Fatal("NodeError did not see the node's error")
	}
	if nerr.Reason != shardlite.ReasonOther || nerr.RawReason != "quota" || nerr.Status != 503 || nerr.Message != "the quota is out" {
		t.Errorf("error = %+v", nerr)
	}
	// An empty value is still the node's error.
	if nerr, ok := shardlite.NodeError(reply(500, "", http.Header{"Shardlite-Error": {""}})); !ok || nerr.Reason != shardlite.ReasonOther || nerr.RawReason != "" {
		t.Errorf("an empty Shardlite-Error: %+v, %v", nerr, ok)
	}
}

func TestNodeErrorReadsTheRetryAfterAsTheNodeSentIt(t *testing.T) {
	for name, tc := range map[string]struct {
		value string
		want  time.Duration
		ok    bool
	}{
		"absent":           {"", 0, false},
		"seconds":          {"3", 3 * time.Second, true},
		"zero, at once":    {"0", 0, true},
		"spaces":           {" 1 ", time.Second, true},
		"a date":           {"Wed, 21 Oct 2026 07:28:00 GMT", 0, false},
		"negative":         {"-1", 0, false},
		"too big to count": {"99999999999999999", 0, false},
		"garbage":          {"soon", 0, false},
	} {
		t.Run(name, func(t *testing.T) {
			h := http.Header{"Shardlite-Error": {"moved"}}
			if tc.value != "" {
				h.Set("Retry-After", tc.value)
			}
			nerr, ok := shardlite.NodeError(reply(http.StatusServiceUnavailable, "", h))
			if !ok {
				t.Fatal("not the node's error")
			}
			if d, has := nerr.RetryAfter(); d != tc.want || has != tc.ok {
				t.Errorf("RetryAfter = %v, %v, want %v, %v", d, has, tc.want, tc.ok)
			}
		})
	}
}

func TestNodeErrorReadsAtMost512BytesOfTheMessage(t *testing.T) {
	long := strings.Repeat("x", 2000)
	nerr, ok := shardlite.NodeError(reply(500, long, http.Header{"Shardlite-Error": {"internal"}}))
	if !ok || len(nerr.Message) != 512 {
		t.Errorf("message of %d bytes, %v, want 512", len(nerr.Message), ok)
	}
}

func TestSyncReturnsTheNodesError(t *testing.T) {
	app := newNode(t, answering(func(r *http.Request) *http.Response {
		return reply(http.StatusServiceUnavailable, "lease lost\n", http.Header{"Shardlite-Error": {"lease-lost"}, "Retry-After": {"1"}})
	}))
	err := app.Sync(t.Context())
	var nerr *shardlite.Error
	if !errors.As(err, &nerr) {
		t.Fatalf("Sync = %v, want an *Error", err)
	}
	if d, ok := nerr.RetryAfter(); nerr.Status != 503 || nerr.Reason != shardlite.ReasonLeaseLost || nerr.Message != "lease lost" || !ok || d != time.Second {
		t.Errorf("error = %+v, retry %v", nerr, d)
	}
}

func TestSyncWithoutTheNodesErrorIsAPlainError(t *testing.T) {
	// A proxy in the way answers, not the node.
	app := newNode(t, answering(func(r *http.Request) *http.Response { return reply(http.StatusBadGateway, "bad gateway\n", nil) }))
	err := app.Sync(t.Context())
	var nerr *shardlite.Error
	if err == nil || errors.As(err, &nerr) || !strings.Contains(err.Error(), "502") || !strings.Contains(err.Error(), "bad gateway") {
		t.Errorf("Sync = %v, want a plain error with the status and the body", err)
	}
}

func TestSyncWrapsATransportError(t *testing.T) {
	boom := errors.New("connection refused")
	cfg := testConfig()
	cfg.HTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, boom })}
	err := newNode(t, cfg).Sync(t.Context())
	var nerr *shardlite.Error
	if !errors.Is(err, boom) || errors.As(err, &nerr) {
		t.Errorf("Sync = %v, want the transport error wrapped and no *Error", err)
	}
}
