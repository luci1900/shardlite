package node

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/luci1900/shardlite/internal/kind"
	shardlite "github.com/luci1900/shardlite/sdk/go"
)

// sdkOpen is the opener of the SDK tests' apps.
func sdkOpen(uri string) (*sql.DB, error) { return sql.Open("sqlite", uri) }

// sdkConfig is the Config of an app that declares the kinds of the node's rig
// (testKinds), plus extra.
func sdkConfig(extra ...shardlite.Kind) shardlite.Config {
	return shardlite.Config{Open: sdkOpen, Kinds: append([]shardlite.Kind{
		{Name: "users", Public: true, Sync: shardlite.SyncNone, IdleTimeout: 300 * time.Millisecond},
		{Name: "wallet", Public: true, IdleTimeout: 300 * time.Millisecond},
		{Name: "slow", Public: true, Sync: shardlite.SyncNone, IdleTimeout: time.Hour},
		{Name: "audit"},
		{Name: "lobby", Public: true, IdleTimeout: 300 * time.Millisecond},
		{Name: "arena", Public: true, Sync: shardlite.SyncNone, IdleTimeout: 300 * time.Millisecond},
	}, extra...)}
}

// conformance is sdk/testdata/conformance.json, which each SDK's copy of the
// node's reasons is checked against.
type conformance struct {
	// Reasons are the node's reasons an app can see (all but not-owner).
	Reasons []string `json:"reasons"`
}

func loadConformance(t *testing.T) conformance {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "sdk", "testdata", "conformance.json"))
	if err != nil {
		t.Fatal(err)
	}
	var c conformance
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		t.Fatalf("sdk/testdata/conformance.json: %v", err)
	}
	return c
}

// appVisible has an entry for every reason, so the mapliteral linter fails when
// a reason is added without saying whether apps can see it. The fixture lists
// the ones they can.
var appVisible = map[reason]bool{
	reasonBadRequest:       true,
	reasonUnknownShard:     true,
	reasonNotActive:        true,
	reasonNoEndpoint:       true,
	reasonNotOwner:         false, // between nodes only: the forwarding node turns it into moved
	reasonHops:             true,
	reasonAppUnreachable:   true,
	reasonAppFailed:        true,
	reasonOwnerUnreachable: true,
	reasonOwnerFailed:      true,
	reasonMoved:            true,
	reasonDraining:         true,
	reasonLeaseLost:        true,
	reasonBadFile:          true,
	reasonSyncUnmet:        true,
	reasonNotReady:         true,
	reasonTimeout:          true,
	reasonInternal:         true,
}

// The fixture's reasons are the node's but for not-owner, so adding or removing
// one in reason.go fails here until the fixture follows.
func TestSDKConformanceReasonsAreTheNodes(t *testing.T) {
	t.Parallel()
	var want []string
	for r, visible := range appVisible {
		if visible {
			want = append(want, string(r))
		}
	}
	slices.Sort(want)
	got := slices.Clone(loadConformance(t).Reasons)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("sdk/testdata/conformance.json lists the reasons %v, the node's that apps can see are %v", got, want)
	}
}

// The Go SDK knows every reason the node lets apps see: NodeError reads
// each as its own constant, not as ReasonOther. The other direction (no
// constant the node lacks) is the SDK's own test against the fixture.
func TestSDKReasonsAreTheNodes(t *testing.T) {
	t.Parallel()
	for r, visible := range appVisible {
		if !visible {
			continue
		}
		res := &http.Response{StatusCode: http.StatusBadGateway, Header: http.Header{"Shardlite-Error": {string(r)}}, Body: http.NoBody}
		nerr, ok := shardlite.NodeError(res)
		if !ok || nerr.Reason == shardlite.ReasonOther || string(nerr.Reason) != string(r) {
			t.Errorf("NodeError reads the node's %q as %+v", r, nerr)
		}
	}
}

// The names the SDK keeps private are the node's: the paths, the headers a
// call's options set and the sync path.
func TestSDKNamesAreTheNodes(t *testing.T) {
	t.Parallel()
	if p, v := shardlite.ReadyPath, appReadyPath; p != v {
		t.Errorf("the SDK names the ready path %q where the node names %q", p, v)
	}
	if p, v := shardlite.RemindersPath, remindersPath; p != v {
		t.Errorf("the SDK names the reminders path %q where the node names %q", p, v)
	}
	if shardlite.HeaderTarget != hdrTarget || shardlite.HeaderContext != hdrContext {
		t.Errorf("the SDK names %q and %q where the node names %q and %q", shardlite.HeaderTarget, shardlite.HeaderContext, hdrTarget, hdrContext)
	}
	var sent http.Header
	var paths []string
	cfg := sdkConfig()
	cfg.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		sent = r.Header
		paths = append(paths, r.URL.Path)
		return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody, Header: http.Header{}}, nil
	})}
	app, err := shardlite.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	slow := shardlite.Kind{Name: "slow"}.Shard("there")
	res, err := app.CallAsApp(t.Context(), slow, http.MethodGet, "/", nil,
		shardlite.IfActive(), shardlite.IfExists(), shardlite.WithSync(shardlite.SyncNone))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	for _, h := range []string{hdrIfActive, hdrIfExists, hdrSync} {
		if sent.Get(h) == "" {
			t.Errorf("a call with the options sent no %s", h)
		}
	}
	paths = nil
	if err := app.Sync(t.Context()); err != nil || !slices.Equal(paths, []string{apiPrefix + "sync"}) {
		t.Errorf("Sync = %v, sent to %v, want the node's %s", err, paths, apiPrefix+"sync")
	}
}

// roundTripFunc is an http.RoundTripper that answers from a function.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// The declaration the SDK builds from a Config is read by the node's parser as
// the kinds the rig declares, with the app's version and a library's dotted
// kind.
func TestSDKReadyIsWhatTheNodeParses(t *testing.T) {
	t.Parallel()
	parse := func(cfg shardlite.Config) kind.App {
		t.Helper()
		app, err := shardlite.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		app.Ready().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, appReadyPath, nil))
		body, _ := io.ReadAll(rec.Result().Body)
		parsed, err := kind.ParseApp(body, time.Minute)
		if err != nil {
			t.Fatalf("the node refuses %s: %v", body, err)
		}
		return parsed
	}
	want, err := kind.ParseApp([]byte(testKinds), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got := parse(sdkConfig()); !reflect.DeepEqual(got, want) {
		t.Errorf("the node reads the SDK's declaration as %+v, the rig declares %+v", got, want)
	}

	cfg := sdkConfig(shardlite.Kind{Name: "matchmaker.queue", Public: true, Sync: shardlite.SyncStorage, IdleTimeout: 90 * time.Second})
	cfg.Version = "2026.10.0"
	got := parse(cfg)
	if q := got.Kinds["matchmaker.queue"]; got.Version != "2026.10.0" || !q.Public || q.Sync != kind.SyncStorage || q.IdleTimeout != 90*time.Second {
		t.Errorf("the node reads %+v", got)
	}
}
