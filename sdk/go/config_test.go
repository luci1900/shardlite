package shardlite_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	shardlite "github.com/luci1900/shardlite/sdk/go"
)

func TestNewRefusesABadConfig(t *testing.T) {
	long := strings.Repeat("a", 65)
	for name, tc := range map[string]struct {
		mutate func(*shardlite.Config)
		want   string // in the error
	}{
		"no opener":            {func(c *shardlite.Config) { c.Open = nil }, "Open"},
		"an empty name":        {func(c *shardlite.Config) { c.Kinds = []shardlite.Kind{{Name: ""}} }, "valid kind"},
		"a capital":            {func(c *shardlite.Config) { c.Kinds = []shardlite.Kind{{Name: "Users"}} }, `"Users"`},
		"a slash":              {func(c *shardlite.Config) { c.Kinds = []shardlite.Kind{{Name: "a/b"}} }, "valid kind"},
		"a leading digit":      {func(c *shardlite.Config) { c.Kinds = []shardlite.Kind{{Name: "1a"}} }, "valid kind"},
		"an empty segment":     {func(c *shardlite.Config) { c.Kinds = []shardlite.Kind{{Name: "a..b"}} }, "valid kind"},
		"a trailing dot":       {func(c *shardlite.Config) { c.Kinds = []shardlite.Kind{{Name: "a."}} }, "valid kind"},
		"too long":             {func(c *shardlite.Config) { c.Kinds = []shardlite.Kind{{Name: long}} }, "valid kind"},
		"the node's own":       {func(c *shardlite.Config) { c.Kinds = []shardlite.Kind{{Name: "_wake"}} }, "node's own"},
		"a kind twice":         {func(c *shardlite.Config) { c.Kinds = []shardlite.Kind{users, guild, {Name: "users", Public: false}} }, "twice"},
		"an unknown sync":      {func(c *shardlite.Config) { c.Kinds = []shardlite.Kind{{Name: "a", Sync: "disk"}} }, "Sync"},
		"a negative timeout":   {func(c *shardlite.Config) { c.Kinds = []shardlite.Kind{{Name: "a", IdleTimeout: -time.Second}} }, "IdleTimeout"},
		"a negative, by a bit": {func(c *shardlite.Config) { c.Kinds = []shardlite.Kind{{Name: "a", IdleTimeout: -1}} }, "IdleTimeout"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig()
			tc.mutate(&cfg)
			n, err := shardlite.New(cfg)
			if err == nil || n != nil {
				t.Fatalf("New = %v, %v, want an error", n, err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
	// None is fine, and a library's dotted kind too.
	cfg := testConfig()
	cfg.Kinds = nil
	if _, err := shardlite.New(cfg); err != nil {
		t.Errorf("no kinds: %v", err)
	}
	cfg.Kinds = []shardlite.Kind{{Name: "matchmaker.queue", Sync: shardlite.SyncNone, IdleTimeout: time.Minute}, {Name: strings.Repeat("a", 64)}}
	if _, err := shardlite.New(cfg); err != nil {
		t.Errorf("good kinds: %v", err)
	}
}

func serveReady(n *shardlite.Node) *http.Response {
	rec := httptest.NewRecorder()
	n.Ready().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, shardlite.ReadyPath, nil))
	return rec.Result()
}

func TestReadyServesTheVersionAndTheKinds(t *testing.T) {
	cfg := testConfig()
	cfg.Version = "2026.10.0"
	cfg.Kinds = []shardlite.Kind{
		{Name: "users", Public: true, IdleTimeout: 10 * time.Minute},
		{Name: "guild"},
		{Name: "chat", Public: true, Sync: shardlite.SyncNone},
		{Name: "wallet", Sync: shardlite.SyncStorage, IdleTimeout: 1500 * time.Millisecond},
	}
	res := serveReady(newNode(t, cfg))
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("Ready answered %d %q", res.StatusCode, res.Header.Get("Content-Type"))
	}
	// A setting at its zero value is left out, so the node applies its default.
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("%s: %v", body, err)
	}
	want := `{"version":"2026.10.0","kinds":{"users":{"public":true,"idle_timeout":"10m0s"},"guild":{},"chat":{"public":true,"sync":"none"},"wallet":{"sync":"storage","idle_timeout":"1.5s"}}}`
	var wantJSON map[string]any
	json.Unmarshal([]byte(want), &wantJSON)
	if !jsonEqual(got, wantJSON) {
		t.Errorf("Ready served %s, want %s", body, want)
	}
}

func jsonEqual(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

func TestReadyWithNoKindsDeclaresAnEmptyObject(t *testing.T) {
	cfg := testConfig()
	cfg.Kinds = nil
	body, _ := io.ReadAll(serveReady(newNode(t, cfg)).Body)
	if string(body) != `{"kinds":{}}` {
		t.Errorf("Ready served %s: the node wants a kinds object, even an empty one", body)
	}
}

func TestReadyIsNotReadyWhileHealthFails(t *testing.T) {
	cfg := testConfig()
	var sick atomic.Pointer[error]
	cfg.Health = func(ctx context.Context) error {
		if err := sick.Load(); err != nil {
			return *err
		}
		return nil
	}
	n := newNode(t, cfg)
	if res := serveReady(n); res.StatusCode != http.StatusOK {
		t.Fatalf("a healthy app answered %d", res.StatusCode)
	}
	err := errors.New("migrating")
	sick.Store(&err)
	res := serveReady(n)
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(body), "migrating") {
		t.Errorf("a sick app answered %d %q, want 503 with the error", res.StatusCode, body)
	}
	sick.Store(nil)
	if res := serveReady(n); res.StatusCode != http.StatusOK {
		t.Errorf("an app that recovered answered %d", res.StatusCode)
	}
}
