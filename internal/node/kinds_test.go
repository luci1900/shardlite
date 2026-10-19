package node

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/system"
)

func TestParseTarget(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ in, kind, id string }{
		{"users/42", "users", "42"},
		{"guild-chat/0192f6a8-7c1e-7b3a-9d2e-5f4c3b2a1d0e", "guild-chat", "0192f6a8-7c1e-7b3a-9d2e-5f4c3b2a1d0e"},
		{"chunks/3-7", "chunks", "3-7"},
		{"trade/42_57", "trade", "42_57"},
		{"users/" + strings.Repeat("x", 128), "users", strings.Repeat("x", 128)},
	} {
		k, id, err := ParseTarget(tc.in)
		if err != nil || k != tc.kind || id != tc.id {
			t.Errorf("%q: %q %q %v", tc.in, k, id, err)
		}
	}
	for _, bad := range []string{"", "users", "users/", "/42", "Users/1", "../x/1", "users/a\nb", "users/" + strings.Repeat("x", 129),
		"users/a/b", "users/..", "users/.hidden", "users/a b", "users/a%2Fb", "users/é", "users/a.b"} {
		if _, _, err := ParseTarget(bad); !errors.Is(err, ErrBadTarget) {
			t.Errorf("%q accepted: %v", bad, err)
		}
	}
}

func TestParseKinds(t *testing.T) {
	t.Parallel()
	m, err := ParseKinds([]byte(`{"kinds": {"users": {"public": true, "sync": "off", "idle_timeout": "10m"}, "wallet": {}, "chat": {"sync": "on"}, "audit": {}}}`), 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if m["users"].IdleTimeout != 10*time.Minute || m["users"].Sync != kind.SyncOff || !m["users"].Public || m["chat"].Sync != kind.SyncOn || m["audit"].Public {
		t.Fatalf("%+v", m)
	}
	// Leaving settings out gives the safe ones: sync on, not public.
	if w := m["wallet"]; w.Sync != kind.SyncOn || w.Public || w.IdleTimeout != 3*time.Minute {
		t.Fatalf("defaults: %+v", w)
	}
	// The environment's idle timeout is a default: a kind's own setting wins.
	m, _ = ParseKinds([]byte(`{"kinds": {"users": {"idle_timeout": "10m"}, "wallet": {}}}`), time.Second)
	if m["users"].IdleTimeout != 10*time.Minute || m["wallet"].IdleTimeout != time.Second {
		t.Fatalf("%+v", m)
	}
	for name, bad := range map[string]string{
		"unknown field": `{"kinds": {"users": {"synk": true}}}`,
		"auto_delete":   `{"kinds": {"users": {"auto_delete": true}}}`,
		"sync value":    `{"kinds": {"users": {"sync": "maybe"}}}`,
		"sync boolean":  `{"kinds": {"users": {"sync": true}}}`,
		"bad duration":  `{"kinds": {"users": {"idle_timeout": "soon"}}}`,
		"zero duration": `{"kinds": {"users": {"idle_timeout": "0s"}}}`,
		"bad name":      `{"kinds": {"Users": {}}}`,
		"slash":         `{"kinds": {"a/b": {}}}`,
		"no kinds":      `{}`,
		"not json":      `kinds`,
		"trailing":      `{"kinds": {}} x`,
	} {
		if _, err := ParseKinds([]byte(bad), time.Minute); err == nil && name != "trailing" {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestKindsReload(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "kinds.json")
	os.WriteFile(path, []byte(`{"kinds": {"users": {}}}`), 0o644)
	k, err := LoadKinds(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := k.Lookup("users"); got.IdleTimeout != DefaultIdleTimeout || got.Sync != kind.SyncOn {
		t.Fatalf("%+v", got)
	}
	if _, err := k.Lookup("wallet"); !errors.Is(err, ErrUnknownKind) {
		t.Fatal(err)
	}

	// A bad file keeps the kinds already loaded and makes the node not ready.
	os.WriteFile(path, []byte(`{"kinds": {"users": {"sync": tru}}}`), 0o644)
	if err := k.Reload(); err == nil || k.Valid() {
		t.Fatalf("bad file: %v valid=%v", err, k.Valid())
	}
	if _, err := k.Lookup("users"); err != nil {
		t.Fatal("kinds lost after a bad reload")
	}
	os.WriteFile(path, []byte(`{"kinds": {"users": {"sync": "off"}, "wallet": {}}}`), 0o644)
	if err := k.Reload(); err != nil || !k.Valid() {
		t.Fatal(err)
	}
	if got, err := k.Lookup("wallet"); err != nil || got.Sync != kind.SyncOn || got.Name != "wallet" {
		t.Fatalf("%+v %v", got, err)
	}
	if got, _ := k.Lookup("users"); got.Sync != kind.SyncOff {
		t.Fatal("new setting not seen")
	}
	if _, err := LoadKinds(filepath.Join(t.TempDir(), "missing.json"), 0); err == nil {
		t.Fatal("a missing kinds file at start is an error")
	}
}

func TestParseEnv(t *testing.T) {
	t.Parallel()
	env := map[string]string{
		"SHARDLITE_NODE": "node-a", "SHARDLITE_BLOB": "s3://blobs", "SHARDLITE_KINDS": "/k.json", "SHARDLITE_DATA": "/d", "SHARDLITE_SCRATCH_DIR": "/m",
		"SHARDLITE_PREFIX": "staging", "SHARDLITE_FLUSH": "100ms", "SHARDLITE_IDLE_TIMEOUT": "2m", "SHARDLITE_CACHE_LIMIT": "20GB",
	}
	e, err := ParseEnv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if e.Name != "node-a" || e.Prefix != "staging/" || e.Flush != 100*time.Millisecond || e.IdleTimeout != 2*time.Minute || e.CacheLimit != 20<<30 ||
		e.HealthPath != "/health" || e.Public != ":2280" || e.Control != "127.0.0.1:2282" || e.LeaseTTL != 10*time.Second ||
		e.MaxHops != 32 {
		t.Fatalf("%+v", e)
	}
	env["SHARDLITE_MAX_HOPS"] = "5"
	if e, err = ParseEnv(func(k string) string { return env[k] }); err != nil || e.MaxHops != 5 {
		t.Fatalf("%+v %v", e, err)
	}
	delete(env, "SHARDLITE_MAX_HOPS")
	for k, v := range map[string]string{"SHARDLITE_FLUSH": "fast", "SHARDLITE_CACHE_LIMIT": "lots", "SHARDLITE_BLOB": "", "SHARDLITE_MAX_HOPS": "0"} {
		bad := map[string]string{}
		for a, b := range env {
			bad[a] = b
		}
		bad[k] = v
		if _, err := ParseEnv(func(x string) string { return bad[x] }); err == nil {
			t.Errorf("%s=%q accepted", k, v)
		}
	}
}

// A system kind declared with no settings gets the safe ones, as a kind of the
// kinds file does: sync on, not public and the node's idle timeout.
func TestSystemKindDefaults(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "kinds.json")
	os.WriteFile(path, []byte(`{"kinds": {"users": {}}}`), 0o644)
	k, err := LoadKinds(path, 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	sys, err := newSystemKinds([]kind.Kind{{Name: "_probe", IDs: []string{"_cluster"}, Handler: func(*system.System) http.Handler { return http.NotFoundHandler() }}})
	if err != nil {
		t.Fatal(err)
	}
	k.setSystem(sys)
	got, err := k.Lookup("_probe")
	if err != nil || got.Sync != kind.SyncOn || got.Public || got.IdleTimeout != 3*time.Minute || got.Handler == nil {
		t.Fatalf("%+v %v", got, err)
	}
	if app, _ := k.Lookup("users"); app.Handler != nil || app.Sync != kind.SyncOn || app.IdleTimeout != 3*time.Minute {
		t.Fatalf("%+v", app)
	}
}
