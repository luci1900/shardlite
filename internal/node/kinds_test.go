package node

import (
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/system"
)

func TestParseTarget(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ in, kind, id string }{
		{"test.users/42", "test.users", "42"},
		{"test.guild-chat/0192f6a8-7c1e-7b3a-9d2e-5f4c3b2a1d0e", "test.guild-chat", "0192f6a8-7c1e-7b3a-9d2e-5f4c3b2a1d0e"},
		{"test.chunks/3-7", "test.chunks", "3-7"},
		{"test.trade/42_57", "test.trade", "42_57"},
		{"test.users/" + strings.Repeat("x", 128), "test.users", strings.Repeat("x", 128)},
		{"acme.billing.purchase/7", "acme.billing.purchase", "7"},
		{"_system.dispatch/_cluster", "_system.dispatch", "_cluster"},
	} {
		n, err := ParseTarget(tc.in)
		if err != nil || string(n.Kind()) != tc.kind || string(n.ID()) != tc.id {
			t.Errorf("%q: %v %v", tc.in, n, err)
		}
	}
	// A kind with no app is a malformed name, and so is one whose app or kind
	// is not spelled right.
	for _, bad := range []string{"", "users", "users/", "/42", "Users/1", "../x/1", "users/a\nb", "users/" + strings.Repeat("x", 129),
		"users/a/b", "users/..", "users/.hidden", "users/a b", "users/a%2Fb", "users/é", "users/a.b",
		"test.Users/1", "test.users./1", "test..users/1", ".users/1"} {
		if _, err := ParseTarget(bad); !errors.Is(err, ErrBadTarget) {
			t.Errorf("%q accepted: %v", bad, err)
		}
	}
}

// A declaration the ready poll accepted makes its kinds current, and the next
// one replaces them. (The poll itself is tested in ready_test.go.)
func TestKindsUpdate(t *testing.T) {
	t.Parallel()
	k := NewKinds(0)
	a, err := k.Parse([]byte(`{"app": "test", "kinds": {"users": {}}}`))
	if err != nil {
		t.Fatal(err)
	}
	k.Update(a)
	// A kind that names no sync parses to storage, the conservative tier.
	if got, _ := k.Lookup("test.users"); got.IdleTimeout != DefaultIdleTimeout || got.Sync != kind.SyncStorage {
		t.Fatalf("%+v", got)
	}
	if _, err := k.Lookup("test.wallet"); !errors.Is(err, ErrUnknownKind) {
		t.Fatal(err)
	}

	// A later declaration replaces the kinds: a kind that dropped out is
	// unknown, and a changed setting is seen.
	a, err = k.Parse([]byte(`{"app": "test", "kinds": {"users": {"sync": "none"}, "wallet": {}}}`))
	if err != nil {
		t.Fatal(err)
	}
	k.Update(a)
	if got, err := k.Lookup("test.wallet"); err != nil || got.Sync != kind.SyncStorage || got.Name != "test.wallet" {
		t.Fatalf("%+v %v", got, err)
	}
	if got, _ := k.Lookup("test.users"); got.Sync != kind.SyncNone {
		t.Fatal("new setting not seen")
	}
	a, err = k.Parse([]byte(`{"app": "test", "kinds": {"wallet": {}}}`))
	if err != nil {
		t.Fatal(err)
	}
	k.Update(a)
	if _, err := k.Lookup("test.users"); !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("a kind that dropped out: %v", err)
	}
}

func TestParseEnv(t *testing.T) {
	t.Parallel()
	env := map[string]string{
		"SHARDLITE_NODE": "node-a", "SHARDLITE_BLOB": "s3://blobs", "SHARDLITE_DATA": "/d",
		"SHARDLITE_PREFIX": "staging", "SHARDLITE_FLUSH": "100ms", "SHARDLITE_IDLE_TIMEOUT": "2m", "SHARDLITE_CACHE_LIMIT": "20GB",
	}
	e, err := ParseEnv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if e.Name != "node-a" || e.Prefix != "staging/" || e.Flush != 100*time.Millisecond || e.IdleTimeout != 2*time.Minute || e.CacheLimit != 20<<30 ||
		e.Public != ":2280" || e.Control != "127.0.0.1:2282" || e.LeaseTTL != 10*time.Second ||
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

// A system kind built into the declaration gets the safe ones, as a kind of an
// app's declaration does: the `storage` tier, not public and the node's idle timeout.
func TestSystemKindDefaults(t *testing.T) {
	t.Parallel()
	k := NewKinds(3 * time.Minute)
	app, sys, err := newSystemApp([]kind.Kind{{Name: "_system.probe", IDs: []shard.ID{"_cluster"}, Handler: func(*system.System) http.Handler { return http.NotFoundHandler() }}}, 3*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	k.setSystem(app)
	if app.Name != "_system" || len(app.Kinds) != 1 {
		t.Fatalf("%+v", app)
	}
	if app.Kinds["_system.probe"].IdleTimeout != 3*time.Minute || sys["_system.probe"].IdleTimeout != 3*time.Minute {
		t.Fatalf("%+v", app.Kinds)
	}
	got, err := k.Lookup("_system.probe")
	if err != nil || got.Sync != kind.SyncStorage || got.Public || got.IdleTimeout != 3*time.Minute || got.Handler == nil {
		t.Fatalf("%+v %v", got, err)
	}
}

// The node's own kinds are named under the node's own app, and no other app may
// hold one.
func TestSystemKindNames(t *testing.T) {
	t.Parallel()
	handler := func(*system.System) http.Handler { return http.NotFoundHandler() }
	for _, name := range []string{"_system.probe", "_system.dispatch"} {
		if _, _, err := newSystemApp([]kind.Kind{{Name: shard.Kind(name), IDs: []shard.ID{"_cluster"}, Handler: handler}}, 0); err != nil {
			t.Errorf("%q refused: %v", name, err)
		}
	}
	for _, name := range []string{"_probe", "probe", "test.probe", "_system.", "_system.probe.x", "_System.probe"} {
		if _, _, err := newSystemApp([]kind.Kind{{Name: shard.Kind(name), IDs: []shard.ID{"_cluster"}, Handler: handler}}, 0); err == nil {
			t.Errorf("%q accepted", name)
		}
	}
}
