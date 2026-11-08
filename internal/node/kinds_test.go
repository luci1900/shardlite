package node

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/kind"
)

func TestParseTarget(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ in, kind, id string }{
		{"users/42", "users", "42"},
		{"guild-chat/0192f6a8-7c1e-7b3a-9d2e-5f4c3b2a1d0e", "guild-chat", "0192f6a8-7c1e-7b3a-9d2e-5f4c3b2a1d0e"},
		{"chunks/3-7", "chunks", "3-7"},
		{"trade/42_57", "trade", "42_57"},
		{"users/" + strings.Repeat("x", 128), "users", strings.Repeat("x", 128)},
		{"matchmaker.queue/7", "matchmaker.queue", "7"}, // a dot is an ordinary character in a kind
		{"_wake/1", "_wake", "1"},                       // the node's kinds parse too, and are answered as undeclared
	} {
		n, err := ParseTarget(tc.in)
		if err != nil || string(n.Kind()) != tc.kind || string(n.ID()) != tc.id {
			t.Errorf("%q: %v %v", tc.in, n, err)
		}
	}
	// A name with no id is malformed, and so is one whose kind is not spelled right.
	for _, bad := range []string{"", "users", "users/", "/42", "Users/1", "../x/1", "users/a\nb", "users/" + strings.Repeat("x", 129),
		"users/a/b", "users/..", "users/.hidden", "users/a b", "users/a%2Fb", "users/é", "users/a.b",
		"a.Users/1", "users./1", "a..users/1", ".users/1"} {
		if _, err := ParseTarget(bad); !errors.Is(err, ErrBadTarget) {
			t.Errorf("%q accepted: %v", bad, err)
		}
	}
}

// A declaration the declaration poll accepted makes its kinds current, and the next
// one replaces them. (The poll itself is tested in declarations_test.go.)
func TestKindsUpdate(t *testing.T) {
	t.Parallel()
	k := NewKinds(0)
	a, err := k.Parse([]byte(`{"kinds": {"users": {}}}`))
	if err != nil {
		t.Fatal(err)
	}
	k.Update(a)
	// A kind that names no sync parses to storage, the conservative tier.
	if got, _ := k.Lookup("users"); got.IdleTimeout != DefaultIdleTimeout || got.Sync != kind.SyncStorage {
		t.Fatalf("%+v", got)
	}
	if _, err := k.Lookup("wallet"); !errors.Is(err, ErrUnknownKind) {
		t.Fatal(err)
	}

	// A later declaration replaces the kinds: a kind that dropped out is
	// unknown, and a changed setting is seen.
	a, err = k.Parse([]byte(`{"kinds": {"users": {"sync": "none"}, "wallet": {}}}`))
	if err != nil {
		t.Fatal(err)
	}
	k.Update(a)
	if got, err := k.Lookup("wallet"); err != nil || got.Sync != kind.SyncStorage || got.Name != "wallet" {
		t.Fatalf("%+v %v", got, err)
	}
	if got, _ := k.Lookup("users"); got.Sync != kind.SyncNone {
		t.Fatal("new setting not seen")
	}
	a, err = k.Parse([]byte(`{"kinds": {"wallet": {}}}`))
	if err != nil {
		t.Fatal(err)
	}
	k.Update(a)
	if _, err := k.Lookup("users"); !errors.Is(err, ErrUnknownKind) {
		t.Fatalf("a kind that dropped out: %v", err)
	}
}

func TestParseEnv(t *testing.T) {
	t.Parallel()
	env := map[string]string{
		"SHARDLITE_NODE": "node-a", "SHARDLITE_BLOB": "s3://blobs", "SHARDLITE_DATA": "/d",
		"SHARDLITE_PREFIX": "staging", "SHARDLITE_FLUSH": "100ms", "SHARDLITE_IDLE_TIMEOUT": "2m",
	}
	e, err := ParseEnv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if e.Name != "node-a" || e.Prefix != "staging/" || e.Flush != 100*time.Millisecond || e.IdleTimeout != 2*time.Minute ||
		e.Public != ":2280" || e.Control != "127.0.0.1:2282" || e.LeaseTTL != 10*time.Second ||
		e.MaxHops != 32 {
		t.Fatalf("%+v", e)
	}
	env["SHARDLITE_MAX_HOPS"] = "5"
	if e, err = ParseEnv(func(k string) string { return env[k] }); err != nil || e.MaxHops != 5 {
		t.Fatalf("%+v %v", e, err)
	}
	delete(env, "SHARDLITE_MAX_HOPS")
	for k, v := range map[string]string{"SHARDLITE_FLUSH": "fast", "SHARDLITE_DELTA_SIZE": "lots", "SHARDLITE_BLOB": "", "SHARDLITE_MAX_HOPS": "0"} {
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
