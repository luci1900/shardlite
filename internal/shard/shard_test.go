package shard

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseName(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in         string
		kind, id   string
		app, local string
	}{
		{"game.users/42", "game.users", "42", "game", "users"},
		{"acme.billing.purchase/x-y_1", "acme.billing.purchase", "x-y_1", "acme.billing", "purchase"},
		{"test.users/" + strings.Repeat("a", MaxIDLen), "test.users", strings.Repeat("a", MaxIDLen), "test", "users"},
		{"_system.dispatch/_cluster", "_system.dispatch", "_cluster", "_system", "dispatch"},
		{"a.b-c_d/a", "a.b-c_d", "a", "a", "b-c_d"},
	} {
		n, err := ParseName(tc.in)
		if err != nil {
			t.Errorf("%s: %v", tc.in, err)
			continue
		}
		if n.String() != tc.in || string(n.Kind()) != tc.kind || string(n.ID()) != tc.id {
			t.Errorf("%s parsed to %q, kind %q, id %q", tc.in, n, n.Kind(), n.ID())
		}
		app, local, ok := Split(n.Kind())
		if !ok || string(app) != tc.app || local != tc.local {
			t.Errorf("%s split to %q, %q, %v", tc.in, app, local, ok)
		}
	}

	for _, bad := range []string{
		"", "users", "users/1", "game.users", "game.users/", "/42",
		"game.Users/42", "game..users/42", "game.users/a.b", "game.users/a b",
		"game.users/" + strings.Repeat("a", MaxIDLen+1),
		strings.Repeat("a", MaxAppLen+1) + ".users/42",
		"game." + strings.Repeat("a", MaxKindLen+1) + "/42",
		"game.users/42/43",
	} {
		if n, err := ParseName(bad); err == nil {
			t.Errorf("%q parsed to %q", bad, n)
		}
	}

	if ValidKindName("users") || !ValidKindName("game.users") || ValidKind("Users") || ValidApp("Game") || ValidApp("_system") != true || Reserved("game") || !Reserved("_system") {
		t.Error("the name rules disagree with the parser")
	}
	if got := Qualify("game", "users"); got != "game.users" {
		t.Errorf("Qualify: %q", got)
	}
	if n := MustParse("game.users/42"); n != New("game.users", "42") {
		t.Errorf("MustParse: %q", n)
	}
}

func TestNameZero(t *testing.T) {
	t.Parallel()
	var n Name
	if !n.IsZero() || n.String() != "" {
		t.Fatalf("zero name: %q, zero %v", n, n.IsZero())
	}
	if _, err := n.MarshalText(); err == nil {
		t.Error("a zero name encoded")
	}
	if _, err := n.Value(); err == nil {
		t.Error("a zero name stored")
	}
	if n.System() {
		t.Error("a zero name is a system shard")
	}
}

func TestNameText(t *testing.T) {
	t.Parallel()
	n := MustParse("game.users/42")
	b, err := n.MarshalText()
	if err != nil || string(b) != "game.users/42" {
		t.Fatalf("MarshalText: %q, %v", b, err)
	}
	var got Name
	if err := got.UnmarshalText(b); err != nil || got != n {
		t.Fatalf("UnmarshalText: %q, %v", got, err)
	}
	if err := got.UnmarshalText([]byte("users")); err == nil {
		t.Error("UnmarshalText accepted a bad name")
	}

	// A map key and a value are the same string a header carries.
	raw, err := json.Marshal(map[Name]Name{n: n})
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"game.users/42":"game.users/42"}` {
		t.Fatalf("JSON: %s", raw)
	}
	var back map[Name]Name
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back[n] != n {
		t.Fatalf("JSON round trip: %+v", back)
	}
	if _, err := json.Marshal(Name{}); err == nil {
		t.Error("a zero name encoded as JSON")
	}
}

func TestNameSQL(t *testing.T) {
	t.Parallel()
	n := MustParse("game.users/42")
	v, err := n.Value()
	if err != nil || v != "game.users/42" {
		t.Fatalf("Value: %v, %v", v, err)
	}
	for _, src := range []any{"game.users/42", []byte("game.users/42")} {
		var got Name
		if err := got.Scan(src); err != nil || got != n {
			t.Fatalf("Scan(%T): %q, %v", src, got, err)
		}
	}
	var got Name
	for _, src := range []any{nil, 42, "users"} {
		if err := got.Scan(src); err == nil {
			t.Errorf("Scan(%v) accepted it", src)
		}
	}
}

func TestNameSystem(t *testing.T) {
	t.Parallel()
	if !MustParse("_system.sweep/_cluster").System() || MustParse("game.users/42").System() {
		t.Error("System disagrees with the reserved app")
	}
}
