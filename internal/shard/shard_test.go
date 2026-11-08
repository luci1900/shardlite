package shard

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseName(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in       string
		kind, id string
	}{
		{"users/42", "users", "42"},
		{"guild-chat/x-y_1", "guild-chat", "x-y_1"},
		{"users/" + strings.Repeat("a", MaxIDLen), "users", strings.Repeat("a", MaxIDLen)},
		// A dot is an ordinary character in a kind's name: the node never splits at it.
		{"matchmaker.queue/7", "matchmaker.queue", "7"},
		{"a.b-c_d.e1/a", "a.b-c_d.e1", "a"},
		// A kind starting with "_" is the node's, and a valid name for one.
		{"_wake/1", "_wake", "1"},
		{"_node.probe/_cluster", "_node.probe", "_cluster"},
		{strings.Repeat("a", MaxKindLen) + "/1", strings.Repeat("a", MaxKindLen), "1"},
	} {
		n, err := ParseName(tc.in)
		if err != nil {
			t.Errorf("%s: %v", tc.in, err)
			continue
		}
		if n.String() != tc.in || string(n.Kind()) != tc.kind || string(n.ID()) != tc.id {
			t.Errorf("%s parsed to %q, kind %q, id %q", tc.in, n, n.Kind(), n.ID())
		}
	}

	for _, bad := range []string{
		"", "users", "users/", "/42", "Users/42", "users/a.b", "users/a b", "users/42/43",
		"a..b/1", ".a/1", "a./1", "a.b./1", "a.1b/1", "1a/1", "-a/1", "a.-b/1", "A.b/1", "a.B/1", "a b/1",
		"__wake/1", "_/1", "_.a/1", "_1/1",
		"users/" + strings.Repeat("a", MaxIDLen+1),
		strings.Repeat("a", MaxKindLen+1) + "/1",
	} {
		if n, err := ParseName(bad); err == nil {
			t.Errorf("%q parsed to %q", bad, n)
		}
	}

	// The length limit is on the whole kind, dots included.
	if k := strings.Repeat("ab.", 21) + "a"; len(k) != MaxKindLen || !ValidKind(k) {
		t.Errorf("a %d character kind with dots was refused", len(k))
	}
	if k := strings.Repeat("ab.", 21) + "ab"; len(k) != MaxKindLen+1 || ValidKind(k) {
		t.Errorf("a %d character kind with dots was accepted", len(k))
	}

	if ValidKind("Users") || ValidKind("") || !ValidKind("users") || !ValidKind("a.b") || ValidKind("a..b") {
		t.Error("the name rules disagree with the parser")
	}
	if n := MustParse("users/42"); n != New("users", "42") {
		t.Errorf("MustParse: %q", n)
	}
}

func TestKindReserved(t *testing.T) {
	t.Parallel()
	for k, want := range map[Kind]bool{"users": false, "matchmaker.queue": false, "_wake": true, "_node.probe": true} {
		if got := k.Reserved(); got != want {
			t.Errorf("%q: Reserved is %v, want %v", k, got, want)
		}
		if !ValidKind(string(k)) {
			t.Errorf("%q is not a valid kind", k)
		}
	}
	if n := MustParse("_wake/1"); !n.Kind().Reserved() {
		t.Error("a name parsed from a reserved kind is not reserved")
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
}

func TestNameText(t *testing.T) {
	t.Parallel()
	n := MustParse("users/42")
	b, err := n.MarshalText()
	if err != nil || string(b) != "users/42" {
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
	if string(raw) != `{"users/42":"users/42"}` {
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

// A lane is a hash of the name that never changes, since keys in storage lead with it.
func TestNameLane(t *testing.T) {
	t.Parallel()
	for name, want := range map[string]string{"users/42": "6", "wallet/42": "f", "slow/1": "c"} {
		if got := MustParse(name).Lane().String(); got != want {
			t.Errorf("%s is in lane %s, want %s", name, got, want)
		}
	}
	seen := map[Lane]bool{}
	for i := range 200 {
		l := MustParse("users/" + string(rune('a'+i%26)) + string(rune('a'+i/26))).Lane()
		if l >= Lanes || len(l.String()) != 1 {
			t.Fatalf("lane %d is out of range", l)
		}
		seen[l] = true
	}
	if len(seen) != Lanes {
		t.Fatalf("%d lanes used, want %d", len(seen), Lanes)
	}
}
