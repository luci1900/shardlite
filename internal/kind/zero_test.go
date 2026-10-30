package kind

import (
	"slices"
	"testing"
)

// TestZeroValues pins the zero-value policy of every closed set in the package
// (docs/internals.md, Failing loudly). The table is hand-written because Go
// cannot enumerate a type's constants at runtime; a member added without a
// case in String or Valid fails make lint, and a change of policy fails here.
func TestZeroValues(t *testing.T) {
	t.Parallel()
	// The zero is the record: the conservative tier, and the declaration's
	// default for a kind that names none.
	testZero(t, "Sync", Sync(0), true, []Sync{SyncStorage, SyncNone, SyncPeer})
}

// TestParseSyncRejectsUnknown holds the one decoder of a closed set to the
// rule: an unknown value is rejected, never mapped onto the zero. A
// declaration naming a tier this node doesn't know fails its read.
func TestParseSyncRejectsUnknown(t *testing.T) {
	t.Parallel()
	if s, err := ParseSync("fast"); err == nil {
		t.Fatalf("ParseSync(\"fast\") = %v, want an error", s)
	}
	for name, want := range map[string]Sync{"": SyncStorage, "peer": SyncPeer, "none": SyncNone, "storage": SyncStorage} {
		if s, err := ParseSync(name); err != nil || s != want {
			t.Errorf("ParseSync(%q) = %v, %v, want %v", name, s, err, want)
		}
	}
}

// testZero asserts one closed set's policy: zero is T(0), safe says whether
// T(0) is one of the members (an inherently safe zero, which causes no
// action) or not (a named Unknown, which no switch may treat as an answer),
// and members are every named member, each with a name of its own.
func testZero[T interface {
	comparable
	String() string
	Valid() bool
}](t *testing.T, name string, zero T, safe bool, members []T) {
	t.Helper()
	if got := zero.Valid(); got != safe {
		t.Errorf("%s(0).Valid() = %v, want %v", name, got, safe)
	}
	if got := slices.Contains(members, zero); got != safe {
		t.Errorf("%s(0) is a member: %v, want %v", name, got, safe)
	}
	seen := map[string]bool{}
	for _, m := range members {
		if s := m.String(); s == "" {
			t.Errorf("%s(%v) has no name", name, m)
		} else if seen[s] {
			t.Errorf("%s(%v) shares the name %q with another member", name, m, s)
		}
		seen[m.String()] = true
		if !m.Valid() {
			t.Errorf("%s(%v) is a named member and is not valid", name, m)
		}
	}
}
