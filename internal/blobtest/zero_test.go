package blobtest

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
	// The zero is "", no operation at all: a hook that receives it is being
	// handed a value nobody named.
	testZero(t, "Op", Op(""), false, []Op{OpCreate, OpUpdate, OpGet, OpGetRange, OpHead, OpDelete, OpList})
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
