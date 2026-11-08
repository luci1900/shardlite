package node

import (
	"log/slog"
	"net/http"
	"testing"

	"github.com/luci1900/shardlite/internal/cluster"
)

func TestParseNode(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []string
		want   cluster.Node // empty with ok false if the header must be refused
		ok     bool
	}{
		{"absent", nil, "", true},
		{"plain", []string{"node-b"}, "node-b", true},
		{"a digit first", []string{"0"}, "0", true},
		{"dots and underscores", []string{"a.b_c-9"}, "a.b_c-9", true},

		{"empty", []string{""}, "", false},
		{"a space", []string{"a b"}, "", false},
		{"a slash", []string{"a/b"}, "", false},
		{"a quoted string", []string{`"n1"`}, "", false},
		{"twice", []string{"n1", "n2"}, "", false},
		{"a list", []string{"n1, n2"}, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			for _, v := range tc.values {
				h.Add(hdrNode, v)
			}
			got, err := parseNode(h)
			if (err == nil) != tc.ok || got != tc.want {
				t.Fatalf("read %q, %v from %q, want %q (ok %v)", got, err, tc.values, tc.want, tc.ok)
			}
		})
	}
}

func TestParsePlacement(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []string
		avoid  bool
		ok     bool // false if the header must be refused
	}{
		{"absent", nil, false, true},
		{"avoid", []string{`avoid`}, true, true},
		{"avoid with a parameter it does not know", []string{`avoid;for=5`}, true, true},
		{"avoid with parameters of every type", []string{`avoid;a=1;b="x";c=y;d=?0;e`}, true, true},
		{"avoid with spaces around it", []string{` avoid `}, true, true},

		{"garbage", []string{`???`}, false, false},
		{"empty", []string{``}, false, false},
		{"another token", []string{`accept`}, false, false},
		{"a different case", []string{`Avoid`}, false, false},
		{"a string", []string{`"avoid"`}, false, false},
		{"an integer", []string{`1`}, false, false},
		{"a boolean", []string{`?1`}, false, false},
		{"an inner list", []string{`(avoid)`}, false, false},
		{"a list of items", []string{`avoid, avoid`}, false, false},
		{"a dictionary", []string{`avoid=1`}, false, false},
		{"a cut off parameter", []string{`avoid;`}, false, false},
		{"over two lines", []string{`avoid`, `;for=5`}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			for _, v := range tc.values {
				h.Add(hdrPlacement, v)
			}
			avoid, err := parsePlacement(h)
			if (err == nil) != tc.ok || avoid != tc.avoid {
				t.Fatalf("read %v, %v from %q, want %v (ok %v)", avoid, err, tc.values, tc.avoid, tc.ok)
			}
		})
	}
}

// A node stamps its name on a forward and avoid only while it drains, and what it stamps reads back.
func TestStampPeerHeaders(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	m := &Shardset{n: &Node{Config: Config{Name: "n0"}}, tab: newTable(log), log: log}
	stamped := func() http.Header {
		h := http.Header{}
		m.stampNode(h)
		m.stampPlacement(h)
		return h
	}
	h := stamped()
	if node, err := parseNode(h); err != nil || node != "n0" {
		t.Fatalf("stamped the node %q, %v", node, err)
	}
	if len(h.Values(hdrPlacement)) != 0 {
		t.Fatalf("a node that doesn't drain stamped %v", h.Values(hdrPlacement))
	}
	m.draining.Store(true)
	h = stamped()
	if avoid, err := parsePlacement(h); err != nil || !avoid || h.Get(hdrPlacement) != "avoid" {
		t.Fatalf("a draining node stamped %q: %v, %v", h.Get(hdrPlacement), avoid, err)
	}
	m.draining.Store(false)
	if len(stamped().Values(hdrPlacement)) != 0 {
		t.Fatal("a node that stopped draining still stamps avoid")
	}
}

// The fault hook decides what goes on the wire: the value as it is, another one or none.
func TestPeerHeaderFaultRewritesWhatIsSent(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	stamp := func(f func(string, string) string) http.Header {
		m := &Shardset{n: &Node{Config: Config{Name: "n0", Faults: Faults{PeerHeader: f}}}, tab: newTable(log), log: log}
		m.draining.Store(true)
		h := http.Header{}
		m.stampNode(h)
		m.stampPlacement(h)
		return h
	}
	if h := stamp(nil); h.Get(hdrNode) != "n0" || h.Get(hdrPlacement) != "avoid" {
		t.Fatalf("with no fault sent %v", h)
	}
	if h := stamp(func(string, string) string { return "" }); len(h) != 0 {
		t.Fatalf("dropped headers were sent: %v", h)
	}
	// The hook is given the header's name with the value: garbling one header leaves the other alone.
	h := stamp(func(name, value string) string {
		if name == hdrPlacement {
			return "???"
		}
		return value
	})
	if name, err := parseNode(h); err != nil || name != "n0" {
		t.Fatalf("the name was touched with the placement: %q, %v (sent %v)", name, err, h)
	}
	if _, err := parsePlacement(h); err == nil {
		t.Fatalf("a garbled placement reads: %v", h)
	}
}
