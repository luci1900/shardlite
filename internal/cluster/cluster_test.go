package cluster

import (
	"strings"
	"testing"
)

func TestValidNode(t *testing.T) {
	t.Parallel()
	valid := []string{
		"n1",
		"node-1.example.com",
		"a_b",
		strings.Repeat("a", 128),
	}
	for _, name := range valid {
		if !ValidNode(name) {
			t.Errorf("ValidNode(%q) = false, want true", name)
		}
	}
	invalid := map[string]string{
		"":                       "empty",
		"pods/a":                 "a slash",
		"a b":                    "a space",
		"näme":                   "a non-ASCII letter",
		strings.Repeat("a", 129): "129 characters",
	}
	for name, why := range invalid {
		if ValidNode(name) {
			t.Errorf("ValidNode(%q) = true, want false (%s)", name, why)
		}
	}
}
