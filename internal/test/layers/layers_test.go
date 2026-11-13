// Package layers checks the module's import graph against its layers, so the
// directory layout says how the code depends on itself and a change that breaks
// that fails here.
package layers

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
)

const module = "github.com/luci1900/shardlite/"

// layers are the module's packages from the bottom up. A package imports only
// packages of a lower rank or of its own group, so the leaves at rank 0 import
// nothing of the module's but their own group.
var layers = []struct {
	group string
	rank  int
}{
	{"internal/model", 0},    // the names and numbers of the protocol
	{"internal/syncs", 0},    // how goroutines wake each other and are run
	{"internal/tempfile", 0}, // the bytes that don't fit in a request
	{"internal/test/fault", 0},
	{"internal/capture", 1}, // a shard's SQLite file
	{"internal/storage", 2}, // what the cluster keeps in blob storage
	{"internal/node", 3},
	{"cmd", 4},
}

// rig is the test rig: anything goes inside it, and outside it only cmd/bench
// and the fault build of cmd/shardlite import it. test/fault is the exception,
// a leaf that production imports to name its hooks.
const rig = "internal/test"

func in(pkg, dir string) bool { return pkg == dir || strings.HasPrefix(pkg, dir+"/") }

func layer(pkg string) (group string, rank int, ok bool) {
	for _, l := range layers {
		if in(pkg, l.group) {
			return l.group, l.rank, true
		}
	}
	return "", 0, false
}

// imports lists each package of the module with the module's packages it
// imports (tests' imports aside), for the build tags given.
func imports(t *testing.T, tags string) map[string][]string {
	t.Helper()
	out := goList(t, "-tags="+tags, "-f", `{{.ImportPath}}{{range .Imports}} {{.}}{{end}}`, "./...")
	graph := map[string][]string{}
	for line := range strings.Lines(out) {
		fields := strings.Fields(line)
		pkg, ok := strings.CutPrefix(fields[0], module)
		if !ok {
			continue
		}
		graph[pkg] = nil
		for _, imp := range fields[1:] {
			if imp, ok := strings.CutPrefix(imp, module); ok {
				graph[pkg] = append(graph[pkg], imp)
			}
		}
	}
	return graph
}

func goList(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("go", append([]string{"list"}, args...)...)
	cmd.Dir = "../../.." // the module's root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list %s: %v", strings.Join(args, " "), err)
	}
	return string(out)
}

func TestLayers(t *testing.T) {
	for _, tags := range []string{"", "fault"} {
		graph := imports(t, tags)
		if len(graph) == 0 {
			t.Fatalf("tags %q: go list found no packages", tags)
		}
		for pkg, imps := range graph {
			group, rank, placed := layer(pkg)
			if !placed {
				if !in(pkg, rig) {
					t.Errorf("%s is in no layer: add it to layers", pkg)
				}
				continue // the rig imports what it likes
			}
			for _, imp := range imps {
				if _, _, ok := layer(imp); !ok && in(imp, rig) {
					if pkg != "cmd/bench" && (pkg != "cmd/shardlite" || tags != "fault") {
						t.Errorf("tags %q: %s imports %s, which is the test rig's", tags, pkg, imp)
					}
					continue
				}
				impGroup, impRank, _ := layer(imp)
				if impGroup != group && impRank >= rank {
					t.Errorf("tags %q: %s (rank %d) imports %s (rank %d): a package imports only lower ranks or its own group", tags, pkg, rank, imp, impRank)
				}
			}
		}
	}
}

// TestBinaryHasNoTesting checks that the shardlite binary, the fault build
// too, doesn't link the testing package, which would bring the test flags
// with it.
func TestBinaryHasNoTesting(t *testing.T) {
	for _, tags := range []string{"", "fault"} {
		deps := strings.Fields(goList(t, "-tags="+tags, "-deps", "./cmd/shardlite"))
		if slices.Contains(deps, "testing") {
			t.Errorf("tags %q: cmd/shardlite links the testing package", tags)
		}
	}
}
