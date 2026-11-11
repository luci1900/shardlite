//go:build model

package capture_test

import (
	"encoding/json"
	"io/fs"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The tests that need Quint. The model suite runs them (see the testwatch
// README), and each fails rather than skips without Quint, so that a missing
// tool never reads as a pass.

func quint(t *testing.T) string {
	t.Helper()
	p, err := exec.LookPath("quint")
	if err != nil {
		t.Fatal("quint is not on the path, and the model tests need it (see internal/tools/testwatch/README.md)")
	}
	return p
}

// The committed traces are the ones generate.sh makes from the model as it is
// now. Each file's #meta says when it was made, so only the states are compared.
func TestModelTracesCurrent(t *testing.T) {
	quint(t)
	dir := t.TempDir()
	if out, err := exec.Command("sh", "testdata/traces/generate.sh", dir).CombinedOutput(); err != nil {
		t.Fatalf("generate.sh: %v\n%s", err, out)
	}
	made, committed := itfFiles(t, dir), itfFiles(t, "testdata/traces")
	if !slices.Equal(made, committed) {
		t.Fatalf("generate.sh makes %v, testdata/traces holds %v", made, committed)
	}
	for _, rel := range made {
		if !reflect.DeepEqual(states(t, filepath.Join(dir, rel)), states(t, filepath.Join("testdata/traces", rel))) {
			t.Errorf("%s is not what generate.sh makes from the model now: run it and review the diff", rel)
		}
	}
}

// itfFiles is the traces under a directory, as paths relative to it.
func itfFiles(t *testing.T, dir string) []string {
	t.Helper()
	var paths []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".itf.json") {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		paths = append(paths, rel)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return paths
}

func states(t *testing.T, path string) any {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var itf struct{ States any }
	if err := json.Unmarshal(b, &itf); err != nil {
		t.Fatal(err)
	}
	return itf.States
}

// Random traces of the protocol, made from a new seed each run, played against
// the code as the committed ones are. SHARDLITE_SEED repeats a run and
// SHARDLITE_TRACES sets how many traces it makes (100 by default). A trace that
// fails is kept and its path logged, to be added to testdata/traces when the
// failure is fixed.
func TestModelRandomTraces(t *testing.T) {
	q := quint(t)
	seed := rand.Uint64()
	if v := os.Getenv("SHARDLITE_SEED"); v != "" {
		var err error
		if seed, err = strconv.ParseUint(v, 10, 64); err != nil {
			t.Fatal(err)
		}
	}
	n := 100
	if v := os.Getenv("SHARDLITE_TRACES"); v != "" {
		var err error
		if n, err = strconv.Atoi(v); err != nil || n < 1 {
			t.Fatalf("SHARDLITE_TRACES=%q is not a positive number", v)
		}
	}
	t.Logf("seed: SHARDLITE_SEED=%d, %d traces", seed, n)

	dir := t.TempDir()
	cmd := exec.Command(q, "run", "../../specs/capture.qnt", "--main=captureProtocol", "--invariant=safe",
		"--max-steps=40", "--max-samples="+strconv.Itoa(n), "--n-traces="+strconv.Itoa(n),
		"--seed="+strconv.FormatUint(seed, 10), "--mbt", "--out-itf="+filepath.Join(dir, "{seq}.itf.json"))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("quint run (safe should hold): %v\n%s", err, out)
	}
	paths := itfFiles(t, dir)
	if len(paths) != n {
		t.Fatalf("quint made %d traces, not %d", len(paths), n)
	}
	for _, rel := range paths {
		p := filepath.Join(dir, rel)
		t.Run(strings.TrimSuffix(rel, ".itf.json"), func(t *testing.T) {
			t.Parallel()
			t.Cleanup(func() {
				if t.Failed() {
					keep(t, p)
				}
			})
			newReplayer(t).play(loadTrace(t, p))
		})
	}
}

// keep copies a failed trace out of the test's temporary directory.
func keep(t *testing.T, path string) {
	dir, err := os.MkdirTemp("", "shardlite-trace-")
	if err == nil {
		var b []byte
		if b, err = os.ReadFile(path); err == nil {
			dst := filepath.Join(dir, filepath.Base(path))
			if err = os.WriteFile(dst, b, 0o644); err == nil {
				t.Logf("the trace is kept at %s", dst)
				return
			}
		}
	}
	t.Logf("the trace could not be kept: %v", err)
}
