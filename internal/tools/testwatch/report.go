package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"
)

type testEvent struct {
	Action  string
	Package string
	Test    string
	Output  string
	Elapsed float64
}

// report reads go test -json and prints the output of every failed test and of
// every test that never finished, a line per package, and the n slowest tests
// (subtests included).
//
// The stack dump of a binary that was stopped (by testwatch, or by -timeout)
// is printed with its package's failure. go test -json gives each line of
// output to the test that last printed a line of its own, which with parallel
// tests is rarely the stuck one, and is often one that has passed already. So
// from the first line of a dump on, every line of the package's output is
// taken as the dump's.
func report(args []string, in io.Reader, out io.Writer) int {
	fs := flag.NewFlagSet("report", flag.ContinueOnError)
	n := fs.Int("n", 20, "how many of the slowest tests to print")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	type result struct {
		name    string
		elapsed float64
	}
	var (
		tests    []result
		outputs  = map[string]*strings.Builder{} // by package and test ("" for the package's own)
		packages []string
		failed   bool
		running  = map[string]bool{}             // tests started and not finished, by key
		order    []string                        // the keys of running, in the order they started
		dumps    = map[string]*strings.Builder{} // by package, from the first line of a dump on
	)
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var e testEvent
		if json.Unmarshal(sc.Bytes(), &e) != nil {
			continue
		}
		key := e.Package + " " + e.Test
		switch e.Action {
		case "run":
			running[key] = true
			order = append(order, key)
		case "skip":
			delete(running, key)
		case "output":
			if dumps[e.Package] == nil && startsDump(e.Output) {
				dumps[e.Package] = &strings.Builder{}
			}
			if d := dumps[e.Package]; d != nil {
				d.WriteString(e.Output)
				continue
			}
			if outputs[key] == nil {
				outputs[key] = &strings.Builder{}
			}
			outputs[key].WriteString(e.Output)
		case "pass", "fail":
			delete(running, key)
			if e.Test != "" {
				tests = append(tests, result{key, e.Elapsed})
				if e.Action == "fail" {
					failed = true
					if b := outputs[key]; b != nil {
						fmt.Fprintf(out, "--- FAIL: %s\n%s\n", key, b)
					}
				}
				continue
			}
			word := "ok  "
			if e.Action == "fail" {
				word, failed = "FAIL", true
				if b := outputs[key]; b != nil { // build failures
					fmt.Fprintf(out, "%s\n", b)
				}
				if d := dumps[e.Package]; d != nil {
					fmt.Fprintf(out, "--- STOPPED: %s\n%s\n", e.Package, d)
				}
			}
			packages = append(packages, fmt.Sprintf("%s  %-48s %.1fs", word, e.Package, e.Elapsed))
		}
	}
	for _, key := range order {
		if running[key] {
			failed = true
			fmt.Fprintf(out, "--- NOT FINISHED: %s\n", key)
			if b := outputs[key]; b != nil {
				fmt.Fprintf(out, "%s\n", b)
			}
		}
	}
	for _, p := range packages {
		fmt.Fprintln(out, p)
	}
	sort.SliceStable(tests, func(i, j int) bool { return tests[i].elapsed > tests[j].elapsed })
	if len(tests) > *n {
		tests = tests[:*n]
	}
	fmt.Fprintf(out, "\nslowest tests:\n")
	for _, r := range tests {
		fmt.Fprintf(out, "%8.2fs  %s\n", r.elapsed, r.name)
	}
	if failed {
		return 1
	}
	return 0
}

// startsDump reports whether a line of output is the first of a stopped
// binary's stack dump: testwatch's own line before its SIGQUIT, the runtime's
// for a SIGQUIT from elsewhere, or the panic of go test's -timeout.
func startsDump(line string) bool {
	for _, p := range []string{"testwatch: ", "SIGQUIT: quit", "panic: test timed out"} {
		if strings.HasPrefix(line, p) {
			return true
		}
	}
	return false
}
