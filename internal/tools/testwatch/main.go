// Command testwatch runs a test binary and catches it when it gets stuck. It is
// meant for go test's -exec flag:
//
//	go test -exec testwatch ./internal/...
//
// It starts the binary as its own child in a new process group and samples the
// child's CPU time every few seconds (with ps, which works on macOS and Linux).
// If the CPU time has not moved (by more than a hundredth of the limit) for the
// idle limit (30 seconds), it sends SIGQUIT, so the Go runtime prints every
// goroutine's stack, waits a few seconds, then kills the process group (which
// takes the testapp children with it). The binary's output and exit status
// pass through, so go test behaves as usual. TESTWATCH_IDLE and
// TESTWATCH_SAMPLE (Go durations) change the limits.
//
// "testwatch report [-n 20]" reads go test -json on stdin and prints the
// output of failed tests, a line per package and the slowest tests.
package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	defaultIdle   = 30 * time.Second
	defaultSample = 3 * time.Second
	quitGrace     = 5 * time.Second
)

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "report" {
		os.Exit(report(os.Args[2:], os.Stdin, os.Stdout))
	}
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: testwatch binary [args...] (for go test -exec) or testwatch report [-n 20] < test.json")
		os.Exit(2)
	}
	os.Exit(run(os.Args[1], os.Args[2:], os.Stdout, os.Stderr, durationEnv("TESTWATCH_IDLE", defaultIdle), durationEnv("TESTWATCH_SAMPLE", defaultSample), quitGrace))
}

func durationEnv(name string, def time.Duration) time.Duration {
	if v := os.Getenv(name); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
}

// run starts the binary with its output going to stdout and stderr, and returns
// the exit code testwatch should exit with.
func run(bin string, args []string, stdout, stderr io.Writer, idle, sample, grace time.Duration) int {
	cmd := exec.Command(bin, args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, stdout, stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(stderr, "testwatch: %v\n", err)
		return 1
	}
	pid := cmd.Process.Pid
	killGroup := func() { syscall.Kill(-pid, syscall.SIGKILL) }

	// An interrupt of go test reaches us and not the child's group, so pass it on.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigs)

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	tick := time.NewTicker(sample)
	defer tick.Stop()
	// A stuck binary still has timers firing (a writer's ticker, the race
	// runtime), so CPU time that grows by less than a hundredth of the idle limit
	// over it does not count as moving.
	minMove := idle / 100
	var last time.Duration
	lastMoved := time.Now()
	for {
		select {
		case err := <-done:
			return exitCode(err, stderr)
		case s := <-sigs:
			syscall.Kill(-pid, s.(syscall.Signal))
		case <-tick.C:
			cpu, err := cpuTime(pid)
			if err != nil {
				continue // it is probably just exiting
			}
			if cpu-last >= minMove || cpu < last {
				last, lastMoved = cpu, time.Now()
				continue
			}
			if time.Since(lastMoved) < idle {
				continue
			}
			fmt.Fprintf(stderr, "\ntestwatch: %s has used no CPU for %v, sending SIGQUIT for the goroutine dump\n", bin, idle)
			syscall.Kill(pid, syscall.SIGQUIT)
			select {
			case <-done:
			case <-time.After(grace):
			}
			killGroup()
			select {
			case <-done:
			case <-time.After(grace):
			}
			fmt.Fprintf(stderr, "testwatch: killed %s (stuck)\n", bin)
			return 1
		}
	}
}

func exitCode(err error, stderr io.Writer) int {
	if err == nil {
		return 0
	}
	if ee, ok := err.(*exec.ExitError); ok {
		if c := ee.ExitCode(); c >= 0 {
			return c
		}
		return 1 // killed by a signal
	}
	fmt.Fprintf(stderr, "testwatch: %v\n", err)
	return 1
}

// cpuTime is the CPU time the process has used so far.
func cpuTime(pid int) (time.Duration, error) {
	out, err := exec.Command("ps", "-o", "cputime=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, err
	}
	return parseCPU(string(out))
}

// parseCPU reads ps's cputime: [[dd-]hh:]mm:ss with optional fractional seconds
// (macOS prints 0:01.23, Linux 00:00:01).
func parseCPU(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	var days int64
	if d, rest, ok := strings.Cut(s, "-"); ok {
		n, err := strconv.ParseInt(d, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cputime %q", s)
		}
		days, s = n, rest
	}
	parts := strings.Split(s, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, fmt.Errorf("cputime %q", s)
	}
	secs, err := strconv.ParseFloat(parts[len(parts)-1], 64)
	if err != nil {
		return 0, fmt.Errorf("cputime %q", s)
	}
	total := time.Duration(secs * float64(time.Second))
	units := []time.Duration{time.Minute, time.Hour}
	for i := 0; i < len(parts)-1; i++ {
		n, err := strconv.ParseInt(parts[len(parts)-2-i], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cputime %q", s)
		}
		total += time.Duration(n) * units[i]
	}
	return total + time.Duration(days)*24*time.Hour, nil
}
