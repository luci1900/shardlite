package apptest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"regexp"
	"sync"
	"syscall"
	"time"
)

// Proc is a process whose output is kept, for the tests and tools that run the
// node and an app as separate processes (App is the one with a line protocol).
type Proc struct {
	Name string
	Cmd  *exec.Cmd
	Done chan struct{} // closes when the process has ended
	Err  error         // how it ended, once Done has closed

	mu  sync.Mutex
	out bytes.Buffer
}

// StartProc starts bin with env added to this process's own, keeping what it
// writes to its output and error, and copying that to tee if tee is not nil.
func StartProc(name, bin string, env []string, tee io.Writer, args ...string) (*Proc, error) {
	p := &Proc{Name: name, Done: make(chan struct{})}
	p.Cmd = exec.Command(bin, args...)
	p.Cmd.Env = append(os.Environ(), env...)
	var w io.Writer = (*procOut)(p)
	if tee != nil {
		w = io.MultiWriter(w, tee)
	}
	p.Cmd.Stdout, p.Cmd.Stderr = w, w
	if err := p.Cmd.Start(); err != nil {
		return nil, err
	}
	go func() { p.Err = p.Cmd.Wait(); close(p.Done) }()
	return p, nil
}

type procOut Proc

func (o *procOut) Write(b []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.out.Write(b)
}

// Output is what the process has written so far.
func (p *Proc) Output() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.out.String()
}

// WaitFor waits for a line of output matching re and returns its submatches. It
// fails when the process ends first or the timeout passes, with the output so
// far in the error.
func (p *Proc) WaitFor(re string, timeout time.Duration) ([]string, error) {
	rx := regexp.MustCompile(re)
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if m := rx.FindStringSubmatch(p.Output()); m != nil {
			return m, nil
		}
		select {
		case <-p.Done:
			return nil, fmt.Errorf("%s exited (%v) before printing %q:\n%s", p.Name, p.Err, re, p.Output())
		case <-deadline.C:
			return nil, fmt.Errorf("%s: no %q within %v:\n%s", p.Name, re, timeout, p.Output())
		case <-tick.C:
		}
	}
}

// Kill ends the process with SIGKILL and waits for it.
func (p *Proc) Kill() {
	if p.Cmd.Process != nil {
		p.Cmd.Process.Signal(syscall.SIGKILL)
	}
	<-p.Done
}

// Signal sends s to the process.
func (p *Proc) Signal(s syscall.Signal) { p.Cmd.Process.Signal(s) }

// Term asks the process to stop and waits for its exit status, killing it if
// it has not stopped within timeout.
func (p *Proc) Term(timeout time.Duration) error {
	p.Signal(syscall.SIGTERM)
	select {
	case <-p.Done:
		return p.Err
	case <-time.After(timeout):
		p.Kill()
		return errors.New("did not stop on SIGTERM")
	}
}

// FreeAddr returns a loopback address that nothing listens on at the moment.
func FreeAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.Addr().String(), nil
}
