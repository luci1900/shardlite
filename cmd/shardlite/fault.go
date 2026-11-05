//go:build fault

package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/blobtest"
	"github.com/luci1900/shardlite/internal/journal"
	"github.com/luci1900/shardlite/internal/kill"
	"github.com/luci1900/shardlite/internal/node"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// faultOptions arms the node's fault hooks from three settings, which exist
// only in a binary built with the fault tag:
//
//	SHARDLITE_KILL_AT         a kill point (internal/kill): the process dies with SIGKILL when it reaches it
//	SHARDLITE_KILL_WHEN       a path: the kill point is armed only once a file exists there, so a test arms it after the node is up and settled
//	SHARDLITE_STORAGE_FAULT   mode:permille:delay, for the journal's segment writes: mode "fail" (an error before
//	                          the write), "lost" (the write happens and the caller sees an error) or "none", the
//	                          permille how many in a thousand fail, and the delay (a Go duration) the longest
//	                          random wait before each write
//	SHARDLITE_TEMP_FAULT      op:permille: "create", "append" or "dup" on the node's temporary files fails at that rate
//	SHARDLITE_MAX_FILES       how many files a manifest lists before the next snapshot is a rebase (a few, to reach one)
//
// A value that does not parse fails the start: a setting that armed nothing
// would leave a test passing without having tested anything.
func faultOptions(getenv func(string) string) ([]node.RunOption, error) {
	var f node.Faults
	var at func(kill.Point)
	if v := getenv("SHARDLITE_KILL_AT"); v != "" {
		point, ok := kill.ByName(v)
		if !ok {
			return nil, fmt.Errorf("SHARDLITE_KILL_AT is %q, which is no kill point", v)
		}
		when := getenv("SHARDLITE_KILL_WHEN")
		at = func(p kill.Point) {
			if p != point {
				return
			}
			if _, err := os.Stat(when); when == "" || err == nil {
				syscall.Kill(os.Getpid(), syscall.SIGKILL)
				select {}
			}
		}
		f.At = at
	}
	if v := getenv("SHARDLITE_STORAGE_FAULT"); v != "" {
		hook, err := storageFault(v, at)
		if err != nil {
			return nil, err
		}
		f.Backend = func(b blob.Backend) blob.Backend {
			h := blobtest.WithHook(b)
			h.SetHook(hook)
			return h
		}
	} else if at != nil {
		// The segment points are in the storage hook, which is there to call them.
		f.Backend = func(b blob.Backend) blob.Backend {
			h := blobtest.WithHook(b)
			h.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
				segmentPoint(at, op, key, after)
				return nil
			})
			return h
		}
	}
	if v := getenv("SHARDLITE_TEMP_FAULT"); v != "" {
		hook, err := tempFault(v)
		if err != nil {
			return nil, err
		}
		f.Temp = hook
	}
	if v := getenv("SHARDLITE_MAX_FILES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("SHARDLITE_MAX_FILES is %q, want a positive number", v)
		}
		f.MaxFiles = n
	}
	return []node.RunOption{node.WithFaults(f)}, nil
}

// segmentPoint calls the kill points of a segment's write.
func segmentPoint(at func(kill.Point), op blobtest.Op, key string, after bool) {
	if at == nil || op != blobtest.OpCreate || !strings.HasPrefix(key, journal.Root) {
		return
	}
	if after {
		at(kill.SegmentAfter)
	} else {
		at(kill.SegmentBefore)
	}
}

func storageFault(v string, at func(kill.Point)) (blobtest.Hook, error) {
	bad := func(why string) error {
		return fmt.Errorf("SHARDLITE_STORAGE_FAULT is %q, want mode:permille:delay such as fail:100:5ms (%s)", v, why)
	}
	f := strings.Split(v, ":")
	if len(f) != 3 {
		return nil, bad("three fields")
	}
	mode := f[0]
	switch mode {
	case "fail", "lost", "none":
	default:
		return nil, bad("mode is fail, lost or none")
	}
	permille, err := strconv.Atoi(f[1])
	if err != nil || permille < 0 || permille > 1000 {
		return nil, bad("permille is 0 to 1000")
	}
	delay, err := time.ParseDuration(f[2])
	if err != nil || delay < 0 {
		return nil, bad("delay is a duration")
	}
	return func(ctx context.Context, op blobtest.Op, key string, after bool) error {
		if op != blobtest.OpCreate || !strings.HasPrefix(key, journal.Root) {
			return nil
		}
		segmentPoint(at, op, key, after)
		if !after && delay > 0 {
			time.Sleep(rand.N(delay + 1))
		}
		if rand.IntN(1000) >= permille {
			return nil
		}
		switch {
		case mode == "fail" && !after:
			return injected("storage unavailable")
		case mode == "lost" && after:
			return injected("response lost")
		}
		return nil
	}, nil
}

// injected is the error of a fault, and says so on stderr, so that a test can
// tell a fault that was never reached from one that was survived.
func injected(what string) error {
	fmt.Fprintln(os.Stderr, "fault: injected "+what)
	return errors.New("injected: " + what)
}

func tempFault(v string) (func(tempfile.Op) error, error) {
	bad := func(why string) error {
		return fmt.Errorf("SHARDLITE_TEMP_FAULT is %q, want op:permille such as append:100 (%s)", v, why)
	}
	opName, rate, ok := strings.Cut(v, ":")
	if !ok {
		return nil, bad("two fields")
	}
	op := tempfile.Op(opName)
	switch op {
	case tempfile.OpCreate, tempfile.OpAppend, tempfile.OpDup:
	default:
		return nil, bad("op is create, append or dup")
	}
	permille, err := strconv.Atoi(rate)
	if err != nil || permille < 0 || permille > 1000 {
		return nil, bad("permille is 0 to 1000")
	}
	return func(got tempfile.Op) error {
		if got == op && rand.IntN(1000) < permille {
			return injected("temporary file " + string(op))
		}
		return nil
	}, nil
}
