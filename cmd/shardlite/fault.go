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
	"github.com/luci1900/shardlite/internal/shardfile"
	"github.com/luci1900/shardlite/internal/tempfile"
)

// faultOptions arms the node's fault hooks from the SHARDLITE_FAULT_ settings,
// which exist only in a binary built with the fault tag:
//
//	SHARDLITE_FAULT_KILL_AT       a kill point (internal/kill): the process dies with SIGKILL when it reaches it
//	SHARDLITE_FAULT_KILL_WHEN     a path: the kill point is armed only once a file exists there, so a test arms it after the node is up and settled
//	SHARDLITE_FAULT_STORAGE       mode:permille:delay[:target], or several separated by commas, for
//	                              the creates of the journal's segments (target "segments", the
//	                              default), the creates of LTX files ("files") or the deletes of
//	                              LTX files ("file-deletes"): mode "fail" (an error before the
//	                              call), "lost" (the call happens and the caller sees an error) or
//	                              "none", the permille how many in a thousand fail, and the delay
//	                              (a Go duration) the longest random wait before each call
//	SHARDLITE_FAULT_NO_LEASE_EXPIRY true: the node never judges its own lease expired by time, only
//	                              by a renewal or a confirmation that finds it taken, so a SIGSTOP
//	                              past the TTL is a pause it cannot see (lease.KeeperOptions.NoExpiry)
//	SHARDLITE_FAULT_UNCONFIRMED   true: storage answers leave without confirming that the node still
//	                              owns its shards, which breaks the storage tier, for a test to catch
//	SHARDLITE_FAULT_TEMP          op:permille: "create", "append" or "dup" on the node's temporary files fails at that rate
//	SHARDLITE_FAULT_PEER_HEADER   mode:permille: for that many in a thousand of the values of the peer
//	                              headers the node sends (Shardlite-Node on its forwards and, while it drains,
//	                              Shardlite-Placement on its answers and forwards), "drop" sends none and
//	                              "garble" sends one that does not read
//	SHARDLITE_FAULT_MAX_FILES     how many files a manifest lists before the next snapshot is a rebase (a few, to reach one)
//
// A value that does not parse fails the start: a setting that armed nothing
// would leave a test passing without having tested anything.
func faultOptions(getenv func(string) string) ([]node.RunOption, error) {
	var f node.Faults
	var at func(kill.Point)
	if v := getenv("SHARDLITE_FAULT_KILL_AT"); v != "" {
		point, ok := kill.ByName(v)
		if !ok {
			return nil, fmt.Errorf("SHARDLITE_FAULT_KILL_AT is %q, which is no kill point", v)
		}
		when := getenv("SHARDLITE_FAULT_KILL_WHEN")
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
	// The storage hook runs the segments' kill points, then the storage faults.
	var hooks []blobtest.Hook
	if at != nil {
		hooks = append(hooks, func(ctx context.Context, op blobtest.Op, key string, after bool) error {
			segmentPoint(at, op, key, after)
			return nil
		})
	}
	if v := getenv("SHARDLITE_FAULT_STORAGE"); v != "" {
		for one := range strings.SplitSeq(v, ",") {
			hook, err := storageFault(one)
			if err != nil {
				return nil, err
			}
			hooks = append(hooks, hook)
		}
	}
	if len(hooks) > 0 {
		f.Backend = func(b blob.Backend) blob.Backend {
			h := blobtest.WithHook(b)
			h.SetHook(func(ctx context.Context, op blobtest.Op, key string, after bool) error {
				for _, hook := range hooks {
					if err := hook(ctx, op, key, after); err != nil {
						return err
					}
				}
				return nil
			})
			return h
		}
	}
	if v := getenv("SHARDLITE_FAULT_TEMP"); v != "" {
		hook, err := tempFault(v)
		if err != nil {
			return nil, err
		}
		f.Temp = hook
	}
	if v := getenv("SHARDLITE_FAULT_PEER_HEADER"); v != "" {
		hook, err := peerHeaderFault(v)
		if err != nil {
			return nil, err
		}
		f.PeerHeader = hook
	}
	if v := getenv("SHARDLITE_FAULT_MAX_FILES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("SHARDLITE_FAULT_MAX_FILES is %q, want a positive number", v)
		}
		f.MaxFiles = n
	}
	for name, set := range map[string]*bool{"SHARDLITE_FAULT_NO_LEASE_EXPIRY": &f.NoLeaseExpiry, "SHARDLITE_FAULT_UNCONFIRMED": &f.Unconfirmed} {
		if v := getenv(name); v != "" {
			on, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("%s is %q, want true or false", name, v)
			}
			*set = on
		}
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

func storageFault(v string) (blobtest.Hook, error) {
	bad := func(why string) error {
		return fmt.Errorf("SHARDLITE_FAULT_STORAGE is %q, want mode:permille:delay[:target] such as fail:100:5ms (%s)", v, why)
	}
	f := strings.Split(v, ":")
	if len(f) != 3 && len(f) != 4 {
		return nil, bad("three fields and an optional target")
	}
	// What the fault is for: the calls of this op under this prefix.
	op, prefix := blobtest.OpCreate, journal.Root
	if len(f) == 4 {
		switch f[3] {
		case "segments":
		case "files":
			prefix = shardfile.FilesPrefix
		case "file-deletes":
			op, prefix = blobtest.OpDelete, shardfile.FilesPrefix
		default:
			return nil, bad("target is segments, files or file-deletes")
		}
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
	return func(ctx context.Context, got blobtest.Op, key string, after bool) error {
		if got != op || !strings.HasPrefix(key, prefix) {
			return nil
		}
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
		return fmt.Errorf("SHARDLITE_FAULT_TEMP is %q, want op:permille such as append:100 (%s)", v, why)
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

// garbledNode are values of Shardlite-Node that are no node's name: garbage, a
// name that isn't one, and nothing.
var garbledNode = []string{`???`, `"n0"`, ``}

// garbledPlacement are values of Shardlite-Placement that do not read or do not
// say avoid: garbage, the wrong type, another token, a list and a dictionary.
var garbledPlacement = []string{`???`, `"avoid"`, `accept`, `(avoid)`, `avoid=1`}

func peerHeaderFault(v string) (func(name, value string) string, error) {
	bad := func(why string) error {
		return fmt.Errorf("SHARDLITE_FAULT_PEER_HEADER is %q, want mode:permille such as garble:300 (%s)", v, why)
	}
	mode, rate, ok := strings.Cut(v, ":")
	if !ok {
		return nil, bad("two fields")
	}
	if mode != "drop" && mode != "garble" {
		return nil, bad("mode is drop or garble")
	}
	permille, err := strconv.Atoi(rate)
	if err != nil || permille < 0 || permille > 1000 {
		return nil, bad("permille is 0 to 1000")
	}
	return func(name, value string) string {
		if rand.IntN(1000) >= permille {
			return value
		}
		if mode == "drop" {
			injected("peer header " + name + " dropped")
			return ""
		}
		injected("peer header " + name + " garbled")
		garbled := garbledNode
		if name == "Shardlite-Placement" {
			garbled = garbledPlacement
		}
		return garbled[rand.IntN(len(garbled))]
	}, nil
}
