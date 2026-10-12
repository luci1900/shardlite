// Command testnode is the capture side of shardlite in its own process, so
// tests can kill it with SIGKILL at chosen points.
//
//	testnode -blobs <dir> -meta <dir> [-node n1] [-flush 20ms]
//
// Starting is the node's real start (node.Start): it takes the lease with a
// new epoch, adopts the journal the previous process left and starts the writer
// after the fence.
//
// It speaks the same line protocol as testapp (tab separated fields, one
// reply line per command):
//
//	open  <name> <path>   a new shard (the app has created the file), or, if the shard has a manifest, restore it to <path> first
//	sync  <name>          capture; replies once the segment holding the commits is in blob storage
//	delta <name>          write a delta and update the manifest
//	merge <name>          merge all manifest files into one base
//	close <name>          final sync, then stop capturing
//	arm   <point>         die with SIGKILL when execution reaches the point: segment-before, segment-after,
//	                      delta-before-manifest, merge-before-manifest, merge-after-swap, restore-before-rename
//	fault <mode> <permille> <delay-ms>   storage faults on segment PUTs: mode "fail" (error before the write),
//	                      "lost" (write happens, caller sees an error) or "none"; plus a delay on every PUT
//	release <name>        final sync, close Litestream and everything else on the shard, then check -shm; replies "released" or "held" (capture is reopened)
//	exit
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/node"
	"github.com/luci1900/shardlite/internal/segment"
	"github.com/luci1900/shardlite/internal/shardfile"
)

type shard struct {
	h  *shardfile.Handle
	sh *capture.Shard
}

type testNode struct {
	nd     *node.Node
	files  *shardfile.Files
	meta   string
	w      *segment.Writer
	mu     sync.Mutex
	shards map[string]*shard

	armed     atomic.Pointer[string]
	faultMode atomic.Pointer[string]
	faultPm   atomic.Int64
	faultMs   atomic.Int64
}

func die() {
	syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {}
}

func (n *testNode) at(point string) {
	if p := n.armed.Load(); p != nil && *p == point {
		die()
	}
}

func (n *testNode) hook(ctx context.Context, op blob.Op, key string, after bool) error {
	if op != blob.OpCreate || !strings.HasPrefix(key, "journal/") {
		return nil
	}
	if after {
		n.at("segment-after")
	} else {
		n.at("segment-before")
		if ms := n.faultMs.Load(); ms > 0 {
			time.Sleep(time.Duration(rand.Int64N(ms+1)) * time.Millisecond)
		}
	}
	mode := n.faultMode.Load()
	if mode == nil || rand.Int64N(1000) >= n.faultPm.Load() {
		return nil
	}
	switch {
	case *mode == "fail" && !after:
		return errors.New("injected: storage unavailable")
	case *mode == "lost" && after:
		return errors.New("injected: response lost")
	}
	return nil
}

func main() {
	blobs := flag.String("blobs", "", "blob directory")
	meta := flag.String("meta", "", "root of the Litestream metadata directories (tmpfs in production)")
	name := flag.String("node", "n1", "node name")
	flush := flag.Duration("flush", 20*time.Millisecond, "segment flush interval")
	flag.Parse()

	local, err := blob.NewLocal(*blobs)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// This process is killed, never the machine: the page cache is enough.
	// (SHARDLITE_FSYNC=1 turns fsync back on, to measure what skipping it saves.)
	local.NoSync = os.Getenv("SHARDLITE_FSYNC") == ""
	nd, err := node.Start(context.Background(), node.Config{
		Name:    *name,
		Backend: local,
		Flush:   *flush,
		Blob:    blob.Options{Backoff: 2 * time.Millisecond},
		Keeper:  coord.KeeperOptions{Interval: time.Second, Margin: 5 * time.Second},
		Lease:   coord.RegistryOptions{TTL: time.Minute, Blob: blob.Options{Backoff: 2 * time.Millisecond}},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	n := &testNode{nd: nd, files: nd.Files, meta: *meta, w: nd.Writer, shards: map[string]*shard{}}
	local.SetHook(n.hook)

	sc := bufio.NewScanner(os.Stdin)
	out := bufio.NewWriter(os.Stdout)
	for sc.Scan() {
		f := strings.Split(sc.Text(), "\t")
		if f[0] == "exit" {
			fmt.Fprintln(out, "ok")
			out.Flush()
			return
		}
		v, err := n.do(f)
		if err != nil {
			fmt.Fprintf(out, "err %s\n", strings.ReplaceAll(err.Error(), "\n", " "))
		} else {
			fmt.Fprintf(out, "ok %s\n", v)
		}
		out.Flush()
	}
}

func removeLocal(path string) {
	for _, p := range []string{path, path + "-wal", path + "-shm", path + ".restoring"} {
		os.RemoveAll(p)
	}
}

// activate starts a new activation of a shard. A restored shard is registered
// with Litestream and synced before any app opens the file: that first sync is
// the snapshot of the restored state, which is not uploaded.
func (n *testNode) activate(ctx context.Context, name, path string, h *shardfile.Handle, restored *capture.Restored) error {
	sh, err := capture.New(capture.Config{Name: name, Path: path, Generation: h.Manifest.Generation, MetaRoot: n.meta, Writer: n.w, Restored: restored})
	if err != nil {
		return err
	}
	if restored != nil {
		if err := sh.Sync(ctx); err != nil {
			return err
		}
	}
	n.mu.Lock()
	n.shards[name] = &shard{h: h, sh: sh}
	n.mu.Unlock()
	n.nd.Track(sh)
	return nil
}

func (n *testNode) get(name string) (*shard, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := n.shards[name]
	if s == nil {
		return nil, fmt.Errorf("%s not open", name)
	}
	return s, nil
}

func (n *testNode) do(f []string) (string, error) {
	ctx := context.Background()
	if len(f) < 2 {
		return "", fmt.Errorf("bad command %q", f)
	}
	cmd, name := f[0], f[1]
	switch cmd {
	case "arm":
		n.armed.Store(&name)
		return "", nil
	case "fault":
		var p, ms int64
		fmt.Sscan(f[2], &p)
		fmt.Sscan(f[3], &ms)
		mode := name
		n.faultMode.Store(&mode)
		n.faultPm.Store(p)
		n.faultMs.Store(ms)
		return "", nil
	case "open":
		path := f[2]
		h, created, err := n.files.Open(ctx, name)
		if err != nil {
			return "", err
		}
		var restored *capture.Restored
		how := "created"
		if !created {
			if err := h.Claim(ctx); err != nil {
				return "", err
			}
			// A restart is a crash: whatever is on local disk may hold commits that never
			// reached the journal, so discard it and restore from what is durable.
			removeLocal(path)
			res, err := n.files.Restore(ctx, name, path, shardfile.RestoreOptions{BeforeRename: func() error { n.at("restore-before-rename"); return nil }})
			switch {
			case errors.Is(err, shardfile.ErrNothingToRestore):
				return "", fmt.Errorf("nothing to restore for %s: the app must create the file again, then use openempty", name)
			case err != nil:
				return "", err
			}
			h, restored = res.Handle, res.Capture
			how = fmt.Sprintf("restored %d", res.State.Position)
		}
		if err := n.activate(ctx, name, path, h, restored); err != nil {
			return "", err
		}
		return how, nil
	case "openempty":
		// A shard with a manifest but nothing durable: capture the file the app created again.
		h, _, err := n.files.Open(ctx, name)
		if err != nil {
			return "", err
		}
		if err := h.Claim(ctx); err != nil {
			return "", err
		}
		return "", n.activate(ctx, name, f[2], h, nil)
	}
	s, err := n.get(name)
	if err != nil {
		return "", err
	}
	switch cmd {
	case "sync":
		return "", s.sh.Sync(ctx)
	case "delta":
		return "", s.h.WriteDelta(ctx, s.sh, shardfile.Hooks{AfterFileWrite: func() error { n.at("delta-before-manifest"); return nil }})
	case "merge":
		return "", s.h.Merge(ctx, shardfile.Hooks{
			AfterFileWrite: func() error { n.at("merge-before-manifest"); return nil },
			AfterMergeSwap: func() error { n.at("merge-after-swap"); return nil },
		})
	case "release":
		released, err := s.sh.Release(ctx)
		if err != nil {
			return "", err
		}
		if released {
			n.nd.Untrack(s.sh)
			n.mu.Lock()
			delete(n.shards, name)
			n.mu.Unlock()
			return "released", nil
		}
		return "held", nil
	case "close":
		err := s.sh.Close(ctx)
		n.nd.Untrack(s.sh)
		n.mu.Lock()
		delete(n.shards, name)
		n.mu.Unlock()
		return "", err
	}
	return "", fmt.Errorf("unknown command %q", cmd)
}
