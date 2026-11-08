//go:build fault

package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
)

// The measurements run the real binary and read its peak memory from the
// resource usage the kernel kept for it, so they need the process to be over.
// They are skipped without SHARDLITE_MEASURE_MB.

func measureMB(t *testing.T) int {
	mb, _ := strconv.Atoi(os.Getenv("SHARDLITE_MEASURE_MB"))
	if mb == 0 {
		t.Skip("SHARDLITE_MEASURE_MB is not set")
	}
	return mb
}

// maxrssMB kills the node and returns its peak resident memory in megabytes.
func (s *stack) maxrssMB() float64 {
	s.t.Helper()
	s.node.kill()
	ru, ok := s.node.cmd.ProcessState.SysUsage().(*syscall.Rusage)
	if !ok {
		s.t.Fatal("no resource usage for the node")
	}
	rss := float64(ru.Maxrss)
	if runtime.GOOS != "darwin" {
		rss *= 1024 // kilobytes elsewhere
	}
	return rss / (1 << 20)
}

// build writes a shard of about mb megabytes in commits of rows rows of rowBytes
// each, with a delta every few, and stops the node gracefully so every tail is a
// delta. It returns the number of commits.
func (s *stack) buildShard(shard string, mb, rows, rowBytes int) int {
	s.t.Helper()
	commits := max(1, mb*(1<<20)/(rows*rowBytes))
	t0 := time.Now()
	for range commits {
		st, body, err := doRequest(longClient, "POST", "http://"+s.pub+at(shard, fmt.Sprintf("/bulk?rows=%d&bytes=%d", rows, rowBytes)), shard, "")
		if err != nil || st != 204 {
			s.t.Fatalf("bulk: %d %q %v", st, body, err)
		}
	}
	if err := s.node.term(5 * time.Minute); err != nil {
		s.t.Fatalf("stopping the node: %v", err)
	}
	apptest.Eventually(s.t, apptest.Timeout, "the app to close its databases", func() bool { return s.appDebug().OpenDBs == 0 })
	s.t.Logf("built a shard of %d commits in %v", commits, time.Since(t0).Round(time.Second))
	return commits
}

// manifestFiles is how many files the shard's manifest lists.
func (v *verifier) manifestFiles(shard string) int {
	m, _, err := v.files.GetManifest(context.Background(), shardName(shard))
	if err != nil {
		v.t.Fatal(err)
	}
	return len(m.Files)
}

// activate asks for the shard, which restores it, and waits for the answer.
func (s *stack) activate(shard string) time.Duration {
	s.t.Helper()
	t0 := time.Now()
	if st, body, err := doRequest(longClient, "GET", "http://"+s.pub+at(shard, "/count"), shard, ""); err != nil || st != 200 {
		s.t.Fatalf("activate %s: %d %q %v", shard, st, body, err)
	}
	return time.Since(t0)
}

// TestFaultMeasureMemory builds a shard of SHARDLITE_MEASURE_MB megabytes in
// blob storage (as many deltas), then measures the peak resident memory of a node
// that restores it, and of one that restores it and then replaces its files with
// one base.
func TestFaultMeasureMemory(t *testing.T) {
	mb := measureMB(t)
	const shard = "wallet/m1"
	s := newStack(t, "SHARDLITE_DELTA_SIZE=20000000", "SHARDLITE_FAULT_MAX_FILES=1000")
	v := newVerifier(t, s)
	s.create(t, shard)
	s.buildShard(shard, mb, 2500, 1000)

	// Restoring alone.
	s.startNode()
	took := s.activate(shard)
	t.Logf("restore: %v, node peak memory %.0f MB", took.Round(time.Millisecond), s.maxrssMB())
	apptest.Eventually(t, apptest.Timeout, "the app to close its databases", func() bool { return s.appDebug().OpenDBs == 0 })

	// Restoring, then replacing the files with one base.
	s.env = append(s.env, "SHARDLITE_FAULT_MAX_FILES=1")
	s.startNode()
	s.activate(shard)
	t0 := time.Now()
	apptest.Eventually(t, 10*time.Minute, "the files to become one base", func() bool { return v.manifestFiles(shard) == 1 })
	t.Logf("restore then rebase: %v, node peak memory %.0f MB", time.Since(t0).Round(time.Millisecond), s.maxrssMB())

	// What it left is the whole shard in one file.
	if _, ok, err := v.tokens(shard); err != nil || !ok {
		t.Fatalf("restore of the base: ok=%v %v", ok, err)
	}
}

// TestFaultMeasureLargeShard measures a large shard, on Linux:
//
//  1. a shard of SHARDLITE_MEASURE_MB megabytes restored and activated: the node's
//     peak resident memory, the bytes in the temporary directory and the time;
//  2. an import of that size into an empty shard in one transaction, then a
//     VACUUM, each through to its delta: the same figures.
func TestFaultMeasureLargeShard(t *testing.T) {
	mb := measureMB(t)
	const shard, shard2 = "wallet/m1", "wallet/m2"
	s := newStack(t, "SHARDLITE_DELTA_SIZE=20000000", "SHARDLITE_FAULT_MAX_FILES=1000")
	v := newVerifier(t, s)
	tmp := filepath.Join(s.dir, "data", ".tmp")
	s.create(t, shard)
	s.buildShard(shard, mb, 5000, 1000)

	// 1. Restore and activate.
	s.startNode()
	w := watch(tmp, s.node.cmd.Process.Pid)
	took := s.activate(shard)
	w.Stop()
	t.Logf("1. restore and activate a %d MB shard: %v; %s", mb, took.Round(time.Millisecond), w)
	apptest.Eventually(t, apptest.Timeout, "the app to close its databases", func() bool { return s.appDebug().OpenDBs == 0 })
	t.Logf("   node peak memory %.0f MB", s.maxrssMB())

	// 2. An import in one transaction into an empty shard, then a VACUUM.
	s.startNode()
	s.create(t, shard2)
	w = watch(tmp, s.node.cmd.Process.Pid)
	t0 := time.Now()
	st, body, err := doRequest(longClient, "POST", "http://"+s.pub+at(shard2, fmt.Sprintf("/bulk?rows=%d&bytes=1000000", mb)), shard2, "")
	if err != nil || st != 204 {
		t.Fatalf("import: %d %q %v", st, body, err)
	}
	t.Logf("2a. import of %d MB in one transaction, through its sync: %v", mb, time.Since(t0).Round(time.Millisecond))
	position := func() uint64 {
		m, _, err := v.files.GetManifest(context.Background(), shardName(shard2))
		if err != nil {
			return 0
		}
		return uint64(m.LastPosition())
	}
	apptest.Eventually(t, 10*time.Minute, "the import's delta", func() bool { return position() > 1 })
	t.Logf("2b. through its delta: %v; %s", time.Since(t0).Round(time.Millisecond), w.Stop())
	before := position()
	w = watch(tmp, s.node.cmd.Process.Pid)
	t0 = time.Now()
	if st, body, err := doRequest(longClient, "POST", "http://"+s.pub+at(shard2, "/vacuum"), shard2, ""); err != nil || st != 204 {
		t.Fatalf("vacuum: %d %q %v", st, body, err)
	}
	t.Logf("2c. VACUUM through the sync: %v", time.Since(t0).Round(time.Millisecond))
	apptest.Eventually(t, 10*time.Minute, "the VACUUM's delta", func() bool { return position() > before })
	t.Logf("2d. VACUUM through its delta: %v; %s", time.Since(t0).Round(time.Millisecond), w.Stop())
	t.Logf("    node peak memory %.0f MB", s.maxrssMB())

	if _, ok, err := v.tokens(shard2); err != nil || !ok {
		t.Fatalf("restore of the imported shard: ok=%v %v", ok, err)
	}
}

// openTemp is the size, and the number, of the files the process holds open
// under dir: the unnamed temporary files the node keeps tails, deltas and
// downloads in, which a listing of dir never shows. A file held through several
// descriptors (a log and its dups) is counted once. Linux only: it reads
// /proc/<pid>/fd, and is zero elsewhere.
func openTemp(pid int, dir string) (bytes int64, files int) {
	ents, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		return 0, 0
	}
	seen := map[uint64]bool{}
	for _, e := range ents {
		link := fmt.Sprintf("/proc/%d/fd/%s", pid, e.Name())
		target, err := os.Readlink(link)
		if err != nil || !strings.HasPrefix(target, dir+"/") {
			continue
		}
		fi, err := os.Stat(link) // follows to the file, named or not
		if err != nil {
			continue
		}
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !ok || seen[uint64(st.Ino)] {
			continue
		}
		seen[uint64(st.Ino)] = true
		bytes += fi.Size()
	}
	return bytes, len(seen)
}

// writtenBytes is the process's write_bytes from /proc/<pid>/io: what it has
// caused to be written to storage, or -1 off Linux.
func writtenBytes(pid int) int64 {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/io", pid))
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(b), "\n") {
		if f := strings.Fields(line); len(f) == 2 && f[0] == "write_bytes:" {
			n, _ := strconv.ParseInt(f[1], 10, 64)
			return n
		}
	}
	return -1
}

// shmem is the Shmem line of /proc/meminfo (tmpfs and shared memory), in bytes,
// or -1 off Linux.
func shmem() int64 {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(b), "\n") {
		if f := strings.Fields(line); len(f) >= 2 && f[0] == "Shmem:" {
			kb, _ := strconv.ParseInt(f[1], 10, 64)
			return kb << 10
		}
	}
	return -1
}

// watcher samples the node's open temporary files and Shmem while something runs,
// and reports the highest of each, with the bytes the node wrote meanwhile.
type watcher struct {
	stop      chan struct{}
	done      chan struct{}
	temp, shm int64
	files     int
	wrote0    int64
	wrote     int64
}

func watch(temp string, pid int) *watcher {
	w := &watcher{stop: make(chan struct{}), done: make(chan struct{}), wrote0: writtenBytes(pid)}
	base := shmem()
	sample := func() {
		bytes, files := openTemp(pid, temp)
		w.temp, w.files = max(w.temp, bytes), max(w.files, files)
		w.wrote = writtenBytes(pid)
		if base >= 0 {
			w.shm = max(w.shm, shmem()-base)
		}
	}
	go func() {
		defer close(w.done)
		for {
			sample()
			select {
			case <-w.stop:
				sample()
				return
			case <-time.After(25 * time.Millisecond):
			}
		}
	}()
	return w
}

func (w *watcher) Stop() *watcher { close(w.stop); <-w.done; return w }

func (w *watcher) String() string {
	s := fmt.Sprintf("open temporary files peak %.0f MB in %d files", float64(w.temp)/(1<<20), w.files)
	if w.wrote0 >= 0 {
		s += fmt.Sprintf(", node wrote %.0f MB", float64(w.wrote-w.wrote0)/(1<<20))
	}
	if w.shm >= 0 && w.shm != 0 {
		s += fmt.Sprintf(", Shmem peak +%.0f MB", float64(w.shm)/(1<<20))
	}
	return s
}
