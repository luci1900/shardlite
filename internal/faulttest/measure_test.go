//go:build fault

package faulttest

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/s3test"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// TestMeasureMemory builds a shard of SHARDLITE_MEASURE_MB megabytes of random
// data in blob storage (as many deltas), then measures the peak resident memory
// of a node process that restores it, and of one that restores it and then
// replaces its files with one base (SHARDLITE_MEASURE_STEP: "rebase", or
// "merge" in trees from before stage 1y). It is skipped without the variable.
func TestMeasureMemory(t *testing.T) {
	mb, _ := strconv.Atoi(os.Getenv("SHARDLITE_MEASURE_MB"))
	if mb == 0 {
		t.Skip("SHARDLITE_MEASURE_MB is not set")
	}
	step := os.Getenv("SHARDLITE_MEASURE_STEP")
	if step == "" {
		step = "rebase"
	}
	st := s3test.Setup(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "fault-users-1.db")
	start := func() *apptest.App {
		n, err := apptest.StartAt(apptest.Build(t, "./internal/testnode"), st.Args()...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(n.Kill)
		return n
	}
	rss := func(n *apptest.App) float64 {
		v, err := strconv.ParseInt(n.Must(t, "maxrss", "-"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return float64(v) / (1 << 20)
	}

	// Build the shard: 2.5 MB per commit, a delta every 8.
	const rowsPerCommit, rowBytes = 2500, 1000
	commits := mb * (1 << 20) / (rowsPerCommit * rowBytes)
	node, app := start(), apptest.Start(t)
	app.Must(t, "open", "a", path)
	node.Must(t, "open", shardName, path)
	t0 := time.Now()
	for i := 1; i <= commits; i++ {
		app.Must(t, "insert", "a", strconv.Itoa(rowsPerCommit), strconv.Itoa(rowBytes))
		node.Must(t, "sync", shardName)
		if i%8 == 0 {
			node.Must(t, "delta", shardName)
		}
	}
	node.Must(t, "delta", shardName)
	app.Must(t, "close", "a")
	app.Exit()
	node.Must(t, "close", shardName)
	node.Kill()
	fi, _ := os.Stat(path)
	t.Logf("built a shard of %d commits, database file %.0f MB, in %v", commits, float64(fi.Size())/(1<<20), time.Since(t0).Round(time.Second))

	// Restoring alone.
	os.Remove(path)
	n := start()
	t0 = time.Now()
	n.Must(t, "open", shardName, path)
	t.Logf("restore: %v, node peak memory %.0f MB", time.Since(t0).Round(time.Millisecond), rss(n))
	n.Kill()

	// Restoring, then replacing the files with one base.
	os.Remove(path)
	n = start()
	n.Must(t, "open", shardName, path)
	afterRestore := rss(n)
	t0 = time.Now()
	n.Must(t, step, shardName)
	t.Logf("restore then %s: %v, node peak memory %.0f MB (%.0f MB after the restore)", step, time.Since(t0).Round(time.Millisecond), rss(n), afterRestore)
	n.Must(t, "close", shardName)
	n.Kill()

	// What it left is the whole shard in one file.
	reader := (&rig{t: t, store: st}).readFiles()
	m, _, err := reader.GetManifest(context.Background(), shard.MustParse(shardName))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) != 1 {
		t.Fatalf("manifest files %v", m.Files)
	}
	dst := filepath.Join(t.TempDir(), "check.db")
	if _, err := reader.Restore(context.Background(), shard.MustParse(shardName), dst, shardfile.RestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+dst+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM kv`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if want := commits * rowsPerCommit; count != want {
		t.Fatalf("%d rows, want %s", count, fmt.Sprint(want))
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

func tempBytes(n *apptest.App, dir string) int64 {
	b, _ := openTemp(n.Pid(), dir)
	return b
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

// watch samples the node's open temporary files and Shmem while something runs,
// and reports the highest of each, with the bytes the node wrote meanwhile.
type watcher struct {
	stop      chan struct{}
	done      chan struct{}
	temp, shm int64
	files     int
	pid       int
	wrote0    int64
	wrote     int64
}

func watch(temp string, pid int) *watcher {
	w := &watcher{stop: make(chan struct{}), done: make(chan struct{}), pid: pid, wrote0: writtenBytes(pid)}
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

// TestMeasureStage1z measures what stage 1z is about, on Linux:
//
//  1. a shard of SHARDLITE_MEASURE_MB megabytes restored and activated: the node's
//     peak resident memory, the bytes in the temporary directory and the time;
//  2. an import of that size into an empty shard in one transaction, then a
//     VACUUM, each through to its delta: the same figures.
//
// The node is a process of its own for each, so its peak is its own. It is
// skipped without the variable.
func TestMeasureStage1z(t *testing.T) {
	mb, _ := strconv.Atoi(os.Getenv("SHARDLITE_MEASURE_MB"))
	if mb == 0 {
		t.Skip("SHARDLITE_MEASURE_MB is not set")
	}
	st := s3test.Setup(t)
	dir := t.TempDir()
	tmp := filepath.Join(dir, "tmp")
	os.MkdirAll(tmp, 0o755)
	start := func() *apptest.App {
		n, err := apptest.StartAt(apptest.Build(t, "./internal/testnode"), append(st.Args(), "-tmp", tmp)...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(n.Kill)
		return n
	}
	rss := func(n *apptest.App) float64 {
		v, err := strconv.ParseInt(n.Must(t, "maxrss", "-"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return float64(v) / (1 << 20)
	}

	// 1. Build a shard of about mb megabytes in blob storage, then restore it.
	path := filepath.Join(dir, "fault-users-1.db")
	const rowsPerCommit, rowBytes = 5000, 1000
	commits := mb * (1 << 20) / (rowsPerCommit * rowBytes)
	node, app := start(), apptest.Start(t)
	app.Must(t, "open", "a", path)
	node.Must(t, "open", shardName, path)
	t0 := time.Now()
	for i := 1; i <= commits; i++ {
		app.Must(t, "insert", "a", strconv.Itoa(rowsPerCommit), strconv.Itoa(rowBytes))
		node.Must(t, "sync", shardName)
		if i%8 == 0 {
			node.Must(t, "delta", shardName)
		}
	}
	node.Must(t, "delta", shardName)
	app.Must(t, "close", "a")
	app.Exit()
	node.Must(t, "close", shardName)
	node.Kill()
	fi, _ := os.Stat(path)
	t.Logf("built a shard of %d commits, database file %.0f MB, in %v", commits, float64(fi.Size())/(1<<20), time.Since(t0).Round(time.Second))
	for _, p := range []string{"", "-wal", "-shm"} {
		os.Remove(path + p)
	}
	os.RemoveAll(tmp)

	n := start()
	w := watch(tmp, n.Pid())
	how := n.Must(t, "open", shardName, path)
	w.Stop()
	t.Logf("1. restore and activate a %.0f MB shard: %s", float64(fi.Size())/(1<<20), how)
	t.Logf("   node peak memory %.0f MB; %s", rss(n), w)
	// A few commits, as an active shard has, and the temporary directory after each.
	app = apptest.Start(t)
	app.Must(t, "open", "a", path)
	for range 3 {
		app.Must(t, "insert", "a", "10", "1000")
		n.Must(t, "sync", shardName)
	}
	t.Logf("   after three commits: open temporary files %.1f MB, node peak memory %.0f MB", float64(tempBytes(n, tmp))/(1<<20), rss(n))
	app.Must(t, "close", "a")
	app.Exit()
	n.Must(t, "close", shardName)
	n.Kill()
	os.RemoveAll(tmp)

	// 2. An import in one transaction into an empty shard, then a VACUUM.
	path2, name2 := filepath.Join(dir, "fault-users-2.db"), "fault.users/2"
	n = start()
	app = apptest.Start(t)
	app.Must(t, "open", "a", path2)
	n.Must(t, "open", name2, path2)
	n.Must(t, "sync", name2) // the first sync of a new shard is its (empty) first position, as at activation
	rows := mb               // rows of 1 MB
	w = watch(tmp, n.Pid())
	t0 = time.Now()
	app.Must(t, "insert", "a", strconv.Itoa(rows), "1000000")
	imported := time.Since(t0)
	n.Must(t, "sync", name2)
	synced := time.Since(t0)
	w.Stop()
	t.Logf("2a. import of %d MB in one transaction: app %v, through the sync %v", rows, imported.Round(time.Millisecond), synced.Round(time.Millisecond))
	t.Logf("    node peak memory %.0f MB after the sync; %s", rss(n), w)
	w = watch(tmp, n.Pid())
	t0 = time.Now()
	n.Must(t, "delta", name2)
	w.Stop()
	t.Logf("2b. its delta: %v, node peak memory %.0f MB; %s", time.Since(t0).Round(time.Millisecond), rss(n), w)
	t.Logf("    open temporary files now: %.1f MB", float64(tempBytes(n, tmp))/(1<<20))

	w = watch(tmp, n.Pid())
	t0 = time.Now()
	app.Must(t, "exec", "a", "VACUUM")
	n.Must(t, "sync", name2)
	t.Logf("2c. VACUUM through the sync: %v", time.Since(t0).Round(time.Millisecond))
	n.Must(t, "delta", name2)
	w.Stop()
	t.Logf("2d. VACUUM through its delta: %v, node peak memory %.0f MB; %s", time.Since(t0).Round(time.Millisecond), rss(n), w)
	t.Logf("    open temporary files now: %.1f MB", float64(tempBytes(n, tmp))/(1<<20))
	app.Must(t, "close", "a")
	app.Exit()
	n.Must(t, "close", name2)
	n.Kill()

	// Both restore to what the app had.
	reader := (&rig{t: t, store: st}).readFiles()
	dst := filepath.Join(t.TempDir(), "check.db")
	if _, err := reader.Restore(context.Background(), shard.MustParse(name2), dst, shardfile.RestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+dst+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM kv`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != rows {
		t.Fatalf("%d rows, want %d", count, rows)
	}
}
