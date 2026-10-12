package node

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

func countEntries(dir string) int {
	es, err := os.ReadDir(dir)
	if err != nil {
		return -1
	}
	return len(es)
}

func countMaps() int {
	b, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		return -1
	}
	return strings.Count(string(b), "\n")
}

func processCosts() (fds, maps int) {
	dir := "/proc/self/fd"
	if runtime.GOOS == "darwin" {
		dir = "/dev/fd"
	}
	return countEntries(dir), countMaps()
}

// TestMeasurePerShardCost measures what an active shard costs the node process
// in descriptors and memory mappings, and checks that the constants the startup
// warning uses (descriptorsPerShard and mappingsPerShard) are not lower. Run it
// with -v on Linux to see the figures. Mappings are only counted on Linux. The
// baselines (what the process holds before any shard) are only logged: in a
// test binary they include whatever earlier tests left open.
func TestMeasurePerShardCost(t *testing.T) {
	const n = 300
	r := newMRig(t)
	fds0, maps0 := processCosts()
	var refs []*Ref
	for i := range n {
		refs = append(refs, r.acquire(fmt.Sprintf("slow/m%d", i)))
	}
	defer func() {
		for _, ref := range refs {
			ref.Release()
		}
	}()
	time.Sleep(300 * time.Millisecond) // a few flush cycles: the shards are all synced and settled
	fds1, maps1 := processCosts()
	perFD := float64(fds1-fds0) / n
	perMap := float64(maps1-maps0) / n
	t.Logf("%d active shards: descriptors %d -> %d (%.2f per shard), mappings %d -> %d (%.2f per shard); baseline before any shard: %d descriptors, %d mappings",
		n, fds0, fds1, perFD, maps0, maps1, perMap, fds0, maps0)
	if perFD > descriptorsPerShard {
		t.Errorf("a shard costs %.2f descriptors, more than descriptorsPerShard (%d)", perFD, descriptorsPerShard)
	}
	if maps0 >= 0 {
		if perMap > mappingsPerShard {
			t.Errorf("a shard costs %.2f mappings, more than mappingsPerShard (%d)", perMap, mappingsPerShard)
		}
	}
}
