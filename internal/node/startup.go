package node

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// What one active shard costs the node process, rounded up from measurements
// (TestMeasurePerShardCost, run with -v on Linux): the descriptors and memory
// mappings the node holds for a shard (capture's connection, its read
// transaction, its -shm mapping, our read-only mapping of it and SQLite's
// per-connection memory, and the log of its tail), and what the node holds before it has any (mostly OS
// thread stacks, two mappings each, a few hundred threads under load). The
// app's own descriptors are in the app's process.
//
// Measured on Linux with 300 shards: 8.00 descriptors and 6.8 mappings each. A
// shard holds one more while a delta reads its tail (a dup of its log), and the
// segment writer holds a dup of a log for each segment queued but not yet
// written, which a slow store or peer lets pile up, so the constant has more
// than the margin the mappings have.
const (
	descriptorsPerShard = 10
	mappingsPerShard    = 8
	baseDescriptors     = 64
	baseMappings        = 1000
)

// minActiveShards is the number of active shards under which the node warns
// about its limits.
const minActiveShards = 10_000

// limits are the system limits that bound the number of active shards. A zero
// value is unknown (or, for the descriptor limit, unlimited).
type limits struct {
	maxMapCount int64  // vm.max_map_count, Linux only
	fdHard      uint64 // the hard RLIMIT_NOFILE (Go raises the soft limit to it at startup)
}

func readLimits() limits {
	var l limits
	if b, err := os.ReadFile("/proc/sys/vm/max_map_count"); err == nil {
		l.maxMapCount, _ = strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	}
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err == nil && rl.Max != ^uint64(0)>>1 && rl.Max != ^uint64(0) {
		l.fdHard = uint64(rl.Max)
	}
	return l
}

// capacity is how many active shards each limit allows (-1 for a limit that is
// unknown or unlimited).
func (l limits) capacity() (byDescriptors, byMappings int64) {
	byDescriptors, byMappings = -1, -1
	if l.fdHard > 0 {
		byDescriptors = max(0, (int64(l.fdHard)-baseDescriptors)/descriptorsPerShard)
	}
	if l.maxMapCount > 0 {
		byMappings = max(0, (l.maxMapCount-baseMappings)/mappingsPerShard)
	}
	return byDescriptors, byMappings
}

// startupChecks are the checks on the node's surroundings that run before it
// takes its lease. Its dependencies are fields so tests can inject them.
type startupChecks struct {
	fsType fsTypeFunc
	limits func() limits
	log    *slog.Logger
}

func defaultStartupChecks(log *slog.Logger) startupChecks {
	return startupChecks{fsType: filesystemOf, limits: readLimits, log: log}
}

// refuse returns an error if the data directory is on a network filesystem.
func (c startupChecks) refuse(dataDir string) error {
	return refuseNetworkFS(c.fsType, dataDir, "SHARDLITE_DATA")
}

// warn logs what is likely to go wrong without refusing to start.
func (c startupChecks) warn() {
	l := c.limits()
	byFD, byMaps := l.capacity()
	c.log.Info("how many active shards this node's limits allow",
		"by_open_files", capacityString(byFD), "open_file_hard_limit", l.fdHard,
		"by_memory_mappings", capacityString(byMaps), "vm_max_map_count", l.maxMapCount)
	if byFD >= 0 && byFD < minActiveShards {
		c.log.Warn(fmt.Sprintf("the hard open file limit allows only about %d active shards (about %d descriptors each): raise it (ulimit -n or LimitNOFILE)", byFD, descriptorsPerShard), "limit", l.fdHard, "wanted_shards", minActiveShards)
	}
	if byMaps >= 0 && byMaps < minActiveShards {
		c.log.Warn(fmt.Sprintf("vm.max_map_count allows only about %d active shards (about %d memory mappings each): raise it (sysctl vm.max_map_count)", byMaps, mappingsPerShard), "limit", l.maxMapCount, "wanted_shards", minActiveShards)
	}
}

func capacityString(n int64) string {
	if n < 0 {
		return "unknown"
	}
	return strconv.FormatInt(n, 10)
}
