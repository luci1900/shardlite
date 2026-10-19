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
// mappings the node holds for a shard (Litestream's connection, its -shm
// mapping, our read-only mapping of it and SQLite's per-connection memory), and
// what the node holds before it has any (mostly OS thread stacks, two mappings
// each, a few hundred threads under load). The app's own descriptors are in the
// app's process.
//
// Measured on Linux with 300 shards: 6.0 descriptors and 7.05 mappings each
// (5.0 and 6.4 before stage 1o: the reserved reminders table is created, read
// and written through a second connection in Litestream's pool, which stays
// open). Earlier benchmarks (stages 0b and 1f) saw 6 to 7 descriptors and 6.3
// to 6.8 mappings per shard at up to 10,000 shards.
const (
	descriptorsPerShard = 7
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
func (c startupChecks) warn(scratchDir string) {
	if hasTmpfs {
		if name, err := c.fsType(scratchDir); err == nil && name != "tmpfs" && name != "ramfs" {
			c.log.Warn("the scratch directory is not on tmpfs: every sync of every shard does an fsync there, which costs real disk writes (put SHARDLITE_SCRATCH_DIR on tmpfs, such as under /dev/shm)", "dir", scratchDir, "filesystem", orOther(name))
		}
	}
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

func orOther(s string) string {
	if s == "" {
		return "other (not tmpfs)"
	}
	return s
}
