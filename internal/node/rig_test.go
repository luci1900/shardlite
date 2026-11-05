package node

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/luci1900/shardlite/internal/apptest"
	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/capture"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/harness"
)

const testShard = "test.users/42"

// writeShard runs a few app commits and syncs them, as the shardfile tests'
// write does for its harness.
func writeShard(t *testing.T, e *harness.Env, app *apptest.App, sh *capture.Shard, i int) {
	t.Helper()
	app.Must(t, "insert", "a", "2", "300")
	app.Must(t, "exec", "a", fmt.Sprintf("UPDATE kv SET payload = zeroblob(%d) WHERE id = 1", 50+i))
	if err := sh.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// leaseOf is a node's lease as it is in blob storage, read through a registry
// of the test's own.
func nodeLease(t *testing.T, c *testCluster, name cluster.Node) *coord.NodeLease {
	t.Helper()
	l, err := coord.NewRegistry(blob.WithPrefix(c.mem, ""), name, coord.RegistryOptions{}).Get(context.Background(), name)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// logLines is a logger whose lines a test counts by message.
type logLines struct {
	mu    sync.Mutex
	lines []string
}

func (l *logLines) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, string(p))
	return len(p), nil
}

// count is how many lines so far contain s.
func (l *logLines) count(s string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range l.lines {
		if strings.Contains(line, s) {
			n++
		}
	}
	return n
}

// leaseInStorage is a node's lease as it is in blob storage now.
func (r *mrig) leaseInStorage(name cluster.Node) *coord.NodeLease {
	r.t.Helper()
	l, err := coord.NewRegistry(r.local, name, coord.RegistryOptions{}).Get(context.Background(), name)
	if err != nil {
		r.t.Fatal(err)
	}
	return l
}
