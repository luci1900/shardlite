package kill_test

import (
	"testing"

	"github.com/luci1900/shardlite/internal/kill"
)

// A rig arms a point by name, so every point must be one it can name.
func TestOrphanEntryPointsAreNamed(t *testing.T) {
	t.Parallel()
	for _, p := range []kill.Point{
		kill.OrphanEntryBeforeFile, kill.OrphanEntryBeforeDeleteShard,
	} {
		if got, ok := kill.ByName(string(p)); !ok || got != p {
			t.Errorf("ByName(%q) = %q, %v", p, got, ok)
		}
	}
}
