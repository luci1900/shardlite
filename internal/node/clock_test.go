package node

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/kind"
)

// A sync that names no tier is refused, with and without commits waiting on
// it: the zero of a closed set never acknowledges by accident.
func TestGateRefusesASyncThatIsNoTier(t *testing.T) {
	t.Parallel()
	r := newUnstartedMRig(t, nil)
	r.startNode()
	r.startShardset(nil)
	ref := r.acquire("wallet/1")
	defer ref.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, written := range []bool{false, true} {
		if written {
			r.write(ref, "a")
		}
		var un *unavailable
		err := r.m.Gate(ctx, ref, kind.Sync(99))
		if !errors.As(err, &un) || !strings.Contains(err.Error(), "not a durability tier") {
			t.Fatalf("a sync outside the set (commits waiting: %v) answered %v, want the tier unmet", written, err)
		}
	}
}
