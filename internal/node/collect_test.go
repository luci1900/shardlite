package node

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/luci1900/shardlite/internal/dueindex"
	"github.com/luci1900/shardlite/internal/harness"
	"github.com/luci1900/shardlite/internal/reconcile"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// Every node of a cluster runs the collector on the lanes it ranks first for: the
// orphan index's entries of the 16 lanes are all handled, the LTX files they name
// that no manifest lists are deleted, and each entry is deleted with them. Passes
// repeat every second here, so the entries made after the nodes started are found.
func TestEachNodeCollectsItsOwnLanes(t *testing.T) {
	clk := harness.NewFakeClock()
	c := newClusterWith(t, 2, func(o *ShardsetOptions) {
		o.Clock = clk
		o.Leases.TTL = 1000 * time.Hour
		o.Collect = &reconcile.Options{Pace: time.Millisecond, Every: time.Second}
	})
	store := c.nodes[0].m.Load().n.Store
	index := shardfile.OrphanIndex(store)
	ctx := context.Background()
	var keys []string
	for i := 0; len(keys) < shard.Lanes; i++ {
		name := shardName(fmt.Sprintf("users/%d", i))
		if name.Lane() == shard.Lane(len(keys)) {
			f, err := shardfile.ParseFile("1-1.1")
			if err != nil {
				t.Fatal(err)
			}
			key := shardfile.FileKey(name, "g", f)
			if _, err := store.Create(ctx, key, strings.NewReader("x")); err != nil {
				t.Fatal(err)
			}
			keys = append(keys, key)
			e := shardfile.Orphan{Minute: dueindex.MinuteAt(clk.Now().Add(shardfile.DefaultAgeRule)), Shard: name, Generation: "g"}.Entry()
			if err := index.Create(ctx, e); err != nil {
				t.Fatal(err)
			}
		}
	}
	clk.Advance(shardfile.DefaultAgeRule + time.Hour) // the files are old now and the entries due
	advanceUntil(t, clk, 100*time.Millisecond, "every lane to be collected", func() bool {
		for _, key := range keys {
			if _, err := store.Head(ctx, key); err == nil {
				return false
			}
		}
		for i := range shard.Lanes {
			got, err := index.Due(ctx, shard.Lane(i), clk.Now().Add(1000*time.Hour), 10)
			if err != nil || len(got) > 0 {
				return false
			}
		}
		return true
	})
	for _, n := range c.nodes {
		if n.m.Load().collecting.Passes.Load() == 0 {
			t.Errorf("%s's collector never ran", n.env.Name)
		}
	}
}
