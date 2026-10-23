package node

import (
	"context"
	"database/sql"
	"testing"
)

// A release that did not go through (the app still has the shard open) leaves
// the shard usable: the node can still query it through capture's connection.
func TestShardIsUsableAfterAReleaseThatDidNotGoThrough(t *testing.T) {
	t.Parallel()
	r := newMRig(t)
	ref := r.acquire("test.slow/g1")
	r.write(ref, "a") // the app keeps its connection open
	ref.Release()
	a := r.shard("test.slow/g1")
	for i := range 3 {
		if ok, err := a.deactivateNow(context.Background()); ok || err != nil {
			t.Fatalf("deactivate %d: %v %v", i, ok, err)
		}
		if err := a.useDB(context.Background(), func(db *sql.DB) error { return db.PingContext(context.Background()) }); err != nil {
			t.Fatalf("after %d held releases: %v", i, err)
		}
	}
}
