package node

import (
	"errors"
	"testing"

	"github.com/luci1900/shardlite/internal/node/dispatch"
	"github.com/luci1900/shardlite/internal/storage/blob"
)

// A node's request for the activate path makes an existing shard active where it
// is served and says whether it was active already, and with Shardlite-If-Exists
// a shard with no manifest is not created.
func TestActivateMakesAnExistingShardActive(t *testing.T) {
	c := newCluster(t, 2)
	n0 := c.nodes[0]
	const target = "users/a"
	n0.unowned(target)
	hdr := map[string]string{"Shardlite-Origin": originNode, "Shardlite-Target": target, hdrIfExists: "true"}

	if got := n0.peerDo("POST", dispatch.Activate, hdr, ""); got.status != 200 {
		t.Fatalf("activating got %d, want 200", got.status)
	}
	if !n0.m.Load().IsActive(shardName(target)) {
		t.Fatal("the shard is not active after its activation")
	}
	if got := n0.peerDo("POST", dispatch.Activate, hdr, ""); got.status != 204 {
		t.Fatalf("activating an active shard got %d, want 204", got.status)
	}

	const missing = "users/nope"
	hdr["Shardlite-Target"] = missing
	if got := n0.peerDo("POST", dispatch.Activate, hdr, ""); got.status != 404 {
		t.Fatalf("activating a shard with no manifest got %d, want 404", got.status)
	}
	if n0.m.Load().IsActive(shardName(missing)) {
		t.Fatal("a shard with no manifest was activated")
	}
	if _, _, err := n0.m.Load().n.Files.GetManifest(t.Context(), shardName(missing)); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("the manifest of a shard activated with Shardlite-If-Exists: %v, want none", err)
	}
}

// The app can't send the activate path: the control API refuses every path under
// /_shardlite/ it doesn't serve, and the public one does too.
func TestActivateIsNotTheApps(t *testing.T) {
	c := newCluster(t, 1)
	n0 := c.nodes[0]
	n0.create("users/a")
	hdr := map[string]string{"Shardlite-Target": "users/a", hdrIfExists: "true"}
	for name, addr := range map[string]string{"the control API": n0.Control, "the public listener": n0.Public} {
		resp, body := send(t, "POST", "http://"+addr+dispatch.Activate, hdr, "")
		if resp.StatusCode != 404 {
			t.Errorf("%s answered %d %q, want 404", name, resp.StatusCode, body)
		}
	}
}
