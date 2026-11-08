package node

import (
	"bytes"
	"context"
	"github.com/luci1900/shardlite/internal/apptest"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// A shard whose file is not the one it holds is not served: the request that
// activates it answers 503, and the file is left in blob storage.
func TestShardWithABadFileIsNotServed(t *testing.T) {
	t.Parallel()
	r := newSRig(t)
	ctx := context.Background()
	ref := r.acquire("users/8")
	r.write(ref, "a")
	if err := r.m.Gate(ctx, ref, kind.SyncStorage); err != nil {
		t.Fatal(err)
	}
	r.app.Must(t, "close", "a")
	ref.Release()
	apptest.Eventually(t, apptest.Timeout, "deactivation", func() bool { return r.released("users/8") })

	// The file in blob storage is damaged: a byte in the middle of it is
	// flipped, so its checksums do not match.
	damageFile(t, r.mrig, "users/8")

	resp, body := r.req("GET", r.pub, "/x", map[string]string{"Shardlite-Target": "users/8"}, "")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("the shard with a damaged file: %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, "not the one the shard holds") {
		t.Fatalf("the shard with a damaged file: %s", body)
	}
}

// damageFile flips a byte in the middle of a shard's last file.
func damageFile(t *testing.T, r *mrig, target string) {
	t.Helper()
	ctx := context.Background()
	h, err := r.n.Files.Attach(ctx, shardName(target))
	if err != nil {
		t.Fatal(err)
	}
	files := h.Manifest.Files
	f := files[len(files)-1]
	key := shardfile.FileKey(shardName(target), h.Manifest.Generation, f)
	rc, _, err := r.n.Files.Blob.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 0xff
	if err := r.n.Files.Blob.Delete(ctx, key, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := r.n.Files.Blob.Create(ctx, key, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
}
