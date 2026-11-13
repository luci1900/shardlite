package blobtest

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/luci1900/shardlite/internal/storage/blob"
)

// The hook maps failures onto outcomes: before the operation is "not applied",
// after it is "unknown" (the write happened, the response was lost).
func TestHookOutcomes(t *testing.T) {
	t.Parallel()
	h, ctx := WithHook(blob.NewMemory()), context.Background()
	boom := errors.New("boom")
	h.SetHook(func(ctx context.Context, op Op, key string, after bool) error {
		if op == OpCreate && key == "before" && !after || op == OpCreate && key == "after" && after {
			return boom
		}
		return nil
	})
	if _, err := h.Create(ctx, "before", strings.NewReader("v"), blob.WriteID{}); !errors.Is(err, blob.ErrNotApplied) {
		t.Fatalf("got %v, want blob.ErrNotApplied", err)
	}
	if _, err := h.Head(ctx, "before"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("a not-applied create wrote the object: %v", err)
	}
	if _, err := h.Create(ctx, "after", strings.NewReader("v"), blob.WriteID{}); !errors.Is(err, blob.ErrUnknown) {
		t.Fatalf("got %v, want blob.ErrUnknown", err)
	}
	if _, err := h.Head(ctx, "after"); err != nil {
		t.Fatalf("an unknown-outcome create should have happened: %v", err)
	}
	// Every operation goes through the hook, a listing with its prefix as the key.
	// The map is keyed by name because it records what the hook saw, rather than
	// dispatching on it: an enum-keyed map literal must hold every member.
	seen := map[string]string{}
	h.SetHook(func(ctx context.Context, op Op, key string, after bool) error {
		if !after {
			seen[op.String()] = key
		}
		return nil
	})
	rc, _, _ := h.Get(ctx, "after")
	rc.Close()
	rc, _ = h.GetRange(ctx, "after", 0, 1)
	rc.Close()
	h.Head(ctx, "after")
	for range h.List(ctx, "af", "") {
	}
	h.Delete(ctx, "after", "")
	if len(seen) != 5 || seen[OpList.String()] != "af" || seen[OpDelete.String()] != "after" {
		t.Fatalf("hook saw %v", seen)
	}
	// A lost listing is an error, not a partial listing.
	h.SetHook(func(ctx context.Context, op Op, key string, after bool) error {
		if op == OpList && after {
			return boom
		}
		return nil
	})
	for _, err := range h.List(ctx, "", "") {
		if !errors.Is(err, blob.ErrUnknown) {
			t.Fatalf("list: %v, want blob.ErrUnknown", err)
		}
	}
}
