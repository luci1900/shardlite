package shardfile

import "context"

// Touch rewrites the manifest unchanged, to check the handle's ETag is current.
func (h *Handle) Touch(ctx context.Context) error { return h.update(ctx, h.Manifest.clone()) }
