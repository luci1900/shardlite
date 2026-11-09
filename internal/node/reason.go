package node

// reason says why the node answered with an error of its own. It goes out as the
// Shardlite-Error header (and as shardlite-error metadata on a gRPC answer), so a
// client can tell the node's errors from the app's (the node strips every
// Shardlite-* header from an app's response, so an error without it is the
// app's) and tell one cause from another. The set is closed: every error the node
// writes names one of these, and docs/guide.md (Status codes) lists them.
type reason string

const (
	reasonBadRequest       reason = "bad-request"       // a malformed shard name or Shardlite-* header, or a call the node refuses as made
	reasonUnknownShard     reason = "unknown-shard"     // an undeclared kind, a kind a client may not name, or a shard that doesn't exist and may not be created
	reasonNotActive        reason = "not-active"        // Shardlite-If-Active: true on a shard that isn't active
	reasonNoEndpoint       reason = "no-endpoint"       // a path the listener doesn't serve, or a peer request that isn't a forward
	reasonNotOwner         reason = "not-owner"         // a forward for a shard the node found isn't its own (421, between nodes)
	reasonHops             reason = "hops"              // calls chained past SHARDLITE_MAX_HOPS
	reasonAppUnreachable   reason = "app-unreachable"   // the dial to the app failed: the app never had the request
	reasonAppFailed        reason = "app-failed"        // the app had the request and the connection failed
	reasonOwnerUnreachable reason = "owner-unreachable" // the owner's node didn't answer, or is dead and not adopted yet
	reasonOwnerFailed      reason = "owner-failed"      // the owner's node had the forward and the connection failed
	reasonMoved            reason = "moved"             // the shard moved or was claimed elsewhere meanwhile: try again at once
	reasonDraining         reason = "draining"          // the node is stopping and can't serve or place the request
	reasonLeaseLost        reason = "lease-lost"        // the node's lease is lost or expired
	reasonBadFile          reason = "bad-file"          // the shard's files failed their checksums
	reasonSyncUnmet        reason = "sync-unmet"        // the node can't meet the durability tier asked for
	reasonNotReady         reason = "not-ready"         // the readiness check: the app's declaration is missing, the lease isn't good or the node drains
	reasonTimeout          reason = "timeout"           // a timeout inside the node ended the request
	reasonInternal         reason = "internal"          // any other failure of the node itself (storage, say)
)

// reasonError gives the 502 an upstream failure ends in its reason: a failure to
// reach or hear from the app or the owner's node (upstreamError). The status and the retry hint
// still come from the error underneath.
type reasonError struct {
	reason reason
	err    error
}

func (e *reasonError) Error() string { return e.err.Error() }
func (e *reasonError) Unwrap() error { return e.err }
