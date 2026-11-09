package shardlite

// The headers of the contract that an app handles itself. The SDK sets and
// reads the rest: Shardlite-Opened, Shardlite-File and Shardlite-Caller on the
// requests the node sends (Expect, ExpectCall and Caller read them), and
// Shardlite-If-Active, Shardlite-If-Exists and Shardlite-Sync on the calls an
// app makes (IfActive, IfExists and WithSync set them).
const (
	// HeaderContext is what the node knows about the request (the shard it
	// opened and the hops so far). The app passes it back unchanged on the
	// calls it makes while serving the request, as Opened.Call and Request.Call
	// do. A gRPC call carries it as metadata under the key shardlite-context.
	HeaderContext = "Shardlite-Context"
	// HeaderTarget is the shard a call is for. The calls the SDK makes set it,
	// and a gRPC call carries it as metadata under the key shardlite-target.
	HeaderTarget = "Shardlite-Target"
)

// The paths of the contract an app serves.
const (
	// ReadyPath is where the app serves its declaration (see Node.Ready).
	ReadyPath = "/_shardlite/ready"
	// RemindersPath is where the node wakes the app for a shard's due
	// reminders.
	RemindersPath = "/_shardlite/reminders"
)

// The names the SDK reads and sets itself.
const (
	headerOpened   = "Shardlite-Opened"
	headerFile     = "Shardlite-File"
	headerCaller   = "Shardlite-Caller"
	headerIfActive = "Shardlite-If-Active"
	headerIfExists = "Shardlite-If-Exists"
	headerSync     = "Shardlite-Sync"
	headerError    = "Shardlite-Error"

	// syncPath is the control API's sync endpoint (see Node.Sync).
	syncPath = "/_shardlite/sync"
)
