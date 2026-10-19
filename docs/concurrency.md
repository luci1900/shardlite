# shardlite concurrency

How the node's goroutines are owned, how they shut down and how they wait for each other. The rest of the node is in [internals](internals.md).

## Rules

- **Every goroutine has an owner that joins it,** up to the node's `Run`, which returns only after everything it started has ended. The parts that live as long as the node are services (a type with `Serve(ctx) error` and a name) run in groups (`internal/service`): whoever starts a group stops it and waits for it, and a service that returns before then has failed. Nothing restarts. Fork-join inside one call, and the goroutines a service owns, use `errgroup`. The node has no bare `go` statement.
- **A goroutine waits only for goroutines it started, for its own I/O and for node-wide loops that never wait for their callers** (the flush loop, which group commit relies on, is one). Children never wait for their parent or for each other, so waits can't form a loop and can't deadlock.
- **Callbacks from libraries never block on anything the node drives.** Litestream's storage client queues a file and returns, and whoever started the sync waits for durability afterwards.

## The tree

```
Run (selects on its context, a failed service, the writer being fenced and the lease being lost)
└── its stages
    ├── front
    │   ├── public server
    │   ├── health poll
    │   ├── journal expiry
    │   ├── kinds reload on SIGHUP
    │   └── pprof listener, if SHARDLITE_LISTEN_PPROF is set
    ├── callers
    │   ├── control server (the control listener and the Unix socket)
    │   └── peer server (requests from other nodes)
    ├── shards (Catalog)
    │   ├── lease scan, watchdog (the tail alert), cache loop and claimer (system shards' fixed ids)
    │   ├── table: name → shard
    │   │   └── shard: its lifecycle loop
    │   │       └── the shard's reminders loop, while the shard is captured
    │   ├── flusher
    │   │   └── per cycle: a sync for each changed shard, at most GOMAXPROCS at once
    │   ├── reminders (the node's limit on wakes in flight, no goroutines of its own)
    │   └── cache index (a resource, closed last)
    └── journal
        ├── writer (its uploader)
        └── keeper (lease renewal)
```

- **`Run`'s stages** are four groups, `front`, `callers`, `shards` and `journal`, stopped one at a time in that order (see [Shutdown](#shutdown)). `journal` starts as soon as the lease is taken, so the lease is renewed while the rest is built. Every listener is bound before the other stages start.
- **The order follows the dependencies.** `callers` outlives `front` because a control or peer request holds a reference to its shard, and every one must be joined before `Catalog` lets go of its shards. `journal` stops last because the shards write through the writer and need the lease the keeper renews.
- **`Catalog`** is the `shards` stage. It runs its parts in groups of its own, owns everything about the node's shards (active and cached) and knows nothing of the servers. Its parts get what they depend on passed in when the shards are built, so nothing is set afterwards.
- **`table`** runs the shards' lifecycle loops on a context of its own that its stop cancels, and starts none once it has stopped. It maps each name to an entry holding the shard's state, its references and its `shard`. The map is a `sync.Map` and nothing locks the whole table: an activation adds its entry with `LoadOrStore` (whoever finds one already there waits for it), and removal uses `CompareAndDelete`. Each entry's state and references are under the entry's own mutex, so requests for different shards never contend. Removal marks the entry gone first, and a request that found it just before looks the name up again. An entry whose loop ended without letting go of the shard is marked stopped: requests for it fail, and the lease still counts it.
- **A `shard`** is the actor, and its `Serve` is its lifecycle loop. The loop does one operation at a time: activation, a delta or rebase when its timer says so, deactivation when idle, and the deactivations, evictions and deletes others ask for. Other goroutines send it requests and wait for the reply, or give up once it has ended. It runs on a context derived from a base context `Catalog` owns, so stopping `Catalog` doesn't cut off a deactivation. Deactivations, deltas and rebases take a place under node-wide limits (256 deactivations, 64 snapshots of which 4 are rebases, each a channel used as a semaphore).
- **The lease scan** lists `state/leases/`, makes each watch's second look when due and adopts the nodes it finds dead, one after the other. An adoption is blob storage requests and waits for no other part. It stops with the other loops, and one in progress is cancelled (every step is safe to redo). Others read its last listing through an atomic pointer.
- **The capture lock** guards a captured shard's Litestream database, the wal-index mapping and the capture client's tail. It is a lock rather than a loop because capture has no life of its own, only state its callers take turns with. Everything that touches that state (a sync, a snapshot, a use of the connection, the tail, the close and the release) runs holding the lock, on the caller's goroutine. The lock is a channel with room for one, so a caller stops waiting when its context ends, but once it holds the lock its work runs to the end, so a sync is never cut off. Litestream's callbacks into the client run under the lock too, so the client has no lock of its own. A panic under the lock ends the activation, since a caller may recover it (`net/http` does) and Litestream's state can't be trusted after it. What others read without waiting (the tail's position and size) is an immutable summary published through an atomic pointer. The wal-index header is read without the lock, under a read lock that only an unmap takes for writing, so a flush cycle doesn't wait behind a sync.
- **The reminders loop** is the shard's second loop, which nobody sends requests to. It waits for a kick (the flush cycle captured new frames of the shard), its timer or its context, then reads the earliest due time through the connection. If the read isn't durable yet by the gate's rule, it waits for a flush cycle that began after the read, through a channel from the flusher's counter, so the flush cycle never waits for the loop. When the time has come it makes the wake itself, in the loop, after taking a place under the node's limit on wakes in flight. So a shard has one wake at a time, and stopping the loop cancels the wake. It then reads again to judge the wake and keeps the failure count and backoff (only the loop touches them, so no lock). The lifecycle loop starts the loop once the shard is captured and stops it and waits before a release, so nothing uses the capture during the final sync. If the release fails and capture resumes, the loop starts again with no backoff.
- **The segment writer's uploader** is one goroutine that writes the segments, one at a time and in order. A flush wakes it with a `Kick` and waits on a `Counter` of entries written, on the uploader ending or on its own context. A caller that gives up leaves nothing for the next flush to finish: the uploader keeps retrying a segment until its outcome is known, and a queued entry is never dropped. A fence or the end of the service ends the uploader, and a flush then returns the error. The writer knows nothing of leases: the node acknowledges only through `Flusher.Wait`, which checks the keeper, so a flush that completed after the lease ran out marks nothing durable.
- **The servers** give every request a context derived from a base context the server owns, and count their handlers in flight. Stopping a server waits for requests in flight (up to the grace period), then cancels the base context, which ends upgraded connections, and joins every handler. `httputil.ReverseProxy` returns as soon as either direction of an upgraded connection ends, so a copier can run on briefly after it. Each request therefore counts its gates in flight and every read and write on an upgraded connection, and `route` closes them (one started later fails without touching the shard) before it releases the request's references. A forwarded WebSocket is joined the same way on both nodes. The node's own requests (the wake for a shard's reminders) run through the same routing in process, with their origin passed explicitly.
- **Fork-join inside one call** is joined before the call returns: the flush cycle's syncs, a pass of `_dispatch` (up to 256 entries at once), a restore's downloads, letting go of the shards at shutdown (256 at once) and adoption (64 shards at once). Adoption runs at startup before the tree does, beside a keeper of its own, and the node's writer starts after the fence once it has ended.

## Shutdown

`Run` stops the stages one at a time, `front`, `callers`, `shards` and then `journal`. It stops each group and waits for it, so a stage has ended before the next is told to stop. Each stop waits at most 35 s: a service still running then is logged by name and left behind, so a stuck part never holds up the rest.

1. **`front`** stops when the context ends, a service fails, the writer is fenced or the lease is lost (see [Failures](#failures)). The public server stops accepting, waits up to 10 seconds for requests in flight, ends upgraded connections and joins every handler.
2. **`callers`** stops the same way, with its own grace period of up to 10 seconds, so no request holds a shard any more.
3. **`shards`** (`Catalog`, in its own order):
   1. the lease scan, the watchdog, the cache loop and the claimer stop, so nothing activates or adopts any more
   2. the 30-second deadline for letting go starts. Every shard's lifecycle loop stops its reminders loop first, cancelling and joining a wake in progress, so nothing uses a capture during its final sync. Each release's final delta records the shard's `next_reminder`, with its entry in the reminder index created first
   3. every shard is let go, at most 256 at once, retrying transient storage errors until the deadline, with the flusher still running
   4. the table stops, which ends the loops of shards that could not be let go
   5. the flusher stops
   6. the cache index closes.

   If `Node.Err` says the lease is gone or the writer is fenced, `Catalog` lets go of nothing and returns an error saying so.
4. **`journal`** stops once `shards` has ended: the writer (which fails whatever is still queued) and the keeper.
5. **`Run` releases the lease** if every shard was let go and `Node.Err` finds nothing wrong, with a 30-second deadline of its own. Otherwise the lease is left to expire.

The deadlines are separate, not one budget: the public server's grace and the grace of `callers` run one after another, then letting go of shards gets 30 s, then releasing the lease gets another 30 s. The 35 s limit on a group's stop is longer than those, so the deadline is what ends a release.

## Failures

The node asks one question about its journal: `Node.Err` is nil while the lease is good and the writer isn't fenced, and otherwise it is the reason. `Run`, `Catalog` and the lease decision at the end of `Run` all ask it, and no part exists only to watch the journal.

| On | The node |
|---|---|
| A panic anywhere | crashes (nothing recovers panics, except `net/http` in a handler), and adoption recovers its shards on the next start |
| The lease lost, or the writer fenced | stops at once, in the usual order. `Catalog` lets go of nothing and keeps the lease |
| Any other service returning | stops gracefully and exits with the service's error |
| A listener that can't be bound, or the shards failing to build | `Run` returns the error, closing the listeners and stopping the journal |

Nothing restarts. A crashed shard that costs only that shard is in the [roadmap](roadmap.md) (stage 4).

## Waits across the tree

These are the waits between goroutines that aren't parent and child. Each is allowed because the goroutine waited for never waits back.

| Who waits | For | Why it can't loop |
|---|---|---|
| A request (through a server) | `Catalog`: a reference, an activation, a gate | `Catalog` never waits for a request |
| `Catalog` | the writer, the keeper's lease check and blob storage | none of them waits for a shard |
| A flush or a sync | the writer's uploader, through a `Kick` and a `Counter` | the uploader waits only for blob storage |
| A flush cycle, a release, the reminders loop, the lifecycle loop | a capture's lock | the holder waits only for Litestream, SQLite's locks and the writer's mutex |
| A reminders loop | the flusher's counter, and the app's answer to a wake | the flush loop only kicks a reminders loop, and the wake ends with the loop's context or its 30 s deadline |
| A lifecycle loop | its reminders loop, to stop it | the reminders loop never waits for it |
| `Run` | each stage, to stop it | a stage never waits for `Run` |

## The locks that remain

No lock nests, is held across I/O or a wait on another goroutine, or stands in for ownership, with one exception. A lock that guards a few fields for a few lines (a leaf) stays where the code without it would not be simpler. A lock is never rebuilt from atomics (a reader count that something waits on, a state machine packed into a word).

| Lock | Guards | Why it is allowed |
|---|---|---|
| `Shard.mu` (capture) | Litestream's database, the wal-index mapping and the client's tail | the exception: it is held across Litestream's local work and a use of the connection, and its holder takes the writer's mutex to append a file. It never waits for blob storage or another goroutine of the node, so it can't be part of a cycle |
| `entry.mu` (table) | the entry's state, references, last use and change channel | a leaf, held for a few lines with nothing called |
| `Writer.mu` | the queue, the segment number and the stop state | a leaf, never held while a segment is built or sent |
| `inFlight.mu` (servers) | the closed flag | a leaf |
| `notify.Counter` | its value and its waiters | a leaf, never held while waiting |

- **Nesting** is one level: the capture lock's holder takes the writer's mutex. Nothing is taken under the writer's mutex.
- **Semaphores** are channels: the limits on deactivations, deltas and rebases, and on wakes in flight.
- **Atomics** carry values that are published once or written by one goroutine and read by many (the keeper's lease, the capture's tail view, the shard's capture pointers and last sync error, the flusher's counters, the kinds and the stats counters).
- **Once** closes a channel or runs a step one time.
- **Test helpers** keep their own mutexes (the in-memory blob store, `apptest`, `TailSet` and the test node). They are leaves too.
