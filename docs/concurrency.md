# shardlite concurrency

How the node's goroutines are owned, how they shut down and how they wait for each other. The rest of the node is in [internals](internals.md).

## Rules

- **Every goroutine has an owner that joins it,** up to the node's `Run`, which returns only after everything it started has ended. The parts that live as long as the node are services (a type with `Serve(ctx) error` and a name) run in groups (`internal/service`): whoever starts a group stops it and waits for it, and a service that returns before then has failed. Nothing restarts. Fork-join inside one call, and the goroutines a service owns, use `errgroup`. The node has no bare `go` statement.
- **A goroutine waits only for goroutines it started, for its own I/O and for node-wide loops that never wait for their callers** (the flush loop, which group commit relies on, is one). Children never wait for their parent or for each other, so waits can't form a loop and can't deadlock.
- **Callbacks from libraries never block on anything the node drives.** Litestream's storage client queues a file and returns, and whoever started the sync waits for durability afterwards.

## The tree

Who starts each goroutine and joins it:

```
Run (selects on its context, a failed service, the writer being fenced and the lease being lost)
└── its stages
    ├── front
    │   ├── public server
    │   ├── health poll
    │   ├── journal sweeper
    │   ├── kinds reload on SIGHUP
    │   └── pprof listener, if SHARDLITE_LISTEN_PPROF is set
    ├── callers
    │   ├── control server (the control listener and the Unix socket)
    │   └── peer server (requests from other nodes)
    ├── catalog (Catalog)
    │   ├── wake scan, lease scan (listing state/nodes/ and finding dead nodes), watchdog (the tail alert) and cache eviction
    │   ├── table: name → shard
    │   │   └── shard: its lifecycle loop (activation, idle and delta timers, release, delete)
    │   │       └── the shard's messages owner, while the shard is captured
    │   │           └── a delivery for each outbox or inbox row
    │   ├── flusher
    │   │   └── per cycle: a sync for each changed shard, at most GOMAXPROCS at once
    │   ├── messenger (the message limits per receiver and per node, no goroutines of its own)
    │   └── cache index (a resource, closed last)
    └── journal
        ├── writer (its uploader)
        └── keeper (lease renewal)
```

- **`Run`'s stages** are four groups, `front`, `callers`, `catalog` and `journal`, and `Run` stops them one at a time in that order, with the drain between `front` and `callers` (see [Shutdown](#shutdown)). `journal` starts as soon as the lease is taken, so the lease is renewed while the rest is built. Everything else is built and every listener is bound before the other stages start. `Run` selects on its context, on each stage's `Done` (a service that returned), on the writer being fenced and on the lease being lost, and it returns only after every stage has ended.
- **A stage groups the services that stop at the same point.** `front` is the public server and the services that depend on nothing (the health poll, the journal sweeper, the kinds reload and pprof). `journal` is the writer and the keeper.
- **The drain** is a step of `Run` between stopping `front` and stopping `callers`, not a part. `Run` calls `Catalog.Drain`, which gives live messages a few seconds to go out. `Catalog` keeps running meanwhile, and the control server is still up, so the app's handlers can still make calls.
- **`callers` outlives `front` and the drain** because a control request or a request from another node holds a reference to its shard: every one has to be joined before `Catalog` lets go of its shards. The health poll has no dependency, and it and the other services of `front` stop with the public server.
- **`journal` stops last** because the shards write through the writer and need the lease the keeper renews until the last of them is let go of.
- **`catalog`** is the `Catalog` type (named after the Orleans silo's catalog, which does the same job), which runs its parts in groups of its own and owns everything about the node's shards, active and cached. Its parts get what they depend on passed in (the messenger's transport, which is the server's routing, is passed when the shards are built, so nothing is set afterwards). It knows nothing of the servers or the drain, since every request is joined by the time `Run` stops it. It orders its own stop (see [Shutdown](#shutdown)).
- **`table`** runs the shards' lifecycle loops, on a context of its own that its stop cancels, and starts none once it has stopped. It maps each shard's name to an entry holding the shard's state, its references and its `shard`, and holds a short-lived entry for a cached shard while it is evicted or deleted, so every operation on a name goes through it. The map is a `sync.Map` (behind a small typed wrapper), and nothing locks the whole table: an activation adds its entry with `LoadOrStore` (whoever finds one already there waits for it to be ready), and removal uses `CompareAndDelete`, so it can only remove the entry it means to. Each entry's state and references are under the entry's own mutex. Requests take and release their references there, without going through the `shard`, so requests for different shards never contend. Removal marks the entry gone under that mutex first, and a request that found the entry just before looks the name up again. An entry whose shard's loop ended without letting go of it (the shards are stopping) is marked stopped: its capture is still open and the lease still counts it, but requests for it fail.
- **A `shard`** is the actor: one per shard, whose `Serve` is its lifecycle loop, which the table runs. The loop does one operation at a time: activation, a delta or rebase when its timer says so, deactivation when it has been idle for its kind's timeout, release and delete. Other goroutines send it requests and wait for the reply, or give up once it has ended (the same small function serves the messages owner). It runs on a context derived from a base context that `Catalog` owns, not from the context of `Catalog` itself, so stopping `catalog` doesn't cut off a release. Deactivations, deltas and rebases take a place under the node-wide limits (256 deactivations, 64 snapshots of which 4 are rebases, each a channel used as a semaphore).
- **The lease scan** is a loop of `Catalog` that owns its watches of leases that may have expired. It lists `state/nodes/` on its schedule, makes each watch's second look when it is due, and adopts the nodes it finds dead one after the other, on the loop: an adoption is blob storage requests and nothing else, so it waits for no other part. Others read its last listing, which it publishes whole through an atomic pointer. It stops with the wake scan, so no adoption starts once the shards are being let go of, and one in progress is cancelled (every step of an adoption is safe to redo).
- **The capture lock** guards a captured shard's Litestream database, the mapping of its wal-index and the capture client's tail. It is a lock rather than a loop because capture has no life of its own: no timers, nothing it starts and nothing it waits for, only state that its callers take turns with. Loops are for what does have one (a shard's lifecycle, its messages owner, the flush cycle and the scans). Everything that touches that state (a sync, a snapshot, a use of the connection, the tail, the close and the release) runs holding the lock, on the caller's goroutine, one at a time. The lock is a channel with room for one, so a caller stops waiting for it when its context ends, but once it holds the lock its work runs to the end, so a sync is never cut off. Litestream calls the capture's client from inside a sync, so those callbacks run under the lock as well and the client has no lock of its own. A panic under the lock ends the activation before it goes on, since a caller may recover it (`net/http` does for a handler) and Litestream's state can't be trusted after it. What others read without waiting (the tail's position, its size and where the lease says it starts) is an immutable summary published through an atomic pointer under the lock. The wal-index header is read without the lock, under a read lock that only an unmap takes for writing, so a flush cycle and a gate don't wait behind a sync. Once the activation is over the shard belongs to whoever closed or released it.
- **The messages owner** is the shard's second loop, a sequential one that nobody sends requests to. It waits for a kick (the flush cycle has captured new frames of the shard, and that is all the cycle does for messages), its inbox timer or the end of its context, then reads the shard's rows through the connection. If what the read saw isn't durable yet by the gate's rule (the shard has new frames after it), it waits for a cycle that began after the read to complete, through a channel from the flusher's counter, so the flush cycle never waits for an owner. A kick during that wait means a cycle has captured the shard's new frames (one that may have begun before the read, so it doesn't count for it), and the owner reads again at once rather than wait for another. Then it starts a delivery for each row read, and sets the inbox timer. A read cut short by the limits on rows and bytes, or one that failed, is followed by another after the next completed cycle, so a large table is never read in a tight loop. Deliveries record their own results through the shard's connection: an acknowledged outbox row is deleted by its delivery, a row given up on gets its `error` set by its delivery, and an inbox delivery looks its row up again before a retry and after a `2xx`. The rows in flight and the rows given up on are two sets under one leaf mutex, since the reads and the deliveries both change them. A read takes its view of the rows in flight inside its use of the connection, and a delivery deletes its row (or finds it consumed) and leaves the set inside its own, so the capture lock orders the two and a read never starts a row whose delivery has just deleted it. The lifecycle loop starts the owner in a group of its own once the shard is captured, on the loop's context, and stops the group and waits before a release, so nothing uses the capture during the final sync. The group names an owner that doesn't stop in time (`messages` and the shard's name). If the release fails and capture resumes, the loop starts an owner again, which carries on with what the shard keeps for the activation (the rows given up on). Deliveries run in an `errgroup` inside the owner, so stopping the owner cancels and joins them.
- **The segment writer's uploader** is the writer's service, one goroutine that writes the segments, one at a time and in order. A flush wakes it with a `Kick` and waits on a `Counter` of entries written, on the uploader ending or on its own context, whichever comes first. A caller that gives up leaves nothing for the next flush to finish: the uploader keeps retrying a segment until its outcome is known, and a queued entry is never dropped. A fence or the end of its service ends the uploader, and a flush then returns the error. The writer knows nothing of leases and has no waiters: the node acknowledges only through `Flusher.Wait`, which checks the keeper, and the flush cycle checks it after each flush, so a flush that completed after the lease ran out marks nothing captured and nothing durable.
- **The servers** give every request a context derived from a base context the server owns, and count their handlers in flight. Stopping a server waits for requests in flight (up to the grace period), then cancels the base context, which ends upgraded connections, and joins every handler. `httputil.ReverseProxy` ends an upgraded connection when its request's context ends, but its handler returns as soon as either direction ends, so a copier can run on briefly after it. Each request therefore counts its gates in flight, and every read and write on an upgraded connection to the app or to the owner's node, and `route` closes them (a gate, read or write started later fails without touching the shard or the connection) before it releases the request's references. A copier is then left only to return. A forwarded WebSocket is joined the same way on both nodes. The node's own requests (an intake, and the call for an inbox row) run through the same routing in process, and pass what they are as an explicit origin rather than through the request's context.
- **Fork-join inside one call** is joined before the call returns: the flush cycle's syncs, the wake scan's activations (32 at once), a restore's downloads (one per file), letting go of the shards at shutdown (256 at once) and adoption (64 shards at once), which runs at startup before the tree does, beside a keeper of its own that renews the lease until it ends.

## Shutdown

`Run` stops the stages one at a time, each on the order of the dependencies between them: `front`, the drain, `callers`, `catalog` and `journal`. It stops each group and waits for it, so a stage has ended before the next is told to stop, and no part knows what it depends on. Each stop waits at most 35 s: a service still running then is logged by name and left behind, so a stuck part never holds up the rest of a shutdown.

| Stage | Runs until |
|---|---|
| `front`: public server, health poll, journal sweeper, kinds reload, pprof | `Run` stops it, which is first |
| (the drain) | a step of `Run`, between `front` and `callers` |
| `callers`: control server, peer server | the drain has ended |
| `catalog` | the public, control and peer servers have all ended |
| `journal`: writer, keeper | `catalog` has ended |

In order, then:

1. `Run` stops `front` when the context ends, or a service fails, or the writer is fenced, or the lease is lost (see [Failures](#failures)). The public server stops accepting, waits up to 10 seconds for requests in flight, ends upgraded connections and joins every handler. The services with no dependency stop meanwhile.
2. The drain (a step of `Run`, not a service) gives messages that are still live up to 10 seconds to go out (`Catalog.Drain`). The control server is still up, so the app's handlers can still make calls, and receivers are activated as messages need them. It does nothing once the node may not write any more.
3. `callers` stops: the control and peer servers stop the same way as the public one, together, with their own grace period of up to 10 seconds, so no request holds a shard any more.
4. `catalog` stops, in its own order:
   1. the wake scan, the lease scan, the watchdog and cache eviction stop, so nothing activates or adopts any more
   2. the 30-second deadline for letting go starts. Every shard's lifecycle loop stops its messages owner, which joins its deliveries, before any shard is released (a delivery holds a reference to the shard it sends to), and no owner starts again. Each release's final delta records the shard's wake time, with its hint written first
   3. every shard is let go, at most 256 at once, retrying transient storage errors until the deadline, with the flusher still running
   4. the table stops, which ends the loops of shards that could not be let go and their messages owners, and joins them all together
   5. the flusher stops
   6. the cache index closes.

   If `Node.Err` says the lease is gone or the writer is fenced, `catalog` lets go of nothing and returns an error saying so.
5. `journal` stops once `catalog` has ended: the writer (which fails whatever is still queued as its uploader ends) and the keeper, which are both services of the same stage.
6. `Run` releases the lease if every shard was let go and `Node.Err` finds nothing wrong (the lease is good and the writer is not fenced). The release has a 30-second deadline of its own. Otherwise the lease is left to expire.

The deadlines are separate, not one shared budget: the public server's grace (10 s), the drain (10 s) and the grace of `callers` (10 s) run one after another, then letting go of shards gets its own 30 s, which starts when `Catalog` starts its own stop, then releasing the lease gets another 30 s. Every stop of a group waits at most 35 s (the stages' and `Catalog`'s parts' alike), longer than the deadline, so the deadline is what ends a release and nothing is left running.

## Failures

The node asks one question about its journal: `Node.Err` is nil while the lease is good and the writer isn't fenced, and otherwise it is the reason. `Run`, `Catalog` and the lease decision at the end of `Run` all ask it there, and no part exists only to watch the journal.

| On | The node |
|---|---|
| A panic anywhere | crashes (nothing recovers panics, except `net/http` in a handler, see the capture lock), and adoption recovers its shards on the next start |
| The lease lost, or the writer fenced | stops at once: `Run`'s select sees the writer's `Fenced` or the keeper's `Lost` and stops the stages in the usual order. `Catalog` finds `Node.Err` set, so it lets go of nothing and keeps the lease |
| Any other service returning | stops gracefully: its stage's `Done` ends `Run`'s select, and `Run` stops the stages as above and exits with the service's error (from its stage's stop) |
| A listener that can't be bound, or the shards failing to build | `Run` returns the error. The listeners are all bound before the shards are built, so a failed start closes them and stops the journal |

Nothing restarts. A crashed shard that costs only that shard is in the [roadmap](roadmap.md) (stage 4).

## Waits across the tree

These are the waits between goroutines that aren't parent and child. The rules allow each one because the goroutine waited for never waits back.

| Who waits | For | Why it can't loop |
|---|---|---|
| A request (through a server) | `Catalog`: a reference, an activation, a gate, a barrier, an intake | `Catalog` never waits for a request |
| `Catalog` | the writer (captures and flushes), the keeper's lease check (the flusher's, after a flush and in a barrier) and blob storage | none of them waits for a shard |
| A flush or a sync | the writer's uploader, through a `Kick` and a `Counter` | the uploader waits only for blob storage |
| A flush cycle, a release, the messages owner, an intake and the lifecycle loop | a capture's lock, to sync, snapshot, use the connection, read the tail or close | whoever holds it waits only for Litestream, SQLite's locks and the writer's mutex, and never for them. A header read doesn't wait for it at all |
| The keeper | lease information from `Catalog` and the writer | calls that read counts and segment numbers and return |
| A delivery | a shard on another node, through the transport | it ends with its owner's context |
| A messages owner | the flush loop's counter, in its select, for the cycle after a read | the flush loop never waits for an owner: it only kicks one whose shard it captured |
| A shard's lifecycle loop | its messages owner (its child), to stop it | the owner never waits for its lifecycle loop |
| `Run` | each stage, to stop it, one after another | a stage never waits for `Run` |

Inside `Catalog`, the waits are between its own parts: a gate or a release waits for the flusher, a delivery waits for another local shard through `table`, a wake activates a shard. The flusher waits for nothing that waits for it, and the order `Catalog` stops in keeps the rest apart.

## The locks that remain

The rule: no lock nests, is held across I/O or a wait on another goroutine, or stands in for ownership. A lock that guards a few fields for a few lines (a leaf) stays where the code without it would not be simpler, since for a few fields a mutex is usually the most expressive and simple choice. A lock is never rebuilt from atomics (a reader count that something waits on, a state machine packed into a word). Only the capture lock breaks the rule, for the reasons in its row below.

| Lock | Guards | Why it is a leaf |
|---|---|---|
| `Shard.mu` (capture) | Litestream's database, the wal-index mapping and the client's tail | not a leaf: it is held across Litestream's local work (syncs, snapshots, the close) and a use of the connection, which can wait for SQLite's write lock, and its holder takes the writer's mutex to append a file. It never waits for blob storage or for another goroutine of the node, so it can't be part of a cycle |
| `entry.mu` (table) | the entry's state, references, last use and the channel that announces a change | held for a few lines and nothing is called while it is held |
| `Writer.mu` (segment) | the queue, the segment number and the stop state | the segment is built and sent without it, and a holder of the capture lock takes it only to append a file |
| `inFlight.mu` (servers) | the closed flag, so that closing joins every call let in | held for the length of a flag check |
| `notify.Counter` | its value and its waiters | held to add, load or register a waiter, never while waiting |
| `messenger.mu` | the receivers by name and each one's count of senders | held to add a sender, or take one away and remove the receiver it leaves empty, never while waiting on a semaphore |
| `kept.mu` (messages) | a shard's rows in flight and its rows given up on | held to copy or change the sets, never across a query: a read copies its view, then queries |

Also:

- **Nesting.** One level: the capture lock's holder takes the writer's mutex to append a file, and a messages read or delivery takes `kept.mu` inside its use of the connection (that is the ordering rule above). Both are leaves that never have another lock taken under them, the client has no lock and the keeper's lease is an atomic, so nothing can nest further. The writer knows nothing of the keeper.
- **Lock-free map.** The table's entries are a `sync.Map` behind a small typed wrapper.
- **Semaphores** are channels: the node-wide limits on deactivations, deltas and rebases, and the messenger's limits per receiver and per node.
- **Atomics** carry values that are published once or written by one goroutine and read by many:
  - the keeper's latest lease (stored by its renewal loop only) and final error
  - the capture's published tail view, written under its lock, and whether its activation is over
  - the shard's capture pointers (the current capture and the one the lease counts), its last sync error, the header as of its last completed capture, its messages owner and its next idle look
  - whether the messages owner has ended
  - the flusher's cycle counters and the number of waiters
  - the lists of kinds and whether they are valid, and the node's source of tail counts
  - the messenger's count of intakes
  - the cache limit, the stats counters, the server's health flag, the segment writer's flush count, the store's write numbers and the references counted by spooled files
- **Once** is used to close a channel or run a step one time: a part's ending and a request's reference.
- **Test helpers** keep their own mutexes (the in-memory blob store, `apptest`'s log and its app, `TailSet` for programs that capture shards without `Catalog`, and the test node). They are leaves too.
