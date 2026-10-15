# shardlite concurrency

How the node's goroutines are owned, how they shut down and how they wait for each other. The rest of the node is in [internals](internals.md).

## Rules

- **Every goroutine has an owner that joins it,** up to the node's `Run`, which returns only after everything it started has ended. The parts that live as long as the node are [suture](https://github.com/thejerf/suture) services in a tree of supervisors. Fork-join inside one call, and the goroutines a service owns, use `errgroup`. The one bare `go` statement left is a captured shard's capture loop, which the capture's `Close` joins (or the `Release` that ends the activation).
- **A goroutine waits only for goroutines it started, for its own I/O and for node-wide loops that never wait for their callers** (the flush loop, which group commit relies on, is one). Children never wait for their parent or for each other, so waits can't form a loop and can't deadlock.
- **Callbacks from libraries never block on anything the node drives.** Litestream's storage client queues a file and returns, and whoever started the sync waits for durability afterwards.

## The tree

Who starts each goroutine and joins it:

```
Run (selects on its context, a failed service, the writer being fenced and the lease being lost)
└── node
    ├── front
    │   ├── public server
    │   ├── health poll
    │   ├── journal sweeper
    │   ├── kinds reload on SIGHUP
    │   └── pprof listener, if SHARDLITE_PPROF is set
    ├── control server (the control listener and the Unix socket)
    ├── shards (Shards)
    │   ├── wake scan, watchdog (the tail alert) and cache eviction
    │   ├── table: name → shard
    │   │   ├── shard: its lifecycle loop (activation, idle and delta timers, release, delete)
    │   │   │   └── capture loop, while the shard is captured (started by the capture, joined by its Close or Release)
    │   │   └── the shard's messages owner, while the shard is captured
    │   │       ├── a delivery for each outbox or inbox row
    │   │       └── a child for the wake hints in blob storage, one job at a time
    │   ├── flusher
    │   │   └── per cycle: a sync for each changed shard, at most GOMAXPROCS at once
    │   ├── messenger (the message limits per receiver and per node, no goroutines of its own)
    │   └── cache index (a resource, closed last)
    └── journal
        ├── writer (its uploader)
        └── keeper (lease renewal)
```

- **The `node` supervisor's children** are the four stages, `front`, the control server, `shards` and `journal`, and `Run` stops them one at a time in that order, with the drain between `front` and the control server (see [Shutdown](#shutdown)). `front` and `journal` are plain suture supervisors. `journal` starts as soon as the lease is taken, so the lease is renewed while the rest is built. Everything else is built and every listener is bound before the other stages start. `Run` is not a supervisor: it builds `node` and watches it from outside (it selects on its context, on a service that failed, on the writer being fenced and on the lease being lost), and it returns only after `node` and everything under it has ended.
- **A stage groups the services that stop at the same point.** `front` is the public server and the services that depend on nothing (the health poll, the journal sweeper, the kinds reload and pprof). `journal` is the writer and the keeper.
- **The drain** is a step of `Run` between stopping `front` and stopping the control server, not a part. `Run` calls `Shards.Drain`, which gives live messages a few seconds to go out. `Shards` keeps running meanwhile, and the control server is still up, so the app's handlers can still make calls.
- **A stage with one service has no supervisor of its own.** The service sits directly under `node`, as the control server and `shards` do. Once a stage gains a second service that stops at the same point (an HTTP/3 listener next to the control listener, say), a supervisor named after the stage takes the first service's place, with both under it.
- **The control server outlives `front` and the drain** because a control request holds a reference to its shard: every control request has to be joined before `Shards` lets go of its shards. The health poll has no dependency, and it and the other services of `front` stop with the public server.
- **`journal` stops last** because the shards write through the writer and need the lease the keeper renews until the last of them is let go of.
- **`shards`** is the `Shards` type, which keeps a supervisor of its own and owns everything about the node's shards, active and cached. Its parts get what they depend on passed in (the messenger's transport, which is the server's routing, is passed when the shards are built, so nothing is set afterwards). It knows nothing of the servers or the drain, since every request is joined by the time `Run` stops it. It orders its own stop (see [Shutdown](#shutdown)).
- **`table`** has a supervisor of its own (`Shards` adds it to its own), which runs the shards and their messages owners. It maps each shard's name to an entry holding the shard's state, its references and its `shard`, and holds a short-lived entry for a cached shard while it is evicted or deleted, so every operation on a name goes through it. The map is a `sync.Map` (behind a small typed wrapper), and nothing locks the whole table: an activation adds its entry with `LoadOrStore` (whoever finds one already there waits for it to be ready), and removal uses `CompareAndDelete`, so it can only remove the entry it means to. Each entry's state and references are under the entry's own mutex. Requests take and release their references there, without going through the `shard`, so requests for different shards never contend. Removal marks the entry gone under that mutex first, and a request that found the entry just before looks the name up again. An entry whose shard's loop ended without letting go of it (the shards are stopping) is marked stopped: its capture is still open and the lease still counts it, but requests for it fail.
- **A `shard`** is the actor: one per shard, a service of the table's supervisor whose `Serve` is its lifecycle loop, so suture removes it once the loop ends. The loop does one operation at a time: activation, a delta or rebase when its timer says so, deactivation when it has been idle for its kind's timeout, release and delete. Other goroutines send it requests and wait for the reply, or give up once it has ended (the same small function serves the messages owner). It runs on a context derived from a base context that `Shards` owns, not from the context of `Shards` itself, so stopping `shards` doesn't cut off a release. Deactivations, deltas and rebases take a place under the node-wide limits (256 deactivations, 64 snapshots of which 4 are rebases, each a channel used as a semaphore).
- **The capture loop** is the actor that owns a captured shard's Litestream database, the mapping of its wal-index and the capture client's tail. `capture.New` starts it and a `Close` or a `Release` that ends the activation joins it. Everything that touches those (a sync, a snapshot, a use of the connection, the tail, the close and the release) is sent to it as a function and runs one at a time, so syncs, snapshots and the close are kept apart by the loop and not by a lock. Litestream calls the capture's client from inside a sync, so those callbacks run on the loop as well and the client has no lock. What others read without waiting (the tail's position, its size and where the lease says it starts) is an immutable summary the loop publishes through an atomic pointer. The wal-index header is read on the caller's goroutine, under a read lock that only an unmap takes for writing, so a flush cycle and a gate don't wait behind a sync. Once the loop has ended the shard belongs to whoever closed or released it. The caller of a request that has been handed over waits for it to finish (its context only ends the wait to hand it over), so a sync is never cut off.
- **The messages owner** is the shard's second loop. It owns the shard's outbox and inbox bookkeeping, its inbox timer and its wake hints, and nothing else touches them: the deliveries' results, the lifecycle loop's questions and the hints the wake scan found all arrive as events on its channel. It reads the shard's rows itself, on its loop: when the flush cycle has captured new frames of the shard (the cycle kicks it, and that is all the cycle does for messages) and, at most once per completed cycle, when it wants a read of its own (rows left unread, a delivery's question, a due row). A read's rows go out once the read is durable by the gate's rule: at once if the shard has no new frames after it, and otherwise once a cycle that began after it has completed, which the owner waits for in its select (a channel from the flusher's counter), never in a call. So the flush cycle never waits for an owner, and a slow commit on one shard never holds it. Blob storage for the hints runs in a child, one job at a time, with the result coming back as an event, so a slow store never delays a read. The lifecycle loop adds the owner to the table's supervisor, beside the shard, once the shard is captured, and before a release asks it to write the wake hints, then removes it there and waits, so nothing uses the capture during the final sync. Suture reports an owner that doesn't stop in time under its own name (`messages` and the shard's name). If the release fails and capture resumes, it adds an owner again, which carries on with what the last one kept (the rows given up on and the hints). The owner closes the channel that tells senders it has ended when its loop returns. The one owner whose loop never runs is one added once the table's supervisor has stopped, and the lifecycle loop ends that one at once. Deliveries and the hint child run in `errgroup`s inside the owner, so stopping the owner cancels and joins them.
- **The segment writer's uploader** is the writer's service, one goroutine that writes the segments, one at a time and in order. A flush wakes it with a `Kick` and waits on a `Counter` of entries written, on the uploader ending or on its own context, whichever comes first. A caller that gives up leaves nothing for the next flush to finish: the uploader keeps retrying a segment until its outcome is known, and a queued entry is never dropped. A fence or the end of its service ends the uploader, and a flush then returns the error. The writer knows nothing of leases and has no waiters: the node acknowledges only through `Flusher.Wait`, which checks the keeper, and the flush cycle checks it after each flush, so a flush that completed after the lease ran out marks nothing captured and nothing durable.
- **The servers** give every request a context derived from a base context the server owns, and count their handlers in flight. Stopping a server waits for requests in flight (up to the grace period), then cancels the base context, which ends upgraded connections, and joins every handler. `httputil.ReverseProxy` ends an upgraded connection when its request's context ends, but its handler returns as soon as either direction ends, so a copier can run on briefly after it. Each request therefore counts its gates in flight, and `route` closes them (a gate entered later fails without touching the shard) before it releases the request's references. The node's own requests (an intake, and the call for an inbox row) run through the same routing in process, and pass what they are as an explicit origin rather than through the request's context.
- **Fork-join inside one call** is joined before the call returns: the flush cycle's syncs, the wake scan's activations (32 at once), a restore's downloads (one per file), letting go of the shards at shutdown (256 at once) and adoption (64 shards at once), which runs at startup before the tree does.

## Shutdown

`Run` stops the stages one at a time, each on the order of the dependencies between them: `front`, the drain, the control server, `shards` and `journal`. It removes each stage from `node` and waits for it (`RemoveAndWait`), so a stage has ended before the next is told to stop, and no part knows what it depends on. Suture's timeout is the backstop.

| Stage | Runs until |
|---|---|
| `front`: public server, health poll, journal sweeper, kinds reload, pprof | `Run` stops it, which is first |
| (the drain) | a step of `Run`, between `front` and the control server |
| control server | the drain has ended |
| `shards` | the public and control servers have both ended |
| `journal`: writer, keeper | `shards` has ended |

In order, then:

1. `Run` stops `front` when the context ends, or a service fails, or the writer is fenced, or the lease is lost (see [Failures](#failures)). The public server stops accepting, waits up to 10 seconds for requests in flight, ends upgraded connections and joins every handler. The services with no dependency stop meanwhile.
2. The drain (a step of `Run`, not a service) gives messages that are still live up to 10 seconds to go out (`Shards.Drain`). The control server is still up, so the app's handlers can still make calls, and receivers are activated as messages need them. It does nothing once the node may not write any more.
3. The control server stops the same way as the public one, with its own grace period of up to 10 seconds, so no request holds a shard any more.
4. `shards` stops, in its own order:
   1. the wake scan, the watchdog and cache eviction stop, so nothing activates any more
   2. the 30-second deadline for letting go starts. Every shard's deliveries stop and are joined before any shard is released (a delivery holds a reference to the shard it sends to). The owners keep answering, since each release asks its owner for the wake hints
   3. every shard is let go, at most 256 at once, retrying transient storage errors until the deadline, with the flusher still running
   4. the table stops, which ends the loops of shards that could not be let go and their messages owners, and joins them all together
   5. the flusher stops
   6. the cache index closes.

   If `Node.Err` says the lease is gone or the writer is fenced, `shards` lets go of nothing and returns an error saying so.
5. `journal` stops once `shards` has ended: the writer (which fails whatever is still queued as its uploader ends) and the keeper, which are both services of the same stage.
6. `Run` releases the lease if every shard was let go and `Node.Err` finds nothing wrong (the lease is good and the writer is not fenced). The release has a 30-second deadline of its own. Otherwise the lease is left to expire.

The deadlines are separate, not one shared budget: the public server's grace (10 s), the drain (10 s) and the control server's grace (10 s) run one after another, then letting go of shards gets its own 30 s, which starts when `Shards` starts its own stop, then releasing the lease gets another 30 s. The supervisors inside `Shards` wait 35 s for their services (as `node` and the stages do), longer than the deadline, so the deadline is what ends a release and nothing is left running. `Run` waits twice that for a stage to be removed, so the supervisor's own timeout reports first.

## Failures

The node asks one question about its journal: `Node.Err` is nil while the lease is good and the writer isn't fenced, and otherwise it is the reason, marked fatal. `Run`, `Shards` and the lease decision at the end of `Run` all ask it there, and no part exists only to watch the journal.

| On | The node |
|---|---|
| A panic anywhere | crashes (suture passes panics through), and adoption recovers its shards on the next start |
| The lease lost, or the writer fenced | stops at once: `Run`'s select sees the writer's `Fenced` or the keeper's `Lost` and stops the stages in the usual order. `Shards` finds `Node.Err` set, so it lets go of nothing and keeps the lease |
| Any other service returning an error | stops gracefully: the service sends the error to `Run` (it ends without telling suture), which stops the stages as above and exits with an error |
| A listener that can't be bound, or the shards failing to build | `Run` returns the error. The listeners are all bound before the shards are built, so a failed start closes them and stops the journal |

Nothing restarts. A service that fails reports to `Run` and ends, and doesn't return its error to suture, since suture would then stop the supervisors at once and not in order. A crashed shard that costs only that shard is in the [roadmap](roadmap.md) (stage 4).

## Waits across the tree

These are the waits between goroutines that aren't parent and child. The rules allow each one because the goroutine waited for never waits back.

| Who waits | For | Why it can't loop |
|---|---|---|
| A request (through a server) | `Shards`: a reference, an activation, a gate, a barrier, an intake | `Shards` never waits for a request |
| `Shards` | the writer (captures and flushes), the keeper's lease check (the flusher's, after a flush and in a barrier) and blob storage | none of them waits for a shard |
| A flush or a sync | the writer's uploader, through a `Kick` and a `Counter` | the uploader waits only for blob storage |
| A flush cycle, a release, the messages owner and the lifecycle loop | a capture's loop, to sync, snapshot, use the connection, read the tail or close | the loop waits only for Litestream, SQLite's locks and the writer's mutex, and never for them. A header read doesn't wait for it at all |
| The keeper | lease information from `Shards` and the writer | calls that read counts and segment numbers and return |
| A delivery | a shard on another node, through the transport | it ends with its owner's context |
| A messages owner | the flush loop's counter, in its select, for the cycle after a read | the flush loop never waits for an owner: it only kicks one whose shard it captured |
| A shard's lifecycle loop | its messages owner (a sibling on the table's supervisor), to ask whether rows are live, to write the wake hints and to stop it | the owner never waits for its lifecycle loop |
| `Run` | each stage, to stop it, one after another | a stage never waits for `Run` |

Inside `Shards`, the waits are between its own parts: a gate or a release waits for the flusher, a delivery waits for another local shard through `table`, a wake activates a shard. The flusher waits for nothing that waits for it, and the order `Shards` stops in keeps the rest apart.

## The locks that remain

The rule: no lock nests, is held across I/O or a wait on another goroutine, or stands in for ownership. A lock that guards a few fields for a few lines (a leaf) stays where the code without it would not be simpler, since for a few fields a mutex is usually the most expressive and simple choice. A lock is never rebuilt from atomics (a reader count that something waits on, a state machine packed into a word). Nothing nests now, and what is left is below.

| Lock | Guards | Why it is a leaf |
|---|---|---|
| `entry.mu` (table) | the entry's state, references, last use and the channel that announces a change | held for a few lines and nothing is called while it is held |
| `Writer.mu` (segment) | the queue, the segment number and the stop state | the segment is built and sent without it, and the capture's loop takes it only to append a file |
| `inFlight.mu` (servers) | the closed flag, so that closing joins every call let in | held for the length of a flag check |
| `notify.Counter` | its value and its waiters | held to add, load or register a waiter, never while waiting |
| `messenger.mu` | the receivers by name and each one's count of senders | held to add a sender, or take one away and remove the receiver it leaves empty, never while waiting on a semaphore |

Also:

- **Nesting.** None. The chain of the capture hold, the client's mutex, the writer's mutex and the keeper's is gone: the capture's loop is not a lock, the client has none, and the keeper's lease is an atomic. The writer knows nothing of the keeper, and its mutex never has another lock taken under it.
- **Lock-free map.** The table's entries are a `sync.Map` behind a small typed wrapper.
- **Semaphores** are channels: the node-wide limits on deactivations, deltas and rebases, and the messenger's limits per receiver and per node.
- **Atomics** carry values that are published once or written by one goroutine and read by many:
  - the keeper's latest lease (stored by its renewal loop only) and final error
  - the capture's published tail view, written by its loop
  - the shard's capture pointers (the current capture and the one the lease counts), its last sync error, the header as of its last completed capture, its messages owner, its next idle look and whether its kept-shard log line was written
  - whether the messages owner has ended
  - the flusher's cycle counters and the number of waiters
  - the lists of kinds and whether they are valid, and the node's source of tail counts
  - the messenger's count of intakes
  - the cache limit, the stats counters, the server's health flag, the segment writer's flush count, the store's write numbers and the references counted by spooled files
- **Once** is used to close a channel or run a step one time: a part's ending and a request's reference.
- **Test helpers** keep their own mutexes (the in-memory blob store, `apptest`'s log and its app, `TailSet` for programs that capture shards without `Shards`, and the test node). They are leaves too.
