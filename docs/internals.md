# shardlite internals

How the node works: its architecture and the rules that keep it correct. It describes the overall shape and the details that matter, and leaves out what the code says plainly. The app-facing behaviour is in the [guide](guide.md), and the protocol's models are in [specs/](../specs/).

## Terms

| Term | Meaning |
|---|---|
| **Node lease** | An object each node holds and renews. Every ownership depends on it. |
| **Node journal** | Numbered segments, one per flush interval, holding the node's shards' commits since their last snapshots: the shared WAL. |
| **Snapshot** | A shard as of one commit, as immutable LTX files: a base and deltas. An inactive shard lives only in its snapshot. |
| **Manifest** | A shard's state in storage: owner and epoch, files, the journal holding later commits, when it next needs waking. |
| **Reminder index** | Empty objects saying a shard may need waking at a minute. `_system.dispatch` reads them. |

The other terms are in the [overview](overview.md#concepts).

## The shape of a node

The node is one Go process. It reads each shard's WAL itself and encodes what it reads as [LTX](https://github.com/superfly/ltx) files. Capture is driven, never polled: the flush loop spots commits by mapping each active shard's wal-index header, which SQLite updates on every commit, so a sync costs microseconds. Nothing shard-sized lives in memory: tails, deltas, restores and rebases all go through unnamed files in `{data}/.tmp`.

Blob storage is the source of truth: manifests, LTX files, one journal per node, the reminder index. The node's disk holds the working copy of each active shard and a cache of recently used ones. A cached shard keeps its owner while the lease holds, so reactivating it costs no blob request, and when the disk fills the least recently used are evicted: the manifest released by CAS and the files dropped.

Leases and manifests are objects changed with conditional writes, which are linearizable on every supported store, so there is no leader, no election and no set of stable nodes. Every write carries a CRC32 the store verifies and every read checks the checksum the store returns, so corruption fails the read that finds it. Callers go through `blob.Store`, which makes writes safe to retry (see [Lost responses](#lost-responses)). The node checks its store's conditional writes on a scratch key at startup and refuses to start if they misbehave.

Only two things live outside shards: a lease (which node is alive, in which epoch) and a journal (commits durable before any file holds them), neither of which can live inside a database whose owner they decide. Everything else is a **shard**: a database stored the same way, with a kind, the claim rule and a lifecycle. The test for anything new: state shared across nodes goes in a shard, an entry written while no owner can be relied on is an index object (see [The reminder index](#reminder-index)), and only what a shard needs to exist is a primitive.

## Failing loudly

The dangerous failure is not the crash, which is loud and costs a restart, but the wrong answer nothing reports: a segment with the right size and wrong bytes, an acknowledgement for a write that is not there. Two rules decide much of the node's shape: every mechanism must report its own failure, and every mechanism we write ourselves must be one the fault rig can make fail (capture's read of a WAL, the temporary files, the writer's landing of a segment, the applier's write of a page), each checked by killing a node mid-step.

A closed set answers for its zero value under one of two policies: the zero is an inherently safe member that causes no action (a position of 0 is nothing yet, no store, nothing written), or it is a named `Unknown` that `Valid()` rejects and every switch handles, so it cannot land in a default written for another reason. A zero that is the first real state, or one that enables an action, is not allowed, and a decoder rejects an unknown value rather than mapping it onto the zero, so a newer node's state fails loudly instead of being downgraded. Each package that owns a closed set pins the policy in a zero-value test, and the linter's `mapliteral` mode fails an enum-keyed table missing a member.

## Ownership and fencing

A lease alone can't prevent two writers (a pause, a delay), so the storage must reject a stale owner's writes, as in Kleppmann's [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html). Here that is the journals (one writer each) with the manifests, specified in Quint.

### Node leases

Each node holds one lease, renewed by CAS every few seconds, carrying its epoch, address, whether it serves and the range of its journal still needed. A shard's ownership is valid only while its owner's lease is, so one expired lease releases all of a crashed node's shards.

- **Elapsed time, not timestamps.** The holder treats its lease as expired at the start of its last successful renewal plus the TTL less a fifth, by its own clocks; another node, once it has seen the lease unchanged for a TTL on its own monotonic clock. Leases need clock rates that agree, not synchronised clocks, and a node that loses its lease exits and restarts as from a crash.
- **Checking the clock.** The node compares its clock with the `Date` header of the store's responses. An offset over a second warns and one over the lease TTL stops the start. It fails nothing else: `none` answers rest on working clocks, and this is where an operator learns that one is not.
- **Noticing dead nodes.** Nodes list `state/leases/` in turn, so the cluster lists about every 2 seconds. A listing's last-modified times are a hint: seeing an old one, the node reads the lease, looks again a TTL later and adopts only if it is unchanged.

### Manifest

A manifest is created with a conditional create and changed only by CAS, at ownership changes and snapshots, never on writes or activating an owned shard (its fields are in `internal/shardfile/manifest.go`).

- **`next_reminder`** is set by the owner's deltas, rebases and deactivations, its entry in the reminder index created first. Adoption's deltas set it to now, and system shards never set it.
- **`rev`** goes up on every write, so no two versions share bytes: a manifest released and claimed again by the same node would otherwise return to an earlier version, which the pruning rule reads differently.
- Ownership is invalid if `node_epoch` doesn't match the owner's current lease or that lease has expired, and a shard deleted and created again gets a new `generation`, so it never reads the old shard's files.

### Fencing

- Each journal has one writer. Segments are numbered and created once, and a writer finding its number taken has been fenced and stops for good.
- An adopter fences a dead node's journal with an empty segment at the first free number after the lease's recorded range; a zombie can then only write below the fence, where nothing is part of any shard's history. The fence in storage decides the journal.
- A zombie's LTX files count once a manifest CAS adopts them, and a stale owner's CAS fails once anyone else has changed the manifest.
- This needs no listings: segments are found by number, and single-object reads are strongly consistent on every store with conditional writes.

### Confirming ownership

A `storage` answer leaves once ownership is confirmed after the answer was read: evidence, begun after the chunk was read from the app, that no other node can yet have acknowledged anything for the node's shards. The rule underneath is that every way another node takes a shard from a live owner writes the lease first: an adoption marks it, a restart takes a new epoch, a release marks it adopted, and eviction, handoff and drains are started by the owner, which stops serving first. So a lease that still shows this incarnation's epoch, not released, not marked by another incarnation and not adopted up to the epoch, means nobody has begun a takeover. Two kinds of evidence count:

- **A segment created in storage** by a flush cycle that began after the chunk was read. An adopter marks the lease, fetches, then fences, and a new owner claims only after the fence, so a segment that lands came before any other owner's acknowledgement. This is why output after a commit needs no lease read.
- **A read of the node's own lease** begun after the chunk was read. The `Confirmer` (`internal/coord`) runs at most one at a time and a waiter joins only a read that begins after it arrived, so on an idle node a burst of reads shares one GET and under write load the segments release the waiters. The lease's expiry is left out of the condition, since the clocks are what this replaces.

A confirmation that finds the lease taken fails the keeper with `ErrLeaseLost`, so the node stops and restarts as from a crash, and one that cannot reach the store fails the answer with `503`, never answering unconfirmed. A request confirms once: its first chunk is read after the request began, and the shard's file only grows while the request holds it. Streams are therefore linearizable at their start and then follow the node's copy until it notices the loss of the shard. `none` answers are not confirmed.

### Consistency

Per shard, every read and write is linearizable on the `storage` tier, and on `none` until the node crashes (see the [guide](guide.md#consistency)). Writes: one owner per shard, SQLite's commit order, an acknowledgement waiting for the commit to reach the journal, and a zombie's acknowledged commits below the fence its adopter reads. Reads on `storage` are confirmed, so they need no assumption about clocks or pauses. Reads on `none` check the node's lease before output leaves, and claiming a dead owner's shard needs an adopter that watched the lease stay unchanged for a TTL and fenced the journal, so for those clock rates must agree within the lease's margin (20%) and no pause may hide from the machine's own clocks.

### Lost responses

Any write can succeed while its response is lost. `blob.Store` stores a write id with each object and, when a retry meets "exists" or "conflict", reads it back to recognise its own attempt, and `journal.Store` gives the journals the same, so a caller sees success, a conflict, or an unknown outcome. The segment writer retries the same segment until the outcome is known, and a manifest handle compares the stored manifest with the one it tried. The node never retries a request the app may have seen.

### Takeover

- **Eviction** (a full cache, rebalancing, draining): the owner deactivates the shard if it is active, releases its manifest by CAS and drops its files, and the next node to need it claims and restores. An unknown release counts as landed without resuming capture on a maybe-unowned shard.
- **Crash.** A node whose lease has expired is adopted by the first live node to notice, every step a conditional write safe to redo: **mark** the lease as being adopted, **fence** the dead journal, **read** the footers up to the fence, then per shard still naming the dead journal create a reminder entry due now and add the tail as a delta by CAS (with `next_reminder` now and the journal cleared), **finish** by CAS on the lease (adopted to the dead epoch, the journal range past the fence), and **sweep** the segments below the fence, fences staying. What a dead node held only on its own disk is gone.
- **Restart** is treated as a crash, one recovery path: the node takes a new epoch, adopts its own journal before serving anything and starts its writer after the fence on an empty cache. A `none` commit that storage did not yet have is lost in any crash of the node, process or host. While another node is adopting it and it looks alive, the restarting node waits.
- **Drain** (scaling down) fails readiness, refuses claims (`503` with `Retry-After: 0`) and stops, evicting each shard as it lets go; a shard the app holds open waits for adoption. **Stop** is the same final state with the lease released, and the node stays listed as alive until its lease is a TTL old, so a dispatch to it fails and goes elsewhere, whose claim the released lease allows.

The reminder entry comes before the delta because journal expiry deletes covered segments without checking any lease: an adopter dying between the two would leave a shard nobody knows to wake (`noAdoptionEntry` in `specs/dispatch.qnt`). A move copies nothing between journals. The dead node's shards keep it as their owner until nothing of them can be in its journal (the manifest names no journal, or the node adopted up to the owner's epoch), and since no timestamp decides it, a lease that looks expired still owns its shards until then.

## Capture and snapshots

### The read rule

While a shard is captured, the node holds one read transaction begun under the write lock right after a sync and a checkpoint that copied everything, so it gets read mark 0. SQLite then copies no frame into the database file and never restarts the WAL while a frame is waiting to be copied, the only restarts it allows coming after the node's own checkpoint (the app's next write, or its restart or truncating checkpoint), when every frame of the old WAL is captured. So no uncaptured frame is ever copied or lost.

- **The locked restart.** While the transaction is held the WAL only grows, so past a threshold of frames the node takes the write lock, syncs, ends the transaction, has everything copied in (a passive checkpoint) and begins a new one. It must be passive, since a truncating checkpoint can't run under the node's own lock and frames committed in the gap would be wiped uncaptured.
- **The app's checkpoints do nothing while a shard is captured:** a passive one copies nothing, and a restart or truncating one answers `SQLITE_BUSY`.

Each sync appends the LTX file it encodes to the shard's tail, append-only unnamed files in `{data}/.tmp` and the ranges of them holding the commits not yet in a delta. `specs/capture.qnt` checks the rule: every committed frame is in a captured file before it can leave the WAL (`noLoss`), the files replayed give the database at the captured end (`replay`), and a snapshot gives the database at the position it began (`snapOk`).

### Checksums

Every LTX file carries the database's checksum before it and after it (LTX's whole-database checksum, the XOR of each page's CRC with its number). Capture keeps every page's CRC in memory, so the checksums cost one CRC per changed page. Deltas check the chain, each file's before being the one before's after. A rebase compares capture's checksum with the snapshot's, and a mismatch (capture went wrong) is logged and counted, the rebase written anyway since it is the database as SQLite reads it. A file without checksums fails like any broken file. A restore verifies every file, and the first mismatch fails it with the file named: the shard is not served and nothing is deleted, so an operator has every file.

### Node journals and snapshots

The same split as a database's WAL and data files: the journal the shared WAL of a node's shards, the LTX files each shard's data files, a snapshot the checkpoint. The journal's store is `internal/journal`: numbered segments created once, durable before the create returns, read by number (a head of size 0 is a fence), behind an interface with a conformance suite. Every create carries the write-id handling of [Lost responses](#lost-responses), so a lost response costs a retry, never a fence.

- **Segments.** Every flush interval one segment, a footer naming its shards, the body composed from the LTX files capture queued so the bytes are written once.
- **Snapshots as deltas.** When a shard deactivates, or its tail is old or large, the node writes the pages captured since the last snapshot as one delta and adds it by CAS, without reading the database file, so a cached shard never has a tail and the lease's journal range only covers active shards.
- **Rebasing.** Once a manifest lists many files, the next snapshot is a full copy of the live database as a new base, bounding how many files a restore reads. A few run at once per node.
- **Cleaning the journal.** Each node deletes its own non-empty segments once a day old, and an alert comes long before if a tail is old, since nothing tracks which segments are in use. Fences are never deleted, since a paused zombie could then write into a freed number.
- **The node's own database** (`internal/node/localdb.go`) holds the cache index, in SQLite because each row carries the CRC of every page of its shard. Nothing in it outlives a start.

## Durability gates

- A chunk the app wrote is held until a flush that started after it was read has captured it and the store its tier names has what the flush queued, since every flush captures every active shard's commits up to its start. Chunks waiting together share one flush (group commit).
- `none` waits for nothing past the lease check, and `storage` for the PUT: a flush that started after the chunk was read has to have created its segment. A `sync` outside the set is an error, never a tier that waits for nothing.
- Every chunk first needs the node's lease to be good, and a flush that completed after the lease ran out acknowledges nothing.
- A `storage` chunk then waits to be confirmed unless its request has been (see [Confirming ownership](#confirming-ownership)). A chunk that waited for its commit's segment is confirmed by it unless the cycle that passed created none.
- A call to another shard is bytes leaving the caller, so it waits for the caller's commits to meet its kind's tier.

## Data and state

| Role | Objects | Changes by | Rule |
|---|---|---|---|
| **Data** | journal segments, LTX files | create once, delete by garbage collection | never modified |
| **State** | leases, manifests | compare-and-swap by the owner | the truth, naming the data it points at |
| **Index** | reminder index entries | created by anyone before the state change they are for, moved and deleted by the dispatcher | deleted only when the manifest doesn't back it |

Journal segments go to blob storage, which is the only copy: a restore, an adoption and a restart all read it, and the fence is created there. Listings stay per prefix (leases, a node's journal, the index up to the current minute, the sweeper's `data/shards/`), so none grows with the rest.

### Reminder index

An inactive shard's reminder needs someone to wake it. `next_reminder` in the manifest says when, and the index says which manifests to look at when: an entry is an empty object anyone can create, needing no owner and no clock, read only by `_system.dispatch`. A minute is enough, since a shard's own loop fires at the second once it is active. An entry's key (see `internal/remindex`) names the UTC minute, a shard hash prefix and the manifest version the entry backs.

- **Index first.** Before a manifest write that sets `next_reminder` to a new minute, the writer creates the entry for that minute and its base, a failed create failing the write. A write whose reminder is in the minute the manifest already has creates none.
- **Each write has its own entry.** An entry under another base never counts: two writes sharing one would let the dispatcher prune it on the first's terms while the second still relies on it.
- **The pruning rule.** An entry may be deleted when the manifest doesn't back it (absent, or `next_reminder` absent or after the entry's minute) and the manifest's current version is not the entry's base: the write the entry was made for replaces that version, so once the manifest has moved off it, the write has landed or can never land, while if the version is still the base the write may be in flight.
- **A backed entry is never deleted.** It is dispatched and moved forward, which covers a shard active after a dispatch whose node then crashes with nothing of it in the journal.
- **Anyone may apply the rule,** so a second dispatcher or a zombie can't make it unsafe. One dispatcher is for cost, not correctness.

`specs/dispatch.qnt` checks that a shard with a reminder in its durable state always has a backed entry or a tail an adoption will still write a delta for.

## System kinds

Some of the node's own work needs exactly one runner, and somewhere to keep state. A shard already has both, so the node keeps shards of its own under its own app (`_system`), served by a system app built into the node. **A system kind may be late, never wrong**, so nothing a shard needs to activate can be a system kind. A system kind behaves as an app kind wherever practical: declared in code with the same settings and defaults, the list the same on every node of a version, and not public. The claimer keeps each fixed id claimed, the claim rule idempotent so one node ends up with each, and activation applies the kind's migrations and seeds a reminder due now (again if an outage left it stale). Fixed ids never idle and set no `next_reminder`, and a node gives up a shard a newer node has migrated unchanged.

| Kind | Ids | What it does |
|---|---|---|
| `_system.sweep` | `_cluster` | deletes the LTX files no manifest lists (see [Garbage](#garbage)) |
| `_system.dispatch` | `_cluster` | wakes the shards whose reminders are due while they are inactive |

Both are the pattern: one node per id, brought back after a crash or a move, state that moves with the owner, its reminder the timer, the lease checked before a delete or a send, and every step safe to redo.

`_system.dispatch` lists the reminder index up to the current minute and sends each due shard a **dispatch**, a call that makes the shard active on its owner or claims it on a node picked for load. A pass lists with no cursor, so a cold start, a move and catching up after an outage are the same case. Per entry it prunes by the rule above, or dispatches the backed entry and moves it forward whatever the answer, so the shard stays stateless. The receiver never creates a shard, and one another live node owns answers `421` naming it.

## Lifecycle

### Activation

1. **Owned by another node with a valid lease:** forward the request there.
2. **Cached here:** reopen the files and start capturing, with no blob request.
3. **Unowned, or claimable after its owner's adoption:** claim by CAS and restore. A manifest with nothing to restore starts over in a new generation.
4. **No manifest:** a call creates it with a conditional create, which is the claim. A client request never creates a shard, so random ids from clients cost a manifest read and leave nothing behind.

Placement prefers the node that received the request, and concurrent requests share one activation, run by the shard's own loop. A shard deactivates after its kind's idle timeout unless a request, connection or due reminder holds it, its files staying in the cache, and a shard holding nothing of the app's (never written, emptied, or carrying the delete marker) is deleted instead.

### Letting go of a shard

Deactivating, evicting or deleting a shard is only safe once the app has closed it, since a connection could still write to a file nobody captures. The node asks SQLite's own lock: every connection holds a shared lock on byte 128 of the `-shm` file, tested with `F_GETLK` as SQLite does, after the node has closed every handle of its own (a process's `fcntl` locks drop with any descriptor, so it opens a shard's files only through capture), comparing the wal-index header before and after to catch a commit made just before the app closed.

If another process still has the shard open, capture resumes and the node doesn't let go: an idle deactivation tries again later and an eviction or delete is refused. A resumed release continues where capture left off when the WAL is the same one; if the app checkpointed and restarted the WAL instead, the uncaptured frames are gone, so the node writes the whole database as one file counted as one commit.

### Routing and forwarding

- **Forwards.** A request for a shard owned elsewhere goes to the owner's peer listener, never behind the load balancer, so node-only headers are trusted there, and shardless requests are never forwarded. The owner serves a forward as the original, carrying the origin, caller and hops. Gates stay where the writes are: the caller's on the caller's node, the callee's the owner's.
- **A forward is never forwarded again:** an owner that finds the shard isn't its own answers `421` naming the owner it found, and the forwarding node corrects its cache and answers `503` with `Retry-After: 0`. The node never retries a request itself, keeping no bodies, so two stale nodes can't bounce one, and a forward failing after the owner acknowledged it is a `502`, since the app may have seen it.
- **Relays** have their own origin, `relay`, and are served as the client's request they are (see the [guide](guide.md#serving)). **Dispatches** are the only requests a node makes of its own accord.
- **Finding the owner:** a bounded LRU hint, filled when a claim finds another owner and emptied by a `421`, by this node's own claim and by the lease scan finding a node dead. A stale entry costs one `503`, a miss one manifest read.
- **Fail fast:** the owner's node acknowledges every forward with `102 Processing` before its app sees anything, so a forward without an answer within a second never reached the app, and the forwarding node reads the owner's lease and answers `503` at once: retry in a second while the lease is valid, after the expiry until adoption, at once once adopted, when the next attempt claims the shard.

### Reminders

The node knows nothing of what a reminder means: it reads one number from the shard's table and calls the app. While the shard is active its loop wakes the app (`POST /_shardlite/reminders`, in process, never forwarded) once the earliest due reminder is due and what the read saw is durable by the gate's rule, one wake at a time under a node-wide limit. While it is not, its index entry wakes it through `_system.dispatch`. A reminder is always covered: in the tail until the next delta (adoption creates an entry due now for every tail it writes a delta for), and in the entry created before that delta afterwards. The `next_reminder` recorded with a delta must match the state the delta's files reach: anything committed beyond what was read means recording now instead, since it may have removed work the files hold. The node writes only the reserved table, at activation, and never a reminder.

## Concurrency

How the node's goroutines are owned, how they shut down and how they wait for each other. The per-part detail lives in `internal/node`; these are the rules it follows.

- **Every goroutine has an owner that joins it,** up to `Run`, which returns only after everything it started has ended. The long-lived parts are services (`internal/service`) run in groups, stopped by whoever started them, and one that returns before it is stopped has failed. Nothing restarts. Fork-join and the goroutines a service owns use `errgroup`, and the node has no bare `go` statement.
- **A goroutine waits only for goroutines it started, for its own I/O and for node-wide loops that never wait for their callers**, so waits can't form a loop: nothing `Catalog` depends on (the writer, the keeper, blob storage) waits for `Catalog`.
- **Callbacks from libraries never block on anything the node drives.** A callback runs to the end under the lock guarding the state it touches, and whoever started the work waits for durability afterwards.
- **The capture lock** guards a captured shard's WAL state, transactions, wal-index mapping and tail. Its holder runs to the end, so a sync is never cut off. No other lock nests, is held across I/O or a wait on another goroutine, or stands in for ownership. Semaphores are channels.

`Run` runs four stages, `front`, `callers`, `shards` and `journal`, stopped one at a time in that order: `callers` outlives `front` because a request holds a reference to its shard, everything is joined before `shards` lets go of its shards, and `journal` stops last since the shards write through the writer and need the lease. `journal` starts as soon as the lease is taken, so the lease is renewed while the rest is built, and every listener is bound before the other stages.

### Shutdown

`front` then `callers` stop on the context ending, a service failing, the writer fenced or the lease lost: each server stops accepting, waits up to 10 seconds for requests in flight and joins its handlers. `shards` stops its loops, then lets go of every shard (256 at once, transient storage errors retried until a 30-second deadline), each final delta recording `next_reminder` with its entry created first; if the lease is gone or the writer is fenced, nothing is let go. `journal` stops last, and `Run` releases the lease only if every shard was let go and nothing is wrong. Each stop has its own deadline of at most 35 s, so a stuck part never holds up the rest.

### Failures

`Node.Err` is nil while the lease is good and the writer isn't fenced, otherwise it is the reason. A panic crashes the node and adoption recovers its shards on the next start. The lease lost or the writer fenced stops at once, letting nothing go and keeping the lease. Any other service failing stops the node gracefully with its error. A listener that can't bind or shards that fail to build make `Run` return the error. Nothing restarts; a crashed shard that costs only that shard is on the [roadmap](roadmap.md).

## Garbage

Some writes leave LTX files no manifest lists: a rebase or delta whose manifest update never lands, and a deleted shard's files when its node crashed first. The sweeper (`_system.sweep/_cluster`) deletes them, walking `data/shards/` in batches from a cursor kept in its shard: files no manifest lists, older than the journal's expiry (by then no upload can still be on its way to a manifest), the lease checked before each delete. Journal segments are each node's own to expire. Dead nodes' leases stay: a node of the same name starting again at epoch 1 would collide with its old files, and retiring leases is on the [roadmap](roadmap.md).

## The models

Each model in `specs/` is about one part of the code and assumes what the others check.

- `shard.qnt` is one shard: its file, its journal and when a reply may go out.
- `cluster.qnt` is ownership and adoption: leases, journals, fences, manifests and the claim rule, with storage alone as the truth.
- `dispatch.qnt` is the [reminder index](#reminder-index). It assumes the cluster: a crash leaves a tail to an adopter, and an adoption is an entry and then a delta.
- `capture.qnt` is the [read rule](#the-read-rule). It assumes SQLite: the WAL restarts only when every frame is copied into the database file, and a checkpoint copies a frame only past every reader's mark.

Only capture's traces are replayed against the code: `internal/capture/replay_test.go` plays each trace in `internal/capture/testdata/traces/` (made by `quint run --mbt --out-itf`, the command beside them) step by step against a real SQLite file and compares what capture promises, not the model's variables. The traces are committed so that `make test` needs no Quint. The cluster's models have no such seam in the code, which is spread over several packages.

## Testing

- **Fault injection** kills and fails the real `shardlite` binary, built with the `fault` tag (`e2e`, `fault_*_test.go`). The tag adds settings that exist in no other build (`SHARDLITE_KILL_AT`, `SHARDLITE_KILL_WHEN`, `SHARDLITE_STORAGE_FAULT`, `SHARDLITE_TEMP_FAULT`, `SHARDLITE_MAX_FILES`; see `cmd/shardlite/fault.go`). A test arms a kill point (`internal/kill`) after the node is up, drives it through the e2e app and checks that every write a `storage` request acknowledged is in blob storage after a restore, which verifies every checksum, and that the database passes `integrity_check`. It covers crashes between every step of capture, snapshots and restore (in a random order too), apps that checkpoint themselves or reconnect all the time, and failing storage and temporary files.
- **Fencing** is tested by killing nodes and by taking a live node's lease the way an adopter does while the node hears of nothing: a `storage` read must then fail and the node must stop, and a `none` read answers (`internal/node`, `TestHiddenPauseFailsStorageReads`). The clock check is tested with a proxy that moves the `Date` header of the store's responses (`internal/s3test`, `Skew`). Pausing and partitioning real processes is on the [roadmap](roadmap.md).
- **The protocol** is checked in Quint before it is implemented, and capture's model is replayed against the code. **Time** (lease expiry, timeouts) runs on `testing/synctest`'s fake clock.

## The code

| Package | What it is |
|---|---|
| `internal/pos`, `internal/shard`, `internal/cluster` | named types for positions, epochs and segments; for shard, app, kind and node names, with the name syntax |
| `internal/coord` | node leases: their renewal, the clock watch and the confirmer that reads the node's own for the gates |
| `internal/blob`, `internal/blobtest` | object storage, the write ids of [Lost responses](#lost-responses) and the startup check; its fault decorator |
| `internal/journal`, `internal/segment` | the journal's store and conformance suite; segment bodies, footers and the writer |
| `internal/capture` | reading a shard's WAL, the [read rule](#the-read-rule) and the sidecar invariant |
| `internal/shardfile`, `internal/remindex` | the manifest, deltas, snapshots, restores and adoption; the reminder index's keys and [pruning rule](#reminder-index) |
| `internal/system` | the system kinds |
| `internal/service` | the services each stage runs in |
| `internal/node` | `Run` and everything it stages: catalog, actor, flusher, gates, routing, reminders, servers |
| `internal/kill` | the fault rig's kill points |
| `internal/harness` | the storage pieces tests share |
| `specs/` | the Quint models the protocol is written and checked in |
