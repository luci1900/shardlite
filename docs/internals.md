# shardlite internals

How the node works: its architecture and the rules that keep it correct. The app-facing behaviour is in the [guide](guide.md), and the protocol's models are in [specs/](../specs/).

## Terms

| Term | Meaning |
|---|---|
| **Node lease** | An object each node holds and renews. Every ownership depends on it. |
| **Node journal** | Numbered segments, one per flush interval, holding the node's shards' commits since their last snapshots: the shared WAL. |
| **Snapshot** | A shard as of one commit, as immutable LTX files: a base and deltas. An inactive shard lives only in its snapshot. |
| **Manifest** | A shard's record: owner and epoch, files, the journal holding later commits, when it next needs waking. |
| **Reminder index** | Empty objects saying a shard may need waking at a minute. `_system.dispatch` reads them. |

The other terms are in the [overview](overview.md#concepts).

## The shape of a node

The node is one Go process. It reads each shard's WAL itself and encodes what it reads as LTX files ([LTX](https://github.com/superfly/ltx)). Capture is driven, never polled: the flush loop spots commits by mapping each active shard's wal-index header, which SQLite updates on every commit, so a sync costs microseconds. Nothing shard-sized lives in memory; restores, rebases and large deltas go through `{data}/.tmp`.

Blob storage is the system of record: manifests, LTX files, one journal per node, the reminder index. The node's disk holds the working copy of each active shard and a cache of recently used ones: a cached shard keeps its owner while the lease holds, so reactivating it costs no blob request, and when disk fills the least recently used are evicted, the manifest released by CAS and the files dropped.

Leases and manifests are objects changed with conditional writes, which are linearizable on every supported store, so there is no leader, no election and no set of stable nodes. A store must support create if absent, conditional update, conditional delete, get, head, delete and listing (a listing may lag, since listings only trigger checks). Every write carries a CRC32 the store verifies and every read checks the checksum the store returns, so corruption fails the read that finds it. Callers go through `blob.Store`, which makes writes safe to retry (see [Lost responses](#lost-responses)). ETags are opaque, and stores differ on whether identical bytes give the same one. The node checks its store's conditional writes on a scratch key at startup and refuses to start if they misbehave.

Only two things live outside shards: a lease (which node is alive, in which epoch) and a journal (commits durable before any file holds them), neither of which can live inside a database whose owner they decide. Everything else is a **shard**: a database stored the same way (a manifest naming LTX files, a tail in its owner's journal), with a kind, the claim rule and a lifecycle. The test for anything new: state shared across nodes goes in a shard, an entry written while no owner can be relied on is an index object (see [The reminder index](#reminder-index)), and only what a shard needs to exist is a primitive.

## Failing loudly

The dangerous failure is not the crash, which is loud and costs a restart, but the wrong answer nothing reports: a segment with the right size and wrong bytes, an acknowledgement for a write that is not there. Two rules decide much of the node's shape: every mechanism must report its own failure, and every mechanism we write ourselves must be one the fault rig can make fail. The formats the node leans on carry their own checks, so what is left is where the rig has to reach: capture's read of a shard's WAL, the writer's landing of a segment in the record, the applier's write of a page, the local store's commit, each checked by killing a node mid-step.

A closed set answers for its zero value under one of two policies: either the zero is an inherently safe member that causes no action (a position of 0 is nothing yet, no store, nothing written), or it is a named `Unknown` that `Valid()` rejects and every switch must handle, so it cannot land in a default written for another reason. A zero that is the first real state, or one that enables an action, is not allowed, and a decoder rejects an unknown value rather than mapping it onto the zero, so a newer node's state fails loudly instead of being downgraded. Each package that owns a closed set pins the policy in a zero-value test of its own, hand-written because Go cannot enumerate a type's constants at runtime, and the linter's `mapliteral` mode fails an enum-keyed table that is missing a member.

## Ownership and fencing

A lease alone can't prevent two writers (a pause, a delay), so the storage must reject a stale owner's writes, as in Kleppmann's [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html). Here that is the journals (one writer each) with the manifests, specified in Quint.

### Node leases

Each node holds one lease, renewed by CAS every few seconds, carrying its epoch, address, whether it serves and the range of its journal still needed. A shard's ownership is valid only while its owner's lease is, so one expired lease releases all of a crashed node's shards.

The lease also names the node's mirrors: the nodes whose disks hold a copy of its journal (see [Node journals and snapshots](#node-journals-and-snapshots)). They are chosen once, when the node takes its lease, from the listing of leases: live nodes that can hold a journal (a persistent local database), least loaded first, the node itself excluded. The set is sticky: repair changes it, renewals republish it unchanged, and nothing in correctness depends on who the mirrors are, so the relation needs no symmetry and no coordination. A start that cannot read the listing names none, and the acknowledgements fall back to the record until a repair names replacements.

- **Elapsed time, not timestamps.** The holder treats its lease as expired at the start of its last successful renewal plus the TTL less a fifth, by its own clocks; another node, once it has seen the lease unchanged for a TTL on its own monotonic clock. Leases need clock rates that agree, not synchronised clocks, and a node that loses its lease exits and restarts as from a crash.
- **Noticing dead nodes.** Nodes list `state/leases/` in turn, so the cluster lists about every 2 seconds. A listing's last-modified times are a hint: seeing an old one, the node reads the lease, looks again a TTL later and adopts only if it is unchanged.
- **Checking an owner.** A node forwarding a request reads the owner's lease, and again when the owner doesn't answer, to choose between failing fast and claiming (see [Routing and forwarding](#routing-and-forwarding)).

### Manifest

A manifest is created with a conditional create and changed only by CAS, at ownership changes and snapshots, never on writes or activating an owned shard. Its fields and key are documented by `internal/shardfile/manifest.go`.

- **`next_reminder`** is set by the owner's deltas, rebases and deactivations, its entry in the reminder index created first. Claims, releases and evictions keep it, adoption's deltas set it to now, a new or restarted shard has none, and system shards never set it.
- **`rev`** goes up on every write, so no two versions share bytes and so the same ETag on a store hashing bodies: a manifest released and claimed again by the same node would otherwise return to an earlier version, which the pruning rule reads differently (see [The reminder index](#reminder-index)).
- Ownership is invalid if `node_epoch` doesn't match the owner's current lease or that lease has expired, and a shard deleted and created again gets a new `generation`, so it never reads the old shard's files.

### Fencing

- Each journal has one writer. Segments are numbered and created once, and a writer finding its number taken has been fenced and stops for good.
- An adopter fences a dead node's journal with an empty segment at the first free number after the lease's recorded range; a zombie can then only write below the fence, where nothing is part of any shard's history. The fence goes above what the mirrors were fetched (see [Takeover](#takeover)): a fence written first would strand what a healthy mirror holds below it.
- A zombie's LTX files count once a manifest CAS adopts them, and a stale owner's CAS fails once anyone else has changed the manifest. A delta it adds before then may hold commits it never acknowledged, which keeps the history consistent since nobody saw them acknowledged.
- This needs no listings: segments are found by number, and single-object reads are strongly consistent on every store with conditional writes.

### Consistency

Per shard, with sync on, every read and write is linearizable (see the [guide](guide.md#consistency)). Writes: one owner per shard, SQLite's commit order, an acknowledgement waiting for the commit to reach the journal, and a zombie's acknowledged commits below the fence its adopter reads. Reads: every gate checks the node's lease before output leaves, and claiming a dead owner's shard needs an adopter that watched the lease stay unchanged for a TTL and fenced the journal. So clock rates must agree within the lease's margin (20%), and no pause may hide from the machine's own clocks. A read followed by a commit needs no clock assumption: its answer waits for the commit's segment, which a fenced node can't write.

### Lost responses

Any write can succeed while its response is lost. `blob.Store` stores a write id with each object and, when a retry meets "exists" or "conflict", reads it back to recognise its own attempt, and `journal.Store` gives the journals the same, so a caller sees success, a conflict, or an unknown outcome. Two handle unknown outcomes themselves: the segment writer retries the same segment until the outcome is known, and a manifest handle compares the stored manifest with the one it tried. The node never retries a request the app may have seen.

### Takeover

- **Eviction** (a full cache, rebalancing, draining): the owner deactivates the shard if it is active, releases its manifest by CAS and drops its files, and the next node to need it claims and restores. Tail and release are two writes, so an unknown release counts as landed without resuming capture on a maybe-unowned shard, and `next_reminder` and its entry stay for `_system.dispatch` to wake it.
- **Crash.** A node whose lease has expired (as [Node leases](#node-leases) decides) is adopted by the first live node to notice, every step a conditional write safe to redo, so a half-finished adoption is replaced: **Mark** the lease as being adopted by CAS, **fetch** the mirrors the lease names, so the segments they hold above the run the record landed are copied into it before anything is fenced (checked as the bytes leave, reusing the write id the mirror stored, so the record sees the owner's own attempt; a mirror that does not answer within a short timeout is given up on, and the window it held is the loss the ensemble states rather than prevents), **fence** the dead journal, **read** the footers up to the fence, then per shard still naming the dead journal create a reminder entry due now keyed by the manifest version the delta replaces and add the tail as a delta by CAS with `next_reminder` now and the journal cleared, **finish** by CAS on the lease, adopted to the dead epoch with the journal range past the fence, and **sweep** the segments below the fence, fences staying.
- **Restart** is treated as a crash, one recovery path: the node takes a new epoch, adopts its own journal before serving anything, renewing as it goes, and starts its writer after the fence on an empty cache, a crash midway having left only the fence for the next life to adopt again. It lands its own tail from its local store before the fence, and asks the mirrors its previous life named beside them, the fetch a peer's adoption runs. While another node is adopting it and looks alive, the restarting node waits, and a released or already adopted past life only needs the fence to find where its journal starts.
- **Drain** (scaling down) fails readiness, refuses claims (`503` with `Retry-After: 0`) and stops, evicting each shard as it lets go rather than caching it; a shard the app holds open waits for adoption.
- **Stop** is the same final state: every shard let go, each final delta recording `next_reminder` with its entry, the lease released, and the node listed as alive until its lease is a TTL old, so a dispatch to it fails and goes elsewhere, which claims the shard because the released lease allows it: its reminders are not late by a TTL.

The entry comes before the delta because journal expiry deletes covered segments without checking any lease: once the delta is in, a zombie may delete the tail's segments, and an adopter dying between the two leaves a shard nobody knows to wake (`noAdoptionEntry` in `specs/dispatch.qnt`). A move copies nothing between journals. The dead node's shards keep it as their owner until nothing of them can be in its journal (the manifest names no journal, or the node adopted up to the owner's epoch, a release counting): until then they can't be claimed, and since no timestamp decides it, a lease that looks expired still owns its shards. Writes that hadn't reached the record are lost, except for kinds with sync on.

## Capture and snapshots

A delta holds only what was captured since the last snapshot; with no database to read from, a delta is the whole of the shard, every page written as one file counted as one commit. Deactivation closes capture.

### The read rule

While a shard is captured, the node holds one read transaction begun under the write lock right after a sync and a checkpoint that copied everything, so it gets read mark 0. SQLite then copies no frame into the database file and never restarts the WAL while a frame is waiting to be copied, the only restart it allows being at the app's first write after the node's own checkpoint, when every frame of the old WAL is captured and in the database file. So no uncaptured frame is ever copied or lost.

- **The locked restart.** While the transaction is held the WAL only grows, so past a threshold of frames the node takes the write lock, syncs, ends the transaction, has everything copied in (a passive checkpoint) and begins a new one (mark 0). It must be passive, since a truncating checkpoint can't run under the node's own lock and frames committed in the gap would be wiped uncaptured.
- **The app's checkpoints do nothing while a shard is captured.** The node holds mark 0, so a passive checkpoint copies nothing and a restart or truncating one answers `SQLITE_BUSY`.

`specs/capture.qnt` checks the rule: every committed frame is in a captured file before it can leave the WAL (`noLoss`), the files replayed give the database at the captured end (`replay`), and a snapshot gives the database at the position it began (`snapOk`).

### Checksums

Every LTX file carries the database's checksum before it and after it: LTX's whole-database checksum, the XOR of each page's CRC with its number. Capture keeps every page's CRC in memory and updates it from the pages each sync already holds, so the checksums cost one CRC per changed page. Deltas check the chain, each tail file's checksum before it being the one before's after it, a break failing the delta since a file that doesn't follow the one before is a bug in what was written (files from before the checksums existed are applied unchecked). Rebases compare capture's checksum with the snapshot's: a mismatch means capture went wrong, and the node logs the shard and both checksums, counts it, writes the rebase anyway (it is the database as SQLite reads it) and carries on from the snapshot's CRCs. Restores verify every file, the applier checking each before against the database it has built and its after against the file's: the first mismatch fails the restore with the file named, and the shard is not served, nothing deleted, so an operator has every file.

### Node journals and snapshots

The same split as a database's WAL and data files: the journal the shared WAL of a node's shards, the LTX files each shard's data files, a snapshot the checkpoint.

The journal's store is `internal/journal`: numbered segments created once, durable before the create returns, read by number (a head of size 0 is a fence). Two backends, one conformance suite: `journal.Blob` in the record, whose conditional creates and listings [Fencing](#fencing) leans on, and `journal.Local` in the node's own database, whose durability the `local` tier acknowledges. Every create carries the write-id handling of [Lost responses](#lost-responses), so a lost response costs a retry, never a fence. The fence is created in the record, any other home holding a copy of what is below it.

- **The mirrors.** `SHARDLITE_STANDBYS` names how many mirrors a node's journal has, 0 or 1 for now: other nodes' disks holding a copy, over the mirror protocol on the peer listener, where forwards and dispatches live. Five requests, the journal's interface as plain HTTP: a create is a `PUT` with `If-None-Match: *` at `/_shardlite/journal/{node}/{seq}`, the write id in a header; a number already taken answers `412` with the write id it holds, `409` a racing conditional write, `404` a missing segment; a `GET` with a `Range` header reads a range, a `HEAD` answers the size and the write id, `GET ?from=N` lists the numbers held, and `DELETE` takes one away. The statuses are the blob backend's, so `journal.Store` reads back and retries against a mirror in exactly the code it uses against a bucket. The client (`journal.Mirror`) and the handler (`journal.NewMirrorHandler`) are the protocol, in `internal/journal`, and the handler answers only after its store has committed the segment, so a 2xx means the bytes are on that node's disk. A create whose write id names a strictly older epoch of the writer's lease than the mirror's view of it is refused: a stale life of a node name is a fence to the mirror, not a duplicate. The view is the lease scan's listing and a read behind it, a lease read when the listing shows one the view has not seen, so the reads are a few per renewal; a view that is expired, or not yet caught up with a renewal, never refuses. A mirror holds the segments in its local database beside its own journal's, and holds no fences: the fence is created in the record alone.

- **Segments.** Every flush interval one segment, a footer naming its shards, the body composed from the LTX files capture queued (`internal/segment/body.go`) so the bytes are written once per store, the record's and the local's writes parallel and the local copy dropped once the record has the segment. Only the record's store fences, the local accepting a number somebody else created as somebody else's, since above the record's fence the segments are already history.
- **The node's own database.** The journal's segments and the index's rows share one SQLite file in the data directory (`internal/localdb`), with `synchronous=FULL`, so what the `local` tier acknowledges is on disk when it answers. The two tables have two fates: the index's rows are cleared at every start (the shard files they pointed at are deleted then too) and the journal's rows are kept, since they hold acknowledged writes. The schema is therefore `user_version` and numbered migration files applied inside one immediate transaction at the first open (`internal/migrate`), so a change to it is a migration rather than a deletion, and the chunk size is part of the format the segments are stored in, which makes changing it a migration too. A database newer than the node's files is left alone: this node cannot read a newer node's chunks, so the local store is treated as empty, the tier's answers fall back to the record and the file keeps whatever tail a newer node left.
- **Snapshots as deltas.** When a shard deactivates, or its tail is old or large, the node writes the pages captured since the last snapshot as one delta and adds it by CAS, without reading the database file, so a cached shard never has a tail and the lease's journal range only covers active shards.
- **Rebasing.** Once a manifest lists many files, the next snapshot is a full copy of the live database as a new base, bounding how many files a restore reads. A few run at once per node.
- **Cleaning the journal.** Each node deletes its own non-empty segments once a day old, and an alert comes long before if a tail is old, since tails become deltas within minutes and nothing tracks which segments are in use. Fences are never deleted, since a paused zombie could then write into a freed number.
- **Restore** downloads the files in parallel, verifies and applies each, then applies the tail, if any. A restore failing its checksums leaves the shard unserved and keeps every file.

## Durability gates

- A chunk the app wrote is held until a flush that started after it was read has captured it and the store its tier names has what the flush queued, since every flush captures every active shard's commits up to its start.
- The tiers name the place they wait in: `local`, the node's own database, or `storage`, the record, which is the default. A tier's wait ends when its store's counter reaches what the flush queued, so `storage` waits for the PUT and `local` only for capturing.
- Every chunk first needs the node's lease to be good, and a flush that completed after the lease ran out acknowledges nothing. Chunks waiting together share one flush (group commit).
- A call to another shard is bytes leaving the caller, so it waits for the caller's commits to meet its kind's tier. A kind with no local store is waited on in the record whatever its tier says, where the honest answer is.

## Data and state

| Role | Objects | Changes by | Rule |
|---|---|---|---|
| **Data** | journal segments, LTX files | create once, delete by garbage collection | never modified |
| **State** | leases, manifests | compare-and-swap by the owner | the truth, naming the data it points at |
| **Index** | reminder index entries | created by anyone before the state change they are for, moved and deleted by the dispatcher | deleted only when the manifest doesn't back it |

Journal segments are written to the record and to the node's tier database, each copy once: the tier's copy is what the `local` tier acknowledges and what a restart reads, the record's is the only one anything else reads, and the fence sits in the record, so nothing the tier holds is above it. Listings stay per prefix (leases, a node's journal, the index up to the current minute, the sweeper's `data/shards/`), so none grows with the rest.

### Reminder index

An inactive shard's reminder needs someone to wake it. `next_reminder` in the manifest says when, and the index says which manifests to look at when: an entry is an empty object anyone can create, needing no owner and no clock, read only by `_system.dispatch` (see [System kinds](#system-kinds)), a minute being enough since a shard's own loop fires at the second once it is active. An entry's key names the UTC minute, a shard hash prefix and the manifest version the entry backs, and the exact key is in `internal/remindex`.

- **Index first.** Before a manifest write that sets `next_reminder` to a new minute (a delta, rebase, deactivation or adoption's delta), the writer creates the entry for that minute and its base, a failed create failing the write: a delta's timer tries again, a deactivation keeps the shard active. A write whose reminder is in the minute the manifest already has creates none, since the entry backing the old one backs the new one.
- **Each write has its own entry.** An entry under another base never counts: two writes sharing one would let the dispatcher prune it on the first's terms while the second still relies on it.
- **The pruning rule.** An entry may be deleted when the manifest doesn't back it (absent, or `next_reminder` absent or after the entry's minute) and the manifest's current version is not the entry's base: the write the entry was made for replaces that version, so once the manifest has moved off it, the write has landed or can never land, while the version is still the base, the write may be in flight. Pruning by comparing entries with manifests alone would delete those in flight.
- **A backed entry is never deleted.** It is dispatched and moved forward, which covers a shard active after a dispatch whose node then crashes with nothing of it in the journal: adoption finds no tail, so its entry still wakes it.
- **Anyone may apply the rule.** It reads only the entry and the manifest, so a second dispatcher, a zombie or an operator's tool can't make it unsafe. One dispatcher is for cost, not correctness.
- **Moving forward.** The dispatcher creates `{base}.{n+1}` at a later minute and deletes the old one, so a crash between the two leaves a duplicate, which costs one more dispatch.

`specs/dispatch.qnt` checks that a shard with a reminder in its durable state always has a backed entry or a tail an adoption will still write a delta for.

## System kinds

Some of the node's own work needs exactly one runner, and somewhere to keep state. A shard already has both, so the node keeps shards of its own under its own app (`_system`), served by a system app built into the node: **a system kind may be late, never wrong**, so nothing a shard needs to activate can be a system kind. A system kind behaves as an app kind wherever practical: declared in code like an app's ready answer, with the same settings and defaults and the list the same on every node of a version, an app's name starting with `_` reserved so none can collide and adding one in a rolling deploy safe. Its app is an `http.Handler` per shard, routed and gated like an app's calls but not public (calls reach it, clients don't), its routes under `/_shardlite/`, which every listener refuses to anything but the node. The claimer keeps each fixed id claimed, the claim rule idempotent so one node ends up with each, and activation applies the kind's migrations and seeds a reminder due now, seeding it again if an outage left it stale, which would otherwise end the shard's runs for good. Fixed ids never idle and set no `next_reminder`; a system shard's `user_version` is the kind's migration version, and a node gives up a shard a newer node has migrated unchanged.

| Kind | Ids | What it does |
|---|---|---|
| `_system.sweep` | `_cluster` | deletes the LTX files no manifest lists (see [Garbage](#garbage)) |
| `_system.dispatch` | `_cluster` | wakes the shards whose reminders are due while they are inactive |

Both kinds are the pattern: one node per id, brought back after a crash or a move, state that moves with the owner (the sweeper's cursor), its reminder the timer, the fence against a zombie, the lease checked before a delete or a send, and every step safe to redo.

`_system.dispatch/_cluster` lists the reminder index up to the current minute and sends each due shard a **dispatch**: a call that makes the shard active on its owner, or claims it on a node picked for load (see [Reminders](#reminders)). Its shard holds only its run reminder and has sync off, so one owner's death leaves reminders late by a lease TTL and a claim, which minute resolution allows. A pass lists the index with no cursor, processed entries leaving the head, so a cold start, a move and catching up after an outage are the same case, and per entry it reads the manifest, prunes by the rule above, or dispatches the backed entry and moves it forward whatever the answer, the backoff carried as the entry's next minute so the shard stays stateless. The call is `POST /_shardlite/dispatch` on the peer listener, or in process: the receiver acquires and never creates, active here answers at once, cached here reactivates, unowned or claimable is claimed and restored, and a shard another live node owns answers `421` naming it. It goes to the owner while the lease scan's last listing has it alive, otherwise to the better of two live nodes picked at random, once more on another if the first refuses, and when the work outgrows one node it splits by id (`_0` to `_n`), needing no migration since the dispatch shard holds nothing.

## Lifecycle

### Activation

1. **Owned by another node with a valid lease:** forward the request there.
2. **Cached here:** reopen the files and start capturing, with no blob request.
3. **Unowned, or claimable after its owner's adoption:** claim by CAS and restore. A manifest with nothing to restore starts over in a new generation.
4. **No manifest:** a call creates it with a conditional create, which is the claim. A client request never creates a shard (it gets `404`), so random ids from clients cost a manifest read and leave nothing behind.

Placement prefers the node that received the request, and concurrent requests share one activation, run by the shard's own loop, so one caller giving up fails nobody else. A shard deactivates after its kind's idle timeout unless a request, connection or due reminder holds it, files staying in the cache, and a shard holding nothing of the app's at deactivation (never written, emptied, or carrying the delete marker) is deleted instead: manifest by conditional delete, then files. Nothing can delete a live shard from outside.

### Letting go of a shard

Deactivating, evicting or deleting a shard is only safe once the app has closed it, since a connection could still write to a file nobody captures. The node asks SQLite's own lock: every connection holds a shared lock on byte 128 of the `-shm` file, tested with `F_GETLK` as SQLite does, after the node has closed every handle of its own (a process's `fcntl` locks drop with any descriptor, so it opens a shard's files only through capture otherwise), comparing the wal-index header before and after to catch a commit made just before the app closed.

If another process still has the shard open, capture resumes and the node doesn't let go: an idle deactivation tries again later, an eviction or delete is refused, and the same check before activating a cached shard catches apps opening files they weren't given. A resumed release continues where capture left off when the WAL is the same one and holds at least what was captured; if the app checkpointed and restarted the WAL instead, the uncaptured frames are gone, so the node writes the whole database as one file counted as one commit. Either way it ends the read transaction, has everything copied in and begins a new one before letting go.

### Routing and forwarding

- **Forwards.** A request for a shard owned elsewhere goes to the owner's peer listener, never behind the load balancer, so node-only headers are trusted there, and shardless requests are never forwarded.
- **The owner serves a forward as the original,** carrying the origin, caller and hops, so the owner applies the same rules and the app sees the same headers either way.
- **Gates stay where the writes are:** the caller's on the caller's node, applied to the body the forward sends, the callee's the owner's.
- **A forward is never forwarded again:** an owner that finds the shard isn't its own answers `421` naming the owner it found, and the forwarding node corrects its cache and answers `503` with `Retry-After: 0`; the node never retries a request itself, keeping no bodies, so two stale nodes can't bounce one, and a forward failing after the owner acknowledged it is a `502`, since the app may have seen it.
- **Dispatches** are the only requests a node makes of its own accord (see [System kinds](#system-kinds)).
- **Finding the owner:** a bounded LRU, filled when a claim finds another owner and emptied by a `421`, by this node's own claim and by the lease scan finding a node dead. A hint: a stale entry costs one `503`, a miss one manifest read and no activation.
- **Fail fast:** the owner's node acknowledges every forward with `102 Processing` before its app sees anything, so a forward without an answer within a second never reached the app, and the forwarding node answers `503` at once — retry in a second while the lease is valid, after the expiry until adoption, at once once adopted, when the next attempt claims the shard.

### Reminders

How the node wakes shards for the [guide](guide.md#reminders) reminders. The node knows nothing of what a reminder means: it reads one number from the shard's table and calls the app.

- **While active**, the shard's loop, after each flush that captured something, reads the earliest due reminder that isn't stale (a day old) and wakes the app (`POST /_shardlite/reminders`, in process, never forwarded) once it is due and what the read saw is durable by the gate's rule, one wake at a time under a node-wide limit. A reminder due keeps the shard active.
- **While not active,** its index entry wakes it through `_system.dispatch`, within a few seconds when the cluster is healthy, later when a minute has many entries.
- **Always covered:** a reminder committed while active is in the tail until the next delta (adoption creates an entry due now for every tail it writes a delta for), and in the entry created before that delta afterwards.
- **The `next_reminder` recorded with a delta must match the state the delta's files reach.** Read between two syncs, anything committed beyond what was read means recording now instead: it may have removed work the files hold, and losing the recording would bring that work back.
- **The node writes** only the reserved table (and a system shard's schema and first reminder), at activation, before the app has the shard, and never a reminder.

## Concurrency

How the node's goroutines are owned, how they shut down and how they wait for each other. The per-part detail lives in `internal/node`; these are the rules it follows.

- **Every goroutine has an owner that joins it,** up to `Run`, which returns only after everything it started has ended. The long-lived parts are services (`internal/service`) run in groups, stopped by whoever started them, and one that returns before it is stopped has failed. Nothing restarts. Fork-join and the goroutines a service owns use `errgroup`, and the node has no bare `go` statement.
- **A goroutine waits only for goroutines it started, for its own I/O and for node-wide loops that never wait for their callers**, so waits can't form a loop. Every other wait is to a goroutine that never waits back: `Catalog`'s never wait for requests, and nothing `Catalog` depends on (the writer, the keeper, blob storage) waits for `Catalog`.
- **Callbacks from libraries never block on anything the node drives.** A callback runs to the end under the lock guarding the state it touches, and whoever started the work waits for durability afterwards.

`Run` runs four stages, `front`, `callers`, `shards` and `journal`, stopped one at a time in that order (see [Shutdown](#shutdown)): `callers` outlives `front` because a request holds a reference to its shard, everything is joined before the `shards` stage lets go of its shards, and `journal` stops last since the shards write through the writer and need the lease. `journal` starts as soon as the lease is taken, so the lease is renewed while the rest is built, and every listener is bound before the other stages.

- **The capture lock** guards the captured shard's WAL state, transactions, wal-index mapping and tail: capture is state its callers take turns with, not a life of its own. Its holder runs to the end, so a sync is never cut off, others read the tail through an atomic, and the locked restart (see [The read rule](#the-read-rule)) sits under it.
- **An uploader per store** writes that store's segments in order, retrying a segment until its outcome is known and never dropping a queued one. The local's queue is unbounded, so a slow disk never holds the record's write, a local write given up after five failures being waited on in the record instead. Unlike the record's, the local never fences. The writer knows nothing of leases: the node acknowledges only through `Flusher.Wait`, which checks the keeper.

### Shutdown

1. **`front`** then **`callers`** stop on the context ending, a service failing, the writer fenced or the lease lost (see [Failures](#failures)): each server stops accepting, waits up to 10 seconds for requests in flight, ends upgraded connections and joins its handlers, so no request holds a shard any more.
2. **`shards`** stops its loops (lease scan, watchdog, cache loop, claimer), then lets go of every shard, 256 at once, transient storage errors retried until a 30-second deadline with the flusher running, every release's reminders loop stopped first and its final delta recording `next_reminder` with its entry created first. Then the table (ending the loops of shards that couldn't be let go), the flusher and the cache index close. If `Node.Err` says the lease is gone or the writer is fenced, nothing is let go and the stopping says why.
3. **`journal`** stops last, the writer failing whatever is still queued and the keeper.
4. **`Run` releases the lease**, deadlined on its own, if every shard was let go and nothing is wrong; otherwise it leaves it to expire.

Each stop waits at most 35 s, so a stuck part never holds up the rest, and the deadlines are separate, not one budget.

### Failures

The node asks one question about its journal: `Node.Err` is nil while the lease is good and the writer isn't fenced, otherwise it is the reason, and `Run`, `Catalog` and the lease decision ask it. A panic crashes the node (only `net/http` recovers one, in a handler) and adoption recovers its shards on the next start; the lease lost or the writer fenced stops at once in the order above, letting nothing go and keeping the lease; any other service failing stops the node gracefully with its error; a listener that can't bind or shards that fail to build make `Run` return the error, closing the listeners and stopping the journal. Nothing restarts; a crashed shard that costs only that shard is on the [roadmap](roadmap.md).

### Locks

No lock nests, is held across I/O or a wait on another goroutine, or stands in for ownership, with the capture lock as the exception (its holder takes the writer's mutex, nothing under that). A lock guarding a few fields for a few lines stays where the code without it wouldn't be simpler, and none is rebuilt from atomics (a reader count something waits on, a state machine packed into a word). Semaphores are channels, atomics carry values published once or written by one and read by many, and `sync.Once` runs a step one time.

## Garbage

Some writes leave LTX files no manifest lists: a rebase or delta whose manifest update never lands, and a deleted shard's files when its node crashed first. The sweeper (`_system.sweep/_cluster`) deletes them, walking `data/shards/` in batches from a cursor kept in its shard: files no manifest lists, older than the journal's expiry (by then no upload can still be on its way to a manifest), the lease checked before each delete and a file already gone counting as deleted, so a batch repeated after a move does no harm. Journal segments are each node's own to expire. Dead nodes' leases stay: a node of the same name starting again at epoch 1 would collide with its old files, and retiring leases is on the [roadmap](roadmap.md).

## Testing

- **Fault injection,** including apps that checkpoint themselves and crashes between every step of capture, snapshots and restore.
- **Fencing** is tested by killing, pausing and partitioning nodes. A paused owner is the classic zombie.
- **The protocol** is checked in Quint before it is implemented.
- **Time** (lease expiry, timeouts) runs on `testing/synctest`'s fake clock.

## The code

| Package | What it is |
|---|---|
| `internal/pos` | the positions of a shard, a lease's epochs and a journal's segments, as separate named types |
| `internal/shard` | the names of shards, apps, kinds, ids and generations, as separate named types, with the name syntax |
| `internal/cluster` | the name of a node, with the name syntax |
| `internal/coord` | node leases: their renewal and the read both the holder and the observers need |
| `internal/blob` | object storage, the write ids of [Lost responses](#lost-responses) and the startup check |
| `internal/blobtest` | the blob backend's fault decorator, for tests |
| `internal/journal` | the journal's store, its blob and local backends and one conformance suite |
| `internal/capture` | reading a shard's WAL, the [read rule](#the-read-rule) and the sidecar invariant |
| `internal/segment` | segment bodies, footers and the writer's counting |
| `internal/shardfile` | the manifest, deltas, snapshots, restores and adoption's shard-side writes |
| `internal/remindex` | the reminder index's keys and the [pruning rule](#reminder-index) |
| `internal/system` | the system kinds (`_system.sweep`, `_system.dispatch`) |
| `internal/localdb` | the node's own database behind `journal.Local` |
| `internal/service` | the services each stage runs in |
| `internal/node` | `Run` and everything it stages: catalog, actor, flusher, gates, routing, reminders, servers |
| `internal/kill` | the fault rig's kill points |
| `internal/testnode` | the capture side of the node as a separate process, so the rig can kill it |
| `internal/harness` | the storage pieces tests share: an in-memory bucket, a writer, files and test apps |
| `specs/` | the Quint models the protocol is written and checked in |
