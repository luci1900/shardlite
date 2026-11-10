# shardlite internals

How the node works: its architecture and the rules that keep it correct. It describes the overall shape and the details that matter, and leaves out what the code says plainly. The app-facing behaviour is in the [guide](guide.md), running a node is in [deployment](deployment.md), and the protocol's models are in [specs/](../specs/).

## Terms

| Term | Meaning |
|---|---|
| **Node lease** | An object each node holds and renews. Every ownership depends on it. |
| **Node journal** | Numbered segments, one per flush interval, holding the node's shards' commits since their last snapshots: the shared WAL. |
| **Snapshot** | A shard as of one commit, as immutable LTX files: a base and deltas. An inactive shard lives only in its snapshot. |
| **Manifest** | A shard's state in storage: owner and epoch, files, the journal holding later commits, when it next needs waking. |
| **Reminder index** | Empty objects saying a shard may need waking at a minute. The dispatcher reads them. |
| **Lane** | One of 16 hash classes of shard names. A shard's index entries, LTX files and manifest (`state/shards/{lane}/{kind}/{id}`) have keys that start with its lane, and each lane's index entries are handled by the node that ranks first for it. |

The other terms are in the [overview](overview.md#concepts).

## The shape of a node

The node is one Go process. It reads each shard's WAL itself and encodes what it reads as [LTX](https://github.com/superfly/ltx) files. Capture is driven, never polled: the flush loop spots commits by mapping each active shard's wal-index header, which SQLite updates on every commit. Nothing shard-sized lives in memory: tails, deltas, restores and rebases all go through unnamed files in `{data}/.tmp`.

Blob storage is the source of truth: manifests, LTX files, one journal per node, the reminder index. The node's disk holds only the working copy of each active shard, deleted when the shard is released, so an inactive shard is in storage and nowhere else.

Leases and manifests are objects changed with conditional writes, which are linearizable on every supported store, so there is no leader and no election. Every write carries a CRC32 the store verifies and every read checks the checksum the store returns. Callers go through `blob.Store`, which makes writes safe to retry (see [Lost responses](#lost-responses)), and the node refuses to start if its store's conditional writes misbehave on a scratch key.

Only a lease and a journal live outside shards, since neither can live inside a database whose owner they decide. Everything else is a **shard**. The test for anything new: state shared across nodes goes in a shard, an entry written while no owner can be relied on is an index object (see [Reminder index](#reminder-index)), and only what a shard needs to exist is a primitive.

## Failing loudly

The dangerous failure is not the crash, which is loud and costs a restart, but the wrong answer nothing reports: a segment with the right size and wrong bytes, an acknowledgement for a write that is not there. So every mechanism must report its own failure, and every mechanism we write ourselves must be one the fault rig can make fail (capture's read of a WAL, the temporary files, the writer's landing of a segment, the applier's write of a page), each checked by killing a node mid-step.

A closed set's zero value is either an inherently safe member that causes no action (a position of 0 is nothing yet) or a named `Unknown` that `Valid()` rejects. A decoder rejects an unknown value rather than mapping it onto the zero, so a newer node's state fails loudly instead of being downgraded.

## Ownership and fencing

A lease alone can't prevent two writers (a pause, a delay), so the storage must reject a stale owner's writes, as in Kleppmann's [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html). Here that is the journals (one writer each) with the manifests, specified in Quint.

### Node leases

Each node holds one lease, renewed by CAS every few seconds, carrying its epoch, address, whether it serves and the range of its journal still needed. A shard's ownership is valid only while its owner's lease is, so one expired lease releases all of a crashed node's shards.

- **Elapsed time, not timestamps.** The holder treats its lease as expired at the start of its last successful renewal plus the TTL less a fifth, by its own clocks. Another node does once it has seen the lease unchanged for a TTL on its own monotonic clock. Leases need clock rates that agree, not synchronised clocks, and a node that loses its lease exits and restarts as from a crash.
- **Checking the clock.** The S3 backend (`internal/blob`) compares the node's clock with the `Date` header of the store's responses and warns when the offset stays past a second. It never stops the node: `none` answers rest on working clocks, and this is where an operator learns that one is not.
- **Noticing dead nodes.** Nodes list `state/leases/` in turn, about every 2 seconds across the cluster. Seeing an old last-modified time, a node reads the lease, looks again a TTL later and adopts only if it is unchanged. An adopter's writes to a dead node's lease are not renewals, so only a new epoch (a restart) counts the node live again (`internal/peers/scan.go`, `internal/node/adopt.go`).

### Manifest

A manifest is created with a conditional create and changed only by CAS, at ownership changes and snapshots, never on writes or activating an owned shard (its fields are in `internal/shardfile/manifest.go`).

- **`next_reminder`** is set by the owner's deltas, rebases and deactivations, its entry in the reminder index created first. Adoption's deltas set it to now.
- **`rev`** goes up on every write, so no two versions share bytes: a manifest released and claimed again by the same node would otherwise return to an earlier version, which the pruning rule reads differently.
- Ownership is invalid if `node_epoch` doesn't match the owner's current lease or that lease has expired, and a shard deleted and created again gets a new `generation`, so it never reads the old shard's files.

### Fencing

- Each journal has one writer. Segments are numbered and created once, and a writer finding its number taken has been fenced and stops for good.
- An adopter fences a dead node's journal with an empty segment at the first free number after the lease's recorded range. A zombie can then only write below the fence, where nothing is part of any shard's history.
- A zombie's LTX files count once a manifest CAS adopts them, and a stale owner's CAS fails once anyone else has changed the manifest.
- This needs no listings: segments are found by number, and single-object reads are strongly consistent on every store with conditional writes.

### Consistency

Per shard, every read and write is linearizable on the `storage` tier, and on `none` until the node crashes (see the [guide](guide.md#consistency)). Writes rest on one owner per shard, SQLite's commit order, an acknowledgement waiting for the commit to reach the journal, and a zombie's acknowledged commits sitting below the fence its adopter reads. Reads on `storage` are confirmed, so they need no assumption about clocks or pauses. Reads on `none` check the node's lease before output leaves, and claiming a dead owner's shard needs an adopter that watched the lease stay unchanged for a TTL and fenced the journal, so clock rates must agree within the lease's margin (20%) and no pause may hide from the machine's own clocks.

#### Confirming ownership

A `storage` answer leaves once ownership is confirmed after the answer was read: evidence, begun after the chunk was read from the app, that no other node can yet have acknowledged anything for the node's shards. Every way another node takes a shard from a live owner writes the lease first (an adoption marks it, a restart takes a new epoch, a release marks it adopted, and the owner starts a deactivation itself after it stops serving the shard). So a lease that still shows this incarnation's epoch, not released, not marked and not adopted, means nobody has begun a takeover. Two kinds of evidence count:

- **A segment created in storage** by a flush cycle that began after the chunk was read. An adopter marks the lease, fetches, then fences, and a new owner claims only after the fence, so a segment that lands came before any other owner's acknowledgement. This is why output after a commit needs no lease read.
- **A read of the node's own lease** begun after the chunk was read. The `Confirmer` (`internal/lease`) runs one at a time and a waiter joins only a read that begins after it arrived, so on an idle node a burst of reads shares one GET and under write load the segments release the waiters. The lease's expiry is left out of the condition, since the clocks are what this replaces.

A confirmation that finds the lease taken fails the keeper with `ErrLost`, so the node stops and restarts as from a crash, and one that cannot reach the store fails the answer with `503`. A request confirms once, at its first chunk, since the shard's file only grows while the request holds it. Streams are therefore linearizable at their start and then follow the node's copy. `none` answers are not confirmed.

### Lost responses

Any write can succeed while its response is lost. `blob.Store` stores a write id with each object and, when a retry meets "exists" or "conflict", reads it back to recognise its own attempt, and `journal.Store` gives the journals the same, so a caller sees success, a conflict, or an unknown outcome. The journal writer retries the same segment until the outcome is known, and a manifest handle compares the stored manifest with the one it tried. The node never retries a request the app may have seen.

### Takeover

A shard changes hands in one of four ways. Each is one conditional write, so stopping anywhere is safe.

#### Deactivation

At the shard's idle timeout or at a stop, the owner lets the shard go and the next node to need it claims and restores. A shard is deactivated and released in one manifest update, the one that records its last delta (a shard with no tail has its next reminder and the release written together), and then its files are deleted.

When that update's outcome is unknown, neither answer is safe to act on: if it landed the shard is unowned with its tail only in the journal, so serving it is wrong, and if it did not the shard is still ours, so dropping it is wrong. The node asks again until it learns the outcome and serves or drops nothing on a guess, so in a storage brownout a shard that has just gone idle keeps its requests waiting until storage answers. If the node stops first it abandons the shard, keeps the lease and leaves either outcome to adoption.

#### Drain

A stop that was asked for (the context ending, as on SIGTERM) starts with a drain. Readiness fails (`503 draining`), the node's answers and forwards say `Shardlite-Placement: avoid` so its peers place nothing on it, and every claim that reaches the node is placed on another node, as a forward or as a `421` naming the pick. With no other node to pick the node claims for itself. The stop then shuts the public server and lets go of every shard (each a deactivation) with the peer and control listeners still up, so a forward for a shard on its way out is answered by another node (the order is in [Concurrency](#concurrency)).

Nothing is moved ahead of time: each shard is claimed elsewhere at its own next request, and a shard with connections is let go of at the stop, its clients reconnecting through the load balancer to another node. After a stop the node stays listed as alive until the lease scan finds its released lease (about a TTL), so a forward to it gets `503` with `Retry-After: 1` until then. A node that fails (a lost lease, a fenced writer, a failed service) doesn't drain: it stops at once, lets nothing go and keeps its lease, and its shards wait for adoption.

#### Crash

A node whose lease has expired is adopted by the first live node to notice (see [Node leases](#node-leases)), every step a conditional write safe to redo: **mark** the lease as being adopted, **fence** the dead journal, **read** its footers up to the fence, then per shard still naming the dead journal create a reminder entry due now and add the tail as a delta by CAS (with `next_reminder` now and the journal cleared), **finish** by CAS on the lease (adopted to the dead epoch, the journal range past the fence) and **sweep** the segments below the fence, fences staying.

The reminder entry comes before the delta because journal expiry deletes covered segments without checking any lease: an adopter dying between the two would leave a shard nobody knows to wake (`noAdoptionEntry` in `specs/dispatch.qnt`). The dead node's shards keep it as their owner until nothing of them can be in its journal, so a lease that looks expired still owns its shards until then. What a dead node held only on its own disk is gone, and the shards it had released when they went idle name nobody and are claimable at once.

#### Restart

A restart is treated as a crash, one recovery path: the node takes a new epoch, adopts its own journal before serving anything and starts its writer after the fence with nothing on disk. A `none` commit that storage did not yet have is lost in any crash of the node, process or host. While another node is adopting it and it looks alive, the restarting node waits.

## Capture and snapshots

### The read rule

While a shard is captured, the node holds one read transaction begun under the write lock right after a sync and a checkpoint that copied everything, so it gets read mark 0. SQLite then copies no frame into the database file and never restarts the WAL while a frame is waiting to be copied, so no uncaptured frame is ever copied or lost.

- **The locked restart.** While the transaction is held the WAL only grows, so past a threshold of frames the node takes the write lock, syncs, ends the transaction, has everything copied in (a passive checkpoint) and begins a new one. It must be passive, since a truncating checkpoint can't run under the node's own lock and frames committed in the gap would be wiped uncaptured.
- **The app's checkpoints do nothing while a shard is captured:** a passive one copies nothing, and a restart or truncating one answers `SQLITE_BUSY`.

`specs/capture.qnt` checks the rule: every committed frame is in a captured file before it can leave the WAL, the files replayed give the database at the captured end, and a snapshot gives the database at the position it began.

### Checksums

Every LTX file carries the database's checksum before it and after it (LTX's whole-database checksum, the XOR of each page's CRC with its number), which capture keeps cheap by holding every page's CRC in memory. Deltas check the chain. A rebase compares capture's checksum with the snapshot's, and a mismatch is logged and counted while the rebase is written anyway, since it is the database as SQLite reads it. A restore verifies every file, and the first mismatch fails it with the file named: the shard is not served and nothing is deleted, so an operator has every file.

### Node journals and snapshots

The same split as a database's WAL and data files: the journal is the shared WAL of a node's shards, the LTX files are each shard's data files, and a snapshot is the checkpoint. A journal is numbered segments created once and durable before the create returns (`internal/journal`), each create carrying the write-id handling of [Lost responses](#lost-responses), so a lost response costs a retry, never a fence.

- **Segments.** Every flush interval one segment, a footer naming its shards, the body composed from the LTX files capture queued so the bytes are written once.
- **Snapshots as deltas.** When a shard deactivates, or its tail is old or large, the node writes the pages captured since the last snapshot as one delta and adds it by CAS without reading the database file, so a shard that is not active never has a tail.
- **Rebasing.** Once a manifest lists many files, the next snapshot is a full copy of the live database as a new base, bounding how many files a restore reads.
- **Cleaning the journal.** Each node deletes its own non-empty segments once a day old, and an alert comes long before if a tail is old. Fences are never deleted, since a paused zombie could then write into a freed number.

## Durability gates

- A chunk the app wrote is held until a flush that started after it was read has captured it and the store its tier names has what the flush queued. Chunks waiting together share one flush (group commit).
- `none` waits for nothing past the lease check, and `storage` for the PUT: a flush that started after the chunk was read has to have created its segment.
- Every chunk first needs the node's lease to be good, and a flush that completed after the lease ran out acknowledges nothing.
- A `storage` chunk then waits to be confirmed unless its request has been (see [Confirming ownership](#confirming-ownership)). A chunk that waited for its commit's segment is confirmed by it unless the cycle that passed created none.
- A call to another shard is bytes leaving the caller, so it waits for the caller's commits to meet its kind's tier. The caller is the shard the call's context names, and a call without one is the app's, which no shard's commits gate.

## Data and state

| Role | Objects | Changes by | Rule |
|---|---|---|---|
| **Data** | journal segments, LTX files | create once, delete by garbage collection | never modified |
| **State** | leases, manifests | compare-and-swap by the owner | the truth, naming the data it points at |
| **Index** | the reminder index's entries | created by anyone before the write they are for, moved and deleted by the [dispatcher](#the-dispatcher) | deleted only by the index's own rule, once the truth they point at no longer needs them |

Journal segments go to blob storage, which is the only copy: a restore, an adoption and a restart all read it, and the fence is created there. Listings stay per prefix (leases, a node's journal, a lane's reminder entries, one generation of a shard's LTX files), so none grows with the rest. Index entries, LTX files and manifests all have keys that start with the shard's lane (`shard.Name.Lane`, a hash of the name), so the store's writes and a launch spike of claims spread over 16 key ranges from the start.

### Reminder index

An inactive shard's reminder needs someone to wake it. `next_reminder` in the manifest says when, and the index says which manifests to look at when: an entry is an empty object anyone can create, needing no owner and no clock, read only by the [dispatcher](#the-dispatcher). A minute is enough, since a shard's own loop fires at the second once it is active. Its key, `index/reminders/{lane}/{minute}/{kind}/{id}/{base}.{n}`, names the shard, the manifest version the entry backs and how many times the dispatcher has moved it. The keys, the listing and the moves are in `internal/shardfile/reminder.go`, beside the rule.

- **Index first.** Before a manifest write that sets `next_reminder` to a new minute, the writer creates the entry for that minute and its base, a failed create failing the write.
- **Each write has its own entry,** so the dispatcher can't prune one on the terms of a write that isn't the one relying on it.
- **The pruning rule.** An entry may be deleted when the manifest doesn't back it (absent, or `next_reminder` absent or after the entry's minute) and the manifest's current version is not the entry's base. The write the entry was made for replaces that version, so once the manifest has moved off it the write has landed or can never land, while if the version is still the base the write may be in flight. A backed entry is never deleted: it is dispatched and moved forward.
- **Anyone may apply the rule,** so several dispatchers at once or a zombie can't make it unsafe.

`specs/dispatch.qnt` checks that a shard with a reminder in its durable state always has a backed entry or a tail an adoption will still write a delta for.

## The dispatcher

Some of the node's work is the cluster's: waking the inactive shards whose reminders are due. The **dispatcher** (`internal/dispatch`) does it, one of the shardset's services on every node, in the shape of a Kubernetes controller (a work queue of keys and a handler that reads the truth and converges). **The dispatcher may be late, never wrong.**

Its work queue is the [reminder index](#reminder-index): empty objects keyed by lane and minute, created by anyone, moved to a later minute and deleted, and listed per lane up to a minute, earliest first. An entry is a key, never a payload: the dispatcher reads the truth (the manifest, its own lease), so an entry handled twice or late is harmless. Handled entries leave the head of the listing, so the dispatcher keeps no cursor or any state of its own, and a cold start and catching up after an outage are the same case.

It passes at the start of every minute, when a minute's entries fall due, again after a second when a pass found entries, and after a doubling wait up to a minute when a pass failed. A pass lists the due entries of the node's lanes and handles them within its concurrency, rate and budget (`dispatch.Options`). It checks the node's lease before each send, delete or move, and writes nothing while the check fails.

The work is split by [lane](#terms). For each lane every node ranks the live nodes of the lease scan's last listing by a rendezvous hash of the node's name and the lane (`dispatch.Runner`), and the first runs that lane. While nodes disagree a lane has two runners or none for a few seconds, and a node joining or leaving moves only the lanes it ranks first for. Before its first listing a node runs nothing.

The dispatcher is idempotent and convergent: every write it makes is conditional or harmless to repeat, so two runners at once only cost work and none for a while only makes it late. Work that needs doing is recorded by an entry before the write that makes it. Work that needed an effect outside storage to happen once would need an ownership record of its own, and none does.

For each due entry it reads the manifest and deletes the entry by the [pruning rule](#reminder-index), or sends the backed shard a **dispatch**: `POST /_shardlite/activate` as the node's own request, with `Shardlite-Target` the shard and `Shardlite-If-Exists: true`. The node answers that path itself once the shard is held where the request lands, and the app never sees it. The request routes as any does and never creates the shard, and the shard's own loop then wakes the app for its due reminders. A dispatched entry moves forward whatever the answer (16 minutes for a shard that was active, otherwise by a step doubling from a minute), and a failed manifest read leaves it for the next pass. A kind starting with `_` is reserved for the node (none is declared today).

## Lifecycle

### Activation

1. **Owned by another node with a valid lease:** forward the request there.
2. **Unowned, or claimable after its owner's adoption:** claim by CAS and restore. A manifest with nothing to restore starts over in a new generation. A claim that loses its CAS reads the manifest again and takes the path of a request that found another owner, or answers `503` with `Retry-After: 0` if the manifest shows no owner yet. The app never saw the request, so it is never a `502`.
3. **No manifest:** a call creates it with a conditional create, which is the claim. A client request never creates a shard, so random ids from clients cost a manifest read and leave nothing behind.

**Placement** decides where steps 2 and 3 happen, after the manifest is read, so a shard active here or owned by another live node is never placed. A client's request and a call with no shard behind it (a login) claim on the node that received them, since the client's session is there. A call from a shard and a dispatch claim on a node picked at random (see [Placement](#placement)), since a matchmaker would otherwise put every match on its own node. The node forwards the call to the pick, which claims or creates the shard as for any forward for an unowned shard. A forward and a wake never place.

**Local files.** Every new local file gets a path of its own, `{kind}/{id}.{generation}.{random}.db` (`pathFor` in `internal/node/localfiles.go`), and none is reused. So the next claim of a shard whose old file an app still has open gets a new file beside it, which is what makes it safe to delete a file the app has open. The node creates the file before it hands the path out, so the app opens it without create (`mode=rw`).

Concurrent requests share one activation, run by the shard's own loop. A shard deactivates after its kind's idle timeout unless a request, connection or due reminder holds it or the app still has its file open. A shard holding nothing of the app's (never written, emptied, or carrying the delete marker) is deleted instead of released.

### Letting go of a shard

Deactivating or deleting a shard is only safe once nothing is writing to it. A request holds its shard, so once none does, the only thing that could write is a handle the app kept open, and the contract allows writes only during a request or a wake. The node asks SQLite's own lock: every connection holds a shared lock on byte 128 of the `-shm` file, tested with `F_GETLK` as SQLite does, after the node has closed every handle of its own (a process's `fcntl` locks drop with any descriptor, so it opens a shard's files only through capture). It compares the wal-index header before and after to catch a commit made just before the app closed. A deactivation is the final sync and this check, then the manifest update of [Takeover](#takeover), then the deletion of the files.

**Held at idle.** If another process still has the shard open, capture resumes and the shard stays active with nothing deleted, since a file deleted under an open handle loses the commits made through it without any signal. A shard that stays active is served and its commits through the handle are captured, so the problem is loud. The looks back off, starting at 5 s and doubling up to the kind's idle timeout, since each is a whole release and resume, and start over when a request uses the shard. The node warns once a hold has outlasted one more idle timeout (a pool that closes slowly is not a leak) and counts it (`Stats.HeldPastIdle`).

**Held at a stop.** Only a stop goes ahead over an open file: once no request holds the shard the release runs with capture told to go ahead, and the files are deleted at once. The app's handle keeps the inode until it closes it. Opening the old path again fails, and a commit through a connection that was already open succeeds on the deleted file and is lost, because SQLite in WAL mode doesn't notice a deleted database. That commit is the app's bug, since it broke both the idle timeout and the write rule, so the let-go logs a warning naming the shard and counts it (`Stats.LetGoHeld`). Holding SQLite's write lock on the deleted file so that such a commit fails was left out (it is on the [roadmap](roadmap.md)).

A resumed release continues where capture left off when the WAL is the same one. If the app checkpointed and restarted the WAL instead, the uncaptured frames are gone, so the node writes the whole database as one file counted as one commit.

### Routing and forwarding

#### Forwards

A request for a shard owned elsewhere goes to the owner's peer listener, never behind the load balancer, so node-only headers are trusted there, and shardless requests are never forwarded. The owner serves a forward as the original, and gates stay where the writes are (the caller's on the caller's node, the callee's the owner's). The owner acknowledges every forward with `102 Processing` before its app sees anything, so a forward without an answer within a second never reached the app, and the forwarding node answers `503` with `Retry-After: 1`.

A forward is never forwarded again. An owner that finds the shard isn't its own answers `421` naming the owner it found, and the forwarding node corrects its cache and answers `503` with `Retry-After: 0`. A forward of a client request for a shard that nobody owns is answered the same way, naming the forwarding node (`sendBack` in `internal/node/shardset.go`): the client's session is there, so that is where the shard should be claimed, where otherwise it would be pinned away from its clients. Two cases are claimed where they land: a draining node's forwards, which are claims it placed, and a manifest that still names this node (after a restart). The node never retries a request itself, keeping no bodies, so two stale nodes can't bounce one.

#### The context

`Shardlite-Context` is what the node hands an app with every request it opens a shard for, and the app passes back unchanged on the calls it makes while serving it: a Structured Fields dictionary (RFC 9651) of `shard` and `hops` (`internal/node/context.go`). The node keeps nothing: the header is the whole of it. A call with a context takes its caller from `shard` and its hops from `hops` plus one, and a call without one is from the app at hop 1. A forward carries the context it was called with, and the owner writes a fresh one for its own app. A context that doesn't parse, lacks a key or names a malformed shard is a `400`, and unknown keys are ignored, so one can grow without every SDK changing.

#### Peer headers and placement

Two headers travel between nodes only, are read on the peer listener's requests and from peers' answers, and are taken out of answers before a client sees them. `Shardlite-Node` names the sender of a forward. `Shardlite-Placement` is a Structured Fields Item, the one value today the token `avoid`: place nothing here. A draining node sets it on every answer from its peer listener and on its forwards. A header that doesn't read is counted (`Stats.BadPeerHeaders`) and ignored, so placement only gets worse for it, never wrong.

A node keeps the peers it avoids, each with why it was learned: the peer said `avoid` (it ends at an answer without it, when the lease scan no longer lists the node live, or a lease TTL after the peer last said it), or a forward to it got no answer in time (it ends at the next listing or any answer). This is `peers.Placer` (`internal/peers/placer.go`).

##### Placement

Placement picks uniformly at random among the live nodes of the lease scan's last listing minus the avoid set, this node a candidate unless it is draining, and with no candidate the node claims for itself. Random is the whole criterion, because placement doesn't load-balance a cluster: a shard moves at its next idle, so all placement needs is that new claims spread.

#### Finding the owner

The owner is a bounded LRU hint, filled when a claim finds another owner and emptied by a `421`, by this node's own claim and by the lease scan finding a node dead. A stale entry costs one `503`, a miss one manifest read. Every deactivation releases its shard, so a stale entry is the normal case for a shard that went idle on the node the entry names: the request is forwarded, answered `421`, turned into a `503` with `Retry-After: 0`, and the client's retry reads the manifest and claims the shard there. That is one client round trip per return of a shard last served elsewhere. Once the owner is dead the lease scan empties its entries, so the next attempt reads the manifest and either waits for the adoption or claims the shard.

#### Errors

Every error the node generates carries `Shardlite-Error: {reason}` (`writeError` in `internal/node/answer.go`, the reasons in `internal/node/reason.go`, and a gRPC answer carries it as a header beside `grpc-status`). The node strips every `Shardlite-*` header from the app's responses, so an error without the header is the app's. The header tells failures apart and is not a boundary, since the app is trusted. A forward passes the owner's answer through with the owner's reason, and the forwarding node sets its own only where the error is its own (`owner-unreachable`, `moved`, `owner-failed`).

A failed dial to the app is a `502` with `Retry-After: 1` on every listener, which says the app never had the request, so a client, a peer or a dispatch can retry it (`neverReached`). A response that fails after the request went out is a bare `502`, since the app may have acted on it.

### Reminders

The node knows nothing of what a reminder means: it reads one number from the shard's table and calls the app. While the shard is active its loop wakes the app (`POST /_shardlite/reminders`, in process, never forwarded) once the earliest due reminder is due and what the read saw is durable by the gate's rule, one wake at a time under a node-wide limit. While it is not, its index entry wakes it through the dispatcher. A reminder is always covered: in the tail until the next delta (adoption creates an entry due now for every tail it writes a delta for), and in the entry created before that delta afterwards. The `next_reminder` recorded with a delta must match the state the delta's files reach: anything committed beyond what was read means recording now instead. The node writes only the reserved table and never a reminder.

## Concurrency

Every goroutine is owned and joined, and the node has no bare `go` statement (the per-part detail is in `internal/node`).

- **Every goroutine has an owner that joins it,** up to `Run`, which returns only after everything it started has ended. The long-lived parts are services (`internal/service`) run in groups and stopped by whoever started them, and one that returns before it is stopped has failed. Nothing restarts. Fork-join and a service's own goroutines use `errgroup`.
- **A goroutine waits only for goroutines it started, for its own I/O and for node-wide loops that never wait for their callers**, so waits can't form a loop: nothing `Shardset` depends on (the writer, the keeper, blob storage) waits for `Shardset`.
- **Callbacks from libraries never block on anything the node drives.** A callback runs to the end under the lock guarding the state it touches, and whoever started the work waits for durability afterwards.
- **The capture lock** guards a captured shard's WAL state, transactions, wal-index mapping and tail. Its holder runs to the end, so a sync is never cut off. No other lock nests, is held across I/O or a wait on another goroutine, or stands in for ownership.

`Run` runs four stages: `front` (the public server and the services that depend on nothing), `callers` (the control and peer servers), `shardset` and `journal`. `journal` starts as soon as the lease is taken, so the lease is renewed while the rest is built. The stop order depends on why the node stops, and `journal` always stops after `shardset`, since the shards write through the writer and need the lease:

- **A stop that was asked for** (the context ending, as on SIGTERM, or the app being lost) drains first (see [Drain](#drain)), then stops `front`, `shardset` and `journal`, releases the lease and only then stops `callers`, so a forward for a shard on its way out is answered by another node rather than met by a closed port.
- **A node that failed** (a service failed, the writer was fenced or the lease lost) stops `front`, `callers`, `shardset` and `journal` in that order, and keeps the lease.

### Shutdown

After the drain, `front` stops: the public server stops accepting and waits up to 10 seconds for requests in flight, which ends upgraded connections. A connection that has sent nothing a second into the stop is closed at once (`silentGrace`, `internal/node/httpserver.go`), since Go's `Shutdown` would wait 5 seconds for it. `shardset` then refuses new activations that can't be placed elsewhere (`503`) and lets go of every shard (256 at once, transient storage errors retried until a 30-second deadline), so a stop costs one manifest write per active shard. A shard a request still holds is tried again until the deadline, and once it has been held for 10 seconds `Run` stops the control and peer servers early, which ends requests that never end (a stream, a long poll, a forwarded WebSocket). If the lease is gone or the writer is fenced, nothing is let go. `Run` releases the lease only if every shard was let go of and nothing is wrong. Each stop has its own deadline of at most 35 s, so a stuck part never holds up the rest.

### Failures

`Node.Err` is nil while the lease is good and the writer isn't fenced, otherwise it is the reason. A panic crashes the node and adoption recovers its shards on the next start. The lease lost or the writer fenced stops at once, letting nothing go and keeping the lease. Any other service failing stops the node gracefully with its error. Nothing restarts, and a panic in one shard's goroutines crashes the whole node.

A lost app is not a failure. Once the app has answered the declaration poll, 30 seconds (`appLostAfter`) without any HTTP answer (the dial fails, the connection resets or the poll times out) stops the node the way SIGTERM does, and `Run` returns `ErrAppLost`, so the command exits non-zero and a supervisor restarts it. Any answer ends the run of silent polls, a non-2xx one and a declaration that doesn't parse included, since the app is there and only not ready. A node that has never heard from its app waits for it, not ready and owning nothing, and never stops for it.

## Garbage

An LTX file no manifest lists is harmless (a restore never reads it) and costs storage only. The write path deletes what it knows is garbage, best effort: a write whose manifest update loses with a conflict deletes the file it just uploaded (the only outcome that means nothing can list it), a rebase once its swap has landed deletes every `.ltx` file the new manifest doesn't list, so what earlier crashes left in a shard still in use goes at its next rebase, and a deleted shard's files go with it. A file the handle holds a write id for is skipped, an upload waiting for its own update.

The sweep needs no age rule: a manifest only adds files its own writer just uploaded, and after the swap the only writer that can list a file in the generation is one taking the shard from the node, which writes the node's lease first. The sweep reads the lease after its listing and deletes only while it still holds. A conflict needs one check of its own, since it can hide the write's own update, landed and then replaced by another writer's (see [Lost responses](#lost-responses)): the write reads the stored manifest first and keeps a file it lists. A failed delete, or a failed read that stops one, logs the shard (and the key, when there is one) and counts in `Stats.LeakedFiles`; a crash leaks silently. A leftover of the node's own epoch never blocks its range's next write, which deletes it and takes the name back. What the write path leaves, a shard's next rebase or a separate cleanup past the journal's expiry takes: by then a writer that has not updated its manifest has lost its tail anyway.

Journal segments are each node's own to expire (see [Node journals and snapshots](#node-journals-and-snapshots)). Dead nodes' leases stay: a node of the same name starting again at epoch 1 would collide with its old files, so nothing retires them and every lease scan lists them.

Local files need no cleanup. Every file the node stops needing is deleted at once (`removeLocal` in `internal/node/localfiles.go`), whether or not an app has it open, since no path is ever reused (see [Activation](#activation)). A start deletes every shard file left in the data directory, since after an OS crash none can be trusted to agree with blob storage, and its shard activates at a new path with its data.

## The models

Each model in `specs/` is about one part of the code and assumes what the others check: `shard.qnt` is one shard (its file, its journal and when a reply may go out), `cluster.qnt` is ownership and adoption (leases, journals, fences, manifests and the claim rule), `dispatch.qnt` is the [reminder index](#reminder-index) (it assumes a crash leaves a tail to an adopter) and `capture.qnt` is the [read rule](#the-read-rule) (it assumes SQLite's WAL behaviour).

Only capture's traces are replayed against the code: `internal/capture/replay_test.go` plays each trace in `internal/capture/testdata/traces/` (made by `quint run --mbt --out-itf`, the command beside them) against a real SQLite file. The traces are committed so that `make test` needs no Quint.

## Testing

- **Fault injection** kills and fails the real `shardlite` binary, built with the `fault` tag (`e2e`, `fault_*_test.go`, the settings in `cmd/shardlite/fault.go`). A test arms a kill point (`internal/kill`) after the node is up, drives it through the e2e app and checks that every write a `storage` request acknowledged is in blob storage after a restore and that the database passes `integrity_check`. It covers crashes between every step of capture, snapshots and restore, failing storage (the journal's segments, the uploads of LTX files and their deletes) and temporary files, a node killed while it stops, and peer headers that two nodes drop or garble. Every scenario ends with the walk in `internal/audit`, which fails when the LTX files no manifest lists and no node logged as a leak outnumber what the scenario's kills explain (per node killed, each of the rig's two shards can leave one upload on its way to the manifest or, killed just after a rebase's swap, the files the swap superseded). The scenario that fails every delete stops its nodes gracefully instead, so it allows none, and it fails unless the node reports a failed delete.
- **Fencing** is tested by killing nodes and by taking a live node's lease the way an adopter does while the node hears of nothing: a `storage` read must then fail and the node must stop, and a `none` read answers (`TestHiddenPauseFailsStorageReads`). Pausing and partitioning real processes is on the [roadmap](roadmap.md).
- **The protocol** is checked in Quint before it is implemented, and **time** (lease expiry, timeouts) runs on `testing/synctest`'s fake clock.

## The code

| Package | What it is |
|---|---|
| `internal/pos`, `internal/shard`, `internal/cluster` | named types for positions, epochs and segments, and for shard, kind and node names |
| `internal/lease` | node leases and the confirmer for the gates |
| `internal/blob`, `internal/blobtest` | object storage with write ids, the startup check and the clock warning, and the in-memory store (`SHARDLITE_BLOB=memory://`) |
| `internal/journal` | a node's journal: its segment store, writer and fencing |
| `internal/capture` | reading a shard's WAL and the [read rule](#the-read-rule) |
| `internal/shardfile` | the manifest, deltas, snapshots, restores and adoption, and the [reminder index](#reminder-index) |
| `internal/dispatch` | the [dispatcher](#the-dispatcher) and the ranking of lanes |
| `internal/peers` | the lease scan, the peers' addresses, placement and the peer headers |
| `internal/node` | `Run` and everything it stages: shardset, actor, flusher, gates, routing, reminders, servers, adoption, local files |
| `internal/kill`, `internal/audit`, `internal/harness` | the fault rig's kill points, the store walk and shared test pieces |
| `specs/` | the Quint models |
| `sdk/go`, `sdk/ts`, `sdk/testdata` | the SDKs (see the guide's [Using the SDK](guide.md#using-the-sdk)) and the node's app-visible reasons they check themselves against |
| `migrate` | numbered SQL migrations run inside the app's own transaction (see the guide's [Schema migrations](guide.md#schema-migrations)), which the node never imports |

