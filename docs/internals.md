# shardlite internals

How the node works: its architecture and the rules that keep it correct. The app-facing behaviour is in the [guide](guide.md), the node's own shards in [system kinds](system-kinds.md), goroutine ownership and shutdown in [concurrency](concurrency.md), and the protocol's models in [specs/](../specs/).

## Terms

| Term | Meaning |
|---|---|
| **Node lease** | An object each node holds and renews. Every ownership the node has depends on it. |
| **Node journal** | Each node's journal in blob storage: numbered segments, one per flush interval, holding every shard's commits since its last snapshot. The shared WAL of the node's shards. |
| **Snapshot** | A shard's state as of one commit, as immutable LTX files: a base and deltas. An inactive shard lives only in its snapshot. |
| **Manifest** | The record of a shard in blob storage: its owner and the owner's epoch, its LTX files, the journal holding later commits and when it next needs waking. |
| **Reminder index** | Empty objects in blob storage that say a shard may need waking at a minute. `_dispatch` reads them. |

The other terms are in the [overview](overview.md#concepts).

## Implementation

The node is written in Go. It uses [Litestream](https://github.com/benbjohnson/litestream) as a library to capture each shard's WAL, and the [LTX library](https://github.com/superfly/ltx) to build deltas and restore. The proxy is the standard library's, with HTTP/2 without TLS for gRPC and between nodes.

- **The node drives capture.** Litestream's own timers and polling are off. The node syncs changed shards from its flush loop, and Litestream's storage client only queues each sync's file into the node's pending segment. The node spots commits by mapping each shard's wal-index header (SQLite updates it on every commit), so a flush costs microseconds per active shard and misses nothing.
- **Positions are the shard's own.** Litestream numbers commits per activation. The node gives each captured commit the shard's position, continuing from the restored one, so Litestream's numbers never reach the journal, manifests or restores. A restored shard is seeded so Litestream continues from its state without a snapshot, which relies on Litestream's on-disk layout (`TestSeedRelianceOnLitestream` guards it).
- **Structured concurrency.** Every goroutine has an owner that joins it, and waits can't form a loop (see [concurrency](concurrency.md)).
- **Nothing shard-sized in memory.** Restores, rebases and large deltas go through temporary files in `{data}/.tmp`, emptied at startup. The scratch directory (Litestream's metadata, which needn't outlive the process) belongs on tmpfs.
- **Per-shard cost.** Capture holds a connection and a read transaction on each active shard. Stage 0 measured about 0.5 MB, 7 file descriptors and 3 goroutines per active shard.

## Ownership and fencing

A lease alone can't prevent two writers (GC pauses, VM pauses, network delays), so the storage being written to must reject a stale owner's writes, as in Kleppmann's [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html). Here that is the node journals (one writer each) together with the manifests. The protocol is specified in Quint.

### Primitives and databases

Leases and journals are the only state outside shards, apart from the index objects below. A lease says which node is alive and in which epoch, and a journal makes a node's commits durable before any file does, so neither can live inside a database whose owner they decide.

Everything else is a **shard**: a database stored the same way (a manifest naming LTX files, with a tail in its owner's journal), with a kind, the claim rule and a lifecycle. The test for anything new: state shared across nodes goes in a shard, an entry that must be written while no owner can be relied on is an index object (see [The reminder index](#the-reminder-index)), created by anyone before the state change that needs it and deleted only when state doesn't back it, and only what a shard needs in order to exist is a primitive.

### Node leases

Each node holds one lease (`state/leases/{node}`) with its epoch, address, whether it serves, its counts, the range of its journal still needed and two fields for adoption (`adopted_by` and `adopted`). It is renewed every few seconds by compare-and-swap. A shard's ownership is valid only while its owner's lease is, which keeps renewal cost per node rather than per shard and lets one expired lease release all of a crashed node's shards.

- **Elapsed time, not timestamps.** The holder treats its lease as expired at the start of its last successful renewal plus the TTL less a fifth, by its own clocks. Another node treats it as expired only once it has seen the lease unchanged for a TTL on its own monotonic clock. So leases need clock rates that agree, not synchronised clocks. A node that loses its lease exits and restarts as from a crash.
- **Noticing dead nodes.** Nodes list `state/leases/` in turn, so the cluster lists about every 2 seconds whatever its size. A listing's last-modified times are only a hint: a node that sees an old one reads the lease and looks again a TTL later, and adopts it only if it is unchanged.
- **Checking an owner.** A node forwarding a request reads the owner's lease for its address, and when the owner doesn't answer, to choose between failing fast and claiming (see [Routing and forwarding](#routing-and-forwarding)).

### Manifest

```
state/shards/users/42
{
  "generation":  "k3x9",      // random, new when the shard is created or starts over
  "owner":       "node-b",
  "node_epoch":  17,
  "files":       ["1-1201.12", "1202-1650.12", "1651-1834.17"],  // {positions}.{writer's epoch}
  "journal":     "node-b",    // the journal holding commits after the last file, if any
  "next_reminder": 29873727,  // Unix minutes: when the shard next needs waking, omitted if never
  "rev":         42           // counts the manifest's writes
}
```

A manifest is created with a conditional create and changed only by compare-and-swap. It is written on ownership changes and snapshots, never on writes or on activating a shard the node already owns. `next_reminder` is set by the owner's deltas, rebases and deactivations from the shard's own tables (a write that finds no reminder omits it), with its entry in the reminder index created first. Claims, releases and evictions keep it, and adoption's deltas set it to now. A new shard and one that starts over have none, and system shards never set it, since the claimer brings them back. `rev` goes up by one on every write, so no two versions of a manifest have the same bytes, and so the same ETag on a store where the ETag is a hash of the body: a manifest released and then claimed again by the same node would otherwise return to an earlier version, which the pruning rule below takes to mean something else. Ownership is invalid if `node_epoch` doesn't match the owner's current lease, or that lease has expired. A shard deleted and created again gets a new generation, so it never reads the old shard's files.

### Fencing

- Each journal has one writer. Segments are numbered and created with a conditional create, so a number is written only once, and a writer that finds its number taken by someone else's segment has been fenced and stops for good.
- An adopter fences a dead node's journal with an empty segment at the first free number after the lease's recorded range. A zombie can then only write below the fence, and nothing it writes afterwards is part of any shard's history.
- A zombie's LTX files only count once a manifest CAS adopts them, and a stale owner's CAS fails once anyone else has changed the manifest. A delta it adds before then may hold commits it never acknowledged, which keeps the history consistent since nobody saw them acknowledged.
- None of this needs listings: segments are found by number, and single-object reads are strongly consistent on every store with conditional writes.

### Consistency

Per shard, with sync on, every read and write is linearizable (see the [guide](guide.md#consistency)). Writes are because a shard has one owner, SQLite orders its commits and an acknowledgement waits until the commit is in the journal. A zombie's acknowledged commits are below the fence, which its adopter reads.

Reads are because every gate checks the node's lease before output leaves, and a claim of a dead owner's shard needs an adopter that has watched the lease stay unchanged for a TTL and fenced the journal. This needs clock rates that agree within the lease's margin (20%) and no pause the paused machine's clocks can't see. A read followed by a commit needs no clock assumption at all: its answer waits for the commit's segment, which a fenced node can't write.

### Lost responses

Any write can succeed while its response is lost. `blob.Store`, which every caller writes through, stores a write id with each object and, when a retry meets "exists" or "conflict", reads it back to recognise its own earlier attempt. Callers therefore see success, a conflict with someone else, or (when every read-back failed too) an outcome that stays unknown. Two callers handle unknown outcomes themselves: the segment writer retries the same segment until the outcome is known, and a manifest handle compares the stored manifest with the one it tried before it writes again. The node never retries a request the app may have seen, for the same reason one level up.

### Takeover

- **Eviction** (a full cache, rebalancing, draining). The owner deactivates the shard if it is active (its tail becomes a delta), releases its manifest by CAS and drops its files. Nothing is sent to another node: the next node to need the shard, for a request or a reminder, claims it and restores from the snapshot. A shard in use or one the app still has open isn't evicted. The delta and the release are two writes, so a release whose outcome is unknown can be treated as landed (the tail is already a delta) without ever resuming capture on a shard that may be unowned. The shard's `next_reminder` and its entry stay, so `_dispatch` wakes it when it comes due and the node it picks claims it.
- **Crash.** A node whose lease has expired (confirmed as in [Node leases](#node-leases)) is adopted by the first live node to notice. Each step is a conditional write that is safe to redo, so an adopter that dies partway is simply replaced:
  1. **Mark** the dead lease as being adopted, by CAS.
  2. **Fence** the dead journal.
  3. **Read** the journal's footers up to the fence.
  4. **Deltas.** For every shard whose manifest still names the dead journal, add a delta of its tail by CAS, set its `next_reminder` to now and clear the journal from the manifest. Before each delta the adopter creates an entry due now in the reminder index, keyed by the manifest version the delta replaces, since the shard was active and its reminders may have no entry anywhere.
  5. **Finish** by CAS on the lease: adopted up to the dead epoch, journal range moved past the fence.
  6. **Sweep** the dead journal's non-empty segments below the fence. Fences stay.

  The entry comes before the delta because journal expiry deletes segments whose tails are covered without checking any lease: once the delta is in, a zombie of the dead node may delete the tail's segments, and an adopter that died between the two would leave a shard nobody knows to wake (`specs/dispatch.qnt` shows this as `noAdoptionEntry`).

  A dead node's shards keep it as their owner until nothing of them can be left in its journal: the manifest names no journal (an adopter wrote the delta), or the node is adopted up to the owner's epoch (a release counts too). Only then can they be claimed, and until then requests for them fail fast. No timestamp decides this, so a lease that merely looks expired still owns its shards. Writes that hadn't reached blob storage are lost, except for kinds with sync on.
- **Restart.** A restart is treated as a crash, so there is one recovery path. A node starting again takes its lease with a new epoch and adopts its own journal in the same steps before it serves anything, renewing as it goes so nobody takes it for dead. Its shards' deltas each get an entry due now first, as in any adoption, and its own writer starts after the fence once the adoption has finished. It starts with an empty cache. A crash midway leaves only the fence after the lease's range, so the next life adopts the same journal again. If another node is already adopting it and looks alive, it waits for that to finish, since its fence could otherwise land in the new life's journal. If its previous life was released or already adopted, it only fences to find where its journal starts.
- **Drain.** A node scaling down fails its readiness check, refuses to claim shards (`503` with `Retry-After: 0`, so the retry lands on another node) and stops, evicting each shard as it lets go of it rather than only caching it. Its shards can then be claimed at once even if one the app holds open keeps the node from releasing its lease (that one waits for adoption).
- **Stop.** A node that stops lets go of every shard, each final delta recording the shard's `next_reminder` with its entry, and releases its lease. Nothing about reminders is left to hand over: the entries are in the index already. The lease scan lists the stopped node as alive until its lease is a TTL old, so a dispatch to it fails and goes to another node, which claims the shard because the released lease allows it. Its reminders are therefore not late by a TTL.

A move therefore copies nothing between journals, and a dead node's journal is gone within minutes.

## Storage

The owner's local disk holds the working copy of each active shard and a cache of recently used ones. Blob storage holds one journal per node, the LTX files of every shard and the reminder index, and is the system of record.

Cached shards cost only disk. The node keeps owning them while its lease is valid, so reactivating one needs no blob storage request. When disk fills, the least recently used are evicted: the manifest is released by CAS and the files dropped. An eviction whose release has an unknown outcome drops the files too, since the tail is already a delta and the next activation simply claims and restores.

### Data and state

Every key starts with its role, and each role has one rule:

| Role | Objects | Changes by | Rule |
|---|---|---|---|
| **Data** | journal segments, LTX files | create once, delete by garbage collection | never modified |
| **State** | leases, manifests | compare-and-swap by the owner | the truth, describing exactly the data it points at |
| **Index** | reminder index entries | created by anyone before the state change they are for, moved and deleted by the dispatcher | an entry is deleted only when the manifest doesn't back it and isn't at the entry's base (see [The reminder index](#the-reminder-index)) |

Listings stay per prefix (`state/leases/`, a node's own journal, `index/reminders/` up to the current minute and the sweeper's `data/shards/`), so none grows with the rest.

### The reminder index

A reminder of a shard that isn't active needs someone to wake the shard. The time a shard next needs waking is in its manifest (`next_reminder`, Unix minutes), and the reminder index in blob storage says which manifests to look at when. An entry is an empty object that anyone can create, that needs no owner and that is deleted by one rule that needs no clock. Only `_dispatch` reads the index (see [System kinds](system-kinds.md#_dispatch)). A minute is all it needs, since a shard's own reminders loop fires at the second once the shard is active.

- **Keys.** `index/reminders/{minute}/{p}/{kind}/{id}/{base}.{n}`. `{minute}` is UTC (`2026-10-19T14:05Z`), so keys list in time order. `{p}` is one hex digit from a hash of the shard's name, so a minute's writes spread over 16 prefixes. `{base}` is a short hash of the ETag of the manifest the write will replace, and `{n}` counts how many times the dispatcher has moved the entry.
- **Index first.** Before a manifest write that sets `next_reminder` to a new minute (a delta, a rebase, a deactivation or an adoption's delta) the writer creates the entry for that minute and its own base. A failed create fails the write: a delta's timer tries again and a deactivation keeps the shard active. There is nothing to flush, since the entry is durable when the create returns. A write whose reminder is in the minute the manifest already has creates none, because the entry that backs the old one backs the new one too. A deactivation with no tail has no delta to carry its next reminder, so it writes the manifest alone if the next reminder differs from the manifest's.
- **Each write has its own entry.** An entry that exists already under another base never counts. Two writes sharing one entry would let the dispatcher prune it on the first write's terms while the second still relies on it.
- **The pruning rule.** An entry may be deleted when the manifest doesn't back it (there is no manifest, or `next_reminder` is absent or after the entry's minute) and the manifest's current version is not the entry's base. The write the entry was made for replaces its base, so once the manifest has moved off that version the write has either landed or can never land, and a manifest that has moved on without backing the entry doesn't need it. While the version is still the base the write may be in flight (a paused owner, or one between its create and its CAS), so the entry stays. This is the race that makes pruning an index by comparing it with the manifests unsafe without the base.
- **A backed entry is never deleted.** It is dispatched and moved forward. This covers a shard that is active after a dispatch and whose node then crashes before anything of it reaches the journal: adoption finds no tail for it, so its entry is what still wakes it.
- **Anyone may apply the rule.** It reads only the entry and the manifest, so a second dispatcher, a zombie or an operator's tool can't make it unsafe. One dispatcher is for cost, not correctness.
- **Moving forward.** The dispatcher creates the entry `{base}.{n+1}` at a later minute and then deletes the old one, so a crash between the two leaves a duplicate, which costs one more dispatch.

`specs/dispatch.qnt` checks that a shard whose durable state has a reminder always has a backed entry or a tail that an adoption will still write a delta for, and that each rule above is needed.

### Node journals and snapshots

The same split as a database's WAL and data files, or HBase's log per server and files per region: each node's journal is the shared WAL of its shards, and each shard's LTX files are its data files. Taking a snapshot is the checkpoint.

- **Segments.** Every flush interval the node writes one segment holding that interval's commits, with a footer saying which shards it holds. The PUT rate depends on the number of nodes, not shards, and an idle node writes nothing.
- **Snapshots as deltas.** When a shard deactivates, or its tail is old or large, the node writes the pages it captured since the last snapshot as one delta and adds it by CAS. The database file is never read for this. A cached shard therefore never has a tail, so the lease's journal range only covers active shards.
- **Rebasing.** Once a manifest lists many files, the next snapshot is a full copy of the live database as a new base, which bounds how many files a restore reads. A few run at once per node.
- **Cleaning the journal.** Each node deletes its own non-empty segments once they are a day old. Tails become deltas within minutes, so nothing tracks which segments are still in use, and the node raises an alert long before then if a tail is old (its deltas are failing). Fences are never deleted: a paused zombie could otherwise write into a freed number.
- **Restore** downloads the files in parallel, verifies and applies each, then applies the tail, if any.

### Durability gates

The gate treats responses, streams and calls to other shards alike, as bytes, so it needs no parsing.

- A chunk the app wrote is held until a flush that started after it was read has completed, since every flush captures every active shard's commits up to its start. The node only counts flushes.
- Every chunk first needs the node's lease to be good, sync on or off. A chunk from a shard with nothing new since its last capture then passes at once. A flush that completed after the lease ran out acknowledges nothing.
- Chunks waiting together share one flush (group commit across every shard of the node).
- A call to another shard is bytes leaving the caller, so it waits for the caller's commits if the caller's kind has sync on.

Spanner's commit wait is similar: the commit is applied, and only the acknowledgement waits.

## Activation

1. **Owned by another node with a valid lease:** forward the request there.
2. **Cached here:** reopen the files and start capturing, with no blob storage request.
3. **Unowned, or claimable after its owner's adoption:** claim by CAS on the manifest and restore. A manifest with nothing to restore starts over in a new generation.
4. **No manifest:** a call creates it with a conditional create, which is the claim. A client request never creates a shard (it gets `404`), so random ids from clients cost a manifest read and leave nothing behind.

Placement prefers the node that received the request. Concurrent requests for one shard share one activation, run by the shard's own loop so that one caller giving up fails no one else. A shard deactivates after its kind's idle timeout unless a request, a connection or a due reminder holds it, and its files stay in the cache. A shard that holds nothing of the app's at deactivation (never written, emptied or carrying the delete marker) is deleted instead: its manifest by conditional delete, then its files. Nothing can delete a live shard from outside.

### Letting go of a shard

Deactivating, evicting or deleting a shard is only safe once the app has closed it, or a connection left open could keep writing to a file nobody captures. The node asks SQLite's own lock: every connection holds a shared lock on byte 128 of the `-shm` file, and the node tests it with `F_GETLK` as SQLite does. `fcntl` locks belong to a process and closing any descriptor drops them all, so the node checks only once it has closed every handle of its own on the shard, and opens a shard's files only through capture otherwise. It also compares the wal-index header before and after the check, to catch a commit made just before the app closed.

If another process still has the shard open, capture resumes and the node doesn't let go: an idle deactivation tries again later, and an eviction or a delete is refused. The same check, before activating a cached shard, catches apps opening files they weren't given.

## Routing and forwarding

- **Forwards.** A request for a shard owned elsewhere is forwarded to the owner's peer listener (never behind the load balancer, so node-only headers are trusted there). Ordinary requests, streams and gRPC share one HTTP/2 connection per pair of nodes, and cancellation becomes a stream reset. Upgrades go over HTTP/1.1 on a connection of their own, since Go can't carry them over HTTP/2. Shardless requests are never forwarded.
- **The owner serves a forward as the original.** The forward carries the request's origin, caller and hops, so the owner applies the same rules (a forwarded client request is still checked against public kinds), and the app sees the same headers either way.
- **Gates stay where the writes are.** The caller's gate is on the caller's node, applied to the body the forward sends. The callee's gate is the owner's. The forwarding node adds none.
- **A forward is never forwarded again.** An owner that finds the shard isn't its own answers `421` naming the owner it found. The forwarding node corrects its cache and answers `503` with `Retry-After: 0`, as databases answer "not leader": the client's next attempt goes to the right place. The node never retries a request itself, so it keeps no bodies and two stale nodes can't bounce a request between them. A forward that fails after the owner acknowledged it is a `502`, since the app may have seen it.
- **Dispatches** are the only requests a node makes of its own accord. `_dispatch` sends them to the peer listener of a node it picks, and the receiver treats one as a wake that acquires the shard.
- **Finding the owner.** A bounded LRU maps shards to the node that owned them, filled when a claim finds another owner and emptied by a `421`, by this node's own claim and for every shard of a node the lease scan finds dead. It is a hint: a stale entry costs one `503`, and a miss costs one manifest read and no activation.
- **Fail fast.** The owner's node acknowledges every forward with `102 Processing` before its app sees anything, so a forward that gets no answer within a second never reached the app. The forwarding node then reads the owner's lease (once for many requests) and answers `503` at once: retry in a second while the lease is valid, after the expiry while adoption is pending, and at once once the owner is adopted (the next attempt claims the shard).

## Reminders

How the node wakes shards for the reminders in the [guide](guide.md#reminders). The node knows nothing of what a reminder means: it reads one number from the shard's table and calls the app.

- **While active**, each shard has a reminders loop. It reads the earliest due reminder that isn't stale (a day old) after each flush that captured something of the shard, and wakes the app (`POST /_shardlite/reminders`, in process, never forwarded) once the reminder is due and what the read saw is durable by the gate's rule. One wake at a time per shard, a node-wide limit and a backoff after failures. A shard with a reminder due doesn't deactivate.
- **While not active**, its entry in the reminder index wakes it, through `_dispatch` (see [The reminder index](#the-reminder-index)). A reminder of an inactive shard fires within a few seconds of its time when the cluster is healthy. A minute with many entries takes longer, since the dispatcher paces them, and so does one that comes due during a failover.
- **Always covered.** A reminder committed while the shard is active is in the tail until the next delta (adoption creates an entry due now for every tail it writes a delta for), and in the entry created before that delta afterwards.
- **The `next_reminder`** recorded with a delta must match the state the delta's files reach. The node reads it between two syncs, and if anything was committed beyond what it read, records now instead: a commit not yet in the files may have removed work they hold, and a crash that loses it would bring that work back. A marked shard's is now.
- **The node writes** only the reserved table (and a system shard's schema and first reminder), at activation, before the app has the shard. It never writes a reminder.

## Rebalancing

Rebalancing (stage 3) moves shards on purpose and rarely, never as a side effect of where requests land. An overloaded node evicts shards (see [Takeover](#takeover)), and the next request or reminder claims each where it lands. A drain rebalances everything away first, so its shards stay available while they move, and stops with little left. Shards that are never idle (open WebSockets) can only move if their connections are ended, which the policy decides.

- Balance on the counts of active shards and connections in leases, as Akka and Orleans do. Node CPU includes the app's and can't be attributed to shards.
- Move only from nodes clearly above average or over their own CPU threshold, choose shards by their request rate, cap moves, prefer idle shards and don't move one again soon.
- When one hot shard overloads a node, move other shards away rather than the hot one, which would only relocate the problem.
- Skip pinned shards.

Apart from these moves there is no data rebalancing. Blob storage holds everything, and local disks are caches.

## Coordination

Coordination needs no service of its own. Leases and manifests are objects changed with conditional writes, which are linearizable on the stores shardlite supports, as SlateDB coordinates its writers. There is no leader, no election and no set of stable nodes. Claims and lookups take tens of milliseconds, but only on the cold path, next to a restore that costs as much.

### Blob storage

Shardlite needs create if absent, conditional update, conditional delete, get, head, delete and listing (which may lag, since listings only trigger checks). Each backend implements one small interface with single attempts. Callers go through `blob.Store`, written once, which makes writes safe to retry (see [Lost responses](#lost-responses)). ETags are opaque: stores differ on whether identical bytes give the same one.

v1 has an S3 backend (covering compatible stores that pass the startup check) and an in-memory one for tests, held to S3's behaviour by one conformance suite. The node checks its store's conditional writes on a scratch key at startup and refuses to start if they misbehave.

## Garbage

Some writes leave LTX files no manifest lists: a rebase or delta whose manifest update never lands (a crash, or a zombie's failed CAS), and a deleted shard whose node crashed before deleting its files. A restore never reads them, so they only cost storage.

The sweeper (`_sweep/_cluster`) deletes them. It walks `data/shards/` in batches from a cursor kept in its shard, one batch per wake, and deletes files no manifest lists once they are older than the journal's expiry (by then no upload can still be on its way to a manifest). It checks the lease before each delete and a file already gone counts as deleted, so a batch repeated after a move does no harm. Journal segments are each node's own to expire. Dead nodes' leases stay, since a node of the same name starting again at epoch 1 would collide with its old files (retiring leases is on the [roadmap](roadmap.md)).

## Startup checks

Refuse to start if the store's conditional writes misbehave or the data directory is on a network filesystem. Warn if the scratch directory is not on tmpfs, or `vm.max_map_count` or the open file limit is too low for the expected active shards.

## Testing

- **Fault injection,** including apps that checkpoint themselves and crashes between every step of capture, snapshots and restore.
- **Fencing** is tested by killing, pausing and partitioning nodes. A paused owner is the classic zombie.
- **The protocol** is checked in Quint before it is implemented.
- **Time** (lease expiry, timeouts) runs on `testing/synctest`'s fake clock.
