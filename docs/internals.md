# shardlite internals

How the node works.

The app-facing behaviour is in the [guide](guide.md).

## Terms

| Term | Meaning |
|---|---|
| **Node lease** | A lease held by each node and renewed regularly. All of a node's ownerships depend on it. |
| **Node journal** | Each node's journal in blob storage: numbered segment objects, one per flush interval, holding every shard's commits since its last snapshot. The shared WAL for the node's shards. |
| **Snapshot** | A shard's state as of one commit, stored as immutable LTX files in blob storage: a base with every page plus deltas with the pages changed since. Taking a snapshot writes a delta, or a new base when the shard is due a rebase. An inactive shard lives only in its snapshot. |
| **Manifest** | The per-shard record, an object in blob storage: owner, the owner's node epoch, the shard's LTX files, and the node journal holding later commits. |

The other terms are in the [overview](overview.md#concepts).

## Implementation

The node is written in Go and uses [Litestream](https://github.com/benbjohnson/litestream) as a library for WAL capture, checkpoint control and LTX files, with the [LTX library](https://github.com/superfly/ltx) for building deltas and restoring. Shardlite pins a Litestream version and follows its fixes, since WAL capture is the hardest part to get right.

- **Driven by the node.** Litestream's compaction timers and WAL polling are turned off (`CompactionMonitorEnabled`, `MonitorInterval`). The node calls `Sync` for changed shards from its flush loop and builds deltas and rebases itself.
- **Structured concurrency.** A goroutine waits only for goroutines it started, for its own I/O and for node-wide loops (such as the flush loop, which group commit relies on) that never wait for their callers. Children never wait for their parent or for each other, so waits can't form a loop and can't deadlock. Every goroutine has an owner that joins it, up to the node's `Run`. Callbacks from libraries (Litestream's storage client, for example) never block on anything the node drives: the client queues a file and returns, and durability is waited for afterwards by whoever started the sync.
- **Spotting changes.** Before Litestream opens a shard, the node maps the first page of its `-shm` read-only and closes the descriptor at once, so no lock is dropped. Each flush it compares the wal-index header there, which SQLite updates on every commit before the commit returns, and syncs the shards whose header changed. That costs a few microseconds per active shard per flush (about a tenth of a core per 10,000 active shards) and misses no commit.
- **A storage client of its own.** Litestream writes each sync through a `ReplicaClient`. Shardlite's client puts every shard's level 0 file into the node's pending segment and returns once that segment's PUT has succeeded, so concurrent syncs across shards share one segment.
- **Numbering per activation.** Each activation starts Litestream from scratch, with a fresh metadata directory and its own commit numbers. A new shard starts at 1, with a first sync of the empty database. A restored or cached shard is seeded so Litestream continues from its current state without a snapshot (see below). As it captures each commit, the node gives it the shard's own position, continuing from the restored one. Journal entries carry that position, and deltas are encoded with positions, so Litestream's numbers never reach manifests or restores.
- **Seeding a restored shard.** Before Litestream opens a restored or cached shard, the node checkpoints away any WAL the last activation left (with a connection of its own, `wal_checkpoint(TRUNCATE)`), writes a fresh WAL holding one committed frame that is a copy of page 1, and writes a level 0 file of its own for Litestream commit 2: no pages, the database's length, the WAL's salts and the end of that frame. Litestream's first sync then finds the WAL where the seed says and continues incrementally, so activation reads a few pages, not the database. A WAL with only a header doesn't work (SQLite starts a new one with new salts at the first write, and Litestream then takes a snapshot), and commit 1 would be a snapshot holding every page. This relies on Litestream's on-disk layout and how `verify` and `Pos` read it, which isn't an API: `TestSeedRelianceOnLitestream` fails if an upgrade changes it. An app that checkpoints with `TRUNCATE` or `RESTART` before its first commit still causes a snapshot.
- **What it leaves in shards.** Litestream keeps `_litestream_seq` and `_litestream_lock` tables in every shard.
- **Metadata on tmpfs.** Litestream stages each sync as an LTX file in a metadata directory per shard, with an fsync and a rename every time. The node keeps these directories in its scratch directory, which belongs on tmpfs: nothing in it needs to outlive the node process (a restart is treated as a crash), and fsyncs there cost nothing. They hold the tails not yet written as deltas, and for a commit that rewrites the database (an import, a `VACUUM`) Litestream's staging copy of it, as large as the commit, until its delta. After a delta the node deletes the files Litestream no longer needs, and shrinks the newest to its header and at most one page.
- **Snapshots of the live database.** A rebase reads the shard through Litestream's `DB.SnapshotReader`, never a descriptor of the node's own. Under the same hold that keeps syncs out, the node syncs and waits so every local commit has a shardlite position, reads the highest Litestream commit number its client has seen and that commit's position, then opens the reader, which fixes the position it covers and takes the checkpoint lock in one step. If Litestream's commit number differs from the client's highest, the snapshot is refused. Syncs resume afterwards and add commits above that position, and the reader only reads the WAL up to the offset it fixed. The reader is closed as soon as the snapshot is copied to a temporary file, before the upload.
- **Temporary files.** Anything the size of a shard (restores, rebases, large deltas) goes through temporary files in `{data}/.tmp`, never through memory or the scratch directory (which is memory on Linux). The node empties that directory at startup, so a crash's leftovers go, and removes each file when done. Restoring a 1 GB shard peaks at about 90 MB of node memory. Deltas are built as a merge of the tail's LTX files (their pages are in order, and the newest wins), written as they go to a buffer that spills to a temporary file past 4 MiB. Tail files, segment bodies and the journal entries a restore or an adopter reads go through the same spool, reference counted so a tail file and the segment holding it share one copy. A commit that rewrites a 1 GB database costs about 2 GB in `{data}/.tmp` while it is in flight and about 100 MB of node memory.
- **Per-shard cost.** Litestream holds a connection and a read transaction on each active shard, as any capture without a VFS must. Stage 0 measured roughly 0.4 to 0.8 MB of memory, 6 to 7 file descriptors and 2 to 3 goroutines per active shard on a laptop, so overhead is low: 5,000 active shards took about 2 GB and 34,000 file descriptors.

The proxy uses the standard library (`httputil.ReverseProxy`, with HTTP/2 without TLS for gRPC), and `quic-go` can add HTTP/3 and WebTransport later. Blob storage goes through a small interface with an S3 backend and an in-memory one for tests (see [Coordination](#coordination)). Fencing is mandatory, since GC pauses are only one of the ways a node can keep acting on an expired lease (VM pauses and network delays are others). Lease expiry and timeouts are tested with `testing/synctest`'s fake clock.

## Ownership and fencing

Follows Kleppmann's [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html): a lease alone cannot prevent two writers (GC pauses, VM pauses, network delays, clock jumps), so the storage being written to must reject writes from stale owners. Here that is the node journals in blob storage (each with a single writer), together with the manifest. The protocol is specified in Quint (see [specs/](../specs/)).

### Node leases

Each node holds one lease, an object in blob storage (`nodes/node-b`) with its epoch, address, whether it serves, counts of active shards and connections, the range of its journal's segments still needed and last renewal time, renewed every few seconds with a conditional write (`If-Match` on its ETag). Other nodes judge a lease valid until its last renewal plus the TTL. That assumes roughly synchronised clocks, so the holder treats its own lease as expired a safety margin early. A node that loses its lease exits and restarts as from a crash. Shard ownership is valid only while the owning node's lease is valid. This keeps renewal cost per node rather than per shard (per-shard leases would mean a million renewals every few seconds at a million shards, and on S3 they would cost tens of thousands of dollars a day). It also lets one expired lease release all of a crashed node's shards at once.

Renewal is the only regular write. The only regular read is a listing of `nodes/`, and other reads happen when something needs them:

- **Checking an owner:** a node forwarding a request reads the owner's lease only when the owner doesn't answer, to choose between failing fast and claiming once the lease has expired.
- **Noticing dead nodes:** each node lists `nodes/` every N × 2 seconds at a random offset, with N the number of nodes in its last listing, so the cluster as a whole lists about every 2 seconds (about 7 USD a month on S3, whatever its size). A renewal rewrites the lease object, so the listing's last-modified times show expired leases without reading them. Any node that sees one adopts it by CAS on its lease object (see [Takeover](#takeover)), so two nodes can't both adopt it. A stale listing only delays detection. A dead node is noticed about a second after its lease expires.
- **Addresses, whether nodes serve and counts** are read when first needed and cached. Counts are read when a node rebalances. They are read again when a manifest names an unknown node, when a node answers that it doesn't serve and after an adoption.

Gossip between nodes could later spread counts faster than lease renewals, if rebalancing needs fresher figures. It would only tell nodes where to look: leases and conditional writes still decide everything.

### Manifest

```
manifests/users/42
{
  "generation":  "k3x9",      // random, set at creation; part of the shard's object paths
  "owner":       "node-b",
  "node_epoch":  17,          // +1 each time node-b acquires its lease
  "files":       ["1-1201.12", "1202-1650.12", "1651-1834.17"],  // base and deltas: shards/users/42/k3x9/{range}.{epoch}.ltx
  "base_at":     1791900000000, // Unix ms when the base was written, so any node knows when the shard is due a rebase
  "journal":     "node-b"     // the node journal holding commits after the last file, if any
}
```

- An object in blob storage, created with `If-None-Match` and changed only by compare-and-swap (`If-Match` on its ETag).
- Created when a shard is first used, written on ownership changes and snapshots, and deleted when a shard the app never wrote to deactivates. Not written on activation of a shard the node already owns or on writes.
- Ownership is invalid if `node_epoch` does not match the owner's current lease, or that lease has expired.
- Ephemeral shards' manifests record `ephemeral` at creation.

### Fencing

- Each node journal has exactly one writer, its node. Segments have sequential numbers (`journal/node-b/1835`) and are created with `If-None-Match`, so a number can be written only once. A writer that finds a number already taken by something other than its own segment has been fenced and stops for good (see [Lost responses](#lost-responses)).
- To fence a dead node's journal, the node adopting it (see [Takeover](#takeover)) starts from the latest segment number recorded in the dead node's lease, probes forward until it finds a free number and creates an empty segment there, which is the fence. If the zombie takes that number first, its segment counts and the adopter tries the next one. A zombie can therefore only write to its own journal, and only below the fence. SlateDB fences its WAL writers the same way, with a zero-byte WAL object.
- A node uploads one segment at a time, so segments succeed in order. If an upload is still running when the next interval comes, its commits go into the following segment. A PUT takes well under the 250 ms interval, so this rarely delays anything. A segment whose outcome stays unknown is retried with the same write id until the outcome is known, holding back later segments and their gates, rather than given up.
- Whatever reached the journal before the fence is part of the shard's history. Nothing the zombie does afterwards is.
- A zombie can still upload LTX files, but a file only counts once a manifest CAS adopts it, and a stale owner's CAS fails. Its files are never used and are garbage collected.
- None of this needs listing objects: segments are found by number, and reads of single objects are strongly consistent on every store with conditional writes.
- A shard that is deleted and created again gets a new generation, so it never reads the old shard's pages.
- Conditional deletes are what lets a manifest be deleted without a zombie deleting its successor's.

A zombie's writes after its journal is fenced are never part of the shard's history, so data in blob storage is never overwritten by a stale owner. Writes it committed locally but never got acknowledged are lost.

### Lost responses

Any write to blob storage can succeed while its response is lost, so a retry can't tell "my write failed" from "my write succeeded and I didn't hear". `blob.Store`, the one type every caller writes through, handles this for every object and every backend (see [Coordination](#coordination)):

- Every write carries a write id (the writer's node, epoch and a counter), stored in the object's metadata.
- `blob.Store` retries ambiguous failures (timeouts, dropped connections, server errors) itself.
- If a retry fails with "exists" or "conflict", it reads the object's metadata. Its own write id means the earlier attempt succeeded, and the write returns that object's ETag. Anything else is a real conflict, returned to the caller.
- A delete whose retry finds nothing counts as done.

Callers therefore only ever see success or a conflict with someone else. For a fence, that is exactly the adopter's question: its own fence, or the zombie's segment.

Calls and forwarded requests have the same problem one level up, which is why the node never retries a request the app may have seen.

### Takeover

- **Graceful handoff** (migration, drain, rebalancing): the owner stops accepting, finishes in-flight work and deactivates the shard and writes a delta for its tail. The new owner claims the manifest by CAS and downloads the snapshot. Later, it can instead copy the SQLite file directly from the old owner's local cache.
- **Crash**: when a node's lease expires, the first live node to notice adopts it by CAS on its lease object (`adopted_by`). The adopter fences the dead journal, reads its segments, writes a delta for every shard with a tail there and adds it to that shard's manifest. It then releases the dead node's shards by CAS on their manifests. Requests for those shards fail fast until then (seconds, longer for a busy dead node), and afterwards claim them as usual. If the adopter dies too, its own lease expires and both journals are adopted. Writes that hadn't reached blob storage are lost, except for shard kinds with sync on.

A move therefore copies nothing between node journals, and a dead node's journal is gone within minutes of its lease expiring.

## Storage

| Tier | Holds | Role |
|---|---|---|
| Owner's local disk | the working copy, plus a cache of recently used shards | all reads and writes |
| Blob storage (S3 or compatible) | one node journal per node and LTX files per shard | system of record, restores |

| Data on a node | Memory and CPU | Disk |
|---|---|---|
| Active shards it owns | open in the app, WAL captured by the node | live SQLite files |
| Cached shards (deactivated) | none | files at rest |

Cached shards cost only disk. They have no app connection, no WAL capture and no open files. The node keeps ownership of them while its lease is valid, so reactivating one needs no blob storage request and no download. When disk fills up, the node evicts the least recently used cached shards. It releases the shard by CAS on its manifest and drops the local files. The shard then lives only in its snapshot, and the next request claims it like any unowned shard. Each node keeps an on-disk index of its cached shards (a SQLite file), since at a million shards an in-memory index would cost around 100 MB.

### Node journals and snapshots

Like a database's WAL and data files, and the same split as HBase's write-ahead log per server and store files per region: each node keeps one journal in blob storage, the shared WAL for its shards, and each shard's LTX files are its data files. Taking a snapshot is the checkpoint.

- **WAL capture.** For each active shard, the node reads committed frames from the shard's `-wal` file and controls checkpoints so no frame is checkpointed away before it is captured. It holds a read transaction and checkpoints itself at safe points, and detects WAL restarts. Litestream does this (see [Implementation](#implementation)).
- **Format.** Commits and snapshots are [LTX](https://github.com/superfly/ltx) files (version 3, the format of LiteFS and Litestream 0.5): a header with the page size, the database size and a range of commit positions, the changed pages compressed one by one, a page index and a trailer with checksums. Restores verify each file's checksums. LTX can also carry a checksum of the whole database before and after each file, which would catch bugs in shardlite's own apply code, but it needs a checksum per page for every active shard (later).
- **Segments.** Every flush interval, the node writes one segment object (`journal/node-b/1835`) holding that interval's commits as one LTX record per commit, plus a footer listing which shards it holds and each entry's positions. A journal entry is already covered by the shard's files if its last position is at most the manifest's last position. The PUT rate depends on the number of nodes, not active shards. Idle nodes write nothing.
- **Snapshots as deltas.** When a shard deactivates, or an active shard's tail is older than `SHARDLITE_DELTA_AGE`, the node writes the pages changed since its last snapshot as one delta (`shards/users/42/k3x9/1651-1834.17.ltx`, with the writing node's epoch, so a crashed node's leftover file never collides with a new one), from pages it already captured. It adds the delta to the manifest by CAS. The SQLite file itself is never read for this. A cached shard therefore never has a tail, so the lease's range of segments only covers active shards, and eviction, handoff and the restart check never find unwritten commits.
- **Rebasing.** A shard is due a rebase once its base is older than the rebase interval (a day) or its manifest lists more than 8 files. A snapshot of a due shard then writes a full copy of the live database as a new base (`1-1834.17.ltx`) instead of a delta, points the manifest at it alone by CAS and deletes the old files. A due shard with no tail is rebased too if that leaves fewer files, so a shard that only gets short sessions rebases on its next activation. A manifest without `base_at` counts as due. At most 4 rebases run at once on a node, and a rebase holds the shard's operation lock, so deactivating that shard waits for it. A shard then has a base and a handful of deltas, a player who changes three pages uploads three pages and a shard pays for a full upload at most once a day.
- **Deltas only where there is no database.** Deactivation closes Litestream before its final delta, and an adopter has only the dead node's journal, so both always write deltas. Rebasing before release would hold the shard for a whole upload.
- **Cleaning the journal.** Segments expire `SHARDLITE_JOURNAL_EXPIRY` after they are written, and the node refuses to start if that is less than 4× `SHARDLITE_TAIL_ALERT`. Each node deletes its own expired segments with a sweeper, on every backend: it lists `journal/{node}/` (which gives sizes) and deletes non-empty segments older than the expiry age. On S3 deletes are free and a listing returns a thousand keys, so this costs a few hundred listings a day per node. The sweeper starts only once the node has adopted its own journal, so a node that was down for longer still finds its old segments. An adopter deletes a dead node's non-empty segments once it has fenced the journal and written the deltas. Only non-empty objects are deleted, so fences stay: a zombie paused for longer than the expiry could otherwise write into a freed number and have its gate acknowledge a write no one reads. Fences are tiny, one per adoption. Tails become deltas within minutes, so nothing needs to track which segments are still in use. The lease records the range of segments still needed, so an adopter knows where to read, and a node logs an alert if any tail is older than `SHARDLITE_TAIL_ALERT`, which means its deltas are failing, long before its segments expire. Node journals therefore hold minutes of live data, and shards that are never used again cost only their LTX files.
- **Restore** downloads the base and deltas in parallel to temporary files, applies each from there and deletes it, then applies the tail, if any, in order, from its spool. Each file's checksums are verified as it is applied. A shard that isn't active has no tail.
- **Restarting.** A restart is treated as a crash, so there is one recovery path. The restarted node knows its previous process is gone, so it adopts its own journal at once instead of waiting for the TTL: a new epoch by CAS on its own lease, then fencing, deltas and releases as any adopter does. It keeps its cache but checks it: a cached file whose last commit matches the latest file in its manifest is reclaimed by CAS on its next activation and reused. A file that is ahead holds commits that were never flushed, so it is discarded and downloaded again, losing at most a flush interval as any crash does. The on-disk index records each file's position, so the check is cheap.
- **Old files.** Rebases delete the files they supersede, and shardlite keeps no history. A rebase that fails after its upload (or a crash in between) leaves an unreferenced base in the bucket, which nothing cleans up yet.
- **Draining.** A node scaling down deactivates its shards and writes deltas for their tails, releases ownership and deletes its journal's non-empty segments before it exits (every tail is a delta by then).

### Durability gates

The gate needs no parsing. The node treats responses, streams and calls to other shards alike, as bytes. A plain HTTP response is just a stream with one or two chunks.

- Every flush captures each active shard's new frames up to the moment it starts. So a chunk the node reads is held until a flush that started after it was read has completed. The node only keeps a counter of flushes.
- Chunks from a shard with no new frames since its last capture pass straight through.
- Chunks waiting at the same time share one flush (group commit). One node journal flush releases every waiting chunk on the node, whatever shard it is for.
- A call to another shard is bytes leaving the caller, so the caller's node holds it until the next flush if the caller's kind has sync on.
- `sync` waits for a flush that started after the call, and fails if the node has been fenced.

Spanner's commit wait is similar: the commit is already applied and only the acknowledgement is held until a point in time has passed, though Spanner waits out clock uncertainty for ordering rather than for durability.

## Activation

1. **Owned by another node with a valid lease:** forward the request there. The owner activates it.
2. **Cached on this node:** open the files, start capturing the WAL and pass the request to the app, which checks its schema version. No blob storage request.
3. **Unowned, or the owner's lease has expired:** claim the shard by CAS on the manifest, then restore it from its snapshot. Then continue as in step 2.
4. **New shard** (no manifest): create the manifest with a conditional create, which claims the shard, and start it as an empty file. Then continue as in step 2.

Placement prefers the local node: an unowned shard is activated on the node that received the request or call for it, if it serves. Otherwise it goes to a node that does.

Concurrent requests for the same shard share one activation (`singleflight`). Shards deactivate after their kind's idle timeout, unless a connection or undelivered messages hold them. Deactivation waits until the shard's last commit is in the node journal, stops capturing, writes its tail as a delta and closes the database. The files stay in the local cache.

### Creating shards

- The node creates the manifest with a conditional create, which claims the shard as any claim does, and starts capturing an empty file before the app gets it. If another node creates it first, the shard is simply owned by that node, and the request goes there like any other. The app never sees a shard its node doesn't own.
- The node creates the reserved tables at activation, before the app gets the shard, as Litestream creates its own.
- When a shard deactivates, the node checks through Litestream's connection, just before the final sync, whether it holds anything of the app's (a table, an index, `user_version` or `application_id`) or rows in shardlite's tables. If not, the node deletes its manifest by conditional delete and its local files instead of keeping it.
- Ephemeral shards are created the same way but never captured. On deactivation the node deletes the local file and the manifest (by conditional delete), so a shard costs two manifest writes in its life.

### Deleting shards

The owner deactivates the shard, deletes the manifest by conditional delete (`If-Match` on its ETag), then its LTX files and local files. Any tail left in its node journal is never read again and is swept with the rest of the journal. A crash before the manifest is deleted leaves the shard as it was.

### Letting go of a shard

Deactivating, moving, evicting or deleting a shard is only safe once the app has closed it, or a connection left open could keep writing to a file the node no longer captures. The node checks SQLite's own lock: every connection to a WAL database holds a shared lock on the deadman switch byte of its `-shm` file (byte 128) while it is open, and SQLite itself uses `F_GETLK` on that byte to tell whether it is the first connection. The node asks the same way, which neither takes the lock nor races a connection that is opening, and works across containers on one kernel.

`fcntl` locks belong to a process, and closing any descriptor on a file drops all of that process's locks on it. So the node only checks a shard while it holds nothing open on that shard's files: it closes Litestream's connection and file handles first, then checks. Closing the check's descriptor then has nothing to drop, and the app's locks are never affected. Apart from this check, the read-only mapping of the wal-index header and seeding before Litestream opens a restored shard, nothing in the node opens a shard's files except through Litestream. Seeding is safe because the app can only open files it was given, and it isn't given the path until activation completes.

If another process still has the shard open, the node reopens capture and doesn't let go. Litestream resumes where it was, or takes a snapshot if the app checkpointed in between. The same happens if the app committed after Litestream's final sync and closed the file before the check: the node compares the wal-index header read just after Litestream closes with one read after the check.

- **Idle deactivation:** the shard stays active, and the node tries again later, with a warning.
- **A move, eviction or delete:** refused with an error. Rebalancing picks another shard.
- **Draining the node:** it waits up to a timeout, then aborts the drain and keeps running with the shards still open, with an error. The node never lets go of a shard that is open.

The same check guards against apps opening files they weren't given. Before activating a cached shard (when it has nothing open on it yet), the node checks that no other process already has it open, and it logs an error for files in its data directory that it doesn't know.

## Routing and forwarding

- Requests for shards owned elsewhere are forwarded to the owner over one long-lived HTTP/2 connection per pair of nodes. WebSockets become a tunnelled stream for their lifetime. Cancellation becomes an HTTP/2 stream reset.
- Finding the owner: the node reads the manifest on a miss and caches where shards live. A stale cache costs one extra hop ("not mine, try X"). Node addresses come from the leases.
- **Fail fast.** If a node stops answering, the forwarding node reads its lease and answers `503` at once instead of letting requests pile up until they time out. A shard has one owner, so there is nowhere else to send them until its lease expires and the shard can be claimed after adoption.
- **Holding during handoff** (later): a request that reaches a shard while it is moving is held until the new owner is ready, then forwarded, for at most a few seconds. After that the node returns `503` with `Retry-After`. Only requests the app hasn't seen are held or retried, so nothing is applied twice and a failure is still reported as one.
- Calls between shards go through the same path. If fan-out becomes a measured bottleneck, a node could batch concurrent calls to the same remote node without any change to the API.
- Traffic between nodes carries a protocol version, and manifests, node journal keys, snapshots and the local index carry a format version, so old and new nodes can work together during a rolling upgrade.

## Message delivery

How the node moves rows through the outbox and inbox described in [Messages](guide.md#messages).

- **Intake.** After a flush cycle in which a shard changed, its node reads new outbox rows through Litestream's connection (never a descriptor of its own) and sends each to its target's node as the call it will become (`POST` to `path` with the body, `Content-Type`, `Shardlite-Hops` and `Shardlite-Caller`), marked as an intake with `Shardlite-Inbox: true`, which only nodes set (it is stripped from apps and clients like other `Shardlite-*` headers). The receiver's node inserts the row into the receiver's inbox in a commit of its own (with `sender` set, `hops` plus one and due now) and answers `202` once that commit is durable. The sender's node then deletes the row. Many intakes to one receiver share its flushes, so a burst waits for one flush rather than one each.
- **Delivery to the handler.** The row holds the call, not an HTTP request: everything else is set by the node. Before each retry the node reads the row again from durable state and drops the attempt if the row is gone or its due time changed.
- **Giving up** is held in memory for as long as the shard stays active, and retry counts are in memory too. The row itself holds nothing about attempts apart from the outbox's `error`.
- **Liveness.** A shard with outbox rows, or inbox rows due now, that the node hasn't given up on doesn't deactivate, and its node keeps delivering without any request arriving. While a shard is active the node keeps a timer for its earliest later inbox row, and after the flush that changes that time it writes a wake hint for it. After a crash, adoption writes a wake hint for now for every shard it replayed a tail for, so the scan wakes them through normal routing, on whichever node claims them. A tail is the only sign an adopter has that a shard may have work, so a delta or a rebase that is about to take the tail of a shard with a live outbox row away writes that shard's hint for now first (and the delta waits for the next pass if it can't). Shards with nothing to deliver cost nothing extra. At shutdown the node keeps delivering for a few seconds before letting go of its shards, and gives each shard that still has outbox rows a wake hint.
- **Wake hints.** An empty object `wakes/{minute}/{shard}`, keyed by the time it is due in UTC at minute resolution (`wakes/2026-10-13T14:05Z/users/42`), for shards that need waking while no node has them active. Keys list in time order, so nodes list `wakes/` from the start up to now, at startup and every minute, which also finds hints missed while no node was running. Wake scans spread over the nodes, as lease listings do. Each due shard is activated through normal routing (never created, since a hint for a shard that no longer exists is just deleted), and its hint is deleted once the shard has nothing left to deliver for that time. The shard's tables are the truth and a hint only means "look", so duplicate or stale hints are harmless.
- **Ephemeral shards** get no wake hints.
- **Pinned kinds** (later). An intake for a pinned shard with no pin still stores the row (the node holds the shard just long enough to insert it), but the rows don't keep the shard active, get no wake hints and aren't delivered until a pin.
- **The node's writes** (deleting delivered outbox rows, setting `error` on those it gave up on, and intake into the inbox) are ordinary commits, captured and shipped like the app's. They stop before a shard's final sync at deactivation.

## Rebalancing

Rebalancing moves shards on purpose, rarely, rather than as a side effect of where requests land:

1. An overloaded node picks shards to move (busy ones first) and hands them off to less loaded nodes (see [Takeover](#takeover)).
2. It closes those shards' WebSocket connections with a reconnect close code, or sends HTTP/2 `GOAWAY` (`Connection: close` on HTTP/1.1) to their HTTP clients.
3. The client reconnects. With a controllable load balancer it reaches the new owner directly. Otherwise it lands anywhere and is forwarded.

Rebalancing skips pinned shards.

Policy:

- Balance on counts of active shards and connections, published in leases, as Akka and Orleans balance on shard and activation counts. Player shards are similar enough for counts to approximate load. Node CPU includes the app's and can't be attributed to shards, so it isn't shared.
- Move only from nodes clearly above average, or from a node over its own CPU threshold even if its counts look average.
- Choose which shards to move by their request rate, which each node measures for its own shards.
- Cap moves per interval.
- Prefer idle shards and connections.
- Don't move a shard again soon after it moved.
- When one hot shard overloads a node, move other shards away rather than the hot one, which would copy it, disrupt its traffic and only relocate the problem. Move only as many as needed: a shard whose work is bound to one core (its writes are serialised) can't use a whole node.

Apart from these moves there is no data rebalancing. Blob storage holds everything, and local disks are caches.

## Coordination

Coordination needs no service of its own. Node leases and manifests are objects in blob storage, changed with conditional writes, which are linearizable on the stores shardlite supports. That is enough for agreement on ownership, the same way SlateDB coordinates its own writers. Every node stays disposable: there is no leader, no election, no consensus protocol of shardlite's own and no set of stable nodes. Claims and lookups take tens of milliseconds, but they only happen on the cold path (unowned shards, moves and routing-cache misses), next to a restore that costs as much. Orleans accepts the same for cold activations.

Node leases and manifests sit behind interfaces:

```go
type NodeRegistry interface {
	Acquire(ctx context.Context, info NodeInfo) (NodeLease, error)                  // info: name, address, whether it serves. The lease carries its epoch
	Renew(ctx context.Context, lease NodeLease, info NodeInfo) (NodeLease, error)   // info can change when the kinds file is reloaded
	Release(ctx context.Context, lease NodeLease) error                             // graceful shutdown
	Get(ctx context.Context, node string) (*NodeLease, error)                       // another node's lease, if valid
	List(ctx context.Context) ([]NodeLease, error)                                  // live nodes, from a listing of nodes/
}

type ManifestStore interface {
	Get(ctx context.Context, shard ShardID) (*Manifest, Version, error)
	Put(ctx context.Context, shard ShardID, m *Manifest, expected *Version) (Version, error) // nil creates
	Delete(ctx context.Context, shard ShardID, expected Version) error                      // conditional: deleted and never-written shards
}
```

### Blob storage

The only primitives shardlite needs are create if absent, conditional update, conditional delete, get, head, delete and listing (which may lag, since listings of `nodes/`, `journal/` and `wakes/` only trigger checks). Each storage backend implements one small interface, used for everything (leases, manifests, segments, LTX files and wake hints). Backends do single attempts and nothing else. Callers never use a backend directly: they go through one `blob.Store` type, written once, which wraps any backend and makes writes safe to retry by storing each write's id in the object's metadata and recognising its own earlier attempts (see [Lost responses](#lost-responses)). `blob.Store` has the same methods. Bodies are seekable, so a retry rewinds a file instead of the store holding it in memory.

```go
type Backend interface {
	Create(ctx context.Context, key string, body io.ReadSeeker, id WriteID) (ETag, error)                  // If-None-Match: *. ErrExists if someone else's object is there
	Update(ctx context.Context, key string, body io.ReadSeeker, expected ETag, id WriteID) (ETag, error)   // If-Match. ErrConflict if someone else changed it
	Get(ctx context.Context, key string) (io.ReadCloser, ETag, error)                     // ErrNotFound if missing
	GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) // segment footers and LTX trailers
	Head(ctx context.Context, key string) (ObjectInfo, error)                             // size (empty segments are fences), ETag, last modified, write id
	Delete(ctx context.Context, key string, expected ETag) error                           // If-Match when expected is set
	List(ctx context.Context, prefix string) iter.Seq2[ObjectInfo, error]                  // key, size, last modified. May lag
}
```

Each backend:

- maps its responses onto a fixed set of outcomes: exists, conflict, not found, not applied (safe to retry) and unknown (the write may have happened). `blob.Store` only reads back the write id after an unknown outcome;
- stores the write id atomically with the object (part of the same request on S3, GCS and Azure).

Callers treat ETags as opaque version tags: S3 and most compatible stores derive them from an object's contents (an MD5 for single-part uploads), while Azure and GCS change them on every write, so nothing may assume that identical bytes do or don't give the same ETag.

v1 has one backend for deployments, the S3 API (which also covers S3-compatible stores that pass the startup check), and an in-memory backend for tests. The memory backend keeps objects in a map under one lock, with ETags that are the contents' MD5 as on S3. Tests that run several nodes in one process share one instance, and tests that run nodes as separate processes use RustFS (which passes the conformance suite). One conformance suite runs against every backend, so the memory backend can't drift from S3's behaviour. Native GCS and Azure backends can follow. Litestream's own storage clients aren't used, since they don't expose conditional writes.

The node checks its store at startup, on a scratch key under its prefix, and refuses to start if the store misbehaves: a conditional create succeeds and a second one fails with exists, an update with a stale ETag fails with conflict, the write id comes back with the object, an update of a missing key fails with not found instead of creating it, and a delete with a stale ETag fails with conflict and leaves the object. It is a handful of requests, so it costs nothing.

## Startup checks

Refuse to start:

- the store's conditional writes misbehave (above);
- the data directory is on a network filesystem (NFS, SMB or FUSE);
- `SHARDLITE_JOURNAL_EXPIRY` is less than 4× `SHARDLITE_TAIL_ALERT`;
- clock skew against the store's `Date` header beyond the lease safety margin (warning at half of it, and watched while running).

Warn:

- the scratch directory is not on tmpfs;
- `vm.max_map_count` or the open file limit (the hard limit, since Go raises the soft one) is too low for the expected active shards.

## Testing

- **Fault injection from the start,** including apps that checkpoint themselves and crashes between every step of capture, snapshots and restore.
- **Fencing** (one writer, a zombie's writes after its journal is fenced ignored, no acknowledged write lost from a kind with sync on) is tested by killing, pausing and partitioning nodes. A paused owner is the classic zombie.
- **The protocol** (leases, fencing and adoption, with lost requests and lost responses modelled) is checked in Quint before it is implemented.
- **Time** (lease expiry, timeouts) runs on `testing/synctest`'s fake clock.
