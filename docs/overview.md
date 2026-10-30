# shardlite

Actors made of SQLite databases, served as virtual actors or pinned to your own servers.

## Goal

Make it easy to scale a game at launch. Game state is split into many small **shards** (a player, a match, a guild, a zone). Each shard is a SQLite database with exactly one writer at a time, living on the node that is serving it. This is sharding taken to the extreme: rather than splitting a players table into a few shards, each player gets their own. Nodes can be added during a launch spike and removed afterwards.

For game teams, a player shard is like a save file per player, always saved, backed up and shared across servers. Its schema is the save format, and migrations are save-file upgrades. Many game teams know that model better than a shared database.

The same shape suits real-time collaboration tools (documents, whiteboards, design files): one shard per document, with every editor's WebSocket on it, its single writer ordering the edits and SQLite holding the document and its history. Idle documents cost only storage.

This is the Orleans / Durable Objects model, with SQLite as the state and with two differences that matter for games:

- Apps can be written in any language or framework, even Unreal dedicated servers.
- Clients can connect directly to the process holding a shard over UDP or WebRTC, not only through an HTTP proxy.

As virtual actors, any shard id can be called and the first call creates the shard, with no create step. The app creates shards and clients reach the ones that exist, so a random id from a client finds nothing and creates nothing. Clients name the shard they want as a routing hint that saves a hop, and the app confirms it from the request itself, relaying to the right shard when the hint is wrong (see [Serving](guide.md#serving)).

Shardlite keeps Orleans' operational trade-offs. Blob storage (S3 or compatible) is the system of record, run by someone else. Nodes keep shards on local disk only as a cache, so they are disposable; the node's own database there holds the journal's copy its `local` tier acknowledges, which answers for the node's crash and not the host's. Durability comes from shipping changes to blob storage, and the storage is the final check against duplicate owners (as Orleans' storage ETags are). Shardlite is a router and a cache in front of blob storage, not a database to operate.

An actor's state is always its SQLite file, never objects in memory. That makes activation and deactivation almost free:

- **Deactivating** is closing the file. Its recent changes are already in the node journal, and the node writes them as a snapshot. The data is already a file, so there is nothing to serialise. In Orleans, deactivating means serialising the grain's state and writing it to a storage service.
- **Activating** is opening the file. SQLite reads pages lazily, so a request only loads the pages it touches. Orleans reads and deserialises the whole state before the first call.
- **Memory follows the working set,** not the number or size of actors. Idle shards can deactivate early, and a node can hold far more actors than fit in memory. The OS file cache keeps recently used pages warm, so a shard that comes back quickly is still cheap.

The trade-off is that reads go through SQLite (microseconds from cache) rather than plain objects, and anything the app keeps in memory outside SQLite is lost on deactivation.

A shard is an actor, but not a turn-based one. Requests for the same shard run concurrently, as they would on any database, and the app's transactions are the concurrency control: a handler that reads and then writes has to do both in one transaction. Orleans and Durable Objects give a grain one turn at a time, so an app written for those loses updates here.

The same goes for shardlite's own state about a shard: it lives in the shard, and anything global only points into it. Reminders are rows in the shard's own table, written in the app's transactions, the way an intrusive list keeps its links inside the elements. Where something outside the shard is needed, such as the reminder index, it holds no payload, just which shard to look at when. So nothing global has to stay consistent with the shards, deleting a shard takes its state with it, and the node's memory is a cache it can lose at any time.

A shardlite app should be no more exposed to abuse than any web app. Clients can name shards directly, which a plain web app never lets them do, so the node makes sure that naming one costs no more than a request for a row that doesn't exist (or that the caller may not see) would cost a web app. What remains is what every web app already handles with the defences it already has: floods, which rate limiting at the load balancer stops, and abuse by authenticated users, which the app limits. A design that lets unauthenticated input cause blob storage writes, restores or state left behind is a gap shardlite opened, and the node closes it. One gap is still open: a guessed id of a shard that exists costs a restore before the app can refuse the request, so kinds clients can name should use unguessable ids (see [Kinds clients can name](guide.md#kinds-clients-can-name)).

## Non-goals

- **No queries or transactions across shards.** Reading two shards means two separate reads, at your own risk. Anything that needs agreement between shards goes through one shard that owns that agreement (see [Consistency model](#consistency-model)). Unlike Orleans, there are no opt-in distributed transactions.
- No consensus or replication between nodes. Each shard has one writer and blob storage is the record, with one peer mirroring each node's journal so an acknowledgement need not wait for it (see [Node journals and snapshots](internals.md#node-journals-and-snapshots)). Reads that may be stale, from any node, come from snapshot reads instead (later, see the [roadmap](roadmap.md#later)).
- No general-purpose distributed database features (cross-shard indexes, query planning).

## Concepts

| Term | Meaning |
|---|---|
| **Shard** | One SQLite database, identified by `app.kind/id` (e.g. `game.users/42`). |
| **Shard kind** | A category of shards (`users`, `zone`) with shared configuration: sync, idle timeout. A kind's name is qualified by the app that serves it (`game.users`), so a shard's name says whose it is. |
| **Node** | The shardlite process (`shardlited`), one per host (a machine, VM or Kubernetes node) and shared by the app processes on it. Written in Go. Disposable. |
| **App** | Any process on the host that uses shardlite, in any language. It serves whole shard kinds or pins specific shards. Its name is the namespace its kinds are qualified by, chosen once, since it is in every stored path. |
| **Owner** | The node allowed to write a shard. A node keeps ownership of its active and cached shards while its lease is valid. |
| **Active shard** | A shard that is open on its owner and shipping changes. Costs memory and CPU. |
| **Local cache** | Shard files kept on a node's disk after deactivation. Evictable. Costs disk only. |
| **Pin** | A process's hold on a shard: the node proxies the shard to that process and never hands it off until the process lets go. The process serves its own clients over any protocol. |
| **Gate** | The node holds a shard's output (responses, streams and calls) until the writes before it meet the kind's tier: nothing for `none`, the node's own database and the journal's peer for `peer`, blob storage for `storage` (the default), as Durable Objects' output gates do and as Orleans code does by awaiting `WriteStateAsync` before returning. A tier the node cannot meet fails the answer; no weaker tier is given instead. |

## Architecture

```mermaid
flowchart TB
  clients[Clients] --> lb["Your load balancer<br/>(TLS termination)"]
  lb --> nodeA
  lb --> nodeB

  subgraph hostA [Host A]
    nodeA[Node] <-->|localhost| appA["App (any language)"]
    appA --- diskA[("Local disk: cache")]
  end

  subgraph hostB [Host B]
    nodeB[Node] <-->|localhost| appB[App]
    appB --- diskB[("Local disk: cache")]
  end

  nodeA <-->|HTTP| nodeB

  nodeA --> blob
  nodeB --> blob
  blob[("Blob storage (S3 or compatible): the system of record<br/>a node journal per node and LTX files per shard<br/>node leases and manifests, changed with conditional writes")]
```

The node does all of the distributed work: leases, ownership, routing, shipping changes to blob storage, durability gates and moving shards between nodes. The app only opens a local SQLite file with its own driver. The app and node share a local filesystem on one kernel.

## Consistency model

- One shard, one writer. Within a shard, transactions are SQLite's (serializable), and every read and write of the shard is linearizable while its owner holds it: the guarantee etcd gives for its linearizable reads, for a whole database. The `storage` tier keeps it across the loss of the owner's host and under any clocks and pauses, because each answer confirms that the node still owns the shard; `peer` while one of the two copies stands, and given working clocks (their rates, not their synchronisation), as `none` does (see [Consistency](guide.md#consistency)).
- Across shards, consistency is the app's problem.
- Anything needing agreement goes through a shard that owns it:
  - A trade is a `trade/{id}` shard running a saga: reserve on both player shards, then commit or release, with idempotent steps and resumable state.
  - Shardlite provides no locks across shards. Apps that want them can write them as data in their shards.
  - Global data (name registry, tournament brackets) lives in a global shard. It is single-writer, so very hot global data (leaderboards) should be sharded or aggregated in batches.
  - Global shards are a single point of availability. If their owner crashes, they are unavailable until the lease expires and another node restores them.
- Payments: keep the storage tier (the default), store payments as an append-only ledger keyed by the provider's transaction id and reconcile against the payment provider.

## Prior art

- HBase and Bigtable: one write-ahead log per server shared by many regions, and log splitting when a server dies. The closest precedent for node journals.
- Neon: the same split as node journals and snapshots (safekeepers for the WAL, image and delta layers in object storage), and generations for fencing page servers.
- Litestream: snapshots plus WAL segments per database, stored as LTX files. Shardlite stores the same LTX files, reads the WAL itself and shares the WAL across a node's shards.
- BookKeeper: garbage collection of shared logs.
- WarpStream: stateless agents on object storage.
- SlateDB: an LSM tree on object storage. Its fencing of WAL writers by conditional creates is the model for node journals.
- mvsqlite: SQLite pages stored in a key-value store.
- ActorDB: a SQLite database per actor, with a schema per actor type, Raft replication and transactions across actors, queried as a database server rather than opened locally.
- Durable Objects: SQLite storage with output gates. Its alarms are what a shard's reminders follow: durable timers kept in the object's storage and retried with backoff, but one per object, where a shard's table holds any number.
- [Durable Actors](https://github.com/TerseAI/durable-actors) (Terse, 2026): an open-source take on Durable Objects. Actors are written in its TypeScript or Python SDK and run inside its runtime, with a Postgres control plane and an LTX chain per actor in object storage. Its `terse-ltx` and `terse-litestream` crates come from it.
- Pat Helland, "Life beyond Distributed Transactions": entities that change atomically only within themselves and talk through at-least-once messages, remembering what each partner has sent. Shardlite's model, almost exactly, and what the guide's durable send follows.
- SQL Server Service Broker: queues as tables inside the database, sent and received in the same transactions as the data, delivered exactly once and in order between databases. The guide's durable send is the same idea with the app's own tables, and gives at-least-once delivery with no order.
- Akka Persistence's at-least-once delivery: an actor keeps the messages it has sent but not had confirmed in its own persisted state, and sends them again until they are, as a shard keeps its sends table.
- Orleans reminders (and Dapr's and Service Fabric's): durable timers for actors, kept in a table the whole cluster shares rather than in the actor's state. A shard's reminders are rows in its own table, and the only part outside it is an empty entry in the reminder index, which says which shard to look at when.
- Restate: virtual objects with durable state, timers and messages between services, journaled in Restate's own log rather than in each object's database.
- Spanner: commit wait, which holds acknowledgements until TrueTime uncertainty has passed.
