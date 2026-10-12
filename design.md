# shardlite design

Actors made of SQLite databases, served as virtual actors or pinned to your own servers.

Status: draft. Nothing here is implemented yet.

## Goal

Make it easy to scale a game at launch. Game state is split into many small **shards** (a player, a match, a guild, a zone). Each shard is a SQLite database with exactly one writer at a time, living on the node that is serving it. This is sharding taken to the extreme: rather than splitting a players table into a few shards, each player gets their own. Nodes can be added during a launch spike and removed afterwards.

For game teams, a player shard is like a save file per player, always saved, backed up and shared across servers. Its schema is the save format, and migrations are save-file upgrades. Many game teams know that model better than a shared database.

This is the Orleans / Durable Objects model, with SQLite as the state and with two differences that matter for games:

- Apps can be written in any language or framework, even Unreal dedicated servers.
- Clients can connect directly to the process holding a shard over UDP or WebRTC, not only through an HTTP proxy.

Shardlite keeps Orleans' operational trade-offs. Blob storage (S3 or compatible) is the system of record, run by someone else. Nodes keep shards on local disk only as a cache, so they are disposable. Durability comes from shipping changes to blob storage, and the storage is the final check against duplicate owners (as Orleans' storage ETags are). Shardlite is a router and a cache in front of blob storage, not a database to operate.

An actor's state is always its SQLite file, never objects in memory. That makes activation and deactivation almost free:

- **Deactivating** is closing the file. Its recent changes are already in the node journal, and the node writes them as a snapshot. The data is already a file, so there is nothing to serialise. In Orleans, deactivating means serialising the grain's state and writing it to a storage service.
- **Activating** is opening the file. SQLite reads pages lazily, so a request only loads the pages it touches. Orleans reads and deserialises the whole state before the first call.
- **Memory follows the working set,** not the number or size of actors. Idle shards can deactivate early, and a node can hold far more actors than fit in memory. The OS file cache keeps recently used pages warm, so a shard that comes back quickly is still cheap.

The trade-off is that reads go through SQLite (microseconds from cache) rather than plain objects, and anything the app keeps in memory outside SQLite is lost on deactivation.

## Non-goals

- **No queries or transactions across shards.** Reading two shards means two separate reads, at your own risk. Anything that needs agreement between shards goes through one shard that owns that agreement (see [Consistency model](#consistency-model)). Unlike Orleans, there are no opt-in distributed transactions.
- No replication between nodes. Each shard has one writer, and blob storage provides durability. Stale reads come from snapshots and owner-fed read replicas instead (later).
- No general-purpose distributed database features (cross-shard indexes, query planning).

## Concepts

| Term | Meaning |
|---|---|
| **Shard** | One SQLite database, identified by `kind/id` (e.g. `users/42`). |
| **Shard kind** | A category of shards (`users`, `zone`) with shared configuration: sync, idle timeout. |
| **Node** | The shardlite process (`shardlited`), one per host (a machine, VM or Kubernetes node) and shared by the app processes on it. Written in Go. Disposable. |
| **App** | Any process on the host that uses shardlite, in any language. It serves whole shard kinds or pins specific shards. |
| **Owner** | The node allowed to write a shard. A node keeps ownership of its active and cached shards while its lease is valid. |
| **Active shard** | A shard that is open on its owner and shipping changes. Costs memory and CPU. |
| **Local cache** | Shard files kept on a node's disk after deactivation. Evictable. Costs disk only. |
| **Node lease** | A lease held by each node and renewed regularly. All of a node's ownerships depend on it. |
| **Node journal** | Each node's journal in blob storage: numbered segment objects, one per flush interval, holding every shard's commits since its last snapshot. The shared WAL for the node's shards. |
| **Snapshot** | A shard's state as of one commit, stored as immutable LTX files in blob storage: a base with every page plus deltas with the pages changed since. Taking a snapshot writes a delta. An inactive shard lives only in its snapshot. |
| **Manifest** | The per-shard record, an object in blob storage: owner, the owner's node epoch, the shard's LTX files, the node journal holding later commits and any pin. |
| **Pin** | A process's hold on a shard, which it serves itself over any protocol. A pinned shard is never handed off until the process unpins it. |
| **Gate** | For kinds with sync on, the node holds a shard's output (responses, streams and calls) until the writes before it are in blob storage. |

## Architecture

```
            clients
               │
        your load balancer (TLS termination)
               │
   ┌───────────┴───────────┐
   │ host A                │        ┌──────── host B ───────┐
   │  node ◄───────────────┼─ h2 ──►│ node                  │
   │   │   ▲               │        │   │                   │
   │   ▼   │ localhost     │        │   ▼                   │
   │  app (any language)   │        │  app                  │
   │   │                   │        │                       │
   │  local disk: cache    │        │  local disk: cache    │
   └───────────┬───────────┘        └───────────┬───────────┘
               │                                │
               ▼                                ▼
   blob storage: system of record (S3 or compatible), one node journal per node, LTX files per shard
   node leases and manifests: objects in blob storage, changed with conditional writes
```

The node does all of the distributed work: leases, ownership, routing, shipping changes to blob storage, durability gates and moving shards between nodes. The app only opens a local SQLite file with its own driver.

The app and node must share a local filesystem on one kernel (the same host, with a shared volume where they run in containers). Network filesystems are not supported, since they break SQLite locking.

On Kubernetes, the node runs as a DaemonSet, one per Kubernetes node, with shard files on a host volume shared with the app pods, so every pod on that node shares one node journal. Clusters that forbid host volumes can instead run one large pod per machine with the node as a sidecar, but then every app deploy restarts the node and loses its cache. Elsewhere it runs as a systemd unit or similar. There is no session protocol between app and node (see [Lifecycle](#lifecycle)).

### Implementation

The node is written in Go and uses [Litestream](https://github.com/benbjohnson/litestream) as a library for WAL capture, checkpoint control and LTX files, with the [LTX library](https://github.com/superfly/ltx) for building deltas, merging and restoring. Shardlite pins a Litestream version and follows its fixes, since WAL capture is the hardest part to get right.

- **Driven by the node.** Litestream's compaction timers and WAL polling are turned off (`CompactionMonitorEnabled`, `MonitorInterval`). The node calls `Sync` for changed shards from its flush loop and builds deltas and merges itself.
- **Structured concurrency.** A goroutine waits only for goroutines it started, for its own I/O and for node-wide loops (such as the flush loop, which group commit relies on) that never wait for their callers. Children never wait for their parent or for each other, so waits can't form a loop and can't deadlock. Every goroutine has an owner that joins it, up to the node's `Run`. Callbacks from libraries (Litestream's storage client, for example) never block on anything the node drives: the client queues a file and returns, and durability is waited for afterwards by whoever started the sync.
- **Spotting changes.** Before Litestream opens a shard, the node maps the first page of its `-shm` read-only and closes the descriptor at once, so no lock is dropped. Each flush it compares the wal-index header there, which SQLite updates on every commit before the commit returns, and syncs the shards whose header changed. That costs a few microseconds per active shard per flush (about a tenth of a core per 10,000 active shards) and misses no commit.
- **A storage client of its own.** Litestream writes each sync through a `ReplicaClient`. Shardlite's client puts every shard's level 0 file into the node's pending segment and returns once that segment's PUT has succeeded, so concurrent syncs across shards share one segment.
- **Numbering per activation.** Each activation starts Litestream from scratch, with a fresh metadata directory and its own commit numbers from 1, so Litestream always treats the shard as a new database. Its first sync is a snapshot of the shard as restored. The node runs it before the app opens the shard and doesn't upload it. As it captures each commit, the node gives it the shard's own position, continuing from the restored one. Journal entries carry that position, and deltas are encoded with positions, so Litestream's numbers never reach manifests or restores.
- **What it leaves in shards.** Litestream keeps `_litestream_seq` and `_litestream_lock` tables in every shard. Apps treat the tables as reserved.
- **Metadata on tmpfs.** Litestream stages each sync as an LTX file in a metadata directory per shard, with an fsync and a rename every time. The node keeps these directories on tmpfs: nothing in them needs to outlive the node process (a restart is treated as a crash), and fsyncs there cost nothing. They hold the tails not yet written as deltas, so tmpfs is sized for those.
- **Per-shard cost.** Litestream holds a connection and a read transaction on each active shard, as any capture without a VFS must. Stage 0 measured roughly 0.4 to 0.8 MB of memory, 6 to 7 file descriptors and 2 to 3 goroutines per active shard on a laptop, so overhead is low: 5,000 active shards took about 2 GB and 34,000 file descriptors. Each active shard also takes a few memory mappings in the node, so both `ulimit -n` and `vm.max_map_count` (65,530 by default, reached at about 10,000 active shards) have to be raised.

The proxy uses the standard library (`httputil.ReverseProxy`, with HTTP/2 without TLS for gRPC), and `quic-go` can add HTTP/3 and WebTransport later. Blob storage goes through a small interface with S3 and local backends (see [Coordination](#coordination)). Fencing is mandatory, since GC pauses are only one of the ways a node can keep acting on an expired lease (VM pauses and network delays are others, see [Ownership and fencing](#ownership-and-fencing)). Lease expiry and timeouts are tested with `testing/synctest`'s fake clock.

## App contract

An app uses shards in two ways, and can do both:

- **Serving** (typical for backends over HTTP and WebSockets): the node routes requests, calls and messages for any shard of a kind the app serves. These are **served shards**, activated on demand wherever requests land, as virtual actors.
- **Pinning** (later, typical for game servers): the app pins one specific shard through the control API, holding it in place, and serves its clients itself, over any protocol: **WebRTC, custom UDP** (which includes Unreal dedicated servers' networking) **and raw TCP**. Nothing is routed to it. These are **pinned shards**.

### Serving

The node sits in front of the app. For each request or connection it works out the shard (see [Routing requests to shards](#routing-requests-to-shards)), activates it if needed and forwards to the app with:

```
Shardlite-Target: users/42
Shardlite-Path:   /data/users/42.k3x9.db
```

`Shardlite-Target` is the same header clients and apps use to name a shard. On requests to the app, the node always sets it from its routing decision, overwriting any value that came in, so the app can trust it. The kind is everything before the first `/` and the id is everything after. Kinds are lowercase letters, digits, `-` and `_`. Ids are 1 to 128 ASCII letters, digits, `-` and `_`, enough for numbers, UUIDs and ULIDs. Anything else gets a `400` before the node does anything with it, so ids go into object keys, file paths and headers as they are, with nothing to escape. Shardless requests have no `Shardlite-Target`. Apps whose URLs include shard ids should still take the shard's identity from this header: a client can send `Shardlite-Target: users/43` to `/users/42/profile`, and the header wins.

The proxy only handles HTTP-based traffic (HTTP, gRPC, server-sent events, WebSocket, WebTransport). Client requests, requests forwarded between nodes and calls between shards are all the same HTTP requests, passed through unchanged apart from `Shardlite-*` headers.

All shardlite headers use the `Shardlite-` prefix. The node strips every `Shardlite-*` header from client requests before they reach the app, so they can't be spoofed (it reads `Shardlite-Target` for routing first). It also strips them from app responses before they reach the client. Client requests for paths under `/_shardlite/` are refused, so the app's shardlite endpoints aren't public. The one exception is `GET /_shardlite/ready`, the node's readiness check for the load balancer.

The app opens `Shardlite-Path` with any SQLite driver. That file is opaque and includes the shard's generation, so an app can only open files it was given, and apps never build paths themselves. The app:

- uses WAL mode with default locking (not `locking_mode=EXCLUSIVE` or a VFS without shared memory);
- only runs passive checkpoints, as SQLite's automatic checkpoints are. `FULL`, `RESTART` and `TRUNCATE` always find the shard busy, since the node holds a read transaction on it;
- closes the shard when it isn't using it: after each request, or from a pool that closes idle connections well within the kind's idle timeout. The node only lets go of a shard once no other process has it open (see [Letting go of a shard](#letting-go-of-a-shard)). After a node restart, a shard the app still has open from before is unavailable until the app closes it, so a short idle close keeps that brief;
- serves a health endpoint;
- follows the [rules for app code](#rules-for-app-code): it handles concurrent requests (including calls back into itself) and never holds a write transaction while calling another shard.

Schema migrations belong to the app, using whatever migration tooling it already uses. The node knows nothing about schemas and enforces no policy. The usual practices for a service in front of a database apply. Recommendations for apps:

- Check the schema version when opening a shard (`PRAGMA user_version` is cheap) and migrate if needed. Migrations are lazy, so only shards opened since a deploy are on the new version.
- Refuse shards newer than the newest version the app can read, as old game builds refuse newer save files.
- Set `PRAGMA application_id` in the first migration to a 32-bit hash of the app's identifier and the shard kind. Check it on open, so a shard is never opened as the wrong kind (after a bad import, for example).
- Prefer expand and contract: additive changes that the previous release already tolerates, with old structures removed in a later release. Rolling back is then just redeploying the previous release.
- For changes that can't be additive, ship down migrations too. Rolling back means migrating affected shards down, which takes time, as it would with any database.

Version-aware placement can later keep shards away from nodes whose app can't open them (see [Later](#later)).

Optional:

- `Shardlite-Sync: on|off` on a response, to override the shard kind's sync setting for that one response. The node strips it.
- The localhost control API (below) for calls between shards.

### Shard kinds

Shard kinds are declared in a kinds file the node reads, kept in the app's repository and deployed with the nodes (later, possibly from blob storage, so every node reads the same version). Adding or changing a kind is a deliberate deploy step, much like running a migration on a shared Postgres database:

```json
{
  "kinds": {
    "users":  { "public": true, "idle_timeout": "10m" },
    "chat":   { "public": true, "sync": "off" },
    "wallet": {},
    "worlds": { "pin_only": true }
  }
}
```

A node either serves every declared kind through its app (the default) or none (`SHARDLITE_SERVES=none`, for game server hosts whose processes only pin shards). Kinds marked `pin_only` are never served by any node, only pinned (worlds run by game servers, for example). Clients can only name kinds marked `public`. Every other kind is served only to calls from the app through the control API. A client that names one gets the same answer as for an undeclared kind, so clients can't activate, create or find shards of that kind, or tell that the kind exists. The node logs why, so a kind that should be `public` is easy to spot in development.

- The kinds file holds the settings the app's code relies on: sync (`"on"` by default, or `"off"`), idle timeout, `public`, `pin_only` and `ephemeral`. Every node has the same file, so a kind's settings don't depend on which app version handles a request. Settings about the environment are environment variables, set by whoever runs the cluster: blob storage and an optional prefix for every object path (`SHARDLITE_PREFIX=staging/`, so several environments or games can share one bucket), flush interval, how long journal segments are kept, load shedding thresholds and a default idle timeout for kinds that don't set one.
- Requests, pins and messages for undeclared kinds are refused, which catches typos.
- Changed kind settings take effect when a node reloads the kinds file, for new activations and new responses.
- The node polls the app's own health endpoint (its path is configured) and reports ready to the load balancer (`GET /_shardlite/ready`) only while the app is healthy, the kinds file is valid and its lease is good, so a bad deploy stops at the first node.
- Each node's lease says whether it serves. A node that serves none forwards requests and calls to the shard's owner, or, for an unowned shard, to a node that serves, which places it. A call for a `pin_only` kind gets a `404`. Clients can't name one, since it isn't public. Adding a kind means declaring it in the kinds file and deploying the app that handles it.

### Routing requests to shards

Routing is never authorisation. It only decides which shard handles a request. Authentication and authorisation are entirely up to the app: the app on the target shard checks the request's credentials however it likes and rejects requests for shards the caller may not use. The node never inspects credentials.

A client that names a shard reaches its activation before any app code runs, which Orleans and Durable Objects avoid by having edge code pick the ids. Clients can therefore activate, create or restore any shard of a kind they can name, which costs blob storage requests and node resources even when the app then rejects the request. Recommendations for apps:

- **Make a kind `public` only if clients need to name it,** usually the player's own shard and anything they hold a long-lived connection to. Every other kind is reached only through calls from the app after it has authenticated the request (from a shardless handler or another shard).
- **Give kinds clients can name unguessable ids** (random UUIDs). Ids an attacker can't guess can't be used to force restores of real shards, push them out of the local cache or find out which exist. Random ids then only cost a create and a delete each.

The node works out the shard from fixed conventions, checked in this order:

1. **Header:** `Shardlite-Target: guildchat/7`. This is the one `Shardlite-*` header the node accepts from clients. It is safe because routing is never authorisation, and a client can name any shard by query parameter anyway. The node strips it after use. gRPC clients send it as metadata.
2. **Query parameter:** `?shardlite-target=guildchat/7`, for example `wss://game.example/chat?shardlite-target=guildchat/7`. The node strips the parameter, so the app sees its own path and query unchanged. This is the convention for browser WebSockets, which can't set headers.
3. **No shard.** A request that matches neither is shardless. It goes to any local app instance, and the node acts as a plain reverse proxy. This is normal traffic: login, message of the day, news, store catalogues and health checks. The app serves it like any stateless service.

A shardless request that belongs to a shard (a login, which has to reach the user's shard where the credentials live) is sent there by the app. Shardlite needs no feature for this. Recommendations for apps:

- **Tell the client where the shard is, by default.** The shardless handler works out the shard and answers with a URL that names it (`/login?shardlite-target=users/42`), and the client sends its request there. A `307` is the same thing packaged so clients follow it themselves, resending the body with the same method. WebSocket handshakes can't follow redirects (browsers fail any answer other than `101`), so for them the answer is the URL in an ordinary response. Either way it costs one client round trip, once per connection, which is fine since shardless requests that need a shard should be rare. With a sticky load balancer, the client's next request usually lands on the same node, so an unowned shard is activated there and stays local for the session. A load balancer that routes by the shard (header or query parameter) can send it straight to the owner.
- **Call the shard where a redirect doesn't work:** for clients that don't follow redirects or a client that shouldn't see the shard. The shardless handler makes an ordinary call with `Shardlite-Target: users/42` through the control API and returns the result, passing on the client's credentials so the shard authenticates the request as usual. An unowned shard is again activated on the local node.
- If the shard is already owned by another node, both are forwarded there, at 0.5–2 ms per request, unless the load balancer routes by shard (see [Routing and forwarding](#routing-and-forwarding)).

Shared data served to shardless requests (a message of the day kept in a global shard, for example) should be cached by the app or shipped with the release, not read from the shard on every request, or that shard becomes hot.

The conventions are fixed by shardlite, not configured per shard kind. This matters during rolling deploys. If apps declared their own route mappings, old and new versions could map the same URL to different shards, depending on which node a request reached, and data would end up split between them. With fixed conventions every node computes the same shard, whatever app version it runs. Changing which shard a client uses is a client change, versioned like any other API change.

Redirects and calls made by shardless handlers are app code, so old and new versions can still disagree there. Recommendations for apps:

- Keep these decisions stable across releases. Working out who a request is about (a token belongs to user 42, so `users/42`) is stable. Policy that changes between releases isn't.
- Change one over two releases: first make the new target handle requests sent to the old one, then switch.
- Have clients use the conventions directly after the first call (login returns the shard names), so few requests need redirecting.

For "my own data", login returns the user's id and the client names `users/{id}` in its requests. The app on the user shard checks that the credentials match.

A WebSocket connection is tied to one shard. A game client usually touches several (its player, its guild, its match). Over HTTP each request is routed separately. Over a WebSocket the client connects to its **home shard** (normally its player shard), and the app calls other shards through the control API, as in Orleans. A client that needs its own stream to another shard (a match with real-time traffic, for example) opens a second connection.

### Pinning

Not in v1. It is described here so v1 doesn't rule it out.

- Ownership is always a node's, and a live owner is never preempted. A shard reaches another node only by a graceful handoff (see [Takeover](#takeover)), whether rebalancing starts it on the owner's side or another node asks for it.
- A process **pins** a shard on its own node, like a pinned pointer that a moving garbage collector must leave alone. The node gets the shard first: by CAS if it's unowned, as any activation does, or by asking the owner for a handoff. A pinned shard is never handed off: rebalancing skips it, and a handoff request or a pin from another process fails with the holder's connection info.
- Ownership therefore follows where the orchestrator (Agones, GameLift) starts the process, not where a request lands. To move a pinned shard, its process unpins it first, and it becomes an ordinary owned shard that the next request activates as usual, unless its kind is `pin_only`.
- The pin carries how clients reach the process, opaque to shardlite and small (an `ip:port`, a URL or whatever the app needs), kept in the manifest. Any app can look a shard up on the control API and learn whether a process has it pinned (and that value), a node owns it or nobody does. A login API uses this to send a player to a running server or to allocate one. Tickets for each client are the app's job: the login API issues them (signed with a key the game server trusts, as in netcode.io) or asks the server for one.
- While a process has a shard pinned, requests and calls for it fail with an error that says so, since the process's memory is the real state. Messages (later) wait in the senders' outboxes and are delivered to the process as a stream of server-sent events from the control API.
- A `pin_only` kind is only reachable by pinning. Clients can't name it, and messages to it wait until a process pins the shard.

Clients learn where to connect from whichever app sends them there (login, matchmaking), as above. An app that serves a kind can also hand clients off itself: the client **signals** over HTTP through the proxy, the signalling request is routed to the owner like any request, and the app returns where to connect plus any token it wants the client to present. Issuing and validating tokens is the app's job, like all auth. For example:

| Transport | Signalling request | Response |
|---|---|---|
| Custom UDP | `POST /connect` with `Shardlite-Target: zone/7` | `ip:port` + a connect token issued by the app (netcode.io style) |
| WebRTC | SDP offer | SDP answer from an ICE-lite server, no trickle (WHIP style) |

Because the clients' traffic doesn't pass through the node, apps holding shards this way must:

- respect a lease deadline provided by the node and stop serving clients past it,
- move their clients when they unpin (re-signalling, or reconnecting to the new owner).

Durability is up to the app, as in Orleans, where a grain chooses when to call `WriteStateAsync`. Writes ship to blob storage in the background. Most real-time state (positions, transient match data) needs nothing more. Before acknowledging a write that matters (a purchase, a level-up), the app calls `sync`. If the node has lost its lease, `sync` fails, so the app doesn't acknowledge a write that will never reach blob storage. A small commit wrapper makes this hard to forget (see [Recommendations for app code](#recommendations-for-app-code)).

A live simulation can't migrate. Moving a pinned shard means `sync`, unpin, a pin on the new host, then clients reconnect. Process lifecycle (warm pools, fleet size) is expected to come from Agones or a process manager.

### Localhost control API

HTTP over localhost TCP or a Unix socket. Both are offered because many HTTP clients don't support Unix sockets without extra work (Java's and .NET's `HttpClient`, Python's `requests`, Ruby's `Net::HTTP`, Unreal's `FHttpModule` and Unity's `UnityWebRequest`). Localhost TCP works everywhere.

Most of the API is headers on the app's normal requests:

```
ANY /{path} + Shardlite-Target: {kind}/{id}            call {path} on the shard's owner
ANY /{path} + Shardlite-If-Active: true              only call the shard if it is already active
```

Calls keep the app's own path, method and body, so they work with whatever HTTP-based protocol the app uses (see [Calls between shards](#calls-between-shards)).

The rest is a few plain HTTP + JSON endpoints, callable from any language even if the app's own protocol is gRPC:

```
POST   /_shardlite/sync                                              wait until everything committed before the call is in blob storage (fails if the node has been fenced)
DELETE /_shardlite/shards/{kind}/{id}                                 delete the shard and its data
```

`sync` is for side effects shardlite can't see: non-HTTP egress such as publishing to Kafka or NATS, external HTTP calls (unless they go through the egress proxy, later) and any other side effect. It is also how processes make important writes to pinned shards durable before answering clients over their own protocols. Responses, streams and calls to other shards are already gated (see [Durability gates](#durability-gates)). `sync` covers commits since the node last started: commits to sync-off shards that a crashed node hadn't flushed are lost, and a later `sync` can't bring them back.

Deleting is routed to the owner like a call. The owner deactivates the shard, deletes the manifest by conditional delete (`If-Match` on its ETag), then its LTX files and local files. Any tail left in its node journal is never read again and is swept with the rest of the journal. A crash before the manifest is deleted leaves the shard as it was, and deleting again finishes the job. A later request for the same id creates a new, empty shard with a new generation, so it never reads the old shard's data. Uses: account deletion and short-lived shards such as trades and finished matches.

Pinning adds endpoints to pin and unpin a shard (with how clients reach the process), to look a shard up and to follow the lease deadline. These are not defined yet.

The node's own endpoints live under `/_shardlite/` so they can't collide with app routes, as do the app's endpoints that the node calls (such as messages, later). The control API is a separate listener from the public one, so apps may send `Shardlite-*` headers to it.

HTTP is required only for the control API. Real-time game traffic never goes through HTTP.

### Lifecycle

For a serving app, a dead node means the app is unreachable: no new requests arrive and no response can be acknowledged. No lifecycle coupling is needed. A restarted node recovers as from a crash, keeping what it can of its local cache (see [Node journals and snapshots](#node-journals-and-snapshots)).

Only apps holding pinned shards need the lease deadline.

### Interface inventory

Everything the app and the node exchange beyond the app's own requests and responses.

The app serves, and the node calls:

| What | Status |
|---|---|
| Health endpoint (path configured) | required |

The node adds to requests to the app:

| Header | Status |
|---|---|
| `Shardlite-Target`, `Shardlite-Path` | v1 |
| `Shardlite-Caller` on every call | v1 |
| `Shardlite-Depth`, `Shardlite-Deadline` | v1 |

The app can add to responses:

| Header | Status |
|---|---|
| `Shardlite-Sync`: override the kind's sync setting | v1 |
| `Shardlite-Prefetch`: warm shards in the background | later |
| `Shardlite-Version`: the shard's schema version | later |

The app calls the control API:

| What | Status |
|---|---|
| Any request with `Shardlite-Target`, `Shardlite-Caller`, `Shardlite-Depth` and `Shardlite-Deadline`, optionally `Shardlite-If-Active` and `Shardlite-Sync` | v1 |
| `POST /_shardlite/sync` | v1 |
| `DELETE /_shardlite/shards/{kind}/{id}` | v1 |
| Pinning, unpinning and looking up shards, lease deadlines | later |

Clients can send `Shardlite-Target`, and every other `Shardlite-*` header from clients is stripped. Apps should pass on `traceparent`, `Shardlite-Depth` and `Shardlite-Deadline` on calls.

## Protocols

| Traffic | Shard identified by | After routing |
|---|---|---|
| HTTP/1.1, HTTP/2, HTTP/3 (later) | header or query parameter, per request | proxied |
| Server-sent events | the request, like any HTTP request | streamed without buffering |
| WebSocket (over HTTP/2 extended CONNECT later) | upgrade request | byte stream |
| WebTransport (later) | session CONNECT request | proxied (node terminates HTTP/3) |
| Custom UDP, WebRTC, raw TCP | a pin, or signalling | direct to the process |

Clients reach nodes through the deployment's own load balancer, which terminates TLS and absorbs hostile traffic. Nodes listen only on the private network. Client addresses are passed on with `X-Forwarded-For`.

### Traffic between nodes

v1 assumes the nodes are on a trusted private network and sends traffic between nodes unencrypted. Encryption and authentication come later. The options are:

- **Built-in mTLS from a shared cluster secret.** Every node derives the same CA from the secret and issues itself a node certificate. Setup is one secret in every environment.
- **Your own certificates** from cert-manager, Vault, step-ca or similar.
- **An encrypted network** (WireGuard, Tailscale) that already handles it, with shardlite staying unencrypted. This works on VMs, bare metal and Kubernetes with low overhead.

Service meshes (Istio, Linkerd) are not a target. They add two proxy hops to every call between nodes (roughly 0.5–1 ms), which is significant next to the forwarding budget. They also put a second proxy beside shardlite's own and only work on Kubernetes. In a cluster that already runs a mesh, shardlite's traffic between nodes should be excluded from it.

## Ownership and fencing

Follows Kleppmann's [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html): a lease alone cannot prevent two writers (GC pauses, VM pauses, network delays, clock jumps), so the storage being written to must reject writes from stale owners. Here that is the node journals in blob storage (each with a single writer), together with the manifest.

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
  "journal":         "node-b",    // the node journal holding commits after the last file, if any
  "last_owner":  "node-a",    // hint for direct pulls from a warm local cache (later)
  "pin":         { "connect": "203.0.113.5:7777" }  // later: set while a process has the shard pinned
}
```

- An object in blob storage, created with `If-None-Match` and changed only by compare-and-swap (`If-Match` on its ETag).
- Created when a shard is first used, written on ownership changes and snapshots, and deleted when a shard the app never wrote to deactivates. Not written on activation of a shard the node already owns or on writes.
- Ownership is invalid if `node_epoch` does not match the owner's current lease, or that lease has expired.

### Fencing

- Each node journal has exactly one writer, its node. Segments have sequential numbers (`journal/node-b/1835`) and are created with `If-None-Match`, so a number can be written only once. A writer that finds a number already taken by something other than its own segment has been fenced and stops for good (see [Lost responses](#lost-responses)).
- To fence a dead node's journal, the node adopting it (see [Takeover](#takeover)) starts from the latest segment number recorded in the dead node's lease, probes forward until it finds a free number and creates an empty segment there, which is the fence. If the zombie takes that number first, its segment counts and the adopter tries the next one. A zombie can therefore only write to its own journal, and only below the fence. SlateDB fences its WAL writers the same way, with a zero-byte WAL object.
- A node uploads one segment at a time, so segments succeed in order. If an upload is still running when the next interval comes, its commits go into the following segment. A PUT takes well under the 250 ms interval, so this rarely delays anything. A segment whose outcome stays unknown is retried with the same write id until the outcome is known, holding back later segments and their gates, rather than given up.
- Whatever reached the journal before the fence is part of the shard's history. Nothing the zombie does afterwards is.
- A zombie can still upload LTX files, but a file only counts once a manifest CAS adopts it, and a stale owner's CAS fails. Its files are never used and are garbage collected.
- None of this needs listing objects: segments are found by number, and reads of single objects are strongly consistent on every store with conditional writes.
- A shard that is deleted and created again gets a new generation, so it never reads the old shard's pages.
- Blob storage must support linearizable conditional writes (`If-None-Match` and `If-Match` on writes, and `If-Match` on deletes), as AWS S3, GCS and Azure Blob do. Node journals, manifests and leases all rely on them, and they are the only primitives shardlite needs besides plain object operations: create if absent, conditional update, conditional delete, get, head, delete and a listing of `nodes/` (which may lag, since it only triggers checks). Conditional deletes are what lets a manifest be deleted without a zombie deleting its successor's. Development uses a local directory. The supported stores are AWS S3 and, for self-hosting and tests, RustFS (which passes shardlite's conformance suite and is the emulator the tests run against). Cloudflare R2 doesn't document conditional deletes, so it fails the startup check until it does. Any other store, Tigris included, gets the startup check and nothing more.

### Lost responses

Any write to blob storage can succeed while its response is lost, so a retry can't tell "my write failed" from "my write succeeded and I didn't hear". `blob.Store`, the one type every caller writes through, handles this for every object and every backend (see [Coordination](#coordination)):

- Every write carries a write id (the writer's node, epoch and a counter), stored in the object's metadata.
- `blob.Store` retries ambiguous failures (timeouts, dropped connections, server errors) itself.
- If a retry fails with "exists" or "conflict", it reads the object's metadata. Its own write id means the earlier attempt succeeded, and the write returns that object's ETag. Anything else is a real conflict, returned to the caller.
- A delete whose retry finds nothing counts as done.

Callers therefore only ever see success or a conflict with someone else. For a fence, that is exactly the adopter's question: its own fence, or the zombie's segment. Stores must support custom metadata on conditional writes and return it on `HEAD`, as S3, R2, GCS and Azure do.

Calls and forwarded requests have the same problem one level up, which is why the node never retries a request the app may have seen, and why calls that change state should carry an idempotency key.

### Takeover

- **Graceful handoff** (migration, drain, rebalancing): the owner stops accepting, finishes in-flight work and deactivates the shard and writes a delta for its tail. The new owner claims the manifest by CAS and downloads the snapshot. Later, it can instead copy the SQLite file directly from the old owner's local cache.
- **Crash**: when a node's lease expires, the first live node to notice adopts it by CAS on its lease object (`adopted_by`). The adopter fences the dead journal, reads its segments, writes a delta for every shard with a tail there and adds it to that shard's manifest. It then releases the dead node's shards by CAS on their manifests and takes over its reminders. Requests for those shards fail fast until then (seconds, longer for a busy dead node), and afterwards claim them as usual. If the adopter dies too, its own lease expires and both journals are adopted. Writes that hadn't reached blob storage are lost, except for shard kinds with sync on.

A move therefore copies nothing between node journals, and a dead node's journal is gone within minutes of its lease expiring.

### Zombies

A zombie's writes after its journal is fenced are never part of the shard's history, so data in blob storage is never overwritten by a stale owner. Writes it committed locally but never got acknowledged are lost.

## Storage and durability

| Tier | Holds | Role |
|---|---|---|
| Owner's local disk | the working copy, plus a cache of recently used shards | all reads and writes |
| Blob storage (S3 or compatible) | one node journal per node and LTX files per shard | system of record, restores |

The local hot path is SQLite in WAL mode on local disk. Local disks can be ephemeral (cloud instance NVMe is fine), since everything that matters is in blob storage.

### What each node stores

| Data | Memory and CPU | Disk |
|---|---|---|
| Active shards it owns | open in the app, WAL captured by the node | live SQLite files |
| Cached shards (deactivated) | none | files at rest |

Cached shards cost only disk. They have no app connection, no WAL capture and no open files. The node keeps ownership of them while its lease is valid, so reactivating one needs no coordinator call and no download. When disk fills up, the node evicts the least recently used cached shards. It releases the shard by CAS on its manifest and drops the local files. The shard then lives only in its snapshot, and the next request claims it like any unowned shard. Each node keeps an on-disk index of its cached shards (a SQLite file), since at a million shards an in-memory index would cost around 100 MB.

### Node journals and snapshots

Like a database's WAL and data files, and the same split as HBase's write-ahead log per server and store files per region: each node keeps one journal in blob storage, the shared WAL for its shards, and each shard's LTX files are its data files. Taking a snapshot is the checkpoint.

- **WAL capture.** For each active shard, the node reads committed frames from the shard's `-wal` file and controls checkpoints so no frame is checkpointed away before it is captured. It holds a read transaction and checkpoints itself at safe points, and detects WAL restarts. Litestream does this (see [Implementation](#implementation)).
- **Format.** Commits and snapshots are [LTX](https://github.com/superfly/ltx) files (version 3, the format of LiteFS and Litestream 0.5): a header with the page size, the database size and a range of commit positions, the changed pages compressed one by one, a page index and a trailer with checksums. Restores verify each file's checksums. LTX can also carry a checksum of the whole database before and after each file, which would catch bugs in shardlite's own apply code, but it needs a checksum per page for every active shard (later).
- **Segments.** Every flush interval, the node writes one segment object (`journal/node-b/1835`) holding that interval's commits as one LTX record per commit, plus a footer listing which shards it holds and each entry's positions. A journal entry is already covered by the shard's files if its last position is at most the manifest's last position. The PUT rate depends on the number of nodes, not active shards: at the default 250 ms interval that is 4 PUTs per second per busy node, about 50 USD a month on S3 Standard. Idle nodes write nothing. A shorter interval makes sync faster and the loss window smaller at a higher cost (about 130 USD a month at 100 ms).
- **Snapshots as deltas.** When a shard deactivates, or an active shard's tail is older than a few minutes, the node writes the pages changed since its last snapshot as one delta (`shards/users/42/k3x9/1651-1834.17.ltx`, with the writing node's epoch, so a crashed node's leftover file never collides with a new one), from pages it already captured. It adds the delta to the manifest by CAS. The SQLite file itself is never read for this. A cached shard therefore never has a tail, so the lease's range of segments only covers active shards, and eviction, handoff and the restart check never find unwritten commits.
- **Merging.** When a shard has more than a few deltas (say 8), the owner merges them into a new base with LTX compaction. A shard then has a base and a handful of deltas, and a player who changes three pages uploads three pages.
- **Cleaning the journal.** Segments expire a set time after they are written (`SHARDLITE_JOURNAL_EXPIRY`, a day by default, and the node refuses to start if it is less than 4× the tail alert below). Each node deletes its own expired segments with a sweeper, on every backend, so no bucket lifecycle rule is needed: it lists `journal/{node}/` (which gives sizes) and deletes non-empty segments older than the expiry age. On S3 deletes are free and a listing returns a thousand keys, so this costs a few hundred listings a day per node. The sweeper starts only once the node has adopted its own journal, so a node that was down for longer still finds its old segments. An adopter deletes a dead node's non-empty segments once it has fenced the journal and written the deltas. Only non-empty objects are deleted, so fences stay: a zombie paused for longer than a day could otherwise write into a freed number and have its gate acknowledge a write no one reads. Fences are tiny, one per adoption. Tails become deltas within minutes, so nothing needs to track which segments are still in use. The lease records the range of segments still needed, so an adopter knows where to read, and a node logs an alert if any tail is older than `SHARDLITE_TAIL_ALERT` (15 minutes by default), which means its deltas are failing, long before its segments expire. Node journals therefore hold minutes of live data, and shards that are never used again cost only their LTX files.
- **Restore** downloads the base and deltas in parallel, then applies the tail, if any, in order, from the owner's memory. Each file's checksums are verified as it is applied. A shard that isn't active has no tail.
- **Restarting.** A restart is treated as a crash, so there is one recovery path. The restarted node knows its previous process is gone, so it adopts its own journal at once instead of waiting for the TTL: a new epoch by CAS on its own lease, then fencing, deltas and releases as any adopter does. It keeps its cache but checks it: a cached file whose last commit matches the latest file in its manifest is reclaimed by CAS on its next activation and reused. A file that is ahead holds commits that were never flushed, so it is discarded and downloaded again, losing at most a flush interval as any crash does. The on-disk index records each file's position, so the check is cheap, and a large node redeployed with new node software keeps almost all of its cache.
- **Loss window.** For shard kinds with sync off, a node crash loses whatever hadn't been flushed. The flush interval sets that window.
- **Old files.** Merges delete the files they supersede, and shardlite keeps no history. A deployment that wants point-in-time restore can turn on bucket versioning, which shardlite neither needs nor manages. Deleting players who never return is the app's decision (retention policies, data deletion requests).

### Cost

Rough estimates on S3 Standard, to be checked by the prototype:

- **Requests:** 4 PUTs a second per busy node for segments at the default 250 ms interval. About 50 USD a month per busy node, whatever the number of active shards.
- **Snapshots:** one PUT (the changed pages only) and one manifest write each time a shard deactivates or its tail ages out (about once per session), about 10 USD per million sessions, plus a merge every few sessions. Infrequent-access storage classes bill small objects as at least 128 KB, so small files stay in the standard class.
- **Restores:** a few GETs in parallel, for the base and deltas.
- **Storage:** a 100 KB player shard is 100 GB for a million players, a few dollars a month, plus old files during their retention.
- **Manifests and leases:** one conditional write per new shard (plus a delete, which is free on S3, for one that is never written), per move and per snapshot, and lease renewals every few seconds per node (well under a dollar a day per node).

### Durability gates

Configured per shard kind with `"sync": "on"` (the default) or `"off"` (see [Shard kinds](#shard-kinds)), enforced by the node for served shards. On is the default, as in Durable Objects, whose output gates are on unless turned off, and as in idiomatic Orleans code, which awaits `WriteStateAsync` before returning. A kind that leaves the setting out then pays latency, never data, and only for responses that follow writes, since output from a shard with nothing new to capture passes straight through. Real-time kinds (chat, presence, streams) turn it off. The setting is a string, like the `Shardlite-Sync` header, so other modes can be added to both later. Borrowed from Durable Objects' output gates. Spanner's commit wait is similar: the commit is already applied and only the acknowledgement is held until a point in time has passed, though Spanner waits out clock uncertainty for ordering rather than for durability.

| Sync | Survives | Typical latency |
|---|---|---|
| off | crashes of the app or node, since committed writes are in the WAL and shipped within a flush interval. Loss of the host loses the last interval. | 0 |
| on | loss of the host (flushed to the node journal in blob storage) | on average half the flush interval plus one PUT, since one flush covers the whole node: roughly 150–200 ms on S3 Standard with the default 250 ms interval, less on S3 Express One Zone (estimated) |

As with Postgres' `synchronous_commit`, the setting only decides whether acknowledgements wait. Every commit is shipped either way.

One rule covers every protocol: nothing leaves a shard before what it committed earlier is durable. The node treats responses, streams (WebSocket, server-sent events, gRPC streaming) and calls to other shards alike, as bytes. A plain HTTP response is just a stream with one or two chunks.

- Every flush captures each active shard's new frames up to the moment it starts. So a chunk the node reads is held until a flush that started after it was read has completed. The node only keeps a counter of flushes. Nothing is parsed.
- Chunks from a shard with no new frames since its last capture pass straight through.
- Chunks waiting at the same time share one flush (group commit). One node journal flush releases every waiting chunk on the node, whatever shard it is for.
- The gate is conservative: a chunk waits for everything committed on the node before it was read, including other concurrent requests.
- A call to another shard is bytes leaving the caller, so the caller's node holds it until the next flush if the caller's kind has sync on, as Durable Objects gates all outgoing messages. Without this, a `wallet` shard could debit gold, call `inventory/42` to grant an item and then lose the debit in a crash, while the item stays granted.
- With sync on, every chunk of a stream can wait for a flush, pings included, so real-time stream shard kinds should turn sync off.
- For a pinned shard, traffic between the process and its clients doesn't pass through the node, so it isn't gated. The process calls `sync` before answering clients.

The app can override its kind's setting for one HTTP response, or one call to another shard, with `Shardlite-Sync: on` or `off`. `sync` is the explicit alternative: it waits until everything committed on the node before the call is in blob storage, for example before an important write's side effect on a kind with sync off. It is how processes make important writes to pinned shards durable.

### Egress proxy

Later. The node could also offer an egress proxy for HTTP and HTTPS, so calls to external services can be gated too. Using it is deployment configuration: set `HTTP_PROXY` and `HTTPS_PROXY` in the app's environment. It is best effort.

- The app's environment then also needs `NO_PROXY=localhost,127.0.0.1`, so calls to the node's control API (calls between shards, `sync`) go to it directly rather than through the egress proxy.
- HTTPS goes through `CONNECT` tunnels. The node gates the tunnel's bytes like any other output. There is no TLS interception or parsing.
- Egress is gated like any other output, by waiting for the next flush, so no egress traffic has to be attributed to a shard.
- Egress waits for the next flush only while the node has pending writes from kinds with sync on, up to 250 ms under load. Otherwise it passes straight through. Telemetry and logging can bypass it by destination (`NO_PROXY`, or an allowlist in an environment variable).
- The app can also skip the gate call by call: `Shardlite-Sync: off` on the request (or on the `CONNECT` request for HTTPS, where the client can set proxy headers) (the node strips it before forwarding), or simply an HTTP client configured without the proxy. Where a datacentre requires all egress to go through its proxy, the header is the way.
- Where a datacentre already requires an egress proxy, the node chains to it.
- It only covers clients that honour the proxy settings. Most HTTP clients do; some need configuration (Node's `fetch`, Java).

Without the proxy, or for anything it can't see, `sync` gates side effects explicitly: non-HTTP egress (Kafka, NATS, raw TCP), clients that bypass the proxy, other side effects and the custom protocols of pinned shards.

## Activation

1. **Owned by another node with a valid lease:** forward the request there. The owner activates it.
2. **Cached on this node:** open the files, start capturing the WAL and pass the request to the app, which checks its schema version. No coordinator call.
3. **Unowned, or the owner's lease has expired:** claim the shard by CAS on the manifest, then restore it from its snapshot. Then continue as in step 2.
4. **New shard** (no manifest): create the manifest with a conditional create, which claims the shard, and start it as an empty file. Then continue as in step 2. See [Creating shards](#creating-shards).

Placement prefers the local node: an unowned shard is activated on the node that received the request or call for it, if it serves. Otherwise it goes to a node that does.

Concurrent requests for the same shard share one activation (`singleflight`). Shards deactivate after an idle timeout set per shard kind (minutes by default). An open connection (WebSocket, WebTransport, a server-sent events stream or a streaming gRPC call) keeps its shard active. The app can close the connection to override this. Deactivation waits until the shard's last commit is in the node journal, stops capturing, writes its tail as a delta and closes the database (see [Node journals and snapshots](#node-journals-and-snapshots)). The files stay in the local cache.

### Letting go of a shard

Deactivating, moving, evicting or deleting a shard is only safe once the app has closed it, or a connection left open could keep writing to a file the node no longer captures. The node checks SQLite's own lock: every connection to a WAL database holds a shared lock on the deadman switch byte of its `-shm` file (byte 128) while it is open, and SQLite itself uses `F_GETLK` on that byte to tell whether it is the first connection. The node asks the same way, which neither takes the lock nor races a connection that is opening, and works across containers on one kernel.

`fcntl` locks belong to a process, and closing any descriptor on a file drops all of that process's locks on it. So the node only checks a shard while it holds nothing open on that shard's files: it closes Litestream's connection and file handles first, then checks. Closing the check's descriptor then has nothing to drop, and the app's locks are never affected. Apart from this check, nothing in the node opens a shard's files except through Litestream.

If another process still has the shard open, the node reopens capture and doesn't let go. Litestream resumes where it was, or takes a snapshot if the app checkpointed in between. The same happens if the app committed after Litestream's final sync and closed the file before the check: the node compares the wal-index header read just after Litestream closes with one read after the check.

- **Idle deactivation:** the shard stays active, and the node tries again later, with a warning.
- **A move, eviction or delete:** refused with an error. Rebalancing picks another shard.
- **Draining the node:** it waits up to a timeout, then aborts the drain and keeps running with the shards still open, with an error. The node never lets go of a shard that is open.

The same check guards against apps opening files they weren't given. Before activating a cached shard (when it has nothing open on it yet), the node checks that no other process already has it open, and it logs an error for files in its data directory that it doesn't know.

Estimated latencies (not measured):

| Case | Added latency |
|---|---|
| Active on this node | ~0.1 ms |
| Owned by another node (active or cached) | +0.5–2 ms forwarding, plus activation there |
| Cached on this node | ~1 ms (open, start capture, app's schema check) |
| Claiming a shard | one conditional write on its manifest (~20–60 ms on S3 Standard) plus a snapshot download |
| New shard | one manifest read (~10–30 ms) and one conditional create (~20–60 ms) |
| Owner crashed | node lease TTL (~5–10 s), then claim and restore |

### Creating shards

Every shard id can be addressed, as Orleans grains can. A request for a shard with no manifest creates it:

- The node creates the manifest with a conditional create, which claims the shard as any claim does, and starts capturing an empty file before the app gets it. If another node creates it first, the shard is simply owned by that node, and the request goes there like any other. The app never sees a shard its node doesn't own.
- When a shard deactivates, the node checks through Litestream's connection, just before the final sync, whether it holds anything of the app's (a table, an index, `user_version` or `application_id`). If not, the node deletes its manifest by conditional delete and its local files instead of keeping it.

A request for a random id (a typo, a stale link or an attacker) therefore costs a conditional create and a conditional delete in blob storage but leaves nothing behind. Rate limiting at the load balancer is the defence against floods of them.

Migrating the schema on open counts as a write. Apps should authenticate a request before opening the shard, so a rejected request leaves nothing behind.

## Routing and forwarding

- Requests for shards owned elsewhere are forwarded to the owner over one long-lived HTTP/2 connection per pair of nodes. WebSockets become a tunnelled stream for their lifetime.
- Each node caches ownership. A stale cache costs one extra hop ("not mine, try X").
- **Fail fast.** If a node stops answering, requests for its shards get a quick `503` instead of piling up until they time out. A shard has one owner, so there is nowhere else to send them until its lease expires.
- **Holding during handoff** (later): a request that reaches a shard while it is moving is held until the new owner is ready, then forwarded, for at most a few seconds. After that the node returns `503` with `Retry-After`. Only requests the app hasn't seen are held or retried, so nothing is applied twice and a failure is still reported as one.
- **Shards don't follow requests.** A request that lands on another node is forwarded, and the shard stays where it is. Shards move only for rebalancing, draining or a crash.
- **Reaching the owner directly** is up to the load balancer:
  - **Sticky load balancer:** placement prefers the local node, so a client's shards are usually activated where it first lands, and stickiness keeps it there.
  - **Controllable load balancer** (HAProxy, nginx, Envoy): route by hashing `Shardlite-Target` (header or query parameter), or by a cookie the node sets.
  - Otherwise the request is forwarded, at 0.5–2 ms.
- Calls between shards go through the same path (see [Calls between shards](#calls-between-shards)).

## Calls between shards

A player normally connects to their user shard. Guilds, groups, trades and other social features are mostly calls between shards, so this is a hot path.

The app always calls its local node and relies on routing. The same app code runs on every node, and a shard call is just another HTTP route in the app:

```
app (users/42) → POST localhost:7000/invite
                 Shardlite-Target: guild/7
                 node: who owns guild/7?
                   local  → POST app/invite with Shardlite-* for guild/7
                   remote → forward to the owner node → its app/invite
```

- **The target is explicit.** The `Shardlite-Target` header names the shard, as it can for clients. The path, method and body pass through unchanged to the owner's app, with the usual `Shardlite-*` headers added. Because nothing about the path changes, this works for any HTTP API: REST, GraphQL or gRPC.
- **Any HTTP-based protocol works.** Apps will usually reuse the protocol their clients speak (REST, GraphQL, gRPC), so the same handlers serve clients and other shards, told apart by `Shardlite-Caller`. WebSockets also work, with `Shardlite-Target` on the upgrade request, which suits long-lived subscriptions such as a user shard following guild events.
- **Local calls still go through the node.** That costs one localhost round trip (about 0.1 ms) and keeps one code path. Remote calls add about 0.5–2 ms.
- **The callee knows the caller.** The app names the shard it is calling from with `Shardlite-Caller: users/42` (copied from the `Shardlite-Target` it received), and the node passes it to the callee. The node also uses it to gate the call. A call with no shard behind it (from a shardless handler, for example) gets `Shardlite-Caller: app`, so every call carries the header. The node strips it from external requests, and the control API only listens on localhost or a Unix socket, so the callee can trust it. The rule for handlers is then simple: a request without `Shardlite-Caller` came from the internet and must be authenticated, and internal-only routes require the header.
- **The callee's gate applies.** A call to a `wallet` shard, whose kind has sync on, returns only once the wallet's writes are in blob storage, so the caller can rely on the answer.
- **Calls activate the target** if it isn't active, like any other request, unless the call sends `Shardlite-If-Active: true` (see [Reaching many shards](#reaching-many-shards)).
- **Call depth is limited**, as in Dapr. The node sends `Shardlite-Depth` with each request to the app, and the app passes it on when it calls another shard, as it would pass on `traceparent`. The node increments it on each call and rejects calls past a configured limit, which stops runaway recursion (for example, two shards calling each other forever). An app that doesn't pass it on starts again from zero.
- **Cancellation propagates.** When a client disconnects or a caller gives up, the node cancels the request to the app and any forwarded or held requests on every hop it controls (HTTP/2 stream resets between nodes). Apps that cancel their own outgoing calls when a request is cancelled (Go's `context`, .NET's `CancellationToken`) get cancellation down the whole call chain. Writes already committed are not rolled back.
- **Deadlines propagate**, as gRPC's `grpc-timeout` does. The node sends `Shardlite-Deadline` (the time left, from the client's or caller's timeout) with each request to the app, and the app passes it on with its calls. Each hop subtracts the time it used. A node never forwards, holds or gates a request past its deadline, so the callee can stop work the caller has given up on.

### gRPC

Games that use gRPC work in both directions:

- **Calls between shards:** the app adds `shardlite-target` metadata (HTTP/2 lowercases header names), usually with a client interceptor. The gRPC method path stays as generated.
- **The node proxies gRPC properly:** HTTP/2 without TLS (h2c) to the app, trailers passed through (gRPC puts its status there) and no response buffering, so streaming RPCs work.
- **The node never parses gRPC messages.** The only gRPC-specific behaviour is for errors the node generates itself (shard not active with `Shardlite-If-Active: true`, owner unreachable, routing failures): for requests with `content-type: application/grpc`, it sends them as gRPC status responses (`grpc-status` and `grpc-message` trailers) rather than plain HTTP errors.
- **External clients:** clients send `shardlite-target` metadata.

### Reaching many shards

Guild chat or a group invite has to reach members spread across many nodes. The app calls each member's shard in parallel and collects the responses. Each call goes to the local node and then out as a stream on the existing HTTP/2 connection to the owner's node, so 200 calls to 20 nodes are 200 small streams over 20 connections. The app gets a response from every target and keeps control of retries and timeouts.

A normal call activates its target, which would wake every offline member's shard for a chat message. To avoid that, a call can ask not to activate:

```
Shardlite-Target: users/57
Shardlite-If-Active: true
```

If the shard isn't active, the node replies `404` (or `NOT_FOUND` for gRPC) without activating it. This is also useful for presence checks.

If fan-out becomes a measured bottleneck, a node could batch concurrent calls to the same remote node without any change to the API.

### Rules for app code

These are part of the contract with shardlite. Apps that break them stall or fail.

- **Handle concurrent requests, including calls back into the app itself.** A call to a shard on the same node arrives at the same app as a new request while the calling request is still waiting. A single-threaded synchronous server deadlocks. Thread pools can run out under load when calls nest, so async servers are the safer choice, or pools must be sized for call depth. Running several app processes per node also helps, since the node can send a nested call to a different local instance.
- **Don't hold a write transaction while calling another shard.** If `users/42` is mid-transaction, calls `guild/7` and the guild calls back into `users/42`, the callback can't get the write lock. Call first and then write, or write, commit and then call.
- **Expect calls to fail and set timeouts.** A call can arrive while its target is activating or moving, and its node can fail. Retry where it makes sense.

### Recommendations for app code

These aren't required, but they suit how shardlite works.

- **Use SQLite for concurrency within a shard.** Requests for the same shard run concurrently, as they would against any database. SQLite allows concurrent readers and one writer. Start transactions that read and then write with `BEGIN IMMEDIATE`: a deferred transaction can fail with `SQLITE_BUSY` when it upgrades to a write lock, and `busy_timeout` doesn't retry that case. State kept in app memory per shard (caches, a simulation) isn't protected by SQLite, so the app handles its concurrency.
- **Check that the client named the right shard.** A shard handler whose route includes an id (`/users/42/inventory`) compares it with `Shardlite-Target` and returns `400` if they differ, so a client that names the wrong shard or none fails in development instead of costing an extra hop in production. One line in the app's routing middleware.
- **Make changes idempotent.** Calls that change state should carry an idempotency key so retries are safe.
- **Choose ids that need no lookup.** A client can choose a new shard's id itself (a UUIDv7, which sorts by time), which makes creating it idempotent and saves a round trip. An id built from other data, such as a trade between two players or a name in a registry, can be a name-based UUID (v5) of that data, so every caller computes the same shard without asking anyone.
- **Sync writes that must not be lost.** For payments and the like, call `sync` after committing and before acknowledging. A small commit wrapper on top of the app's SQLite driver makes this hard to forget.
- **Use an outbox for calls that must happen.** "Write, commit, then call" loses the call if the node crashes in between. Instead, insert the call into an outbox table in the same transaction as the change. After the commit, send it (a post-commit hook works well) with the row id as its idempotency key and delete the row once it succeeds. When the shard activates, resend any rows left behind. Delivery is at least once, and the call gate means a call never goes out for a write that is later lost. Messages (later) would build this in.
- **Build reminders from shards.** Compute timed state (energy, cooldowns) lazily on activation, since most timers don't need to fire. For calls that must happen on time, keep schedules in a scheduler shard (`jobs/0`) indexed by due time, call it from an external cron every minute and let it call the targets that are due. Delivery is at least once. Built-in reminders come later, as delayed messages.
- **Cache shared data.** User shards that often read guild data (member list, message of the day) should cache it and refresh on change notifications.
- **Keep large data in object storage.** A world save of gigabytes doesn't fit a SQLite blob (1 GB at most) and would be shipped whole on every save. Upload it to the app's own bucket under a unique name, then commit the reference in the shard. A crash in between leaves an orphan, never a missing object, and old objects are deleted once a newer reference is committed.


## Persistent worlds

Game servers keep their simulation in memory, and shardlite can hold what outlives it. This is a recommendation, in three forms:

- **Server as caller.** Persistent pieces of the world are shards (`bases/{id}`, `chunks/{x}-{y}`, `containers/{id}`). The server loads a chunk's state when it streams in and writes changes back with calls. Each piece has one writer, so zone servers editing near a boundary are serialised by the shard. A crashed server is replaced by one that loads from shards. This suits loads and building edits, not per-tick state.
- **A world or zone as one pinned shard.** The world save is a SQLite file with serialized chunks as rows, instead of one large file written every few minutes. Only changed pages are shipped, so saving is continuous, the loss window is about one flush interval and snapshots double as backups. Game servers run wherever the orchestrator (Agones, GameLift) puts them, sharing one node per host, and each server pins its world on the local node when it starts. If the host dies, another server pins the world and restarts from it. This makes it easy to write an MMO from scratch: zone servers own zone shards and players own player shards. Shardlite handles storage and failover, and the app sends clients to zones by looking them up.
- **Many private worlds, pinned.** Hosted private worlds (as in Minecraft Realms, or Valheim and ARK servers) are idle most of the time. As shards, idle worlds deactivate and cost only storage. When a player joins, Agones or a process manager starts a server on a host with capacity, and the server pins the world and opens the restored file.

Game servers can run in a different facility from the backend (bare metal for servers, cloud for the rest). Their nodes reach the backend's nodes over the internet with mutual TLS (later, see [Traffic between nodes](#traffic-between-nodes)), and their calls to backend shards and syncs to blob storage pay the latency between facilities. That suits worlds, which call other shards occasionally rather than every tick.

## Rebalancing

New nodes only receive new connections and new shards, so without rebalancing, adding nodes at launch does not help players already connected.

Rebalancing moves shards on purpose, rarely, rather than as a side effect of where requests land:

1. An overloaded node picks shards to move (busy ones first) and hands them off to less loaded nodes (see [Takeover](#takeover)).
2. It closes those shards' WebSocket connections with a reconnect close code, or sends HTTP/2 `GOAWAY` (`Connection: close` on HTTP/1.1) to their HTTP clients. Unlike a `503`, no request fails: in-flight requests finish and the client opens a new connection for the next one.
3. The client reconnects. With a controllable load balancer it reaches the new owner directly. Otherwise it lands anywhere and is forwarded.

Rebalancing skips pinned shards. Their processes unpin them to move.

Policy:

- Balance on counts of active shards and connections, published in leases, as Akka and Orleans balance on shard and activation counts. Player shards are similar enough for counts to approximate load. Node CPU includes the app's and can't be attributed to shards, so it isn't shared.
- Move only from nodes clearly above average, or from a node over its own CPU threshold even if its counts look average.
- Choose which shards to move by their request rate, which each node measures for its own shards.
- Cap moves per interval.
- Prefer idle shards and connections.
- Don't move a shard again soon after it moved.
- When one hot shard overloads a node, move other shards away rather than the hot one, which would copy it, disrupt its traffic and only relocate the problem. Move only as many as needed: a shard whose work is bound to one core (its writes are serialised) can't use a whole node.

Load shedding complements this. Above a CPU or memory threshold, a node replies `503` with `Retry-After` instead of slowing everyone down, and refuses new activations. Shards that are already active keep working. Clients retry, and the load balancer may place them elsewhere.

Each shard also has a cap on requests in flight. Past it, the node replies `429`, so one hot or attacked shard can't use up a node's app threads.

A single shard too hot for any node cannot be split. That is a data-modelling problem.

Apart from these moves there is no data rebalancing. Blob storage holds everything, and local disks are caches.

## Coordination

Coordination needs no service of its own. Node leases and manifests are objects in blob storage, changed with conditional writes, which are linearizable on the stores shardlite supports. That is enough for agreement on ownership, the same way SlateDB coordinates its own writers. Every node stays disposable: there is no leader, no election and no set of stable nodes. Claims and lookups take tens of milliseconds, but they only happen on the cold path (unowned shards, moves and routing-cache misses), next to a restore that costs as much. Orleans accepts the same for cold activations.

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

Each storage backend implements one small interface, used for everything (leases, manifests, segments and LTX files). Backends do single attempts and nothing else. Callers never use a backend directly: they go through one `blob.Store` type, written once, which wraps any backend and makes writes safe to retry by storing each write's id in the object's metadata and recognising its own earlier attempts (see [Lost responses](#lost-responses)). `blob.Store` has the same methods.

```go
type Backend interface {
	Create(ctx context.Context, key string, body io.Reader, id WriteID) (ETag, error)                  // If-None-Match: *. ErrExists if someone else's object is there
	Update(ctx context.Context, key string, body io.Reader, expected ETag, id WriteID) (ETag, error)   // If-Match. ErrConflict if someone else changed it
	Get(ctx context.Context, key string) (io.ReadCloser, ETag, error)                     // ErrNotFound if missing
	GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) // segment footers and LTX trailers
	Head(ctx context.Context, key string) (ObjectInfo, error)                             // size (empty segments are fences), ETag, last modified, write id
	Delete(ctx context.Context, key string, expected ETag) error                           // If-Match when expected is set
	List(ctx context.Context, prefix string) iter.Seq2[ObjectInfo, error]                  // key, size, last modified. May lag
}
```

Each backend:

- maps its responses onto a fixed set of outcomes: exists, conflict, not found, not applied (safe to retry) and unknown (the write may have happened). `blob.Store` only reads back the write id after an unknown outcome;
- stores the write id atomically with the object (part of the same request on S3, GCS and Azure; an extended attribute set on the local backend's temporary file before it is renamed into place).

Callers treat ETags as opaque version tags: S3 and most compatible stores derive them from an object's contents (an MD5 for single-part uploads), while Azure and GCS change them on every write, so nothing may assume that identical bytes do or don't give the same ETag.

v1 has two backends: the S3 API (which also covers S3-compatible stores that pass the startup check) and a local directory for development, with file locking for compare-and-swap. The local backend's ETag is the contents' MD5, as on S3, computed as the object is written and kept in the same extended attribute as the write id, so a `HEAD` or a listing reads one attribute and never an object's contents. Backup tools often drop extended attributes, so an object without one gets it back on first read: the MD5 is computed again (the same ETag as before) with a zero write id, which is harmless since nothing is in flight after a restore. It needs a filesystem with user extended attributes (ext4, XFS, btrfs, ZFS or APFS) and checks for them at startup. Native GCS and Azure backends can follow. Litestream's own storage clients aren't used, since they don't expose conditional writes.

The node checks its store at startup, on a scratch key under its prefix, and refuses to start if the store misbehaves: a conditional create succeeds and a second one fails with exists, an update with a stale ETag fails with conflict, the write id comes back with the object, an update of a missing key fails with not found instead of creating it, and a delete with a stale ETag fails with conflict and leaves the object. A store or proxy that silently ignored `If-Match` would otherwise break fencing without any error. It is a handful of requests, so it costs nothing, and it tests the deployment's actual store (AWS or a self-hosted one) rather than an emulator.

There is no consensus protocol of our own. Agreement comes from blob storage's conditional writes.

Lease expiry is judged from renewal times, so nodes need roughly synchronised clocks (NTP) and a safety margin.

## Admin API

Later. Any node serves an admin API on its own listener (not the public one or the control API). It answers for the whole cluster, using the coordinator and the other nodes: list nodes and their active shards, show a shard's manifest, force a deactivation or a move, delete a shard and restore one to a point in time.

It could also export a shard as a plain SQLite file (a consistent snapshot) and import one. Support staff can then open a player's data in any SQLite tool, developers can reproduce bugs locally with real data, data export requests can be answered with the file and a broken shard can be fixed offline and imported back.

## Tracing

The node passes W3C trace context (`traceparent`) through every hop, including forwarding between nodes and calls between shards. It also adds its own spans (routing, activation, restore, gate waits). Apps that also propagate it get end-to-end traces across shards.

The node also counts shardless requests that lead to calls to a shard, and logs a sample of their paths, so routes where clients don't name the shard they need show up.

## Deployment

Nodes are disposable. Blob storage holds the data, and local disks are caches.

- **Dependencies:** blob storage, plus the deployment's own load balancer in front of the nodes.
- **Disks:** local NVMe is the fastest option, and ephemeral disks are fine. Size them for the working set plus a cache, not for all data.
- **Scaling up** for a launch spike: add nodes. New players and newly activated shards land on them, and rebalancing moves some busy shards from overloaded nodes. Scaling is by CPU and memory, since storage is elsewhere.
- **Scaling down** after the spike: stop serving, deactivate the node's shards and write deltas for their tails, release ownership and remove the node, which deletes its journal's non-empty segments before it exits (every tail is a delta by then). Nothing else has to move.
- **Planned restarts and upgrades:** the node fails its health check so the load balancer stops sending it new connections, drains short requests and closes long-lived connections with a reconnect code. A drain that can't let go of a shard the app still has open aborts after a timeout (see [Letting go of a shard](#letting-go-of-a-shard)). When it comes back it adopts its own journal and keeps the cached files that are still current (see [Node journals and snapshots](#node-journals-and-snapshots)).
- **Mixed node versions:** during a rolling upgrade, old and new nodes forward to each other and read each other's manifests, node journals and snapshots. Traffic between nodes carries a protocol version, and manifests, node journal keys, snapshots and the local index carry a format version, from v1.
- **Testing fencing:** the guarantees (one writer, a zombie's writes after its journal is fenced ignored, no acknowledged write lost from a kind with sync on) are tested by killing, pausing and partitioning nodes, from v1. The protocol itself is checked in Quint (see stage 2).
- **Development:** a single node with a local directory as blob storage, so the real storage path is always exercised.
- **Single machine:** one node with the local backend on a durable volume (EBS or a Kubernetes persistent volume) and shard files on fast instance storage. Writing to a local directory is cheap, so the flush interval can be short. Moving to a cluster later means copying the blob directory into a bucket.

## Platforms

The node targets Linux only. Game servers and backends that use it must run on the same Linux machine (or VM), since they share SQLite files. Game clients and editors can run anywhere and connect over the network.

Developers on other systems run the node, backend and game server in Linux:

- **macOS**: in Docker (Docker Desktop or similar runs a Linux VM). Shard files must live in a Docker volume, not a bind-mounted macOS folder, since file sharing with the VM is slow and SQLite locking isn't reliable across it.
- **Windows**: in WSL2 or Docker, as is common in game development: develop and run the client on Windows, and run Linux builds of the game server and backend. Shard files must live on the Linux filesystem, not under `/mnt/c`, for the same reason.

Game servers running natively on macOS or Windows (an Unreal dedicated server or play-in-editor, for example) are not supported. The node may build on macOS, since Go cross-compiles, but that is unsupported.

## Consistency model

- One shard, one writer. A SQLite transaction within one shard is the strongest guarantee.
- Across shards, consistency is the app's problem.
- Anything needing agreement goes through a shard that owns it:
  - A trade is a `trade/{id}` shard running a saga: reserve on both player shards, then commit or release, with idempotent steps and resumable state.
  - Shardlite provides no locks across shards. Apps that want them can write them as data in their shards.
  - Global data (name registry, tournament brackets) lives in a global shard. It is single-writer, so very hot global data (leaderboards) should be sharded or aggregated in batches.
  - Global shards are a single point of availability. If their owner crashes, they are unavailable until the lease expires and another node restores them.
- Payments: keep sync on (the default), store payments as an append-only ledger keyed by the provider's transaction id and reconcile against the payment provider.

## Roadmap

### Stages

Each stage ends with something runnable, riskiest parts first. Stages 0 to 3 make v1.

#### 0. WAL capture with Litestream

- Litestream as a library, driven by the node, with shardlite's storage client writing level 0 files into segments.
- Restoring a shard and continuing to capture it, with the first snapshot skipped.
- Deltas and merges with the LTX library.
- Measuring the cost per active shard at thousands of shards.
- Fault-injection tests from the start, including apps that checkpoint themselves and crashes between every step.

#### 1. One durable node

Useful on its own: SQLite per actor with continuous backup.

- The node journal as numbered segments, and LTX files per shard with deltas and merging, on a local directory.
- Restore from a base, deltas and tail, with tests that kill processes between every step.
- Serving over HTTP/1.1, HTTP/2 (including gRPC), server-sent events and WebSocket.
- Activation, the local cache of deactivated shards and creating shards on first use.
- Durability gates (sync on or off).
- The localhost control API: `Shardlite-Target` calls (with `Shardlite-If-Active`), `sync` and delete.
- Call depth limit and deadline propagation.
- A node lease and manifests through the `NodeRegistry` and `ManifestStore` interfaces, so restarts are fenced.
- S3-compatible blob storage, tested against a local emulator. Manifests of deleted and never-written shards are deleted by conditional delete.
- Startup checks that refuse to start: the store's conditional writes misbehave, or the data directory is on a network filesystem (NFS, SMB or FUSE).
- Startup checks that warn: Litestream's metadata directory not on tmpfs, and `vm.max_map_count` or the open file limit (the hard limit, since Go raises the soft one) too low for the expected active shards.
- Cancellation propagation (mostly there through request contexts: a test that a cancelled call stops its held and gated requests).
- Ephemeral kinds (`"ephemeral": true`), for soft state such as matchmaking queues, lobbies, presence, throwaway match state and world data that can be lost (terrain destruction). Ownership works as usual, with a manifest created by CAS, so there is still one activation per shard. Nothing is shipped: no WAL capture, node journal, snapshots or merges, and gates never hold their output. On deactivation the node deletes the local file and the manifest (by conditional delete), so a shard costs two manifest writes in its life. After a crash the state is gone, and the next activation starts empty. `sync` has no meaning for them. The manifest records `ephemeral` at creation, and the node refuses to activate a shard with data whose kind has become ephemeral (and logs why), so a change to the kinds file can't silently stop shipping its writes.

#### 2. A cluster

The lease, fencing and adoption protocol is specified in [Quint](https://quint-lang.org), with lost requests and lost responses modelled, and checked before it is implemented.

- Leases and manifests across nodes, with fencing.
- Forwarding between nodes over HTTP/2 on a trusted private network, with gates on calls to other shards.
- Listing leases to notice dead nodes, adopting a crashed node's journal and fencing it.
- Graceful handoff through snapshots.
- Draining a node on request (an admin endpoint or a signal) and scaling down by deactivating and releasing.
- Protocol versions for rolling node upgrades.

#### 3. Ready for a launch

- Placement and rebalancing from the counts in leases.
- Load shedding with `503`, a cap on requests in flight per shard, failing fast for unreachable nodes and `GOAWAY` for shedding HTTP/2 clients.
- Protection against floods of new ids on public kinds: shards that were never written deactivate after a few seconds instead of the kind's idle timeout, and a cap on activations in flight answers `503` beyond it.
- Virtual-hosted-style S3 addressing where a store prefers it (stage 1 always uses path style, which AWS, R2, emulators and self-hosted stores accept).
- LTX files over S3's 5 GB limit for a single PUT, with multipart uploads (LTX files have unique names, so a plain multipart upload can replace the conditional create they use now).
- Distributed tracing with W3C trace context.
- Hash prefixes on per-shard keys (`manifests/7f/users/42`, and the same for LTX files), so a launch spike spreads over many S3 prefixes instead of hitting the limit per prefix (about 3,500 writes a second, raised only gradually) on `manifests/users/`. Changing the key layout later means moving objects, so it should be settled before production data exists.
- Format versions for everything shardlite stores, with a marker per deployment that a node from an older release refuses.
- The `shardlite` command for operators: `ls`, `export`, `import` and `verify`, through the running node, or directly on blob storage when no lease is live.
- Startup checks that refuse to start:
  - clock skew against the store's `Date` header beyond the lease safety margin (warning at half of it, and watching while running);
  - a non-empty prefix without this deployment's format marker.

#### 4. Pinning

- Pins with connection info, lookups and lease deadlines.

### Later

- Direct pulls of a shard's SQLite file from the previous owner's cache (with `sqlite3_rsync`, for example), as a faster alternative to downloading its snapshot.
- Agones integration.
- Commits merged per flush window: a shard's consecutive commits within one flush window become a single LTX record with only the latest version of each page, so a hot shard rewriting the same pages hundreds of times a second sends each page once per flush.
- A middle merge level for large shards, so their base is only rewritten once the middle level reaches a fraction of its size.
- Egress proxy that gates external HTTP calls (see [Egress proxy](#egress-proxy)).
- WebRTC signalling (ICE-lite).
- HTTP/3 and WebTransport.
- Snapshot reads: read-only access to any shard from any node or tool, without touching the owner. A request asks for one with a header, or `Shardlite-Max-Age: 30` allows one. The node serves it through the app with `Shardlite-Path` pointing at a read-only copy in its local cache and a header giving the snapshot's age, with no activation or claim. A shard that isn't active has its current state in its snapshot. For an active shard it is stale by up to the tail age limit, and asking the owner for its tail brings it up to date. Uses: offline players' profiles, admin and support UIs and exports as plain SQLite files for analytics (with DuckDB, for example).
- Read replicas of hot active shards, for reads that must be fresh and are too many to forward. A node that keeps forwarding reads for a shard in a kind that opts in starts from its snapshot and subscribes to the owner, which streams the shard's committed pages over the existing HTTP/2 connection. Staleness is milliseconds, and cost follows that shard's writes only. Following the owner's whole node journal instead would mean reading every shard's writes on that node. If the stream breaks, the replica falls back to forwarding. Ownership and fencing are unaffected.
- Explicit creation as a shard kind setting: shards of that kind are only created by the app, and requests for missing ones get a `404` from the node.
- Activation repartitioning, as in Orleans: move shards that call each other a lot onto the same node.
- Schema versions reported by the app (`Shardlite-Version` on a response, or a reserved table row), opaque numbers that only go up, kept in the manifest when they change. They are hints: a missing or wrong one costs efficiency, never correctness, since the app still refuses shards that are too new. Uses:
  - Version-aware placement: during a rolling deploy, shards migrated by a new app version are never moved to nodes running an older one.
  - Bulk operations: the node sends an app-defined request (`POST /downgrade?to=11`) to every shard of a kind matching a version filter, through normal routing at a capped rate. For down migrations on rollback, backfills, data fixes and eager upgrades. Shards not cached anywhere are claimed and restored to run it.
- Ticks, as in Orleans timers: a shard kind setting (`"tick": "500ms"`) that makes the node call every active shard of that kind at that interval. Not durable, skipped while a shard is busy and stopped when it deactivates. For matchmaking, match phases and countdowns.
- Reserved tables: shardlite's own tables in each shard, prefixed `_shardlite_` (next to Litestream's `_litestream_seq` and `_litestream_lock`), which the app writes in the same transaction as its own changes. The node creates and migrates them additively in every shard at activation, before the app opens the shard, so any shard can start using a feature without a migration step. Unlocks messages (below) and the app's version as a row.
- Messages: durable one-way messages between shards, as actors send. Synchronous calls stay plain HTTP.
  - The app inserts a row into `_shardlite_outbox` (`target`, `name`, `payload`, `content_type`, optional `deliver_after`) in the same transaction as its change. The payload is opaque to shardlite.
  - Once the commit is durable in blob storage, whatever the kind's gate, the sender's node delivers it, in order per sender and target. Messages stay in the sender's outbox until the receiver has applied them. It retries until acknowledged. Messages that keep failing with a `4xx` become dead letters, shown in the admin API.
  - Serving apps get a push: `POST /_shardlite/messages/{name}` with `Shardlite-Target`, `Shardlite-Caller` and the sender's row id. The `2xx` is gated as with sync on, whatever the kind, so a message is never acknowledged before its effect is durable.
  - Pinned processes get a stream instead: server-sent events from the control API, acknowledged with an explicit call after applying and calling `sync`. Until a process pins or an app serves the shard, its messages wait in the senders' outboxes.
  - The receiver applies each message in its own transaction, together with a per-sender high-water mark in a reserved table (`_shardlite_received`). Since delivery is in order, anything at or below the mark is a duplicate and is skipped.
  - Delivery state is kept in the node's own metadata, since it doesn't write to the app's tables. Pending messages move with the shard.
  - Reminders, as in Orleans, are messages to the shard itself with `deliver_after`. A repeating reminder is a handler that inserts its next message. The owning node indexes due times for the shards it owns, active or cached, and activates them as needed. A dead node's index is adopted by a surviving node, which claims each shard as its messages come due.
  - Uses: transfers and trades by custody (the item moves into the trade shard and out again), without sync on the sending kind, since a lost commit loses its messages too. Fan-out in one transaction (XP for 100 players at the end of a match). Side effects once a write is durable, by messaging the shard itself.
- `Shardlite-Prefetch` response header, to warm shards a player is about to use where they live: login or matchmaking sends `Shardlite-Prefetch: users/42, guild/7`, and the node strips it and warms each shard in the background, without opening it or calling the app. A shard owned by another node is warmed by that node from its own journal, so prefetching never moves a shard with a live owner. An unowned shard is claimed, as its first request would do anyway. Missing shards are ignored, and the number per response is capped.
- Point-in-time restore of a shard as an ops tool, from old bucket versions where the deployment keeps them. Having every node write deltas for its active shards before each deploy gives a rollback point if a migration goes badly wrong, at the cost of writes made since.
- Holding requests during a handoff instead of failing them.
- Hibernatable WebSockets, as in Durable Objects: the node holds the connection and delivers each message to the shard as an HTTP request, in order. The app sends through the control API. Connections then survive shard moves and app restarts, and idle connections keep no shard active. A mode per shard kind. Server-sent events fit the same model more simply, since they only push.
- Admin API on any node, answering for the whole cluster.
- Staging and load-test copies of production: the manifests and LTX files copied under another prefix (server-side copies, so nothing passes through the nodes). Active shards write deltas first.
- Whole-database checksums in LTX files, so every restore verifies it produced exactly the right database. Needs a checksum per page for every active shard.
- Leaving journal expiry to the store where it has something cheaper than listing and deleting (a lifecycle rule that can keep fences, for example), with the node's sweeper turned off there.
- A sweeper for LTX files no manifest refers to that were missed by normal cleanup (after crashes mid-write, for example).
- Merging deltas runs on each shard's owner by default. It can move to a separate service if it competes with game traffic on the hosts.
- Maybe: per-shard encryption keys kept with the manifest. Deleting the key makes every copy unreadable at once (old LTX files, dead node journals, versioned buckets), and gives encryption at rest.

## Open questions

- **Flush interval.** It sets the loss window on a node crash (for shard kinds with sync off), the latency of sync and the PUT rate per node. 250 ms for now.
- **WAL capture correctness.** Capturing frames from files another process writes is the riskiest part. Litestream still has open data-loss issues in exactly this area (external checkpoints, WAL resets), so shardlite depends on their fixes and needs its own fault-injection tests from the start.
- Tail age limit and merge thresholds: shorter tails mean faster recovery after a crash, at the cost of more deltas.
- Node lease TTL: short enough for fast failover, long enough to survive VM pauses and network blips.
- **Regions.** v1 is one region. Region-locked games run a separate cluster per region and send players to theirs at login. Global games (a guild with members in EU and NA) would need more. Each shard would have a home region in the manifest, set at creation (where it was first written, or from a hint). Requests from other regions would be forwarded, and moves between regions would be explicit, since blob storage is regional. Data residency would pin a shard's region. Coordination would likely be a coordinator per region plus a small global directory of home regions.
- How to secure traffic between nodes (see [Traffic between nodes](#traffic-between-nodes)).
- Rebalancing thresholds.

## Prior art to read

- HBase and Bigtable: one write-ahead log per server shared by many regions, and log splitting when a server dies. The closest precedent for node journals.
- Neon: the same split as node journals and snapshots (safekeepers for the WAL, image and delta layers in object storage), and generations for fencing page servers.
- Litestream: snapshots plus WAL segments per database, stored as LTX files. Shardlite uses it as a library and shares the WAL across a node's shards.
- BookKeeper: garbage collection of shared logs.
- WarpStream: stateless agents on object storage.
- SlateDB: an LSM tree on object storage. Its fencing of WAL writers by conditional creates is the model for node journals.
- mvsqlite: SQLite pages stored in a key-value store.
- ActorDB: a SQLite database per actor, with a schema per actor type, Raft replication and transactions across actors, queried as a database server rather than opened locally.
- Durable Objects: SQLite storage with output gates.
- [Durable Actors](https://github.com/TerseAI/durable-actors) (Terse, 2026): an open-source take on Durable Objects. Actors are written in its TypeScript or Python SDK and run inside its runtime, with a Postgres control plane and an LTX chain per actor in object storage. Its `terse-ltx` and `terse-litestream` crates come from it.
- Pat Helland, "Life beyond Distributed Transactions": entities that change atomically only within themselves and talk through at-least-once messages, remembering what each partner has sent. Shardlite's model, almost exactly.
- SQL Server Service Broker: queues as tables inside the database, sent and received in the same transactions as the data, delivered exactly once and in order between databases.
- Spanner: commit wait, which holds acknowledgements until TrueTime uncertainty has passed.
