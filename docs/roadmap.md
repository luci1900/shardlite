# shardlite roadmap

## Stages

Each stage ends with something runnable, riskiest parts first. Stages 0 to 3 make v1.

### 0. WAL capture with Litestream

### 1. One durable node

### 2. A cluster

The lease, fencing and adoption protocol is specified in [Quint](https://quint-lang.org), with lost requests and lost responses modelled, and checked before it is implemented.

- Leases and manifests across nodes, with fencing.
- Finding the owner: reading the manifest on a miss, caching where shards live, and correcting the cache when a forward reaches a node that no longer owns the shard, and retrying the request once at the owner that node names.
- Forwarding between nodes over HTTP on a trusted private network: requests, streams and WebSockets, calls with their gates, cancellation as stream resets, deadlines and hops.
- Messages between nodes: intakes forwarded like calls, with `Shardlite-Inbox` honoured only from nodes, and wake hints waking shards through routing on whichever node owns them. Wake scans spread over the nodes, as lease listings are.
- An unreachable owner: the forwarding node reads its lease and answers at once (`503`, or the gRPC equivalent) until the lease expires and the shard can be claimed after adoption.
- Listing leases to notice dead nodes, adopting a crashed node's journal and fencing it.
- Graceful handoff through snapshots.
- Draining a node on request (an admin endpoint or a signal) and scaling down by deactivating and releasing.
- Protocol versions for rolling node upgrades.
- The startup check on clock skew, since leases between nodes rely on clocks.
- Fault tests across several nodes: kills, pauses and partitions between nodes, checking that a zombie's writes are ignored once its journal is fenced and that no acknowledged write is lost from a kind with sync on.

### 3. Placement and pinning

- Placement and rebalancing from the counts in leases.
- Pins held by an open control API request carrying the lease deadline, with requests, calls and inbox deliveries proxied to the pinning process. Pinned kinds.

### 4. Ready for a launch

- Load shedding with `503`, a cap on requests in flight per shard and `GOAWAY` for shedding HTTP/2 clients.
- A cap on how fast a node activates shards from wake hints, so many timers due at once can't flood it. Due shards wait their turn, and the node logs when the cap holds them back.
- The control API over TCP on Kubernetes. A DaemonSet node's localhost isn't reachable from app pods, so the node listens on the host's address and requires a token that it writes into the shared `hostPath` directory. Only pods on that host can read it, so `Shardlite-Caller` stays trustworthy. Without it, apps whose HTTP clients can't use Unix sockets have no control API on Kubernetes.
- Reaching the owner through the load balancer. The node sets a cookie naming the owner of the shard a request was for (the cookie's name is a setting), so load balancers that route by a cookie's value (HAProxy, Envoy, nginx) send the client straight to the owner, and fall back to any node when the owner is down or draining. A client that uses several shards gets the cookie for one of them: its home shard, chosen by a kind setting or as the shard its WebSocket is on (to be decided). Load balancers that only stick a client to the node that answered (AWS ALB, Google Cloud) can't follow the owner, so they rely on placement preferring the local node. Tested setups and a guide section for each.
- Protection against floods of new ids on public kinds: shards that were never written deactivate after a few seconds instead of the kind's idle timeout, and a cap on activations in flight answers `503` beyond it.
- Virtual-hosted-style S3 addressing where a store prefers it (stage 1 always uses path style, which AWS, R2, emulators and self-hosted stores accept).
- LTX files over S3's 5 GB limit for a single PUT, with multipart uploads (LTX files have unique names, so a plain multipart upload can replace the conditional create they use now).
- Distributed tracing with W3C trace context, and counting shardless requests that lead to calls to a shard.
- Metrics for everything the node does: requests, activations, flushes and gate waits, deltas and rebases, storage calls, leases and the egress proxy when it exists.
- Hash prefixes on per-shard keys (`state/shards/7f/users/42`, and the same for LTX files), so a launch spike spreads over many S3 prefixes instead of hitting the limit per prefix (about 3,500 writes a second, raised only gradually) on `state/shards/users/`. Changing the key layout later means moving objects, so it should be settled before production data exists.
- Format versions for everything shardlite stores, with a marker per deployment that a node from an older release refuses.
- The `shardlite` command for operators: `ls`, `export`, `import` and `verify`, through the running node, or directly on blob storage when no lease is live.
- A crashed shard costs only that shard. A panic in a shard's own goroutines quarantines it (its requests get `503` and an alert is logged) while the rest of the node carries on, instead of crashing the process. Once no process has the file open, the node lets the shard go from its local file: a new base written from the file and swapped into the manifest by compare-and-swap, which holds every commit (including ones never captured) and supersedes the journal. The next request activates it as usual. Its messages can restart on their own instead, since their state is rebuilt from the shard's tables.
- No uncaptured commits lost to a held file. A shard file that an app still has open across a restart is deleted once the app closes it, with whatever it committed that was never captured. Letting it go from the file, as for a crashed shard, keeps those commits.
- Saving shards when the segment writer panics: letting them go from their local files (a few rebases at a time) until the shutdown deadline, instead of leaving all of them to adoption.
- A soak test: the fault rig running for hours against a cluster, with kills, pauses, partitions, slow storage and restarts, checking the same guarantees as the per-scenario tests.
- A linearizability check of single shards under faults, as Jepsen does: clients record every read and write with its start and end, and a checker (Jepsen with Knossos, or Porcupine, which is in Go) confirms the history is linearizable per shard for kinds with sync on, while the fault rig kills, pauses and partitions nodes and skews clocks within the limit. Clocks pushed past the limit should show the node stopping rather than a violation.
- A startup check that refuses to start on a non-empty prefix without this deployment's format marker.
- A sweeper that deletes LTX files no manifest refers to (left by crashes mid-write, deltas never adopted and rebases that failed after their upload), once they are older than any write still in flight could be.

## Later

- Large shards: a rebase uploads the whole database, so shards of gigabytes may want a higher file limit or a middle level of merged deltas.
- Batched deletes of acknowledged outbox rows: each is deleted in a commit of its own by its delivery. Deletes waiting a few milliseconds to share a commit would cut the commits a busy sender makes, if they show up in profiles.
- Holding requests that reach a shard during a handoff until the new owner is ready, instead of answering `503`. Worth it if a launch's error rates show the `503`s from rebalancing hurt (see [Routing and forwarding](internals.md#routing-and-forwarding)).
- Keeping the local cache across a node restart, for deployments where node redeploys are frequent or a cold cache is costly (large shards, or nodes redeployed with every app deploy as a sidecar). It needs the cache to be trustworthy after an OS crash: the index and shard files made durable at deactivation, or discarded when the boot id changes, or checked against whole-database checksums once LTX files carry them.
- Cancelling a running rebase when its shard deactivates, if waiting for it (about 9 s for 1 GB) ever matters. The upload can simply be abandoned.
- The [egress proxy](#egress-proxy) that gates external HTTP calls.
- HTTP/3 and WebTransport.
- Forwarded WebSockets as streams on the connection nodes already share, through extended `CONNECT` (RFC 8441 for HTTP/2, RFC 9220 for HTTP/3), instead of a TCP connection of their own per socket. Over HTTP/3, QUIC also keeps one socket's packet loss from stalling the others. Worth it once the library supports it by default (Go's `net/http` has it off, and its client can't send one), or if descriptors between nodes show up as a cost.
- Encryption and authentication of [traffic between nodes](#securing-traffic-between-nodes).
- Shard versions: a shard's state as of one manifest version (its current one, after a delta if it is active, or an old one where the bucket keeps versions), materialised from its base and deltas by the restore path that already exists. One primitive, used four ways:
  - **Snapshot reads:** read-only access to any shard from any node or tool, without touching the owner. A request asks for one with a header, or `Shardlite-Max-Age: 30` allows one. The node serves it through the app with `Shardlite-Path` pointing at a read-only copy in its local cache and a header giving the snapshot's age, with no activation or claim. For an active shard it is stale by up to the tail age limit, and asking the owner for its tail brings it up to date. Uses: offline players' profiles, admin and support UIs, and exports as plain SQLite files for analytics (with DuckDB, for example).
  - **Forking,** as Heroku Postgres forks a database: a new shard made from another's version, its files copied server-side (`CopyObject`) under the new shard's generation and its manifest created by conditional create. For templates (new worlds and instanced dungeons), save slots and reproducing a player's bug on a copy. Copying rather than referencing the source's files, as Neon's branches do, keeps rebases and deletes of the source free of reference counting.
  - **Staging and load-test copies** of production: forking every shard under another prefix, with nothing passing through the nodes.
  - **Point-in-time restore** as an ops tool: making an old version current again. Having every node write deltas for its active shards before each deploy gives a rollback point if a migration goes badly wrong, at the cost of writes made since.
- Explicit creation as a shard kind setting: shards of that kind are only created by the app, and requests for missing ones get a `404` from the node.
- Several apps behind one node, for teams that want to deploy their own process for their own kinds, all in one trust domain. Today a local reverse proxy as `SHARDLITE_APP` does the same with no change to the node.
  - Each kind names its app in the kinds file (`"wallet": {"app": "billing"}`), and the environment gives each app's address (`SHARDLITE_APP_BILLING=http://127.0.0.1:9001`). Kinds that name none, and shardless requests, go to the default app (`SHARDLITE_APP`).
  - The node polls each app's health. It reports ready while the default app is healthy, and requests for a kind whose app isn't get `503`, so one team's bad deploy doesn't take nodes out of the load balancer.
  - Every node that serves runs every app, so placement never needs to know which apps a node runs.
  - Designed with pinning (stage 3): where a shard's traffic goes is then one rule, the pinning process if there is one, else its kind's app, else the default app.
  - Messages and calls between kinds of different apps work as they do now, since the node routes them.
- Activation repartitioning, as in Orleans: move shards that call each other a lot onto the same node.
- Schema versions reported by the app (`Shardlite-Version` on a response, or a reserved table row), opaque numbers that only go up, kept in the manifest when they change. They are hints: a missing or wrong one costs efficiency, never correctness, since the app still refuses shards that are too new. Uses:
  - Version-aware placement: during a rolling deploy, shards migrated by a new app version are never moved to nodes running an older one.
  - Bulk operations: the node sends an app-defined request (`POST /downgrade?to=11`) to every shard of a kind matching a version filter, through normal routing at a capped rate. For down migrations on rollback, backfills, data fixes and eager upgrades. Shards not cached anywhere are claimed and restored to run it.
- More reserved tables as features need them, created and migrated additively at activation as the outbox and inbox are, so any shard can use a feature without a migration step.
- Exactly-once messages: the receiver's node records the outbox ids it took from each sender (and its generation) in a reserved table, in the same commit as the intake, and skips an intake it already took. Entries expire after a week, long after a sender gives up. Invisible to apps, so it can be added without changing anything they write.
- Intakes from apps: `Prefer: respond-async` on a `POST` to the control API puts the call in the target's inbox, answered `202` once it is durable. A direct call with sync on already gives a webhook handler the same guarantee, so this only adds decoupling: the caller is answered before the handler runs, and shardlite retries a failing handler.
- `Shardlite-Prefetch` response header, to warm shards a player is about to use where they live: login or matchmaking sends `Shardlite-Prefetch: users/42, guild/7`, and the node strips it and warms each shard in the background, without opening it or calling the app. A shard owned by another node is warmed by that node from its own journal, so prefetching never moves a shard with a live owner. An unowned shard is claimed, as its first request would do anyway. Missing shards are ignored, and the number per response is capped.
- Hibernatable WebSockets, as in Durable Objects: the node holds the connection and delivers each message to the shard as an HTTP request, in order. The app sends through the control API. Connections then survive shard moves and app restarts, and idle connections keep no shard active. A mode per shard kind. Server-sent events fit the same model more simply, since they only push.
- The [admin API](#admin-api) on any node, answering for the whole cluster.
- An [SDK](#sdk) per language.
- Kinds file read from blob storage, so every node reads the same version.
- Whole-database checksums in LTX files, so every restore verifies it produced exactly the right database. Needs a checksum per page for every active shard.
- Leaving journal expiry to the store where it has something cheaper than listing and deleting (a lifecycle rule that can keep fences, for example), with the node's sweeper turned off there.
- Gossip between nodes to spread load counts faster than lease renewals, if rebalancing needs fresher figures. The same channel could carry changes of owner (handoffs, rebalancing and takeovers), applied only to owner cache entries a node already has, so the first request after a move goes straight to the new owner instead of getting a `421`. Worth it if moves show up as bursts of `421`s, during a drain for example. Claims of shards nobody has cached aren't sent, since that would make gossip a second directory beside the manifests.
- Maybe: per-shard encryption keys kept with the manifest. Deleting the key makes every copy unreadable at once (old LTX files, dead node journals, versioned buckets), and gives encryption at rest.

## Later designs

Features from [Later](#later) that are designed in more detail.

### Admin API

Any node serves an admin API on its own listener (not the public one or the control API). It answers for the whole cluster: list nodes and their active shards, show a shard's manifest, force a deactivation or a move, delete a shard and restore one to a point in time. It could also export a shard as a plain SQLite file (a consistent snapshot) and import one, so support staff can open a player's data in any SQLite tool, developers can reproduce bugs locally with real data, data export requests can be answered with the file and a broken shard can be fixed offline and imported back.

### Securing traffic between nodes

v1 assumes the nodes are on a trusted private network. The options for encrypting and authenticating traffic between nodes:

- **Built-in mTLS from a shared cluster secret.** Every node derives the same CA from the secret and issues itself a node certificate. Setup is one secret in every environment.
- **Your own certificates** from cert-manager, Vault, step-ca or similar.
- **An encrypted network** (WireGuard, Tailscale) that already handles it, with shardlite staying unencrypted. This works on VMs, bare metal and Kubernetes with low overhead.

Service meshes (Istio, Linkerd) are not a target. They add two proxy hops to every call between nodes (roughly 0.5–1 ms), which is significant next to the forwarding budget. They also put a second proxy beside shardlite's own and only work on Kubernetes. In a cluster that already runs a mesh, shardlite's traffic between nodes should be excluded from it.

### Egress proxy

The node could also offer an egress proxy for HTTP and HTTPS, so calls to external services are gated too. Using it is deployment configuration: set `HTTP_PROXY` and `HTTPS_PROXY` in the app's environment. It is best effort.

- The app's environment then also needs `NO_PROXY=localhost,127.0.0.1`, so calls to the node's control API go to it directly rather than through the egress proxy.
- HTTPS goes through `CONNECT` tunnels. The node gates the tunnel's bytes like any other output. There is no TLS interception or parsing.
- Egress is gated by waiting for the next flush, so no egress traffic has to be attributed to a shard. It waits only while the node has pending writes from kinds with sync on, up to a flush interval under load. Otherwise it passes straight through. Telemetry and logging can bypass it by destination (`NO_PROXY`, or an allowlist in an environment variable).
- The app can also skip the gate call by call: `Shardlite-Sync: off` on the request (or on the `CONNECT` request for HTTPS, where the client can set proxy headers), or simply an HTTP client configured without the proxy. Where a datacentre requires all egress to go through its proxy, the header is the way, and the node chains to that proxy.
- Egress is fenced. While the node doesn't hold its lease (lost, or not yet renewed after a pause) or is draining, the proxy refuses new calls and closes open tunnels, so a zombie node can't send side effects (payments, emails, webhooks) for writes that won't survive. Calls that skipped the gate with the header are fenced too, since fencing is about the node, not one write. An app can't easily do this itself, because it doesn't know when the node loses its lease. The refusals (`503`) and closed tunnels are another way it finds out, alongside failing `sync` calls.
- Metrics per destination host: calls, bytes, errors, time waiting at the gate and calls refused by fencing. The proxy sees only the host for HTTPS, which is enough.
- It only covers clients that honour the proxy settings. Most HTTP clients do; some need configuration (Node's `fetch`, Java).

Without the proxy, or for anything it can't see, `sync` gates side effects explicitly.

### SDK

A small library per language that makes the app contract hard to get wrong without hiding it. Everything stays plain HTTP and SQL, so apps without it keep working. It never opens databases or owns a pool: the driver is the app's choice (`mattn/go-sqlite3`, `modernc.org/sqlite` or `ncruces/go-sqlite3` in Go, `better-sqlite3` or `node:sqlite` in Node, `Microsoft.Data.Sqlite` in .NET), so the SDK works alongside it.

- **Request context.** From the request's headers (a map or a getter, so any framework can supply them): the shard (`kind`, `id`), its file, the caller, hops, deadline (as a timeout where the language has one) and inbox id, whether the request is internal (it has `Shardlite-Caller`), and a check that a route's id matches `Shardlite-Target`.
- **Opening shards.** The recommended pragmas as SQL to run on every connection the app opens, and the idle limit its pool must respect so deactivation isn't blocked (one call to `SetConnMaxIdleTime` in Go). A helper that decides from `user_version` and `application_id` whether to migrate or refuse a shard, while migrations stay in the app's own tooling.
- **Writes.** With `BEGIN IMMEDIATE`, the app passes its transaction to the helpers below, and a `SQLITE_BUSY` from `BEGIN` means overload (`503`). With deferred transactions, a callback wrapper reruns the app's code on `SQLITE_BUSY`, so the callback must touch nothing outside the transaction.
- **Calls.** `call(target, method, path, body)` to the control API, copying the caller, hops, deadline and `traceparent` from the current request, with options for `Shardlite-If-Active` and `Shardlite-Sync`. For gRPC, a client interceptor that does the same, so generated stubs just work.
- **Messages and the inbox,** each taking the transaction (the app's own with `BEGIN IMMEDIATE`, or the one the wrapper passes to its callback), so they commit with the app's work: `send(target, path, body)` inserts an outbox row with its hops, `schedule(key, path, body, due)` and `cancel(key)` manage the app's own rows, and `consume()` deletes the row named by the request's `Shardlite-Inbox-Id`.
- **Durability.** `sync()` calls the control API, for purchases and other writes that must not be lost. `commitAndSync(tx)` commits an immediate transaction and then syncs, and the deferred wrapper takes an option to sync after its commit (outside its retries, since by then the commit has happened). A failure is a distinct error meaning the outcome is unknown, so the app doesn't acknowledge: it answers `503`, or the client retries with its idempotency key. Syncing a single HTTP response is a header the app sets itself (`Shardlite-Sync: on`), so the SDK only exports its name.
- **Testing.** A fake for unit tests: call a handler with the headers set and a temporary SQLite file, then inspect and deliver the outbox and inbox by hand.
- **Pinning:** a pin that holds its open request, gives the lease deadline as a callback or a context, and registers the process's address.

Each language has a framework-neutral core and thin adapters for its standard interfaces where it has them (`net/http` in Go, ASP.NET Core in .NET, WSGI and ASGI in Python), as OpenTelemetry does for the same problem. Where a language has several families, as Node and Java do, it gets adapters for the biggest and the core for the rest, and gRPC interceptors cover gRPC everywhere. Not included: routing, an ORM, a migration framework or retry policy for calls, which are the app's choices.

## Open questions

- **Flush interval.** It sets the loss window on a node crash (for shard kinds with sync off), the latency of sync and the PUT rate per node. 250 ms for now.
- **WAL capture correctness.** Capturing frames from files another process writes is the riskiest part. Litestream still has open data-loss issues in exactly this area (external checkpoints, WAL resets), so shardlite depends on their fixes and needs its own fault-injection tests from the start.
- Tail age limit and file limit: shorter tails mean faster recovery after a crash, at the cost of more deltas, and rarer rebases mean more files per restore.
- Node lease TTL: short enough for fast failover, long enough to survive VM pauses and network blips.
- **Regions.** v1 is one region. Region-locked games run a separate cluster per region and send players to theirs at login. Global games (a guild with members in EU and NA) would need more. Each shard would have a home region in the manifest, set at creation (where it was first written, or from a hint). Requests from other regions would be forwarded, and moves between regions would be explicit, since blob storage is regional. Data residency would pin a shard's region.
- How to secure traffic between nodes (see [Securing traffic between nodes](#securing-traffic-between-nodes)).
- Rebalancing thresholds.
- **Throughput of a hot shard's inbox.** The node delivers due inbox rows to a shard's own handlers concurrently, but SQLite has one writer, and handlers that find the write lock taken wait in `busy_timeout`, which sleeps in steps of milliseconds. A hot shard (a leaderboard, a matchmaking queue) may do better with inbox delivery at a low concurrency per shard, or with handlers that take several due rows in one transaction. To measure with the bench: delivery to one shard at concurrency 1, 4 and 32, with and without `BEGIN IMMEDIATE` in the handler.
