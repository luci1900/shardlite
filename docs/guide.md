# shardlite guide

For people who build apps on shardlite and people who run it. The [overview](overview.md) introduces the concepts, and the [reference](#reference) at the end lists every header, endpoint, setting and reserved table. How the node does what it does is in [internals](internals.md).

## Serving

An app uses shards in two ways, and can do both:

- **Serving** (typical for backends over HTTP and WebSockets): the node routes requests and calls for any shard of a kind the app serves. Any id can be called and the first call creates the shard (see [Creating shards](#creating-shards)).
- **Pinning** (typical for game servers): a process pins one specific shard and serves its clients itself, over any protocol. See [Pinning](#pinning).

The node sits in front of the app. For each request it works out the shard (see [Routing requests to shards](#routing-requests-to-shards)), activates it if needed and forwards to the app with:

```
Shardlite-Opened: game.users/42
Shardlite-File:   /data/game.users/42.k3x9.db
```

`Shardlite-Opened` is the shard the node opened for this request, and the node always sets it from its routing decision. It says what the node did, not what the request is for. The app works out which shard a request is for from the request itself (the path, the payload, the first gRPC message, whatever its protocol carries). If that is the shard the node opened, the app uses the open file. If not, or if the node opened nothing, the app relays the request to the right shard through the [control API](#localhost-control-api), passing the client's credentials on, and streams the answer back. A wrong or missing target from a client therefore costs a hop and never correctness. The app marks a relay with `Shardlite-Relay: true` and no `Shardlite-Caller`, and passes on the client's `X-Forwarded-For`, `X-Forwarded-Host` and `X-Forwarded-Proto`. The node serves a relay as the client's request it is, with a client's limits: only kinds clients can name, never creating a shard, no caller, and hops counted as a call's are. A relay that sets `Shardlite-Caller` gets `400`. A client's WebSocket can be relayed. The handler sees a relay as it would have seen the client's request had the hint been right. The kind is everything before the first `/` and the id is everything after. A kind's name is qualified by the app that serves it (`game.users`, `_system.dispatch`), so a shard's name says whose it is (see [Shard kinds](#shard-kinds)). Shardless requests have no `Shardlite-Opened`.

The proxy handles HTTP, gRPC, server-sent events and WebSocket, passing requests through unchanged apart from `Shardlite-*` headers. The node strips those from client requests and app responses, so they can't be spoofed or leak. Client requests for paths under `/_shardlite/` are refused, except the node's own readiness check `GET /_shardlite/ready` (the same path the app serves for its node; clients can't reach the app's through the node). Custom UDP, WebRTC and raw TCP go direct to a process that pins a shard.

### What an app must do

The app and node share a local filesystem on one kernel (the same host, with a shared volume in containers). Network filesystems are not supported, since they break SQLite locking.

The app opens `Shardlite-File` with any SQLite driver of 3.37.0 (2021) or later. The floor is the reserved tables being STRICT: the keyword is part of the file's schema, so an older SQLite cannot open the file at all. That file is opaque, so apps never build paths themselves. The app:

- uses WAL mode with default locking (not `locking_mode=EXCLUSIVE` or a VFS without shared memory);
- only runs passive checkpoints, as SQLite's automatic checkpoints are. `FULL`, `RESTART` and `TRUNCATE` always find the shard busy, since the node holds a read transaction on it;
- closes its connections to a shard within the kind's idle timeout of last using it, for example from a pool that closes idle connections well within that timeout (`SetConnMaxIdleTime` in Go). The node only lets go of a shard once no other process has it open, so a longer-held connection keeps the shard active and blocks moving it. After a node restart, a shard the app still has open from before is unavailable until the app closes it;
- treats the tables whose names start with `_shardlite_` as reserved (see [Reserved tables](#reserved-tables));
- serves `GET /_shardlite/ready`, answering its declaration (see [Shard kinds](#shard-kinds)), and `POST /_shardlite/reminders` if it sets reminders (see [Reminders](#reminders));
- treats the shard the node opened as a hint, except on internal routes (see [Serving](#serving) and [Calls between shards](#calls-between-shards));
- follows the [rules for app code](#rules-for-app-code).

### Schema migrations

Migrations belong to the app, using whatever tooling it already uses. The node knows nothing about schemas. Recommendations:

- Check `PRAGMA user_version` when opening a shard and migrate if needed. Migrations are lazy, so only shards opened since a deploy are on the new version.
- Refuse shards newer than the newest version the app can read.
- Set `PRAGMA application_id` in the first migration to a hash of the app's identifier and the shard kind, and check it on open, so a shard is never opened as the wrong kind.
- Make the tables STRICT, so the save format refuses what it cannot read back: a value of the wrong type is an error at the write instead of a column that holds a surprise.
- Prefer expand and contract, so rolling back is just redeploying the previous release. For changes that can't be additive, ship down migrations too.

### Shard kinds

The app declares itself at its readiness route: `GET /_shardlite/ready` answers a JSON declaration naming the app, its version and the kinds it serves, with how each is configured. The node polls the route, and a 2xx answer parsed as a valid declaration is both the app's health check and the source of its kinds. Adding or changing a kind is a deliberate deploy step:

```json
{
  "app": "game",
  "version": "2026.10.0",
  "kinds": {
    "users":  { "public": true, "idle_timeout": "10m" },
    "chat":   { "public": true, "sync": "none" },
    "wallet": {}
  }
}
```

Keep the declaration in a JSON file checked into the app's repository, and serve it from the route (embedded in the binary or read from the file). The declaration is a contract between the app and the cluster: the review and diff of a change to it belong in version control, and the route just publishes what the repository already says.

The route is under `/_shardlite/`, which the node refuses to client requests, so the declaration reaches the node only. It lists every kind, including ones clients may not name, and the node works to keep clients from learning those exist. It is also the same path as the node's own readiness route, which is the one client requests may make: the load balancer polls the node's, the node polls the app's, and the node's answer includes the app's, since the node is not ready unless the app's declaration has come in.

- The app's name is lowercase letters, digits, underscores and dots, up to 31 characters, and may carry a scope (`acme.billing`). It is part of every stored path, so it is chosen once: renaming an app moves its data. A name starting with `_` is the node's own (see [System kinds](#system-kinds)).
- The version is the app's own, an opaque string. The node logs it with the declaration and does nothing else with it yet; plugin dependency ranges come later (see the [roadmap](roadmap.md#later)).
- A node with an app address (`SHARDLITE_APP`) serves every kind the app declares through it. One without serves none, which is how game server hosts whose processes only pin shards are set up. It forwards requests to the shard's owner or, for an unowned shard, to a node that serves.
- Kinds marked `pinned` are served only by a process that pins the shard.
- Clients can only name kinds marked `public`. Other kinds are reached only by calls from the app. A client that names one gets the same answer as for an undeclared kind, so it can't tell the kind exists. The node logs why, so a kind that should be `public` is easy to spot in development.
- Requests, calls and pins for undeclared kinds are refused, which catches typos. A kind with no app in its name (`users/42`) is a malformed name and is answered `400`.
- Changed settings take effect when the next poll sees a changed declaration, for new activations and new responses. A poll that fails (no answer, not a 2xx, not a valid declaration) changes nothing: the node keeps the kinds of the last declaration that was good and reports not ready until a good one comes.
- The node reports ready (`GET /_shardlite/ready` on the public listener) only while the app's declaration is current and its lease is good, so a bad deploy stops at the first node.

## Routing requests to shards

Routing is never authorisation. It only decides which shard the node opens for a request. The app checks credentials however it likes. The node never inspects them.

The node takes the shard a client names from fixed conventions, in this order:

1. **Header:** `Shardlite-Target: game.guildchat/7`. This is the one `Shardlite-*` header the node accepts from clients. gRPC clients send it as metadata.
2. **Query parameter:** `?shardlite-target=game.guildchat/7`, which the node strips before the app sees the request. This is the convention for browser WebSockets, which can't set headers.
3. **No shard.** The request goes to the app and the node acts as a plain reverse proxy. This is normal traffic: login, news, store catalogues and health checks.

The conventions are fixed, not configured per kind, so every node computes the same shard whatever app version it runs. Per-app route mappings in the node would let old and new versions send the same URL to different shards during a rolling deploy.

All three are hints that save a hop. The name is the client's claim and not the shard's identity, which the app works out itself (see [Serving](#serving)). A right hint gets the request to the owner with the file already open. A wrong one costs a hop, since the app relays the request to the right shard, and an activation of the shard it names, since the node routes by the hint before any app code runs. That is why the advice below on kinds clients can name still holds, and relaying widens nothing: the node serves a relay with a client's limits.

### Kinds clients can name

A client that names a shard reaches its activation before any app code runs. Relaying doesn't widen what a client can reach: a request the app relays for a client gets the same `404` for a kind that isn't `public` and for a shard that doesn't exist, whatever the path or the hint says. Clients can't create shards, but they can activate or restore any existing shard of a kind they can name, which costs blob storage requests and node resources even when the app then rejects the request. An id that doesn't exist costs one blob storage read. Recommendations:

- **Make a kind `public` only if clients need to name it,** usually the player's own shard and anything they hold a long-lived connection to. Every other kind is reached only through calls from the app after it has authenticated the request.
- **Give kinds clients can name unguessable ids** (random UUIDs), so ids can't be used to force restores of real shards, push them out of the cache or find out which exist. A kind with guessable ids, such as an index keyed by email, must stay non-public.
- **Keep hot shared shards behind the app.** A shard many clients can reach sends all their requests to one node and one writer. Put caching or rate limiting in front of it, or shard or batch hot global data as the [overview](overview.md) says.

Rate limiting at the load balancer is still the defence against a flood of requests.

### Shardless requests that need a shard

A shardless route is an ordinary app route. When its request belongs to a shard (a login, which has to reach the user's shard where the credentials live), the app sends it there, the same way it relays any request. Recommendations:

- **Call the shard.** Make an ordinary [call](#calls-between-shards) with `Shardlite-Target: game.users/42`, passing on the client's credentials so the shard authenticates the request as usual. This works where a redirect doesn't.
- **Or tell the client where the shard is.** The handler answers with a URL that names it (`/login?shardlite-target=game.users/42`), or a `307` to it. WebSocket handshakes can't follow redirects, so they get the URL in an ordinary response.
- **Call a new shard before redirecting to it,** since a client can only reach a shard that exists.

For ids the client chooses, the client picks the id and a shardless handler calls the shard's init route. The route records the creator, answers success to the creator on a retry and `409` to anyone else.

Redirects and calls made by shardless handlers are app code, so old and new versions can disagree there. Keep these decisions stable across releases, change one over two releases (first make the new target handle requests sent to the old one, then switch) and have clients use the shard names directly after the first call.

### Connections and home shards

A WebSocket connection is tied to one shard. Over a WebSocket the client connects to its **home shard** (normally its player shard), and the app calls other shards through the control API. A client that needs a stream to another shard opens a second connection.

An open connection (WebSocket, server-sent events or streaming gRPC) keeps its shard active, so authenticate in the handshake (a cookie, or a token in the query string next to `shardlite-target`) and a rejected client gets `401` without holding the shard. The app can close a connection to let the shard deactivate.

### Creating shards

The app creates shards and clients reach them. Any id can be called and the first call creates the shard, empty, before the app sees it. A request a client routes to a shard reaches only a shard that exists, and otherwise gets the same `404` as an undeclared kind.

When a shard deactivates holding nothing of the app's (no table, index, `user_version` or `application_id`), the node deletes it, and a client request for it gets `404` until the app calls it again. The first open migrates the schema, so a shard the app has opened is kept. Only an app with no migrations yet leaves an empty one. The node logs the first such deletion of each kind at info.

A client sees a deletion only as a `404`, the same as for an id that never existed. Clients need one rule: on `404`, go back through the app rather than retry the same target.

## Durability

Each kind sets `"sync"` in its declaration: `"none"`, `"peer"` or `"storage"` (the default). Every commit is shipped to blob storage either way, and the node's own disk holds a copy of every segment whatever the tier. The setting only decides where acknowledgements wait: nowhere, in the node's own database and the journal's peer, or in blob storage.

| Sync | Survives | Typical latency |
|---|---|---|
| none | nothing in particular: any crash of the node can lose the commits not yet in a segment, up to a flush interval and a PUT. What reached the node's own database before a process crash is kept. | none added |
| peer | the loss of either machine alone, while one of the two copies is still there to read: the node's own disk or the peer's. Blob storage has the commit a flush and a PUT later; a failure that takes the last readable copy before then loses the window. | up to `SHARDLITE_FLUSH_MIN` (100 ms) plus one commit on the node's disk and one on the peer's, shared: a few milliseconds |
| storage (the default) | the loss of the host | up to `SHARDLITE_FLUSH_MIN` (100 ms) plus one PUT, shared by everything waiting on the node: roughly 50–200 ms on S3 Standard |

The tiers are named for the place they wait in: `none` waits nowhere, `peer` is the node's disk and another node's, and `storage` is blob storage.

The `peer` tier needs a node standing by: a node that names no peer, or whose peer has fallen out, fails `peer` writes with `503` rather than answer with less durability than was asked. A tier the node cannot meet is an error, never a weaker answer; the same rule holds for `peer` on a node whose own store is missing or has refused a segment. `SHARDLITE_STANDBYS` says who stands by, 1 by default; a cluster of one names none, so deployments that run alone set `"sync": "storage"` (or `"none"`) on their kinds. The repair names a peer for a start that ran alone and replaces one that fell out or died, whether the journal is busy or idle, and `peer` writes work again from then on.

The window the `peer` tier prices: an acknowledged commit is on two machines, but the record has it only a flush and a PUT later. Losing the node's host and then the peer's before the record has it loses that window, and so does losing the peer's host once the node is gone for good. Everything acknowledged is in the record within `SHARDLITE_FLUSH` plus one PUT, a few hundred milliseconds at the defaults, which is when the window closes.

Nothing leaves a shard before what it committed earlier met its tier. The node holds responses, streams and calls to other shards alike. On `none` and `peer`, output from a shard with nothing new to commit passes without waiting, so latency is paid only for output that follows writes. On `storage` it also waits to be confirmed, which is covered under [Consistency](#consistency).

- A call to another shard counts as output, so it is held too. Otherwise a `wallet` shard could debit gold, call `game.inventory/42` to grant an item and then lose the debit in a crash.
- Every chunk of a stream can wait for its tier, pings included, so real-time kinds (chat, presence, streams) should keep a fast tier, `none` or `peer`.
- The app can override its kind's tier for one response or call with `Shardlite-Sync: none`, `peer` or `storage`.

`POST /_shardlite/sync` on the control API waits until everything committed on the node is in blob storage and the node has confirmed that it still owns its shards, and fails if the node has lost its lease. A process that reads, calls `sync` and then answers its own client is linearizable. It is for side effects shardlite can't see: publishing to Kafka, external HTTP calls, and writes to pinned shards that the process acknowledges over its own protocol.

Until the record has it, what a failure can lose depends on the tier: `none` can lose the last flush interval in any crash of the node, and `peer` loses it only if the record's copy is not there yet and the other machine's copy goes too. The commit is in the app's own database file all along, in the node's own database a moment after it, and on the peer's disk for `peer` once acknowledged. To make the tiers faster, lower `SHARDLITE_FLUSH_MIN`. At 20 ms a busy node writes up to 50 journal PUTs a second while output waits, against 10 at the default.

### Consistency

Each shard has one owner, which serves every request for it, and SQLite orders its commits.

| | Writes | Reads | Like |
|---|---|---|---|
| sync none | in the record a flush and a PUT after it is acknowledged: a crash of the node can lose the commits not yet in a segment | linearizable, given working clocks, and after a crash can miss the last flush interval | Postgres with `synchronous_commit = off` |
| sync peer | durable on the node's disk and the peer's once acknowledged, and in the record a flush and a PUT later | linearizable, given working clocks | MySQL semi-synchronous replication, roughly |
| sync storage | linearizable and durable once acknowledged | linearizable under any clocks and pauses | etcd's default reads and Consul's `consistent` reads |

On the storage tier, a shard behaves as one copy of the database: once a write is acknowledged, every read that starts afterwards sees it, on whichever node the request lands. With `peer` this holds while one of the two copies stands: a failure that loses the acknowledged copy inside the window above can lose it, and the read that starts after the failure misses it. `none` promises the same only until the node crashes.

A `storage` answer is confirmed before it leaves: the node knows that nobody else has begun to take its shards since the answer was read. A write is confirmed by its own segment reaching blob storage. A read waits for a segment the node is writing anyway, or for one read of its own lease, shared by every answer waiting at the time. So a read on an idle node pays one blob storage GET, tens of milliseconds on S3, and a busy node pays nothing extra. A request is confirmed once, at its first chunk, and its later chunks only wait for their own commits. A stream is therefore linearizable at its start and after that follows the node's copy of the shard, in order, until the node notices that it has lost the shard (within a lease renewal, 3 s by default). An event is not a read of the shard as it is now. An app that needs that polls with requests. If the lease has been taken the node stops, as for a lost renewal, and if it cannot be read the answer fails with `503` and a retry hint.

"Working clocks" applies to `none` and `peer`, which answer without confirming. It means clock rates on different nodes agree to within 20% (real clocks differ by parts per million, and they need not be synchronised) and no machine is paused in a way its own clocks don't see, which a hypervisor pausing a VM can do. A hot read endpoint that can live with that sends `Shardlite-Sync: peer` or `none` on its response, which skips the confirmation.

The node checks the clocks it can: it compares its own with the `Date` header of the store's responses. While that offset moves by more than a clock of the right rate allows (which a step, a rate more than 20% off and a pause its monotonic clock didn't count all do), `none` and `peer` answers fail with `503` and a retry hint, as an unmet tier does, and `storage` answers are not affected. They recover once the offset has been steady for a few seconds. This narrows the window and does not close it: a node that wakes from a hidden pause answers `none` and `peer` reads until a response from the store shows the jump, which on an idle node is about a lease renewal interval, and a store's time is only good to a second.

The guarantees hold for what goes through the node and what the app reads from the shard's database. An app that answers from its own in-memory copy can serve something older. Keep such a cache no longer than an open connection to the shard and check `PRAGMA data_version` before using it, or keep a version row that every write bumps.

Across shards there are no transactions: two reads of two shards are two separate reads, not a snapshot. Calls happen at most once per attempt, and a [durable send](#durable-send) at least once.

## Localhost control API

The app talks to its node over HTTP on localhost TCP or a Unix socket. It is a separate listener from the public one, so apps may send `Shardlite-*` headers to it. Most of the API is headers on the app's normal requests: any request with `Shardlite-Target` is a [call](#calls-between-shards) to that shard, or a client's request the app relays if it also has `Shardlite-Relay: true` (see [Serving](#serving)). The rest is a few plain HTTP endpoints under `/_shardlite/` (see [Control API](#control-api)).

## Deleting shards

A shard deletes itself. When it deactivates holding a table named `_shardlite_delete`, its data goes, and a later call for the same id creates a new, empty shard that never sees the old data. `CREATE TABLE _shardlite_delete (x)` in any transaction marks it. Its due reminders are handled first, and later ones go with it. Until deactivation the shard is served as usual, so the app can check for the table and refuse requests. Uses: matches, trades and lobbies that end.

There is no call that deletes a shard from outside, since a delete would race with requests and the app's open files. For a deletion decided outside the shard (account deletion, say), add a route to the shard that finishes its work and creates the marker, and call it like any other route. The shard is deleted within its idle timeout. Deleting a shard that isn't active is an operator's job, for the [admin API](roadmap.md#admin-api).

## Calls between shards

Shards reach each other in two ways:

| | Calls | Durable send (reminders and calls) |
|---|---|---|
| Shape | synchronous request and response | a call made by the sender's wake handler, with the sender's commit behind it |
| Delivery | at most once per attempt, retried by the caller | at least once, with the receiver skipping repeats |
| Tied to the caller's commit | no | yes, recorded in the same transaction |
| For | answers needed now: reads, queries, checks | changes that must happen |

The app always calls its local node and relies on routing. A shard call is just another HTTP route in the app:

```
app (game.users/42) → POST localhost:2282/invite
                 Shardlite-Target: game.guild/7
                 node: who owns game.guild/7?
                   local  → POST app/invite with Shardlite-* for game.guild/7
                   remote → forward to the owner node → its app/invite
```

- **The target is explicit.** `Shardlite-Target` names the shard. The path, method and body pass through unchanged, so this works for REST, GraphQL or gRPC, and apps will usually reuse the protocol their clients speak. A call from a shard can't be upgraded to a WebSocket (`400`). For long-lived subscriptions use server-sent events or a gRPC stream. Calls from the app with no shard behind them can upgrade.
- **Cost.** A local call is one localhost round trip (about 0.1 ms). A remote call adds about 0.5–2 ms.
- **The callee knows the caller.** The app sets `Shardlite-Caller: game.users/42` (copied from the `Shardlite-Opened` it received) and the node passes it to the callee. A call with no shard behind it gets `Shardlite-Caller: app`. The node strips the header from external requests, so it grants access to internal routes, which require it. A relayed client request carries none, so a route sees it as it would see the client's. The header is never a reason to skip authenticating a request: a route clients can reach authenticates every request, whoever sends it. A route that only takes calls may take its identity from `Shardlite-Opened`, since app code chose the target and the node strips `Shardlite-*` headers from clients. A route clients can reach always works out the shard from the request.
- **Gates apply on both sides.** The caller's gate holds the call until the caller's kind's tier has its commits. The callee's gate holds the answer, so a call to a `wallet` shard returns only once the wallet's writes are durable.
- **Calls activate and create the target,** unless the call sends `Shardlite-If-Active: true` (answer `404` rather than activate, for presence checks and fan-out to members who may be offline) or `Shardlite-If-Exists: true` (answer `404` rather than create). Use `Shardlite-If-Exists` for any call whose id came from a client, so random ids don't create shards through app code.
- **Hops are limited.** The app passes `Shardlite-Hops` on to its calls as it would `traceparent`. The node increments it and refuses past `SHARDLITE_MAX_HOPS` with `508`, which stops runaway recursion. A relay counts hops the same way, so two apps relaying to each other can't loop forever.
- **Cancellation propagates.** When a client disconnects or a caller gives up, the node cancels the request on every hop it controls. Writes already committed are not rolled back. Timeouts are the app's: set them on its own HTTP or gRPC client. The node has none.
- **The node never retries a request the app may have seen,** so calls that change state should carry an idempotency key.

**gRPC** works in both directions. The app adds `shardlite-target` metadata (usually with a client interceptor) and the node proxies HTTP/2 without TLS, trailers and streams as they are. For its own errors it sends gRPC status responses (`NOT_FOUND` and so on) rather than plain HTTP ones. The node never parses gRPC messages and doesn't read `grpc-timeout`.

**Reaching many shards.** For guild chat or a group invite, the app calls each member's shard in parallel and collects the responses. Use `Shardlite-If-Active: true` so offline members aren't woken. The app keeps control of retries and timeouts.

## Reminders

A reminder is a row a shard sets for itself, with the time it is due: an upgrade finishing, an auction closing, a lobby that must end even if nobody is connected. The node wakes the shard's app when the earliest one is due, as a Durable Objects alarm does. The node creates the table at activation:

```sql
_shardlite_reminders (key TEXT PRIMARY KEY, due INTEGER NOT NULL) STRICT  -- due in Unix seconds
```

- **The rows are the app's.** It inserts, reschedules and deletes them with plain SQL in its own transactions (`INSERT … ON CONFLICT (key) DO UPDATE SET due = excluded.due`, and `DELETE` to cancel). Anything a reminder needs lives in the app's own tables under the same key, so the reminder and its data commit together.
- **The table is STRICT.** A value of the wrong type is refused at the INSERT, where a lax table would store it as whatever the column's affinity makes of it (a date string in `due` would wake nobody, ever). What looks like a number still converts: a numeric string or a whole float into `due` stores as an integer, and a number into `key` stores as text.
- **The wake.** When the earliest due time has come, the node calls `POST /_shardlite/reminders` on the shard's app (no body, `Shardlite-Opened` and `Shardlite-Caller` naming the shard) and gives it 30 seconds to answer. The node chose the shard, so the handler takes its identity from `Shardlite-Opened`. The node answers `404` to every other request under `/_shardlite/`, so the app can trust that this one came from its own node. A wake never acts on a commit that could still be lost.
- **The handler** selects `WHERE due <= unixepoch()` and handles each row in its own transaction, so a row's work and its deletion (or next due time) commit together. It answers `2xx` once everything due is done, and `503` if a row can't be finished.
- **Retries.** Any other status, no answer in time or rows still due after a `2xx` is a failure, and the next wake waits: one second, then twice as long for each failure in a row up to a minute. A handler that keeps setting reminders due now costs one wake per minute.
- **Stale reminders.** A reminder due more than a day ago is ignored: it doesn't wake the shard or keep it active. It stays in the table for the app to clean up (`DELETE FROM _shardlite_reminders WHERE due < unixepoch() - 86400`).
- **Repeats are the app's.** A handler that wants a recurring reminder sets the next `due` in the same transaction as its work. Resolution is a second.
- **Timing.** A shard that is active is woken within about a second of its time. An inactive one is woken by the cluster, usually within a few seconds of the minute the reminder is due in, later when many shards are due in the same minute or a node has failed. Rely on the minute only. A reminder that is due now keeps the shard active, so the app closes its connections after handling.

### Durable send

A call made after a commit is lost if the node crashes in between, and a call made before it can announce a write that is later lost. A shard that must make a call exactly when its own transaction commits uses a reminder as the trigger and its own table to hold the call. The sender records both in the transaction that causes the send (the key is unique per send and the same on every attempt):

```sql
CREATE TABLE sends (key TEXT PRIMARY KEY, target TEXT NOT NULL, path TEXT NOT NULL, body BLOB) STRICT;

BEGIN IMMEDIATE;
  -- the work that causes the send
  INSERT INTO sends (key, target, path, body) VALUES (:key, 'game.players/42', '/rewards', :body);
  INSERT INTO _shardlite_reminders (key, due) VALUES (:key, unixepoch());
COMMIT;
```

The wake handler makes each call with the key as `Idempotency-Key`, and deletes both rows once the call answered `2xx`:

```
on POST /_shardlite/reminders:
  failed = false
  for each (key, target, path, body) in
      SELECT s.* FROM sends s JOIN _shardlite_reminders r USING (key) WHERE r.due <= unixepoch():
    status = call(target, POST path, body, Shardlite-Caller: this shard, Idempotency-Key: key)
    if status is 2xx:
      BEGIN; DELETE FROM sends WHERE key = :key;
             DELETE FROM _shardlite_reminders WHERE key = :key; COMMIT
    else:
      failed = true            -- leave both rows, the node wakes the handler again
  answer 503 if failed, else 200
```

The receiver records `(Shardlite-Caller, Idempotency-Key)` in the same transaction as its work, and skips work it has seen:

```sql
CREATE TABLE seen (caller TEXT NOT NULL, key TEXT NOT NULL, at INTEGER NOT NULL, PRIMARY KEY (caller, key)) STRICT;
```
```
on POST /rewards:
  BEGIN IMMEDIATE
    INSERT INTO seen (caller, key, at) VALUES (:caller, :key, unixepoch()) ON CONFLICT DO NOTHING
    if no row was inserted: COMMIT, answer 200       -- seen before
    do the work
  COMMIT; answer 200
  now and then: DELETE FROM seen WHERE at < unixepoch() - 86400
```

- **At least once, with the work done once.** A failed call is made again at the next wake. A lost acknowledgement makes the call twice, and the receiver's record makes the work happen once.
- **Pruning.** A sender stops trying once its reminder is stale, so a `seen` entry older than a day can be dropped.
- **Calls that can never succeed** (a `4xx` that retrying won't change) should be dropped or marked failed by the handler, or they are retried until the reminder goes stale.
- **No ordering.** An app that needs order adds a sequence number per sender and receiver, and the receiver answers `503` to a number that isn't next.
- **One handler serves every reminder the app sets.** It looks at which of the app's tables has a row under each due key.

Uses: fan-out in one transaction (rewards for 100 players at the end of a match), transfers and trades by custody, and calls that must follow a commit.

## Rules for app code

These are part of the contract with shardlite. Apps that break them stall or fail.

- **A shard is an actor, but not a turn-based one.** Requests for the same shard run concurrently, as they would on any database, so a handler that reads and then writes in separate statements can lose another request's update. The shard's transactions are the concurrency control, so put the read and the write in one transaction or in one statement. Orleans and Durable Objects give a grain one turn at a time, and an app that expects that loses updates here.
- **Handle concurrent requests, including calls back into the app itself.** A call to a shard on the same node arrives at the same app as a new request while the calling request is still waiting. A single-threaded synchronous server deadlocks, and thread pools can run out when calls nest, so async servers are the safer choice.
- **Don't hold a write transaction while calling another shard.** If `game.users/42` is mid-transaction, calls `game.guild/7` and the guild calls back, the callback can't get the write lock. Call first and then write, or write, commit and then call.
- **Expect calls to fail and set timeouts.** A call can arrive while its target is activating or moving, and its node can fail. Retry where it makes sense.

## Recommendations for app code

- **Do the read and the write in one transaction or one statement.** `BEGIN IMMEDIATE` for a transaction, or one statement that does the whole thing (`UPDATE … SET balance = balance - 10 WHERE balance >= 10`). State kept in app memory per shard isn't protected by SQLite.
- **Start write transactions with `BEGIN IMMEDIATE`, or retry them.** A deferred transaction that reads and then writes fails with `SQLITE_BUSY` if any other connection committed in between, and `busy_timeout` doesn't retry that case. The node's bookkeeping commits now and then, so this is rare but happens.
- **Work out the shard from the request and relay on a mismatch.** The route's id, the payload or the first gRPC message says which shard a request is for, whatever `Shardlite-Opened` says. If it is the opened shard, use the open file. Otherwise relay the request to the right shard with `Shardlite-Relay: true` and no `Shardlite-Caller`, passing the client's credentials on. Don't answer from the open file for another shard's request, and don't skip authentication because a request arrived as a call.
- **Choose ids that need no lookup.** A client can choose a new shard's id (a UUIDv7), which makes creating it idempotent. An id built from other data, such as a trade between two players, can be a name-based UUID (v5) of that data.
- **Sync writes that must not be lost** on a `none` kind to bring them to the record, by calling `sync` before acknowledging. A small commit wrapper on the SQLite driver makes this hard to forget.
- **Use a durable send for calls that must happen.**
- **Drive time-based work from requests.** Energy, cooldowns, deadlines and daily resets can be worked out from timestamps whenever a request touches the shard, so they need no timer and a player who never returns costs nothing. Set a reminder only for things that must happen on time while nobody is connected, and spread reminders that many shards share (add a random delay of a few minutes), since the cluster paces them at about 1,000 a second.
- **Keep indexes in index shards, updated by durable sends.** A shard that should be findable (a lobby, a name) sends its entry to an index shard split by what's searched (`game.lobbies-index/eu`), and sends another to remove it. Handlers upsert, and readers skip entries older than a timeout.
- **Delete short-lived shards with the marker,** with a reminder due when the shard should be gone at the latest, so an abandoned one still ends.
- **Split hot shared shards.** By meaning first (season, region, mode), then by hash with an aggregator shard. A single shard too hot for any node is a data-modelling problem.
- **Publish by recording events.** A topic's shard keeps its events in a table with a sequence number. Connected clients get them pushed, and subscriber shards catch up when next touched, so offline subscribers never wake.
- **Cache shared data** (a message of the day, a guild's member list) in the app rather than reading its shard every time, or that shard becomes hot.
- **Keep large data in object storage.** Upload it under a unique name, then commit the reference in the shard. A crash in between leaves an orphan, never a missing object.

## Pinning

For game servers that keep a world or match in memory and talk to their clients over their own protocol (WebRTC, custom UDP, raw TCP). A process pins a shard on its own node with a control API request that stays open. The node claims the shard, or has its owner hand it off, and holds it until the request closes because the process let go or died. A pinned shard is never moved, rebalanced or deactivated, so ownership follows where the orchestrator (Agones, GameLift) starts the process.

- **Requests, calls and reminder wakes** for the shard go to the process if it gave the node a localhost HTTP address when it pinned, with the same routing, gates and hops as served shards. Without one, requests get an error and wakes fail and back off.
- **Pinned kinds** (`"pinned": true`) are served only by a pinning process. A request for one that nobody has pinned gets `503`, so the app knows to start a server.
- **Clients connect to the process directly,** at an address the app gives them from login, matchmaking or a signalling request routed to the shard. Shardlite keeps no connection info and issues no tokens.
- **The process covers what the node can't see.** It stops serving clients past the lease deadline the node sends down the pin, calls `sync` before acknowledging a write that matters, and moves its clients when it unpins.
- **A game server handshake** works well: a lobby asks the orchestrator for a server and sets a reminder due when it should give up. The server pins `game.matches/{id}` and calls `game.lobbies/{id}` to say where clients connect. The lobby stores the address, tells the players and deletes its timeout reminder in one transaction.

**Persistent worlds.** A zone or world can be one pinned shard: the save is a SQLite file with serialized chunks as rows, so only changed pages are shipped, saving is continuous and the loss window is about one flush interval. If the host dies, another server pins the world and restarts from it. Hosted private worlds are idle most of the time, so as shards they cost only storage until a player joins. Alternatively the server can keep persistent pieces of the world (`game.bases/{id}`, `game.chunks/{x}-{y}`) as shards and write changes back with calls, which suits loads and building edits but not per-tick state. Game servers can run in a different facility from the backend, at the cost of the latency between them.

## Running shardlite

Nodes are disposable. Blob storage holds the data, and local disks are caches.

- **Dependencies:** blob storage (see below) and the deployment's own load balancer in front of the nodes.
- **Disks:** local NVMe is fastest, and ephemeral disks are fine. Size them for the working set plus a cache. When the disk fills, the node evicts the least recently used cached shards (`SHARDLITE_CACHE_LIMIT` sets a limit).
- **Kubernetes:** the node as a DaemonSet, with app pods on the same machines, so every pod on a machine shares one node journal. `SHARDLITE_APP` points at a Service with `internalTrafficPolicy: Local`, and the app pods and node share the data directory and the control socket through a `hostPath` volume. A sidecar per pod also works, but every app deploy then restarts the node and loses its cache.
- **Elsewhere:** the node runs as a systemd unit or similar. A dead node means the app is unreachable, so no new requests arrive and no response can be acknowledged.
- **Development:** a single node on RustFS in Docker (the Compose file the tests use), so the real storage path is always exercised.
- **Single machine:** one node with RustFS on a durable volume, with shard files on fast instance storage. RustFS must keep its buckets in strict durability mode, since a store that acknowledges a write before it is on disk breaks the gates' promise. Moving to a cluster later means copying the bucket to S3.
- **Scaling up:** add nodes. New connections and newly activated shards land on them, and rebalancing moves some busy shards from overloaded nodes (see [Load](#load)). Scale by CPU and memory, since storage is elsewhere.
- **Scaling down:** drain the node. It fails its readiness check, stops serving, evicts its shards and exits, and other nodes claim them as they are next needed.
- **Restarts and upgrades:** drain the same way. A restart is treated as a crash, and the node starts with an empty cache, so shards are restored from blob storage as they activate. A shard the app still has open can't be let go of, and is recovered afterwards by adoption, as after a crash. During a rolling upgrade, old and new nodes forward to each other and read each other's data.
- **Failure:** if a node stops answering, requests for its shards get a quick `503` with `Retry-After` until its lease has expired and another node has taken over (10–20 s with the default lease TTL). Writes not yet in blob storage are lost, except what a `local` or `peer` kind holds in the node's own database and what a `peer` kind holds on the peer's disk. The node's own database survives the crash of the node and not the loss of the host.

### Load balancer

Clients reach nodes through the deployment's own load balancer, which terminates TLS and absorbs hostile traffic. Nodes listen only on the private network. The load balancer sets `X-Forwarded-For`, `X-Forwarded-Proto` and `X-Forwarded-Host` on every request (overwriting what clients send), and the node adds the load balancer's address to the end of `X-Forwarded-For`, so the app sees the client's address, protocol and host. The load balancer checks `GET /_shardlite/ready`.

A request that lands on a node that doesn't own its shard is forwarded to the owner, at 0.5–2 ms. Reaching the owner directly is up to the load balancer:

- **Sticky load balancer:** an unowned shard is activated on the node that received the request, and stickiness keeps the client there.
- **Controllable load balancer** (HAProxy, nginx, Envoy): route by hashing `Shardlite-Target` (header or query parameter).
- Otherwise the request is forwarded.

### Blob storage

The supported stores are AWS S3 and, for development, tests and self-hosting, RustFS. Blob storage must support linearizable conditional writes (`If-None-Match` and `If-Match` on writes, `If-Match` on deletes) and custom metadata on conditional writes, returned on `HEAD`. Cloudflare R2 doesn't document conditional deletes, so it fails the startup check until it does. Any other store gets the startup check and nothing more.

The node tests its store at startup and refuses to start if conditional writes misbehave. Writes carry a CRC32 the store verifies and reads check the checksum the store returns, so an object corrupted in the store or in flight fails the read that finds it. `SHARDLITE_PREFIX` puts every object under a prefix (`staging/`), so several environments can share one bucket. No lifecycle rule is needed: each node deletes its own expired journal segments. Shardlite keeps no history of old files. A deployment that wants point-in-time restore can turn on bucket versioning. Deleting players who never return is the app's decision.

### Hosts

The node targets Linux only. Apps that use it must run on the same Linux machine (or VM), since they share SQLite files. Game clients and editors can run anywhere. On macOS, run the node, backend and game server in Docker with shard files in a Docker volume (not a bind-mounted folder, since SQLite locking isn't reliable across file sharing). On Windows, use WSL2 or Docker with shard files on the Linux filesystem (not under `/mnt/c`). Game servers running natively on macOS or Windows are not supported.

Host settings:

- **Open files and memory mappings.** Each active shard takes 8 file descriptors (7 and the log of its tail, once it has commits) and about 7 memory mappings (about 0.4 to 0.8 MB of memory). A delta reading the tail takes one more for a moment, and so does each segment still waiting to be written, which a slow blob store or peer lets pile up. Raise the hard `ulimit -n` and `vm.max_map_count` (65,530 by default, reached at about 10,000 active shards). The node warns at startup if they are too low.
- **Data directory.** `SHARDLITE_DATA` must be on a local filesystem, with room for the largest shards being restored or rebased (at most 4 rebases at once) and about twice the size of any commit that rewrites a whole database. The node refuses to start on NFS, SMB or FUSE. A shard's tail lives in unnamed files in `{data}/.tmp`, gone a delta after their last commit, so a crash leaves nothing there and that directory needs room for the largest commits too. Their space shows in `df` and not in `du`, and a slow blob store or peer keeps the space of an old tail until it catches up.
- **Clocks.** Run NTP. Leases need only clock rates that agree (see [Consistency](#consistency)), but a clock that is far off delays failover. Clocks matter for failover speed and for `none` and `peer` reads, and not for `storage`. The node compares its clock with the store's `Date` header on every response. It logs a warning when the two are more than a second apart (and again when they agree), refuses to start when they are more than the lease TTL apart, and fails `none` and `peer` answers while the offset is not steady (see [Consistency](#consistency)). `GET /_shardlite/node` shows the offset and whether the node is warning or failing. A store that stops sending a `Date` header is logged too, since the check is then blind.

### Traffic between nodes

A node has three listeners: public (clients, through the load balancer), peer (other nodes) and control (the app on the same host). The peer listener (`SHARDLITE_LISTEN_PEER`) speaks HTTP without TLS and must only be reachable from other nodes: never put it behind the load balancer or expose it to clients or the app. Requests on it are trusted to carry node-only headers. It serves the forwards and dispatches between nodes, and the journals a node mirrors for them (see [internals](internals.md#node-journals-and-snapshots)). Traffic between nodes is not encrypted or authenticated, so nodes must be on a trusted private network, or on an encrypted one (WireGuard, Tailscale). Service meshes are not a target, since they add two proxy hops to every call between nodes. In a cluster that runs one, exclude shardlite's traffic between nodes from it.

### Load

- **Rebalancing.** New nodes only receive new connections and new shards, so the node moves busy shards from overloaded nodes to less loaded ones, rarely and with a cap per interval. It closes those shards' WebSocket connections with a reconnect close code, or sends HTTP/2 `GOAWAY` (`Connection: close` on HTTP/1.1) to their HTTP clients. In-flight requests finish. Clients must reconnect on that close code.
- **Load shedding.** Above a CPU or memory threshold, a node replies `503` with `Retry-After` and refuses new activations. Active shards keep working.
- **Hot shards.** Each shard has a cap on requests in flight. Past it, the node replies `429`.

### Cost

Rough estimates on S3 Standard, to be checked by the prototype:

- **Journal segments:** 4 PUTs a second per busy node at the default 250 ms flush interval, about 50 USD a month per busy node, whatever the number of active shards. Idle nodes write nothing. A 100 ms interval costs about 130 USD a month.
- **Snapshots:** one PUT and one manifest write each time a shard deactivates or its unsnapshotted commits age out, about 10 USD per million sessions.
- **Storage:** a 100 KB player shard is 100 GB for a million players, a few dollars a month.
- **Reminders:** about 5 USD per million reminders in new minutes, plus a few requests per reminder when an inactive shard is woken.
- **Manifests, leases and dead-node checks:** well under a dollar a day per node, plus about 7 USD a month for the cluster.

### Latency

Estimated, not measured:

| Case | Added latency |
|---|---|
| Active on this node | ~0.1 ms |
| Owned by another node | +0.5–2 ms forwarding, plus activation there. The first request for a shard also reads its manifest (~10–30 ms) |
| Cached on this node | ~1 ms |
| Claiming a shard | ~20–60 ms plus a snapshot download |
| New shard (created by a call) | ~30–90 ms |
| Owner crashed | up to twice the node lease TTL (10–20 s with the default), then claim and restore |

### Observability

- **Tracing.** The node passes W3C trace context (`traceparent`) through every hop and adds its own spans (routing, activation, restore, gate waits). Apps that also propagate it get end-to-end traces. The node logs a sample of shardless requests that lead to calls to a shard, so routes where clients don't name the shard they need show up.
- **Alerts.** The node logs an alert if a shard's commits have gone longer than 3× `SHARDLITE_DELTA_AGE` without a snapshot, which means its snapshots are failing.
- **Diagnosis.** `SHARDLITE_LISTEN_PPROF` serves Go's pprof handlers.

## Reference

### Shard names

A shard is named `app.kind/id`: the kind qualified by the app that serves it. The app's name is lowercase letters, digits, `_` and `.`, up to 31 characters, starting with a letter (a name starting with `_` is the node's own), and it may carry a scope (`acme.billing`). The kind's name is lowercase letters, digits, `-` and `_`, up to 32 characters, starting with a letter, and never contains a dot: the split is at the last one, so `acme.billing.purchase/42` is the kind `purchase` of the app `acme.billing`. A qualified name is at most 64 characters. Ids are 1 to 128 ASCII letters, digits, `-` and `_`, enough for numbers, UUIDs and ULIDs. Anything else gets a `400`, including a kind with no app (`users/42`).

### Headers

All shardlite headers use the `Shardlite-` prefix. Clients can send `Shardlite-Target`, and every other `Shardlite-*` header from clients is stripped (also with `_` for `-`, as in `Shardlite_Caller`, which CGI-style frameworks read as the same variable). Apps should pass on `traceparent` and `Shardlite-Hops` on calls.

| Header | Set by | Meaning |
|---|---|---|
| `Shardlite-Target` | client (a hint), app on calls, node between nodes | the shard to route to |
| `Shardlite-Opened` | node to app | the shard the node opened for this request |
| `Shardlite-File` | node to app | the opened shard's file |
| `Shardlite-Caller` | app on calls, node to app | the calling shard, or `app` |
| `Shardlite-Relay` | app on the control API | `true`: the request is a client's, relayed. The node serves it with a client's limits (only kinds clients can name, never creating a shard), with no caller and hops counted. With `Shardlite-Caller` it is a `400` |
| `Shardlite-Hops` | app on calls, node to app | calls along one chain |
| `Shardlite-If-Active` | app on calls | `true`: answer `404` rather than activate |
| `Shardlite-If-Exists` | app on calls | `true`: answer `404` rather than create |
| `Shardlite-Sync` | app on a response or call | `none`, `peer` or `storage`, overriding the kind's tier once |

### Control API

On `SHARDLITE_LISTEN_CONTROL` (localhost TCP) and `SHARDLITE_LISTEN_CONTROL_SOCKET` (a Unix socket):

| Request | Meaning |
|---|---|
| `ANY /{path}` with `Shardlite-Target` | call `{path}` on the shard's owner |
| `ANY /{path}` with `Shardlite-Target` and `Shardlite-Relay: true` | relay a client's request for `{path}` to the shard's owner, with a client's limits |
| `POST /_shardlite/sync` | wait until everything committed before the call is in blob storage (fails if the node has lost its lease) |
| `GET /_shardlite/node` | who this node is, the state of its journal's peer (its frontier, whether it is in step, whether it has fallen out) and how its clock stands against the store's (the offset, and whether it warns or is unsteady) |
| pin | an open request that holds the pin and carries the lease deadline, with an optional address to proxy to |

The node polls the app's `GET /_shardlite/ready` (see [Shard kinds](#shard-kinds)) and calls `POST /_shardlite/reminders` (see [Reminders](#reminders)). The public listener serves `GET /_shardlite/ready` to the load balancer. A path under `/_shardlite/` that a listener doesn't serve itself is refused with `404`.

### Status codes

Errors the node generates itself (sent as gRPC status for gRPC requests):

| Code | When |
|---|---|
| `400` | a malformed shard name or `Shardlite-*` header, or a relay that sets `Shardlite-Caller` |
| `404` | an undeclared kind, a kind that isn't `public` named by a client or in a relay, a client request or a relay for a shard that doesn't exist, `Shardlite-If-Active: true` on an inactive shard, `Shardlite-If-Exists: true` on a missing one, or a path under `/_shardlite/` the node doesn't serve |
| `429` | the shard's cap on requests in flight |
| `503` | the owner doesn't answer (with `Retry-After`), the shard moved since the node last forwarded to it (`Retry-After: 0`: try again at once), a pinned kind has no pin, the node is shedding load, or the shard's files failed their checksums |
| `504` | a timeout inside the node ended the request |
| `508` | hops past `SHARDLITE_MAX_HOPS` |

### Declaration

`GET /_shardlite/ready` on the app answers the declaration (see [Shard kinds](#shard-kinds)). `"app"` names the app the kinds belong to, and the node qualifies each kind with it (a change of it is a change of every shard's name). `"version"` is the app's own, an opaque string, optional for now. Each kind under `"kinds"` takes:

| Field | Default | Meaning |
|---|---|---|
| `sync` | `"storage"` | `"storage"`, `"peer"` or `"none"` (see [Durability](#durability)) |
| `idle_timeout` | `SHARDLITE_IDLE_TIMEOUT` | how long a shard stays active without requests, as a Go duration (`"10m"`) |
| `public` | `false` | clients may name the kind |
| `pinned` | `false` | only a pinning process serves the kind |

### Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `SHARDLITE_NODE` | the host name | the node's name: 1 to 128 ASCII letters, digits, `.`, `-` and `_`. It names the node's lease and journal in blob storage |
| `SHARDLITE_ADVERTISE_PEER` | the peer listener's address | how other nodes reach the peer listener (`host:port`). Required when `SHARDLITE_LISTEN_PEER` is a wildcard address such as the default |
| `SHARDLITE_BLOB` | required | `s3://bucket` |
| `SHARDLITE_S3_ENDPOINT` | AWS | the URL of an S3-compatible store |
| `SHARDLITE_PREFIX` | | prefix of every object path, for example `staging/` |
| `SHARDLITE_APP` | | the app's base URL. The node polls `/_shardlite/ready` on it for the app's declaration. Without it the node serves no kinds |
| `SHARDLITE_LISTEN_PUBLIC` | `:2280` | the public listener |
| `SHARDLITE_LISTEN_PEER` | `:2281` | the listener for traffic from other nodes |
| `SHARDLITE_LISTEN_CONTROL` | `127.0.0.1:2282` | the control API on localhost TCP |
| `SHARDLITE_LISTEN_CONTROL_SOCKET` | | the control API on a Unix socket |
| `SHARDLITE_DATA` | required | the node's directory in the host's disk: shard files (the local cache) and the node's own database |
| `SHARDLITE_CACHE_LIMIT` | no limit | bytes of deactivated shards to keep |
| `SHARDLITE_FLUSH` | `250ms` | flush interval |
| `SHARDLITE_FLUSH_MIN` | `100ms` | the shortest time between two flushes, which bounds the wait at a gate |
| `SHARDLITE_IDLE_TIMEOUT` | built in | idle timeout for kinds that don't set one |
| `SHARDLITE_LEASE_TTL` | `10s` | node lease TTL |
| `SHARDLITE_DELTA_AGE` | `15m` | how old a shard's unsnapshotted commits get before a snapshot is written. The tail alert (3×) and journal expiry follow from it |
| `SHARDLITE_DELTA_SIZE` | `256KB` | how many bytes of unsnapshotted commits make a snapshot due, whatever their age |
| `SHARDLITE_MAX_HOPS` | `32` | the hop limit for calls |
| `SHARDLITE_STANDBYS` | `1` | whether the node stands by for others and names a peer for its journal, on the peer's disk: 0 or 1 |
| `SHARDLITE_LISTEN_PPROF` | off | address for Go's pprof handlers |

### Reserved tables

The node creates `_shardlite_reminders` at activation and writes nothing else. A shard whose only content is these tables (and the `sqlite_sequence` that `AUTOINCREMENT` adds), with no rows in shardlite's, counts as never written. The app may create `_shardlite_delete` (any columns), which marks the shard for deletion (see [Deleting shards](#deleting-shards)).

```sql
CREATE TABLE _shardlite_reminders (
  key TEXT PRIMARY KEY,  -- the app's name for the reminder: 'upgrade:barracks'
  due INTEGER NOT NULL   -- Unix seconds
) STRICT;
CREATE INDEX _shardlite_reminders_due ON _shardlite_reminders (due);
```

The table is STRICT: a value of the wrong type is refused at the INSERT (a numeric string still converts into `due`, and a number into `key` as text), and the primary key is implicitly NOT NULL. The keyword is part of the file's schema, which is where the driver floor above comes from.

### System kinds

The node's own kinds, served in process with no app behind them. They are named under the node's own app `_system` (`_system.sweep`, `_system.dispatch`), and their names show up in blob storage and in the logs. Clients can't name them, and the app (or an operator with `curl` on a node's control listener) can call their routes. Each has fixed ids kept active on one node of the cluster. How they work is in [system kinds](internals.md#system-kinds).

| Kind | Ids | What it does |
|---|---|---|
| `_system.sweep` | `_cluster` | deletes LTX files that no manifest lists and that are older than a day |
| `_system.dispatch` | `_cluster` | wakes the shards whose reminders are due while they are inactive |
