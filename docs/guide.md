# shardlite guide

For people who build apps on shardlite and people who run it. The [overview](overview.md) introduces the concepts, and the [reference](#reference) at the end lists every header, endpoint, setting and reserved table. How the node does what it does is in [internals](internals.md).

## Serving

An app uses shards in two ways, and can do both:

- **Serving** (typical for backends over HTTP and WebSockets): the node routes requests, calls and messages for any shard of a kind the app serves. These are **served shards**, activated on demand wherever requests land, as virtual actors.
- **Pinning** (typical for game servers): a process pins one specific shard and serves its clients itself, over any protocol. See [Pinning](#pinning).

The node sits in front of the app. For each request or connection it works out the shard (see [Routing requests to shards](#routing-requests-to-shards)), activates it if needed and forwards to the app with:

```
Shardlite-Target: users/42
Shardlite-Path:   /data/users/42.k3x9.db
```

On requests to the app, the node always sets `Shardlite-Target` from its routing decision, overwriting any value that came in, so the app can trust it. The kind is everything before the first `/` and the id is everything after (see [Shard names](#shard-names)). Shardless requests have no `Shardlite-Target`. Apps whose URLs include shard ids should still take the shard's identity from this header: a client can send `Shardlite-Target: users/43` to `/users/42/profile`, and the header wins.

The proxy handles HTTP-based traffic (HTTP, gRPC, server-sent events and WebSocket). Client requests, requests forwarded between nodes and calls between shards are all the same HTTP requests, passed through unchanged apart from `Shardlite-*` headers. The node strips every `Shardlite-*` header from client requests (after reading `Shardlite-Target` for routing) and from app responses, so they can't be spoofed or leak. So is the underscore spelling (`Shardlite_Caller`), in upper or lower case, since CGI-style frameworks (WSGI, Rack and PHP among them) read it as the same variable. The node doesn't pass on the app's interim `1xx` responses (`103 Early Hints`), which would reach the client before the shard's durability gate. Client requests for paths under `/_shardlite/` are refused, except the node's readiness check `GET /_shardlite/ready`.

| Traffic | Shard identified by | After routing |
|---|---|---|
| HTTP/1.1, HTTP/2 | header or query parameter, per request | proxied |
| Server-sent events | the request, like any HTTP request | streamed without buffering |
| WebSocket | upgrade request | byte stream |
| Custom UDP, WebRTC, raw TCP | a pin, or signalling | direct to the process |

### What an app must do

The app and node must share a local filesystem on one kernel (the same host, with a shared volume where they run in containers). Network filesystems are not supported, since they break SQLite locking.

The app opens `Shardlite-Path` with any SQLite driver. That file is opaque and includes the shard's generation, so apps never build paths themselves. The app:

- uses WAL mode with default locking (not `locking_mode=EXCLUSIVE` or a VFS without shared memory);
- only runs passive checkpoints, as SQLite's automatic checkpoints are. `FULL`, `RESTART` and `TRUNCATE` always find the shard busy, since the node holds a read transaction on it;
- closes its connections to a shard within the kind's idle timeout of last using it, for example from a pool that closes idle connections well within that timeout (`SetConnMaxIdleTime` in Go). Closing after each request also works, at the cost of the connection's page cache and prepared statements. The node only lets go of a shard once no other process has it open, so a connection held longer keeps the shard active and blocks moving it. After a node restart, a shard the app still has open from before is unavailable until the app closes it, so a short idle limit keeps that brief;
- treats the tables whose names start with `_litestream_` or `_shardlite_` as reserved (see [Reserved tables](#reserved-tables));
- serves a health endpoint;
- follows the [rules for app code](#rules-for-app-code).

The node also refuses to activate a cached shard that another process already has open, and logs an error for files in its data directory that it doesn't know, which catches apps opening files they weren't given.

### Schema migrations

Schema migrations belong to the app, using whatever migration tooling it already uses. The node knows nothing about schemas and enforces no policy. The usual practices for a service in front of a database apply. Recommendations:

- Check the schema version when opening a shard (`PRAGMA user_version` is cheap) and migrate if needed. Migrations are lazy, so only shards opened since a deploy are on the new version.
- Refuse shards newer than the newest version the app can read, as old game builds refuse newer save files.
- Set `PRAGMA application_id` in the first migration to a 32-bit hash of the app's identifier and the shard kind. Check it on open, so a shard is never opened as the wrong kind (after a bad import, for example).
- Prefer expand and contract: additive changes that the previous release already tolerates, with old structures removed in a later release. Rolling back is then just redeploying the previous release.
- For changes that can't be additive, ship down migrations too. Rolling back means migrating affected shards down, which takes time, as it would with any database.

### Shard kinds

Shard kinds are declared in a kinds file the node reads, kept in the app's repository and deployed with the nodes. Adding or changing a kind is a deliberate deploy step, much like running a migration on a shared Postgres database:

```json
{
  "kinds": {
    "users":  { "public": true, "idle_timeout": "10m" },
    "chat":   { "public": true, "sync": "off" },
    "wallet": {}
  }
}
```

The kinds file holds the settings the app's code relies on (see [Kinds file](#kinds-file)). Every node has the same file, so a kind's settings don't depend on which app version handles a request. Settings about the environment are [environment variables](#environment-variables), set by whoever runs the cluster.

- A node with an app address (`SHARDLITE_APP`) serves every declared kind through that app. One without serves none, which is how game server hosts whose processes only pin shards are set up. A node that serves none forwards requests and calls to the shard's owner, or, for an unowned shard, to a node that serves, which places it.
- Kinds marked `pinned` are never served by a host's app, only by a process that pins the shard (see [Pinning](#pinning)).
- Clients can only name kinds marked `public`. Every other kind is served only to calls from the app through the control API. A client that names one gets the same answer as for an undeclared kind, so clients can't activate, create or find shards of that kind, or tell that the kind exists. The node logs why, so a kind that should be `public` is easy to spot in development.
- Requests, pins and messages for undeclared kinds are refused, which catches typos.
- Changed kind settings take effect when a node reloads the kinds file, for new activations and new responses.
- The node reports ready to the load balancer (`GET /_shardlite/ready`) only while the app is healthy (if it has one), the kinds file is valid and its lease is good, so a bad deploy stops at the first node.

Adding a kind means declaring it in the kinds file and deploying the app that handles it.

## Routing requests to shards

Routing is never authorisation. It only decides which shard handles a request. Authentication and authorisation are entirely up to the app: the app on the target shard checks the request's credentials however it likes and rejects requests for shards the caller may not use. The node never inspects credentials.

The node works out the shard from fixed conventions, checked in this order:

1. **Header:** `Shardlite-Target: guildchat/7`. This is the one `Shardlite-*` header the node accepts from clients, which is safe since a client can name any shard by query parameter anyway. gRPC clients send it as metadata.
2. **Query parameter:** `?shardlite-target=guildchat/7`, for example `wss://game.example/chat?shardlite-target=guildchat/7`. The node strips the parameter, so the app sees its own path and query unchanged. This is the convention for browser WebSockets, which can't set headers.
3. **No shard.** A request that matches neither is shardless. It goes to the app, and the node acts as a plain reverse proxy. This is normal traffic: login, message of the day, news, store catalogues and health checks. The app serves it like any stateless service.

The conventions are fixed by shardlite, not configured per shard kind. This matters during rolling deploys. If apps declared their own route mappings, old and new versions could map the same URL to different shards, depending on which node a request reached, and data would end up split between them. With fixed conventions every node computes the same shard, whatever app version it runs. Changing which shard a client uses is a client change, versioned like any other API change.

For "my own data", login returns the user's id and the client names `users/{id}` in its requests. The app on the user shard checks that the credentials match.

### Kinds clients can name

A client that names a shard reaches its activation before any app code runs, which Orleans and Durable Objects avoid by having edge code pick the ids. Clients can therefore activate, create or restore any shard of a kind they can name, which costs blob storage requests and node resources even when the app then rejects the request. Recommendations:

- **Make a kind `public` only if clients need to name it,** usually the player's own shard and anything they hold a long-lived connection to. Every other kind is reached only through calls from the app after it has authenticated the request (from a shardless handler or another shard).
- **Give kinds clients can name unguessable ids** (random UUIDs). Ids an attacker can't guess can't be used to force restores of real shards, push them out of the local cache or find out which exist.

### Shardless requests that need a shard

A shardless request that belongs to a shard (a login, which has to reach the user's shard where the credentials live) is sent there by the app. Shardlite needs no feature for this. Recommendations:

- **Tell the client where the shard is, by default.** The shardless handler works out the shard and answers with a URL that names it (`/login?shardlite-target=users/42`), or a `307` to it so clients follow it themselves with the same method and body. WebSocket handshakes can't follow redirects, so they get the URL in an ordinary response. It costs one client round trip per connection. With a sticky load balancer the next request usually lands on the same node, so the shard is activated there.
- **Call the shard where a redirect doesn't work:** for clients that don't follow redirects or a client that shouldn't see the shard. The shardless handler makes an ordinary [call](#calls-between-shards) with `Shardlite-Target: users/42` and returns the result, passing on the client's credentials so the shard authenticates the request as usual. An unowned shard is again activated on the local node.

Redirects and calls made by shardless handlers are app code, so old and new versions can still disagree there:

- Keep these decisions stable across releases. Working out who a request is about (a token belongs to user 42, so `users/42`) is stable. Policy that changes between releases isn't.
- Change one over two releases: first make the new target handle requests sent to the old one, then switch.
- Have clients use the conventions directly after the first call (login returns the shard names), so few requests need redirecting.

### Connections and home shards

A WebSocket connection is tied to one shard. A game client usually touches several (its player, its guild, its match). Over HTTP each request is routed separately. Over a WebSocket the client connects to its **home shard** (normally its player shard), and the app calls other shards through the control API, as in Orleans. A client that needs its own stream to another shard (a match with real-time traffic, for example) opens a second connection.

An open connection (WebSocket, a server-sent events stream or a streaming gRPC call) keeps its shard active. The app can close the connection to let it deactivate.

### Creating shards

Every shard id can be addressed, as Orleans grains can. A request for a shard that doesn't exist creates it, empty, before the app sees it. When a shard deactivates holding nothing of the app's (no table, index, `user_version` or `application_id`), the node deletes it, so a request for a random id (a typo, a stale link or an attacker) leaves nothing behind. It still costs a conditional create, a listing and a conditional delete in blob storage, and rate limiting at the load balancer is the defence against floods of them.

Migrating the schema on open counts as a write. Apps should authenticate a request before opening the shard, so a rejected request leaves nothing behind.

## Durability

Each shard kind sets `"sync"` in the kinds file: `"on"` (the default) or `"off"`. Every commit is shipped to blob storage either way, as with Postgres' `synchronous_commit`. The setting only decides whether acknowledgements wait.

| Sync | Survives | Typical latency |
|---|---|---|
| off | crashes of the app or node, since committed writes are in the WAL and shipped within a flush interval. Loss of the host loses the last interval. | 0 |
| on | loss of the host (flushed to the node journal in blob storage) | on average half the flush interval plus one PUT, since one flush covers the whole node: roughly 150–200 ms on S3 Standard with the default 250 ms interval |

With sync on, one rule covers every protocol: nothing leaves a shard before what it committed earlier is durable. The node holds responses, streams (WebSocket, server-sent events, gRPC streaming) and calls to other shards alike. Output from a shard with nothing new to capture passes without waiting, so a kind that leaves the setting at its default pays latency, never data, and only for output that follows writes.

- The gate is conservative: output waits for everything committed on the node before it was read, including other concurrent requests.
- A call to another shard counts as output, so it is held too. Without this, a `wallet` shard could debit gold, call `inventory/42` to grant an item and then lose the debit in a crash, while the item stays granted.
- With sync on, every chunk of a stream can wait for a flush, pings included, so real-time kinds (chat, presence, streams) should turn sync off.

The app can override its kind's setting for one HTTP response, or one call to another shard, with `Shardlite-Sync: on` or `off`.

`POST /_shardlite/sync` on the control API waits until everything committed on the node before the call is in blob storage, and fails if the node has lost its lease, so the app never acknowledges a write that will never reach blob storage. It is for side effects shardlite can't see: non-HTTP egress such as publishing to Kafka or NATS, external HTTP calls, an important write on a kind with sync off, and writes to pinned shards that the process acknowledges over its own protocol. `sync` covers commits since the node last started: commits to sync-off shards that a crashed node hadn't flushed are lost, and a later `sync` can't bring them back.

For sync-off kinds, the flush interval (`SHARDLITE_FLUSH`, 250 ms by default) sets how much a host loss can lose.

### Consistency

Each shard has one owner, which serves every request for it, and SQLite orders its commits. What a client can rely on depends on the kind's sync setting:

| | Writes | Reads | Like |
|---|---|---|---|
| sync off | acknowledged before they are durable: a host loss loses up to a flush interval | from the one owner, so current, but they can show a write that a host loss then loses | Postgres with `synchronous_commit = off` |
| sync on | linearizable and durable once acknowledged | linearizable, given working clocks | CockroachDB, per key |
| sync on, with a fencing write | | linearizable under any clocks and pauses | Consul's `consistent` reads, ZooKeeper's `sync` |

With sync on, a shard behaves as one copy of the database that every client reads and writes in one order: once a write is acknowledged, every read that starts afterwards sees it, on whichever node the request lands.

"Working clocks" means two things. Clock rates on different nodes agree to within 20% (real clocks differ by parts per million). The clocks are not required to be synchronised: leases are judged by elapsed time, not by comparing timestamps. And no machine is paused in a way its own clocks don't see, which a hypervisor pausing a VM can do (the VM resumes with its clocks behind for a moment). A node that lost its lease, because it was paused or cut off from blob storage, answers nothing once its own deadline has passed. The one exception is an answer it had already checked before a pause its clocks missed.

For reads that must hold even then (a balance shown before a purchase, say), commit a write after the read, in the same transaction or after it:

```sql
BEGIN IMMEDIATE;
SELECT balance FROM wallet WHERE id = 1;
UPDATE fence SET n = n + 1;   -- any write that changes a page
COMMIT;
```

The answer waits at the gate until that commit is in a segment that has reached the node journal. A node that has lost the shard is fenced and can never write that segment, so an answer that gets out proves the read was current. It costs a durable write: up to a flush interval plus a PUT, shared with everything else waiting on the node. On a kind with sync off, send the answer with `Shardlite-Sync: on`, or call `POST /_shardlite/sync` after the commit.

The guarantees hold for answers that go through the node, which is every request a kind serves, and for what the app reads from the shard's database:

- An app that answers from its own in-memory copy of a shard's data can serve something older: another app process on the node may have written since, or the shard may have moved away and back. Keep such a cache no longer than an open connection to the shard and check `PRAGMA data_version` on that connection before using it (it changes whenever another connection has committed). While any process has the file open, the node can't let go of the shard, so the cache can only be as old as the database. Or keep a version row that every write transaction bumps, and compare it on each request.
- `Shardlite-Sync: off` on a response opts that response out, as for sync off.
- Traffic a pinning process sends its own clients is the process's to stop at its pin's deadline.

Across shards there are no transactions: two reads of two shards are two separate linearizable reads, not a snapshot of both. Calls happen at most once per attempt and messages at least once (see [Calls between shards](#calls-between-shards) and [Messages](#messages)).

## Localhost control API

The app talks to its node over HTTP on localhost TCP or a Unix socket. Both are offered because many HTTP clients (Java's `HttpClient` or Unity's `UnityWebRequest`, for example) don't support Unix sockets without extra work. It is a separate listener from the public one, so apps may send `Shardlite-*` headers to it.

Most of the API is headers on the app's normal requests: any request with `Shardlite-Target` is a [call](#calls-between-shards) to that shard. The rest is a few plain HTTP and JSON endpoints under `/_shardlite/`, callable from any language even if the app's own protocol is gRPC (see [Control API](#control-api) in the reference).

## Deleting shards

A shard is deleted in one of two ways. Either way its manifest and LTX files go, a crash part way leaves the shard as it was (deleting again finishes the job), and a later request for the same id creates a new, empty shard that never sees the old shard's data.

- **From outside,** with `DELETE /_shardlite/shards/{kind}/{id}` on the control API. It is routed to the owner like a call, and the owner deactivates the shard and deletes it. A shard in use, or one the app still has open, answers `409`, so the caller tries again later. A shard can't delete itself this way, since its own request holds it. Uses: deletions decided outside the shard, such as account deletion.
- **From inside, with the delete marker.** A shard with a table named `_shardlite_delete` is deleted the next time it deactivates, whatever else it holds: `CREATE TABLE _shardlite_delete (x)` in any transaction marks it. Its pending messages are delivered first, since outbox rows and inbox rows that are due keep a shard active. Inbox rows due later, outbox rows the node gave up on and messages delivered after the marker go with it. Until then the shard is served as usual, so the app can check for the table and refuse requests. Uses: matches, trades and lobbies that end.

A marked shard whose node crashes is deleted on whichever node activates it next. Adoption wakes it if the marker's commit was still in the dead node's journal, and otherwise the hint written with the delta that took the marker does.

## Calls between shards

A player normally connects to their user shard. Guilds, groups, trades and other social features are mostly calls between shards, so this is a hot path.

Shards reach each other in two ways, and the choice is about what the caller needs:

| | Calls | Messages (outbox and inbox) |
|---|---|---|
| Shape | synchronous request and response | asynchronous, no response |
| Delivery | at most once per attempt, retried by the caller | at least once, retried by the node |
| Tied to the caller's commit | no | yes, sent in the same transaction |
| For | answers needed now: reads, queries, checks | changes that must happen, and timers |

Call when you need an answer, and send a message when it must happen. Both reach the same handlers with the same headers, through the same routing.

The app always calls its local node and relies on routing. The same app code runs on every node, and a shard call is just another HTTP route in the app:

```
app (users/42) → POST localhost:2282/invite
                 Shardlite-Target: guild/7
                 node: who owns guild/7?
                   local  → POST app/invite with Shardlite-* for guild/7
                   remote → forward to the owner node → its app/invite
```

- **The target is explicit.** The `Shardlite-Target` header names the shard. The path, method and body pass through unchanged to the owner's app, with the usual `Shardlite-*` headers added, so this works for any HTTP API: REST, GraphQL or gRPC. Apps will usually reuse the protocol their clients speak, so the same handlers serve clients and other shards. WebSockets also work, with `Shardlite-Target` on the upgrade request, which suits long-lived subscriptions such as a user shard following guild events.
- **Local calls still go through the node.** That costs one localhost round trip (about 0.1 ms) and keeps one code path. Remote calls add about 0.5–2 ms.
- **The callee knows the caller.** The app names the shard it is calling from with `Shardlite-Caller: users/42` (copied from the `Shardlite-Target` it received), and the node passes it to the callee. A call with no shard behind it (from a shardless handler, for example) gets `Shardlite-Caller: app`, so every call carries the header. The node strips it from external requests, so the callee can trust it. The rule for handlers is then simple: a request without `Shardlite-Caller` came from the internet and must be authenticated, and internal-only routes require the header.
- **Gates apply on both sides.** The caller's gate holds the call if the caller's kind has sync on. The callee's gate holds the answer, so a call to a `wallet` shard returns only once the wallet's writes are in blob storage, and the caller can rely on it.
- **Calls activate the target** if it isn't active, like any other request, unless the call sends `Shardlite-If-Active: true` (see [Reaching many shards](#reaching-many-shards)).
- **Hops are limited**, as Dapr limits call depth. The node sends `Shardlite-Hops` with each request to the app, and the app passes it on when it calls another shard or sends a message, as it would pass on `traceparent`. The node increments it on each call and message (a forward from node to node is not a hop) and refuses past `SHARDLITE_MAX_HOPS` with `508`, which stops runaway recursion (for example, two shards calling each other forever). An app that doesn't pass it on starts again from zero.
- **Cancellation propagates.** When a client disconnects or a caller gives up, the node cancels the request to the app and any forwarded or held requests on every hop it controls. Apps that cancel their own outgoing calls when a request is cancelled (Go's `context`, .NET's `CancellationToken`) get cancellation down the whole call chain. Writes already committed are not rolled back.
- **Deadlines propagate**, as gRPC's `grpc-timeout` does. The node sends `Shardlite-Deadline` (the time left, from the client's or caller's timeout) with each request to the app, and the app passes it on with its calls. Each hop subtracts the time it used. A node never forwards, holds or gates a request past its deadline, so the callee can stop work the caller has given up on.
- **The node never retries a request the app may have seen,** so calls that change state should carry an idempotency key.

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

If the shard isn't active on its owner, the node replies `404` (or `NOT_FOUND` for gRPC) without activating it. This is also useful for presence checks.

## Messages

Shards talk reliably through two reserved tables in each shard, written in the same transaction as the change that causes them:

- **The outbox,** `_shardlite_outbox`, holds calls to other shards. The node moves each row into the target's inbox once the commit is durable.
- **The inbox,** `_shardlite_inbox`, holds calls to the shard itself, now or later: delivered messages, and rows the app sets for itself ("this upgrade finishes at 14:05"). The node calls the shard's handler when a row is due, and the handler consumes the row in the same transaction as its work.

Nothing is sent for a write that is later lost, and nothing is lost once the write is durable. What a handler commits for a row happens once, but a message can arrive in the inbox twice. That leaves two ideas to hold: a message arrives at least once, so handlers deduplicate, and anything a shard must do later, or must not forget, is a row in its own inbox. The node creates both tables at activation, before the app gets the shard (their schemas are in [Reserved tables](#reserved-tables)).

Sending:

- **Insert a row into the outbox** with the target shard, path and body, and the hops from the request being handled, as calls pass them on.
- **Delivery.** Once the commit is durable, the node delivers the row into the target's inbox and then deletes it from the outbox. The receiver's app isn't involved until the row comes due.
- **At least once.** A lost acknowledgement means the message is delivered again, and it arrives in the inbox twice. Message handlers deduplicate through their own data, usually a natural key in the body (`INSERT … ON CONFLICT DO NOTHING` on the match id). Messages go concurrently, so there is no ordering.
- **No delays.** Outbox rows are sent at once. A delay belongs in an inbox, so only one shard waits for it: the receiver's handler sets a row in its own inbox, or the sender sets one in its own whose handler sends the message later.

Receiving:

- **Keys.** The app may give its own rows a key, so it can replace one (an upsert on the key) or cancel it (a delete). Delivered messages have none.
- **The handler is called** with a `POST` to the row's `path` on the shard, once the row is due and its commit is durable. `Shardlite-Caller` is set to the sender (or the shard itself), `Shardlite-Inbox-Id` to the row's id and `Shardlite-Hops` to its `hops`. The request carries everything the handler needs, and the row is only there to redeliver and acknowledge it, as a queue keeps a message until it is deleted.
- **The handler consumes it.** It deletes the row by id, or for a recurring row upserts its key with the next due time, in the same transaction as its work, so the work and the acknowledgement commit together. A handler can still be called more than once (one that fails or rolls back is called again), but what it commits, including the rows it inserts into the outbox, happens once. Only what it does outside its transaction, such as a direct call, can repeat.

Failures:

- **Retries.** `429`, `5xx` and connection failures are retried with backoff, for both delivery and handlers.
- **Giving up.** The node gives up on a row whose target isn't a valid shard or whose kind isn't declared, whose hops are past the limit, that gets another `4xx`, whose handler answers `2xx` without consuming it (a bug in the handler) or that has failed for more than a day. It logs an error and stops trying for as long as the shard stays active. The row is tried once more on the next activation, so a broken row costs one attempt and one error per activation.
- **Finding stuck messages.** On an outbox row the node gives up on, it sets `error` to the reason (`400 from players/42: unknown reward`, `undeclared kind`), so the sending app can find them with `WHERE error IS NOT NULL`. It is only information: the next attempt deletes the row or overwrites it, and the app may clear it or delete the row. Inbox rows are left as they are, since their failures are the app's own handler's answers, which it can record itself.
- **Liveness.** An active shard sends and handles what it has without a request, and stays active until nothing is left to send or due now. Inbox rows due later don't keep it active: the node wakes the shard when they come due, on whichever node then claims it.

Uses: fan-out in one transaction (rewards for 100 players at the end of a match), transfers and trades by custody (the item moves into a trade shard and out again), work deferred past a response, timers that must fire while nobody is connected (an upgrade finishing, an auction closing) and recurring jobs.

## Rules for app code

These are part of the contract with shardlite. Apps that break them stall or fail.

- **Handle concurrent requests, including calls back into the app itself.** A call to a shard on the same node arrives at the same app as a new request while the calling request is still waiting. A single-threaded synchronous server deadlocks. Thread pools can run out under load when calls nest, so async servers are the safer choice, or pools must be sized for call depth. Running several app processes behind the app's address also helps, so a nested call can land on a different process.
- **Don't hold a write transaction while calling another shard.** If `users/42` is mid-transaction, calls `guild/7` and the guild calls back into `users/42`, the callback can't get the write lock. Call first and then write, or write, commit and then call.
- **Expect calls to fail and set timeouts.** A call can arrive while its target is activating or moving, and its node can fail. Retry where it makes sense.

## Recommendations for app code

These aren't required, but they suit how shardlite works.

- **Use SQLite for concurrency within a shard.** Requests for the same shard run concurrently, as they would against any database. Shards are reentrant, unlike Orleans grains and Dapr actors, which handle one request at a time by default. SQLite allows concurrent readers and one writer. A handler that reads and then writes in separate statements outside a transaction (`SELECT` a balance, then `UPDATE` it) can lose another request's update, so put both in one write transaction or one statement (`UPDATE … SET balance = balance - 10 WHERE balance >= 10`). State kept in app memory per shard (caches, a simulation) isn't protected by SQLite, so the app handles its concurrency.
- **Start write transactions with `BEGIN IMMEDIATE`, or retry them.** SQLite has one write lock for the whole database, so a deferred transaction that reads and then writes fails with `SQLITE_BUSY` if any other connection committed in between, whatever it touched, and `busy_timeout` doesn't retry that case. Litestream's bookkeeping and the node's message delivery commit now and then, so this is rare but does happen. Either take the write lock up front (`BEGIN IMMEDIATE`, or the driver's equivalent), or run each transaction through a helper that takes a callback and retries it on `SQLITE_BUSY`.
- **Check that the client named the right shard.** A shard handler whose route includes an id (`/users/42/inventory`) compares it with `Shardlite-Target` and returns `400` if they differ, so a client that names the wrong shard or none fails in development instead of costing an extra hop in production. One line in the app's routing middleware.
- **Choose ids that need no lookup.** A client can choose a new shard's id itself (a UUIDv7, which sorts by time), which makes creating it idempotent and saves a round trip. An id built from other data, such as a trade between two players or a name in a registry, can be a name-based UUID (v5) of that data, so every caller computes the same shard without asking anyone.
- **Sync writes that must not be lost** on a kind with sync off (payments and the like) by calling `sync` before acknowledging (see [Durability](#durability)). A small commit wrapper on the app's SQLite driver makes this hard to forget.
- **Send a message for calls that must happen.** "Write, commit, then call" loses the call if the node crashes in between. Insert a row into the outbox in the same transaction instead (see [Messages](#messages)).
- **Drive time-based work from requests.** Energy, cooldowns, deadlines and countdowns can be worked out from timestamps whenever a request touches the shard, so they need no timer. Work that repeats while someone waits (a matchmaking queue launching waves) runs when waiting clients poll or send heartbeats, as matchmaking APIs have them do anyway. A daily reset happens lazily: the first request after the reset time applies it, so a player who never returns costs nothing. Set a row in the shard's inbox only for things that must happen on time while nobody is connected, and spread rows that many shards share (add a random delay of a few minutes), since a million rows due in the same minute wake a million shards at once.
- **Keep indexes in index shards, updated through the outbox.** A shard that should be findable (a lobby, a base by trophy count, a name) sends its entry to an index shard split by what's searched (`lobbies-index/eu`), and sends another to remove it. Handlers upsert, so duplicates are harmless, and readers skip entries older than a timeout, since a shard that is abandoned never removes its own.
- **Delete short-lived shards with the marker.** A lobby, match or trade sets a row in its own inbox when it starts, due when it should be gone at the latest. When it ends, or the row comes due, its handler creates `_shardlite_delete` in the same transaction as its last work and final messages, and the shard is deleted once it goes idle (see [Deleting shards](#deleting-shards)). The row makes sure an abandoned shard still ends, crashes included.
- **Split hot shared shards.** By meaning first (season, region, mode), then by hash with an aggregator shard. A single shard too hot for any node cannot be split by shardlite. That is a data-modelling problem.
- **Publish by recording events.** A topic's shard keeps its events in a table with a sequence number. Connected clients get them pushed over WebSockets or server-sent events. Subscriber shards catch up when next touched, asking for events since their stored sequence and applying them and moving it in one transaction, so offline subscribers never wake and each event applies once. Send messages only to subscribers that must act now.
- **Cache shared data.** Data that many requests read (a message of the day, a guild's member list) should be cached by the app, refreshed from the owning shard's events or shipped with the release, not read from its shard every time, or that shard becomes hot.
- **Start game servers with a handshake.** A lobby that needs a server asks the orchestrator for one, passing its own id and the match's, and in the same transaction sets a row in its inbox due when it should give up. The server pins `matches/{id}` and inserts an outbox row in that shard telling `lobbies/{id}` it is ready and where clients connect. The lobby's handler stores the address, tells the players and deletes its timeout row in one transaction. If the timeout fires first, the lobby allocates again or sends the players back to the queue, and tells a server that reports in late to shut down.
- **Keep large data in object storage.** A world save of gigabytes doesn't fit a SQLite blob (1 GB at most) and would be shipped whole on every save. Upload it to the app's own bucket under a unique name, then commit the reference in the shard. A crash in between leaves an orphan, never a missing object, and old objects are deleted once a newer reference is committed.

## Persistent worlds

Game servers keep their simulation in memory, and shardlite can hold what outlives it. This is a recommendation, in three forms:

- **Server as caller.** Persistent pieces of the world are shards (`bases/{id}`, `chunks/{x}-{y}`, `containers/{id}`). The server loads a chunk's state when it streams in and writes changes back with calls. Each piece has one writer, so zone servers editing near a boundary are serialised by the shard. A crashed server is replaced by one that loads from shards. This suits loads and building edits, not per-tick state.
- **A world or zone as one pinned shard.** The world save is a SQLite file with serialized chunks as rows, instead of one large file written every few minutes. Only changed pages are shipped, so saving is continuous, the loss window is about one flush interval and snapshots double as backups. Game servers run wherever the orchestrator (Agones, GameLift) puts them, sharing one node per host, and each server pins its world on the local node when it starts. If the host dies, another server pins the world and restarts from it. This makes it easy to write an MMO from scratch: zone servers own zone shards and players own player shards. Shardlite handles storage and failover, and the app sends clients to zones by looking them up.
- **Many private worlds, pinned.** Hosted private worlds (as in Minecraft Realms, or Valheim and ARK servers) are idle most of the time. As shards, idle worlds deactivate and cost only storage. When a player joins, Agones or a process manager starts a server on a host with capacity, and the server pins the world and opens the restored file.

Game servers can run in a different facility from the backend (bare metal for servers, cloud for the rest). Their nodes reach the backend's nodes over an encrypted network (see [Traffic between nodes](#traffic-between-nodes)), and their calls to backend shards and syncs to blob storage pay the latency between facilities. That suits worlds, which call other shards occasionally rather than every tick.

## Pinning

For game servers that keep a world or match in memory and talk to their clients over their own protocol: WebRTC, custom UDP (including Unreal's networking) or raw TCP. A process pins a shard on its own node with a control API request that stays open. The node claims the shard, or has its owner hand it off, and holds it until the request closes because the process let go or died. A pinned shard is never moved, rebalanced or deactivated, so ownership follows where the orchestrator (Agones, GameLift) starts the process.

- **Requests, calls and messages** for the shard go to the process if it gave the node a localhost HTTP address when it pinned, with the same routing, gates, hops and deadlines as served shards. Without one, requests get an error and messages wait in the inbox.
- **Pinned kinds** (`"pinned": true`) are served only by a pinning process. A request for one that nobody has pinned gets `503`, so the app knows to start a server, and its inbox waits for a pin.
- **Clients connect to the process directly,** at an address the app gives them: from login or matchmaking, or from a signalling request routed to the shard like any other (`POST /connect` returning `ip:port` and a connect token, or an SDP answer for WebRTC). Shardlite keeps no connection info, and issuing tokens is the app's job.
- **The process covers what the node can't see.** It stops serving clients past the lease deadline the node sends down the pin, calls `sync` before acknowledging a write that matters, and moves its clients when it unpins. A live simulation can't migrate: moving it means `sync`, unpin, a pin on the new host and clients reconnecting.

## Deployment

Nodes are disposable. Blob storage holds the data, and local disks are caches.

- **Dependencies:** blob storage (see [Blob storage](#blob-storage)), plus the deployment's own load balancer in front of the nodes.
- **Disks:** local NVMe is the fastest option, and ephemeral disks (cloud instance NVMe) are fine. Size them for the working set plus a cache, not for all data. When disk fills up, the node evicts the least recently used cached shards.
- **Kubernetes:** the node as a DaemonSet, one per Kubernetes node, with app pods on the same machines, so every pod on a machine shares one node journal. `SHARDLITE_APP` points at a Service with `internalTrafficPolicy: Local`, so each node reaches only the app pods beside it. The app pods and the node share the data directory and the control API's Unix socket through a `hostPath` volume. Clusters that forbid host volumes can instead run one large pod per machine with the node as a sidecar, but then every app deploy restarts the node and loses its cache.
- **Elsewhere:** the node runs as a systemd unit or similar. There is no session protocol between app and node: a dead node means the app is unreachable, so no new requests arrive and no response can be acknowledged.
- **Development:** a single node on RustFS in Docker (the Compose file the tests use), so the real storage path is always exercised.
- **Single machine:** one node with RustFS on a durable volume (EBS or a Kubernetes persistent volume) and shard files on fast instance storage. RustFS must keep its buckets in strict durability mode, since a store that acknowledges a write before it is on disk breaks the gates' promise. A store on the same machine answers quickly, so the flush interval can be short. Moving to a cluster later means copying the bucket to S3.
- **Scaling up** for a launch spike: add nodes. New players and newly activated shards land on them, and rebalancing moves some busy shards from overloaded nodes. Scaling is by CPU and memory, since storage is elsewhere.
- **Scaling down** after the spike: drain the node. It stops serving, deactivates its shards, releases them and exits. Nothing else has to move.
- **Planned restarts and upgrades:** the node fails its readiness check so the load balancer stops sending it new connections, drains short requests and closes long-lived connections with a reconnect code. A drain that can't let go of a shard the app still has open aborts after a timeout and keeps running, with an error. A restart is treated as a crash, and the node starts with an empty cache, so a redeployed node restores its shards from blob storage as they activate (keeping the cache across restarts is in the [roadmap](roadmap.md#later)).
- **Mixed node versions:** during a rolling upgrade, old and new nodes forward to each other and read each other's data in blob storage.

### Load balancer

Clients reach nodes through the deployment's own load balancer, which terminates TLS and absorbs hostile traffic. Nodes listen only on the private network. The load balancer sets `X-Forwarded-For`, `X-Forwarded-Proto` and `X-Forwarded-Host` on every request (overwriting what clients send, which is its job). A node adds the load balancer's address to the end of `X-Forwarded-For` and passes `X-Forwarded-Proto` and `X-Forwarded-Host` on as they came, so the app sees the client's address, protocol and host. A request that arrives without them gets the node's own view of it. The load balancer checks `GET /_shardlite/ready`.

A request that lands on a node that doesn't own its shard is forwarded to the owner, at 0.5–2 ms. Reaching the owner directly is up to the load balancer:

- **Sticky load balancer:** an unowned shard is activated on the node that received the request, so a client's shards are usually activated where it first lands, and stickiness keeps it there.
- **Controllable load balancer** (HAProxy, nginx, Envoy): route by hashing `Shardlite-Target` (header or query parameter).
- Otherwise the request is forwarded.

Shards don't follow requests: a forwarded request leaves the shard where it is. Shards move only for rebalancing, draining or a crash. If a node stops answering, requests for its shards get a quick `503` until its lease has expired, another node has confirmed it and adopted the node, and the shards can be claimed (10–20 s with the default lease TTL).

### Blob storage

The supported stores are AWS S3 and, for development, tests and self-hosting, RustFS. Blob storage must support linearizable conditional writes (`If-None-Match` and `If-Match` on writes, and `If-Match` on deletes) and custom metadata on conditional writes, returned on `HEAD`. Cloudflare R2 doesn't document conditional deletes, so it fails the startup check until it does. Any other store, Tigris included, gets the startup check and nothing more.

The node checks its store at startup and refuses to start if conditional writes misbehave, so a store or proxy that silently ignored `If-Match` can't break fencing without any error. It tests the deployment's actual store, so it is worth keeping on.

`SHARDLITE_PREFIX` puts every object under a prefix (`staging/`), so several environments or games can share one bucket. No bucket lifecycle rule is needed: each node deletes its own expired journal segments. Shardlite keeps no history of old files. A deployment that wants point-in-time restore can turn on bucket versioning, which shardlite neither needs nor manages. Deleting players who never return is the app's decision (retention policies, data deletion requests).

### Hosts

The node targets Linux only. Game servers and backends that use it must run on the same Linux machine (or VM), since they share SQLite files. Game clients and editors can run anywhere and connect over the network.

Developers on other systems run the node, backend and game server in Linux:

- **macOS**: in Docker (Docker Desktop or similar runs a Linux VM). Shard files must live in a Docker volume, not a bind-mounted macOS folder, since file sharing with the VM is slow and SQLite locking isn't reliable across it.
- **Windows**: in WSL2 or Docker, as is common in game development: develop and run the client on Windows, and run Linux builds of the game server and backend. Shard files must live on the Linux filesystem, not under `/mnt/c`, for the same reason.

Game servers running natively on macOS or Windows (an Unreal dedicated server or play-in-editor, for example) are not supported. The node may build on macOS, since Go cross-compiles, but that is unsupported.

Host settings:

- **Open files and memory mappings.** Each active shard takes 6 to 7 file descriptors and a few memory mappings in the node (about 0.4 to 0.8 MB of memory). Raise the hard `ulimit -n` (Go raises the soft one) and `vm.max_map_count` (65,530 by default, reached at about 10,000 active shards). The node warns at startup if they are too low.
- **Scratch directory.** `SHARDLITE_SCRATCH_DIR` (`/dev/shm/shardlite/` by default on Linux) belongs on tmpfs. Size it for the active shards' commits not yet written as snapshots, plus the largest commits that rewrite a whole database (an import, a `VACUUM`), which Litestream stages there until their snapshot. The node warns if it isn't on tmpfs.
- **Data directory.** `SHARDLITE_DATA` must be on a local filesystem, with room for temporary files: the largest shards being restored or rebased (at most 4 rebases run at once), and about twice the size of any commit that rewrites a whole database while it is in flight. The node refuses to start on NFS, SMB or FUSE.
- **Clocks.** Run NTP (chrony, or the cloud provider's time service). Leases don't rely on synchronised clocks, only on clock rates that agree (see [Consistency](#consistency)), but a clock that is far off delays failover and makes logs hard to read across nodes. The node warns when the host's clock isn't synchronised or is more than a second from the store's, and refuses to start if it is off by more than the lease TTL.

### Traffic between nodes

A node has three listeners: public (clients, through the load balancer), peer (other nodes) and control (the app on the same host). They are the same split as etcd's client and peer URLs, Orleans' gateway and silo ports, and Dapr's sidecar API beside its port between sidecars.

Nodes talk to each other on a listener of their own, `SHARDLITE_LISTEN_PEER`, which speaks HTTP without TLS (HTTP/2, and HTTP/1.1 for WebSockets). It must only be reachable from other nodes: never put it behind the load balancer or expose it to clients or the app. Requests on it are trusted to carry node-only headers. Nodes must be on a trusted private network: traffic between them is not encrypted or authenticated. Where the network isn't trusted, run the nodes over an encrypted network (WireGuard, Tailscale). Service meshes (Istio, Linkerd) are not a target, since they add two proxy hops (roughly 0.5–1 ms) to every call between nodes. In a cluster that already runs one, exclude shardlite's traffic between nodes from it.

### Cost

Rough estimates on S3 Standard, to be checked by the prototype:

- **Journal segments:** 4 PUTs a second per busy node at the default 250 ms flush interval, about 50 USD a month per busy node, whatever the number of active shards. Idle nodes write nothing. A shorter interval makes sync faster and the loss window smaller at a higher cost (about 130 USD a month at 100 ms).
- **Snapshots:** one PUT (the changed pages only) and one manifest write each time a shard deactivates or its unsnapshotted commits age out (about once per session), about 10 USD per million sessions. A rebase uploads the whole shard once it has more than 8 files, so about every 8 snapshots. Infrequent-access storage classes bill small objects as at least 128 KB, so small files stay in the standard class.
- **Restores:** a few GETs in parallel, for the base and deltas.
- **Storage:** a 100 KB player shard is 100 GB for a million players, a few dollars a month, plus old files during their retention.
- **Manifests and leases:** one conditional write per new shard (plus a listing and a delete, which is free on S3, for one that is never written), per move and per snapshot, and lease renewals every few seconds per node (well under a dollar a day per node). Noticing dead nodes costs about 7 USD a month for the whole cluster.

### Latency

Estimated, not measured:

| Case | Added latency |
|---|---|
| Active on this node | ~0.1 ms |
| Owned by another node (active or cached) | +0.5–2 ms forwarding, plus activation there. The first request for a shard also reads its manifest (~10–30 ms), after which the node remembers the owner |
| Owner moved since the node last forwarded | one more forward (+0.5–2 ms) after the old owner says it moved |
| Cached on this node | ~1 ms (open, start capture, app's schema check) |
| Claiming a shard | one conditional write on its manifest (~20–60 ms on S3 Standard) plus a snapshot download |
| New shard | one manifest read (~10–30 ms) and one conditional create (~20–60 ms) |
| Owner crashed | up to twice the node lease TTL (10–20 s with the default), then claim and restore |

### Load

- **Rebalancing.** New nodes only receive new connections and new shards, so the node moves busy shards from overloaded nodes to less loaded ones, rarely and with a cap per interval. It closes those shards' WebSocket connections with a reconnect close code, or sends HTTP/2 `GOAWAY` (`Connection: close` on HTTP/1.1) to their HTTP clients. No request fails: in-flight requests finish and the client opens a new connection for the next one. Clients must reconnect on that close code.
- **Load shedding.** Above a CPU or memory threshold, a node replies `503` with `Retry-After` instead of slowing everyone down, and refuses new activations. Shards that are already active keep working. Clients retry, and the load balancer may place them elsewhere.
- **Hot shards.** Each shard has a cap on requests in flight. Past it, the node replies `429`, so one hot or attacked shard can't use up a node's app threads.

### Observability

- **Tracing.** The node passes W3C trace context (`traceparent`) through every hop, including forwarding between nodes and calls between shards, and adds its own spans (routing, activation, restore, gate waits). Apps that also propagate it get end-to-end traces across shards. The node also counts shardless requests that lead to calls to a shard, and logs a sample of their paths, so routes where clients don't name the shard they need show up.
- **Alerts.** The node logs an alert if a shard's commits have gone longer than 3× `SHARDLITE_DELTA_AGE` without a snapshot, which means its snapshots are failing.
- **Diagnosis.** `SHARDLITE_LISTEN_PPROF` serves Go's pprof handlers.

## Reference

### Shard names

A shard is named `kind/id`. Kinds are lowercase letters, digits, `-` and `_`. Ids are 1 to 128 ASCII letters, digits, `-` and `_`, enough for numbers, UUIDs and ULIDs. Anything else gets a `400` before the node does anything with it, so ids go into object keys, file paths and headers as they are, with nothing to escape.

### Headers

All shardlite headers use the `Shardlite-` prefix. Clients can send `Shardlite-Target`, and every other `Shardlite-*` header from clients is stripped (also with `_` for `-`, as in `Shardlite_Caller`). Apps should pass on `traceparent`, `Shardlite-Hops` and `Shardlite-Deadline` on calls and messages.

| Header | Set by | Meaning |
|---|---|---|
| `Shardlite-Target` | client, app on calls, node to app | the shard a request is for |
| `Shardlite-Path` | node to app | the shard's file |
| `Shardlite-Caller` | app on calls, node to app | the calling shard, or `app` |
| `Shardlite-Hops` | app on calls, node to app | calls and messages along one chain |
| `Shardlite-Deadline` | app on calls, node to app | the time left |
| `Shardlite-Inbox-Id` | node to app | the inbox row being delivered |
| `Shardlite-If-Active` | app on calls | `true`: answer `404` rather than activate |
| `Shardlite-Sync` | app on a response or call | `on` or `off`, overriding the kind's setting once |

### Control API

On `SHARDLITE_LISTEN_CONTROL` (localhost TCP) and `SHARDLITE_LISTEN_CONTROL_SOCKET` (a Unix socket):

| Request | Meaning |
|---|---|
| `ANY /{path}` with `Shardlite-Target` | call `{path}` on the shard's owner |
| `POST /_shardlite/sync` | wait until everything committed before the call is in blob storage (fails if the node has lost its lease) |
| `DELETE /_shardlite/shards/{kind}/{id}` | delete the shard and its data |
| pin | an open request that holds the pin and carries the lease deadline, with an optional address to proxy to |

The node calls the app's health endpoint (`SHARDLITE_HEALTH_PATH`). The public listener serves `GET /_shardlite/ready` to the load balancer.

### Status codes

Errors the node generates itself (sent as gRPC status for gRPC requests):

| Code | When |
|---|---|
| `400` | a malformed shard name or `Shardlite-*` header |
| `404` | an undeclared kind, a kind that isn't `public` named by a client, or a shard that isn't active with `Shardlite-If-Active: true` |
| `429` | the shard's cap on requests in flight |
| `503` | the owner is unreachable, a pinned kind has no pin, a shard moved twice while its request was being forwarded, a body too large to send again after the owner turned out not to own the shard, or the node is shedding load (with `Retry-After`) |
| `504` | the request's deadline passed |
| `508` | hops past `SHARDLITE_MAX_HOPS` |

### Kinds file

`SHARDLITE_KINDS` names the file. Each kind under `"kinds"` takes:

| Field | Default | Meaning |
|---|---|---|
| `sync` | `"on"` | `"on"` or `"off"` (see [Durability](#durability)). A string, so other modes can be added |
| `idle_timeout` | `SHARDLITE_IDLE_TIMEOUT` | how long a shard stays active without requests, as a Go duration (`"10m"`) |
| `public` | `false` | clients may name the kind |
| `pinned` | `false` | only a pinning process serves the kind |

### Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `SHARDLITE_NODE` | the host name | the node's name |
| `SHARDLITE_ADVERTISE_PEER` | the peer listener's address | how other nodes reach the peer listener (`host:port`). Required when `SHARDLITE_LISTEN_PEER` is a wildcard address such as the default |
| `SHARDLITE_BLOB` | required | `s3://bucket` |
| `SHARDLITE_S3_ENDPOINT` | AWS | the URL of an S3-compatible store |
| `SHARDLITE_PREFIX` | | prefix of every object path, for example `staging/` |
| `SHARDLITE_KINDS` | required | the kinds file |
| `SHARDLITE_APP` | | the app's base URL. Without it the node serves no kinds |
| `SHARDLITE_HEALTH_PATH` | `/health` | the app's health endpoint |
| `SHARDLITE_LISTEN_PUBLIC` | `:2280` | the public listener |
| `SHARDLITE_LISTEN_PEER` | `:2281` | the listener for traffic from other nodes |
| `SHARDLITE_LISTEN_CONTROL` | `127.0.0.1:2282` | the control API on localhost TCP |
| `SHARDLITE_LISTEN_CONTROL_SOCKET` | | the control API on a Unix socket |
| `SHARDLITE_DATA` | required | the directory of shard files (the local cache) |
| `SHARDLITE_SCRATCH_DIR` | `/dev/shm/shardlite/` on Linux | disposable working files, best on tmpfs |
| `SHARDLITE_CACHE_LIMIT` | no limit | bytes of deactivated shards to keep |
| `SHARDLITE_FLUSH` | `250ms` | flush interval |
| `SHARDLITE_IDLE_TIMEOUT` | built in | idle timeout for kinds that don't set one |
| `SHARDLITE_LEASE_TTL` | `10s` | node lease TTL |
| `SHARDLITE_DELTA_AGE` | `15m` | how old a shard's unsnapshotted commits get before a snapshot is written. The tail alert (3×) and journal expiry (a day, or 4× the alert if longer) follow from it |
| `SHARDLITE_DELTA_SIZE` | `256KB` | how many bytes of unsnapshotted commits make a snapshot due, whatever their age |
| `SHARDLITE_MAX_HOPS` | `32` | the hop limit for calls and messages |
| `SHARDLITE_LISTEN_PPROF` | off | address for Go's pprof handlers |

### Reserved tables

Litestream keeps `_litestream_seq` and `_litestream_lock` in every shard, and the node creates the outbox and inbox at activation. A shard whose only content is these tables (and the `sqlite_sequence` that `AUTOINCREMENT` adds), with no rows in shardlite's, counts as never written. The app may create `_shardlite_delete` (any columns), which marks the shard for deletion (see [Deleting shards](#deleting-shards)).

```sql
CREATE TABLE _shardlite_outbox (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  target       TEXT NOT NULL,      -- 'players/42'
  path         TEXT NOT NULL,      -- '/rewards'
  content_type TEXT,               -- 'application/octet-stream' if unset
  body         BLOB,
  hops         INTEGER NOT NULL DEFAULT 0,
  created_at   INTEGER NOT NULL DEFAULT (unixepoch()),
  error        TEXT                -- set by the node when it gives up: why
);

CREATE TABLE _shardlite_inbox (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  key          TEXT UNIQUE,        -- optional, for the app's own rows: 'upgrade:barracks'
  sender       TEXT,               -- the sending shard, or null for the app's own rows
  path         TEXT NOT NULL,      -- '/upgrades/barracks/finish'
  content_type TEXT,
  body         BLOB,
  hops         INTEGER NOT NULL DEFAULT 0,
  due          INTEGER NOT NULL DEFAULT (unixepoch())  -- Unix seconds
);
CREATE INDEX _shardlite_inbox_due ON _shardlite_inbox (due);
```
