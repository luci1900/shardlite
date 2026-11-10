# shardlite guide

For people who build apps on shardlite. Whoever runs the cluster wants [deployment](deployment.md). The [overview](overview.md) introduces the concepts, and the [reference](#reference) at the end lists every header, endpoint and reserved table. How the node does what it does is in [internals](internals.md).

## Serving

The node routes requests and calls for any shard of a kind the app serves. Any id can be called and the first call creates the shard (see [Creating shards](#creating-shards)). A process that keeps a shard's state in memory and serves its clients itself, over any protocol, is a [single writer](#a-single-writer) of that shard: a game server holding a world, for example.

The node sits in front of the app. For each request it works out the shard (see [Routing requests to shards](#routing-requests-to-shards)), activates it if needed and forwards to the app with:

```
Shardlite-Opened:  users/42
Shardlite-File:    /data/users/42.k3x9.7f3a9c1e5b20.db
Shardlite-Context: shard="users/42", hops=0
```

`Shardlite-Opened` is the shard the node opened for this request. `Shardlite-Context` names it with the request's hops, and the app passes it back unchanged on the calls it makes while serving the request (see [Calls between shards](#calls-between-shards)). The app works out which shard a request is for from the request itself (the path, the payload, the first gRPC message). If that is the opened shard, the app uses the open file. If it is another shard, or a shard route came with no hint, the app **refuses** the request with `400` and a body saying the hint doesn't match. A wrong hint is a stale client or a bug, and the refusal tells it so: not `404`, which a client would take for a shard to drop, and not `421`, which HTTP/2 clients retry by themselves. If the node opened nothing, the request is **shardless**: the app serves it, and a shard it needs is one whose id the app worked out itself ("my guild") or took from something it verified (a webhook's signed payload), reached by an ordinary call (see [Shardless requests that need a shard](#shardless-requests-that-need-a-shard)).

The proxy handles HTTP, gRPC, server-sent events and WebSocket, passing requests through unchanged apart from `Shardlite-*` headers, which the node strips from client requests and app responses. Client requests for paths under `/_shardlite/` are refused, except the node's own `GET /_shardlite/ready`. Custom UDP, WebRTC and raw TCP go direct to the app's own servers, such as a game server that is a [single writer](#a-single-writer) of its world.

### What an app must do

The app and node share a local filesystem on one kernel (the same host, with a shared volume in containers). Network filesystems are not supported, since they break SQLite locking. The app:

- opens `Shardlite-File` with any SQLite driver of 3.37.0 (2021) or later (the reserved tables are STRICT), in read-write mode without create (`mode=rw` in a `file:` URI), once per request, and never builds paths itself. The node creates the file before it hands the path out, so an open that fails means the path is no longer the shard's, and an app that created it would make an empty database that nothing captures. The path changes whenever the shard gets a new file, so the connection, its statements and its transactions end with the request. Opening per request costs roughly 50 to 100 µs;
- uses WAL mode with default locking (not `locking_mode=EXCLUSIVE` or a VFS without shared memory) and sets `busy_timeout` (5000 ms, what the SDK runs), since the node takes the write lock now and then and writers would otherwise fail with `SQLITE_BUSY`. A write transaction held across a call waits the five seconds before failing;
- only runs passive checkpoints, as SQLite's automatic ones are. `FULL`, `RESTART` and `TRUNCATE` always find the shard busy, since the node holds a read transaction on it;
- closes the request's connection when the request ends. The node lets go of a shard once no request holds it and the app has closed its file. A file the app keeps open holds the shard active (with a warning once the hold outlasts the kind's idle timeout), and only a stop goes ahead over it (see [internals](internals.md#letting-go-of-a-shard));
- never writes a shard outside a request or a wake, since a write from a background thread, a timer or a connection kept open can land on a file nobody captures;
- treats tables whose names start with `_shardlite_` as reserved (see [Reserved tables](#reserved-tables));
- serves `GET /_shardlite/ready` with its declaration (see [Shard kinds](#shard-kinds)), and `POST /_shardlite/reminders` if it sets reminders;
- on a stop, stops accepting requests and finishes the ones in flight, as any web app behind a load balancer does. The node and the app can stop in either order (see [deployment](deployment.md#stop-order)).

### Schema migrations

Migrations belong to the app, with whatever tooling it already uses. The node knows nothing about schemas. A Go app without tooling can use the `migrate` module (`github.com/luci1900/shardlite/migrate`), which runs in the transaction the app already holds, keeps the version in `user_version` and reads golang-migrate's `{n}_{name}.up.sql` naming.

- Check `PRAGMA user_version` when opening a shard and migrate if needed. Migrations are lazy, so only shards opened since a deploy are on the new version. Refuse shards newer than the app can read.
- Set `PRAGMA application_id` in the first migration to a hash of the app's identifier and the kind, and check it on open, so a shard is never opened as the wrong kind.
- Make the tables STRICT, so the save format refuses what it cannot read back.
- Prefer expand and contract, so a rollback is redeploying the previous release.

### Shard kinds

The app declares itself at `GET /_shardlite/ready`, which answers a JSON declaration with the app's version and its kinds:

```json
{
  "version": "2026.10.0",
  "kinds": {
    "users":  { "public": true, "idle_timeout": "10m" },
    "chat":   { "public": true, "sync": "none" },
    "wallet": {}
  }
}
```

The node polls the route, and a 2xx answer that parses as a valid declaration is both the app's health check and the source of its kinds. The declaration is a contract between the app and the cluster, and its diff belongs in version control: an app on an SDK serves the declaration the SDK builds from its kinds, and an app without one keeps the JSON in a file in the repository (the fields are in the [reference](#declaration)). The route is under `/_shardlite/`, which clients can't reach, so the declaration reaches only the node.

- The version is an opaque string the node logs.
- Kinds from libraries go in the app's declaration under names the app picks (`matchmaker.queue`). A kind named twice makes the node report not ready.
- A node with [`SHARDLITE_APP`](deployment.md#environment-variables) serves every kind the app declares. One without serves no kinds and answers every request for a shard as undeclared, while still doing its share of the [dispatcher](#the-dispatcher)'s work.
- Clients can only name kinds marked `public`. Any other kind gets the same answer as an undeclared one, so a client can't tell the kind exists. The node logs why.
- Requests and calls for undeclared kinds are refused, which catches typos. A name that doesn't fit the [grammar](#shard-names) gets `400`.
- A changed declaration takes effect at the next poll. A poll that fails changes nothing: the node keeps the last good kinds and reports not ready until a good declaration comes.
- The node reports ready (`GET /_shardlite/ready` on the public listener, which the load balancer polls) only while the app's declaration is current, its lease is good and it isn't draining.

## Routing requests to shards

Routing is never authorisation. It only decides which shard the node opens for a request. The app checks credentials however it likes, and the node never inspects them.

The node takes the shard a client names from fixed conventions, in this order:

1. **Header:** `Shardlite-Target: guildchat/7`, the one `Shardlite-*` header the node accepts from clients (gRPC clients send it as metadata).
2. **Query parameter:** `?shardlite-target=guildchat/7`, which the node strips. This is the convention for browser WebSockets, which can't set headers.
3. **No shard.** The node is a plain reverse proxy: login, news, store catalogues and health checks.

The conventions are fixed, not configured per kind or app, so old and new app versions never send the same URL to different shards during a rolling deploy. The header and the parameter are hints. A right one reaches the owner with the file open. A wrong one costs an activation of the shard it names and a refusal from the app (see [Serving](#serving)). A protocol that routes on its payload (the first gRPC message) sends the hint as well, or is served as shardless and resolved by the app.

### Kinds clients can name

A client that names a shard reaches its activation before any app code runs. Clients can't create shards, but they can activate or restore any existing shard of a kind they can name, which costs blob storage requests even when the app then rejects the request. An id that doesn't exist costs one blob read and gets the same `404` as a kind that isn't `public`.

- **Make a kind `public` only if clients need to name it,** usually the player's own shard and anything they hold a long-lived connection to. Every other kind is reached through calls from the app after it has authenticated the request.
- **Give kinds clients can name unguessable ids** (random UUIDs), so ids can't be used to force restores or find out which exist. A kind with guessable ids, such as an index keyed by email, must stay non-public.
- **Keep hot shared shards behind the app.** A shard many clients can reach sends all their requests to one node and one writer. Split it and put caching or rate limiting in front of it (see [Recommendations](#recommendations-for-app-code)).

Rate limiting at the load balancer is still the defence against a flood.

### Shardless requests that need a shard

A shardless route is an ordinary app route. When its request belongs to a shard (a login, which has to reach the player's account, see [Logging in](#logging-in)), the app:

- **calls the shard** with `Shardlite-Target: users/42`, passing on the client's credentials so the shard authenticates as usual (this works where a redirect doesn't), or
- **tells the client where the shard is:** a URL that names it (`/login?shardlite-target=users/42`) or a `307` to it. WebSocket handshakes can't follow redirects, so they get the URL in an ordinary response.

Call a new shard before redirecting to it, since a client can only reach a shard that exists. For ids the client chooses, a shardless handler calls the shard's init route, which records the creator, answers success to the creator on a retry and `409` to anyone else. Rate-limit that route per user, since each new id it accepts is a new shard in storage. Keep these decisions stable across releases and change one over two releases (first make the new target handle requests sent to the old one, then switch).

### Logging in

Players log in with a provider (Steam, PSN, a username and password) and reach one account however they logged in:

1. **A kind per provider, split at least 64 ways** (`steam-ids/<hash % 64>`, non-public). Each row maps an identity to an account id, and a provider's own columns (a password hash) live there.
2. **Login** looks up the row, creating it with a new UUID if it's missing, then calls the account (`users/<uuid>`) with the identity.
3. **The account adds** any identity it's called with to its own list.
4. **Linking and relinking** change the row (linking only if it isn't linked yet), then call the new account as in login.
5. **Before the account relies on its list** (to show it, unlink or delete), it drops entries whose row no longer points at it.

The rows are the truth and the account's list is a cache that logins refill, so nothing has to keep them in step.

### Connections and home shards

A WebSocket connection is tied to one shard, its client's **home shard** (normally the player shard), and the app calls other shards through the control API. A client that needs a stream to another shard opens a second connection. An open connection (WebSocket, server-sent events, streaming gRPC) keeps its shard active, so authenticate in the handshake (a cookie, or a token in the query string next to `shardlite-target`) and a rejected client gets `401` without holding the shard. The app can close a connection to let the shard deactivate.

### Creating shards

The app creates shards and clients reach them. Any id can be called and the first call creates the shard, empty, before the app sees it. A request a client routes to a shard that doesn't exist gets the same `404` as an undeclared kind.

When a shard deactivates holding nothing of the app's (no table, index, `user_version` or `application_id`), the node deletes it, and a client request for it gets `404` until the app calls it again. The first open migrates the schema, so only an app with no migrations yet leaves an empty shard. Clients need one rule: on `404`, go back through the app rather than retry the same target.

## Durability

Each kind sets `"sync"` in its declaration: `"none"` or `"storage"` (the default). Every commit is shipped to blob storage whatever the tier. The setting only decides where acknowledgements wait. Any other name is refused: a declaration naming it makes the node report not ready, and a `Shardlite-Sync` header naming it is a `400`.

| Tier | Waits for | A crash of the node | Loss of the host | Like |
|---|---|---|---|---|
| `none` | nowhere | can lose the commits not yet in storage, up to a flush interval and a PUT | can lose the same, and every commit storage does not have yet | Postgres with `synchronous_commit = off` |
| `storage` (the default) | blob storage | covered | covered | Postgres with WAL shipping made synchronous |

`none` adds no latency, and is right for output that can be rebuilt or lost (presence, positions, chat). `storage` adds up to [`SHARDLITE_FLUSH_MIN`](deployment.md#environment-variables) plus one PUT, shared by the answers waiting: roughly 50–200 ms on S3 Standard. Lowering `SHARDLITE_FLUSH_MIN` makes it faster at the cost of more journal PUTs.

Nothing leaves a shard before what it committed earlier met its tier. The node holds responses, streams and calls to other shards alike (a `wallet` shard can't debit gold, call `inventory/42` to grant an item and then lose the debit in a crash). Output from a shard with nothing new to commit passes without waiting on `none`. Every chunk of a stream can wait for its tier, pings included, so real-time kinds should use `none`. The app can override its kind's tier for one response or call with `Shardlite-Sync: none` or `storage`.

`POST /_shardlite/sync` on the control API waits until everything committed on the node is in blob storage and the node has confirmed that it still owns its shards, and fails if it has lost its lease. A process that reads, calls `sync` and then answers its own client is linearizable. It is for side effects shardlite can't see: publishing to Kafka and external HTTP calls.

### Consistency

Each shard has one owner, which serves every request for it, and SQLite orders its commits.

| | Writes | Reads |
|---|---|---|
| `none` | acknowledged a flush and a PUT before they are in storage: a crash of the node can lose them | linearizable, given working clocks, and after a crash can miss the last flush interval |
| `storage` | linearizable and durable once acknowledged | linearizable under any clocks and pauses, like etcd's default reads and Consul's `consistent` reads |

On `storage` a shard behaves as one copy of the database: once a write is acknowledged, every read that starts afterwards sees it, on whichever node the request lands. `none` promises the same only until the node crashes.

A `storage` answer is confirmed before it leaves: the node checks that nobody has begun to take the shard over since the answer was read, which costs an idle node one blob GET and a busy node nothing. A request is confirmed once, at its first chunk, so a stream is linearizable at its start and then follows the node's copy in order (how is in [internals](internals.md#confirming-ownership)). If the lease has been taken the node stops, and if it cannot be read the answer fails with `503` and a retry hint.

"Working clocks" applies to `none`, which answers without confirming. It means clock rates on different nodes agree to within 20% (they need not be synchronised) and no machine is paused in a way its own clocks don't see, which a hypervisor can do. A hot read endpoint that can live with that sends `Shardlite-Sync: none` on its response to skip the confirmation. The node warns when its clock and the store's differ by over a second, but nothing checks the rate and no answer fails on the clock.

The guarantees hold for what goes through the node and what the app reads from the shard's database. An app that answers from its own in-memory copy can serve something older: keep such a cache no longer than an open connection and check `PRAGMA data_version` before using it. Across shards there are no transactions: two reads of two shards are two separate reads, not a snapshot. Calls happen at most once per attempt, and a [durable send](#durable-send) at least once.

## Localhost control API

The app talks to its node over HTTP on localhost TCP, on a separate listener from the public one, so apps may send `Shardlite-*` headers to it. Most of the API is headers on the app's normal requests: a request with `Shardlite-Target` is a [call](#calls-between-shards). The rest is in the [reference](#control-api).

## Using the SDK

Two SDKs cover the part of this contract that is the same in every app: the Go SDK at `sdk/go` (standard library only) and the TypeScript SDK at `sdk/ts` (no runtime dependencies, so it runs on Node, Bun and Deno). Each one's guide is its own documentation: [sdk/go/doc.go](../sdk/go/doc.go) and [sdk/ts/README.md](../sdk/ts/README.md). The app brings its own SQLite driver, and routing, migrations and retries stay the app's own. Everything stays plain HTTP and SQL, so an app without an SDK works the same.

Both have one shape. An app builds one value from a config (the opener, the kinds, the version) and serves the declaration from it. A route that belongs to a shard calls `Expect` (`expect`) to check what the node opened, or `ExpectCall` for a route that only takes calls and for the wake, and gets the shard's `Opened` or a refusal to answer with `400`. Calls are made on the request's scope, so they carry its context. An app is tested against a real node: `shardlitetest.Start` in Go and `startNode` in TypeScript start one on `memory://`.

## Deleting shards

A shard deletes itself. When it deactivates marked for deletion, its data goes, and a later call for the same id creates a new, empty shard that never sees the old data. The node creates the `_shardlite_delete` table (see [Reserved tables](#reserved-tables)), and a row in it marks the shard: `INSERT OR IGNORE INTO _shardlite_delete VALUES (1)` in any transaction, which the SDK's `MarkDeleted` runs. Deleting the row unmarks it. Its due reminders are handled first, and later ones go with it. Until deactivation the shard is served as usual, so the app can check for the row and refuse requests. Uses: matches, trades and lobbies that end.

No call deletes a shard from outside, since a delete would race with requests and the app's open files. For a deletion decided outside the shard (account deletion, say), add a route to the shard that finishes its work and marks the shard. The shard is deleted within its idle timeout. Deleting a shard that isn't active is left to the operator, and no command does it yet.

## Calls between shards

| | Calls | Durable send (reminders and calls) |
|---|---|---|
| Shape | synchronous request and response | a call made by the sender's wake handler, with the sender's commit behind it |
| Delivery | at most once per attempt, retried by the caller | at least once, with the receiver skipping repeats |
| Tied to the caller's commit | no | yes, recorded in the same transaction |
| For | answers needed now: reads, queries, checks | changes that must happen |

The app always calls its local node, which routes. A shard call is just another HTTP route in the app:

```
app (users/42) → POST localhost:2282/invite
                 Shardlite-Target: guild/7
                 node: who owns guild/7?
                   local  → POST app/invite with Shardlite-* for guild/7
                   remote → forward to the owner node → its app/invite
```

- **The target is explicit.** The path, method and body pass through unchanged, so this works for REST, GraphQL or gRPC. A call from a shard can't be upgraded to a WebSocket (`400`), so use server-sent events or a gRPC stream for subscriptions. A local call costs about 0.1 ms and a remote one adds 0.5–2 ms.
- **The context goes with the call.** The app passes `Shardlite-Context` and `traceparent` back unchanged on every call it makes while serving a request. The node takes the call's caller and hops from the context. A call without one is from the app (no shard behind it, and it can upgrade to a WebSocket).
- **The callee knows the caller.** The node sets `Shardlite-Caller: users/42` on the request it sends the callee, or `app` for a call from the app. The node strips the header from external requests, so it grants access to internal routes, which require it. A route that only takes calls may take its identity from `Shardlite-Opened`. A route clients can reach authenticates every request and works out the shard from the request, whoever sends it.
- **Gates apply on both sides.** The caller's gate holds the call until the caller's kind's tier has its commits, and the callee's holds the answer.
- **Calls activate and create the target,** unless the call sends `Shardlite-If-Active: true` (answer `404` rather than activate, for presence checks and fan-out to members who may be offline) or `Shardlite-If-Exists: true` (answer `404` rather than create). Use `Shardlite-If-Exists` for any call whose id came from a client, so random ids don't create shards through app code.
- **Hops are limited.** The node adds one hop on each call and refuses past [`SHARDLITE_MAX_HOPS`](deployment.md#environment-variables) with `508`, which stops runaway recursion.
- **Cancellation propagates.** When a client disconnects or a caller gives up, the node cancels the request on every hop it controls (writes already committed are not rolled back). Timeouts are the app's, set on its own HTTP or gRPC client.
- **The node never retries a request the app may have seen,** so calls that change state should carry an idempotency key.

**gRPC** works in both directions: the app adds `shardlite-target` and `shardlite-context` metadata (usually with a client interceptor) and the node proxies HTTP/2 without TLS, trailers and streams as they are. Its own errors are gRPC status responses. It never parses messages and doesn't read `grpc-timeout`.

**Reaching many shards.** For guild chat or a group invite, call each member's shard in parallel with `Shardlite-If-Active: true` so offline members aren't woken.

## Reminders

A reminder is a row a shard sets for itself, with the time it is due: an upgrade finishing, an auction closing, a lobby that must end even if nobody is connected. The node wakes the shard's app when the earliest one is due, as a Durable Objects alarm does. The node creates the table at activation:

```sql
_shardlite_reminders (key TEXT PRIMARY KEY, due INTEGER NOT NULL) STRICT  -- due in Unix seconds
```

- **The rows are the app's.** It inserts, reschedules and deletes them with plain SQL in its own transactions (`INSERT … ON CONFLICT (key) DO UPDATE SET due = excluded.due`, and `DELETE` to cancel). Anything a reminder needs lives in the app's own tables under the same key, so the reminder and its data commit together.
- **The wake.** When the earliest due time has come, the node calls `POST /_shardlite/reminders` on the shard's app (no body, with `Shardlite-Opened`, `Shardlite-Context` and `Shardlite-Caller` naming the shard) and gives it 30 seconds to answer. The handler takes its identity from `Shardlite-Opened`, and a call it makes carries the wake's context, so it is from the shard. A wake never acts on a commit that could still be lost.
- **The handler** selects `WHERE due <= unixepoch()` and handles each row in its own transaction, so a row's work and its deletion (or next due time) commit together. It answers `2xx` once everything due is done, and `503` if a row can't be finished.
- **Retries.** Any other status, no answer in time or rows still due after a `2xx` is a failure, and the next wake waits one second, then twice as long for each failure in a row up to a minute.
- **Stale reminders.** A reminder due more than a day ago is ignored: it doesn't wake the shard or keep it active. It stays in the table for the app to clean up (`DELETE FROM _shardlite_reminders WHERE due < unixepoch() - 86400`).
- **Repeats are the app's.** A handler that wants a recurring reminder sets the next `due` in the same transaction as its work. Resolution is a second.
- **Timing.** A shard that is active is woken within about a second of its time. An inactive one is woken by the cluster, usually within a few seconds of the minute the reminder is due in, later when many are due at once. Rely on the minute only. A reminder due now keeps the shard active until its handler answers.

### Durable send

A call made after a commit is lost if the node crashes in between, and a call made before it can announce a write that is later lost. A shard that must make a call exactly when its own transaction commits uses a reminder as the trigger and its own table to hold the call. The sender records both in the transaction that causes the send (the key is unique per send and the same on every attempt):

```sql
CREATE TABLE sends (key TEXT PRIMARY KEY, target TEXT NOT NULL, path TEXT NOT NULL, body BLOB) STRICT;

BEGIN IMMEDIATE;
  -- the work that causes the send
  INSERT INTO sends (key, target, path, body) VALUES (:key, 'players/42', '/rewards', :body);
  INSERT INTO _shardlite_reminders (key, due) VALUES (:key, unixepoch());
COMMIT;
```

The wake handler makes each call with the key as `Idempotency-Key`, and deletes both rows once the call answered `2xx`:

```
on POST /_shardlite/reminders:
  failed = false
  for each (key, target, path, body) in
      SELECT s.* FROM sends s JOIN _shardlite_reminders r USING (key) WHERE r.due <= unixepoch():
    status = call(target, POST path, body, Idempotency-Key: key)
    if status is 2xx:
      BEGIN; DELETE FROM sends WHERE key = :key;
             DELETE FROM _shardlite_reminders WHERE key = :key; COMMIT
    else:
      failed = true            -- leave both rows, the node wakes the handler again
  answer 503 if failed, else 200
```

The receiver records `(Shardlite-Caller, Idempotency-Key)` in the same transaction as its work and skips work it has seen: it inserts into `seen (caller, key, at)` with `ON CONFLICT DO NOTHING` and, if no row was inserted, commits and answers `200` without doing the work. Entries older than a day can be dropped, since a sender stops trying once its reminder is stale.

- **At least once, with the work done once.** A failed call is made again at the next wake. A lost acknowledgement makes the call twice, and the receiver's record makes the work happen once.
- **Calls that can never succeed** (a `4xx` that retrying won't change) should be dropped or marked failed by the handler, or they are retried until the reminder goes stale.
- **No ordering.** An app that needs order adds a sequence number per sender and receiver, and the receiver answers `503` to a number that isn't next.
- **One handler serves every reminder the app sets.** It looks at which of the app's tables has a row under each due key.

Uses: fan-out in one transaction (rewards for 100 players at the end of a match), transfers and trades by custody, and calls that must follow a commit.

## Rules for app code

These are part of the contract with shardlite, beyond [what an app must do](#what-an-app-must-do). Apps that break them stall or fail.

- **Requests for the same shard run concurrently,** as on any database, and the shard's transactions are the concurrency control. Do a read and the write that depends on it in one transaction (`BEGIN IMMEDIATE`) or one statement (`UPDATE … SET balance = balance - 10 WHERE balance >= 10`). State kept in app memory per shard isn't protected by SQLite. Orleans and Durable Objects run one turn at a time, and an app that expects that loses updates here.
- **Start write transactions with `BEGIN IMMEDIATE`, or retry them.** A deferred transaction that reads and then writes fails with `SQLITE_BUSY` if another connection committed in between, and `busy_timeout` doesn't retry that case.
- **Handle concurrent requests, including calls back into the app itself.** A call to a shard on the same node arrives at the same app as a new request while the calling request waits. A single-threaded synchronous server deadlocks, and thread pools can run out when calls nest, so async servers are the safer choice.
- **Don't hold a write transaction while calling another shard.** If `users/42` is mid-transaction, calls `guild/7` and the guild calls back, the callback can't get the write lock. Call first and then write, or commit and then call.
- **Reconnect whenever a connection closes.** A WebSocket or stream can end at any time (a node restart or drain ends every connection of a shard at the stop, or the app closes it at a natural break so that its shard can move), and the close code says nothing more.
- **Expect calls to fail and set timeouts.** A call can arrive while its target is activating or moving, and its node can fail. Retry where it makes sense.
- **Don't skip authentication because a request arrived as a call,** and don't answer from the open file for another shard's request.

## Recommendations for app code

- **Prefer one shard for a player's data.** Adding tables to the player shard usually beats a kind per feature (`inventory/42` and `quests/42` beside `users/42`): one shard gives one transaction across all of it, one restore at login and no calls between the parts, while kinds that split one player can land on different nodes and pay a forward for every call between them. A good exception is something large that most requests don't need (a long chat history). What isn't the player's alone (a trade, a guild, a match) has shards of its own anyway.
- **Choose ids that need no lookup.** A client can choose a new shard's id (a UUIDv7), which makes creating it idempotent. An id built from other data, such as a trade between two players, can be a name-based UUID (v5) of that data.
- **Acknowledge a write that must not be lost on a `none` kind with `Shardlite-Sync: storage`** on that response (the SDKs' `SetSync` and `syncHeader`). Call `sync` right before a side effect the node cannot see (a queue, an external call). Use a durable send for calls that must happen.
- **Drive time-based work from requests.** Energy, cooldowns, deadlines and daily resets can be worked out from timestamps whenever a request touches the shard, so a player who never returns costs nothing. Set a reminder only for things that must happen on time while nobody is connected, and spread reminders that many shards share (add a random delay of a few minutes).
- **Warm shards a player is about to use** by calling them in the background at login or matchmaking: a cheap route, with `Shardlite-If-Exists: true` so a missing one isn't created, and a cap on how many one request warms.
- **Keep indexes in index shards, updated by durable sends** (`lobbies-index/eu`). Handlers upsert, and readers skip entries older than a timeout.
- **Delete short-lived shards with the marker,** and a reminder for when an abandoned one should be gone at the latest.
- **Never give shared state a single shard id.** Split anything many players write (a leaderboard, a queue, a global counter, the [login tables](#logging-in)) at least 64 ways from the start, by meaning first (season, region, mode) and then by hash, with an aggregator shard the buckets report to by durable send. An unused bucket costs nothing, while splitting later is a migration. Cache shared data (a message of the day, a guild's member list) in the app rather than reading its shard every time.
- **Publish by recording events** in the topic's shard with a sequence number: connected clients get them pushed, and subscribers catch up when next touched.
- **Keep large data in object storage.** Upload it under a unique name, then commit the reference in the shard. A crash in between leaves an orphan, never a missing object.

## A single writer

Some processes hold a shard's state in memory for a long time and write it back: a game server running a survival world or an MMO realm, a client syncing a cloud save, a worker that owns a job. Two of them must never both write the same shard, and whatever starts them can't promise that, since a process it thinks is dead can still be running when its replacement starts. The shard fences them itself with an **epoch**, the same idea the node uses for its own journal:

- **Open.** The process's first call (`POST /open` on the shard, say) adds one to an epoch the shard keeps in a table of its own and answers it. From then on only the newest process to open the shard can change it.
- **Save.** Every write carries the epoch, and the shard refuses one that isn't current (`409`, say). An older process finds out at its next save and stops. Its players may see a moment of rollback, and nothing it did after the newer open is kept.
- **Nothing else.** There is no lease to renew and no close. The next open fences the old writer whether it closed or not. Which process runs a world and where its players connect is for whatever allocates them to answer.

Keep the kind on `storage` (the default): an open lost in a crash could hand the same epoch to two processes.

Keep one save in flight and coalesce the rest, as game servers already do: while one waits for its answer, the next gathers every change since. That keeps saves in order without sequence numbers, and the answer to the save that included a change is the signal that it is stored. The process keeps running while it waits, and waits only where a player is told something happened (a trade, loot).

### Showing it to players

While a world runs, its players can ask its process. Other requests for it call it with `Shardlite-If-Active: true`, which answers `404` instead of activating it, so a large hibernated world isn't restored to answer one. Keep the kind out of `public`, and have the world publish what players need to see while it is hibernated by calls to other shards, such as a character summary in the player's shard.

## Running shardlite

How to run the node, its hosts, storage, load balancer and settings are in [deployment](deployment.md).

## Reference

### Shard names

A shard is named `kind/id`. A kind's name is one or more segments joined by dots, each a lowercase letter followed by lowercase letters, digits, `-` and `_`, up to 64 characters in all. A dot is an ordinary character that the node never splits at, so a library can name its kinds `matchmaker.queue` by convention. A name starting with `_` is the node's own and an app can't declare one. Ids are 1 to 128 ASCII letters, digits, `-` and `_`, enough for numbers, UUIDs and ULIDs. Anything else gets a `400`.

### Headers

All shardlite headers use the `Shardlite-` prefix. Clients can send `Shardlite-Target`, and every other `Shardlite-*` header from clients is stripped (also with `_` for `-`, as in `Shardlite_Caller`, which CGI-style frameworks read as the same variable). Apps should pass on `traceparent` and `Shardlite-Context` on calls.

| Header | Set by | Meaning |
|---|---|---|
| `Shardlite-Target` | client (a hint), app on calls, node between nodes | the shard to route to |
| `Shardlite-Opened` | node to app | the shard the node opened for this request |
| `Shardlite-File` | node to app | the opened shard's file, a path that changes whenever the shard gets a new file (open it with `mode=rw`) |
| `Shardlite-Context` | node to app, app on calls, node between nodes | a Structured Fields dictionary (RFC 9651) of `shard` (the opened shard) and `hops`. The app passes it back unchanged on calls, and a call without one is from the app. Unknown keys are ignored |
| `Shardlite-Caller` | node to app | the calling shard, or `app`, as the node read it from the call's context |
| `Shardlite-If-Active` | app on calls | `true`: answer `404` rather than activate |
| `Shardlite-If-Exists` | app on calls | `true`: answer `404` rather than create |
| `Shardlite-Sync` | app on a response or call | `none` or `storage`, overriding the kind's tier once |
| `Shardlite-Error` | node on its own errors | why the node answered with an error, a reason from the [status codes](#status-codes) table. The node strips every `Shardlite-*` header from the app's responses, so an error without it is the app's |

### Control API

On [`SHARDLITE_LISTEN_CONTROL`](deployment.md#environment-variables) (localhost TCP):

| Request | Meaning |
|---|---|
| `ANY /{path}` with `Shardlite-Target` | call `{path}` on the shard's owner |
| `POST /_shardlite/sync` | wait until everything committed before the call is in blob storage (fails if the node has lost its lease) |

The node polls the app's `GET /_shardlite/ready` and calls `POST /_shardlite/reminders`. The public listener serves `GET /_shardlite/ready` to the load balancer, answering `503` while the app's declaration is missing, the lease is not good or the node drains. A path under `/_shardlite/` that a listener doesn't serve itself is refused with `404`.

### Status codes

Errors the node generates itself (sent as gRPC status for gRPC requests) carry `Shardlite-Error: {reason}`, as a header (beside `grpc-status` for gRPC). An error without it is the app's, whatever its status. An error from the owner's node passes through the node that forwarded the request with the owner's reason.

| Code | Reason | When |
|---|---|---|
| `400` | `bad-request` | a malformed shard name or `Shardlite-*` header (a `Shardlite-Context` that doesn't parse, lacks `shard` or `hops`, or names a malformed shard), or a call the node refuses as made |
| `404` | `unknown-shard` | an undeclared kind, a kind that isn't `public` named by a client, a client request for a shard that doesn't exist, or `Shardlite-If-Exists: true` on a missing one |
| `404` | `not-active` | `Shardlite-If-Active: true` on an inactive shard |
| `404` | `no-endpoint` | a path under `/_shardlite/` the node doesn't serve |
| `502` | `app-unreachable` | the node's connection to the app could not be made, so the app never had the request and it is safe to retry (`Retry-After: 1`) |
| `502` | `app-failed` | the request was sent to the app and the connection failed, so the app may have acted on it (no `Retry-After`) |
| `502` | `owner-failed` | the same, for a request forwarded to the owner's node after it acknowledged it |
| `502` | `internal` | any other failure of the node itself, such as storage failing |
| `503` | `owner-unreachable` | the owner doesn't answer, or is dead and not adopted yet (`Retry-After: 1`) |
| `503` | `moved` | the shard was released or moved since the node last forwarded to it, or was claimed by another node at the same moment (`Retry-After: 0`: try again at once) |
| `503` | `draining` | a stopping node that cannot place the request elsewhere |
| `503` | `lease-lost` | the node's lease is lost or expired |
| `503` | `bad-file` | the shard's files failed their checksums |
| `503` | `sync-unmet` | the node cannot meet the durability tier asked for |
| `503` | `not-ready` | the readiness check, while the app's declaration is missing, the lease is not good or the node drains |
| `504` | `timeout` | a timeout inside the node ended the request |
| `508` | `hops` | hops past [`SHARDLITE_MAX_HOPS`](deployment.md#environment-variables) |

Between nodes a forward for a shard the receiving node found isn't its own is answered `421` with `not-owner`, which the forwarding node turns into `503` with `moved`.

### Declaration

`"version"` is the app's own, an opaque string, optional for now. A field the node doesn't know makes the declaration invalid. Each kind under `"kinds"` takes:

| Field | Default | Meaning |
|---|---|---|
| `sync` | `"storage"` | `"storage"` or `"none"` (see [Durability](#durability)) |
| `idle_timeout` | [`SHARDLITE_IDLE_TIMEOUT`](deployment.md#environment-variables) | how long a shard stays active without requests, as a Go duration (`"10m"`) |
| `public` | `false` | clients may name the kind |

### Environment variables

The settings are listed in [deployment.md's table](deployment.md#environment-variables).

### Reserved tables

The node creates `_shardlite_reminders` and `_shardlite_delete` at activation and writes nothing else. A shard whose only content is these tables (and the `sqlite_sequence` that `AUTOINCREMENT` adds), with no rows in shardlite's, counts as never written. A row in `_shardlite_delete` marks the shard for deletion (see [Deleting shards](#deleting-shards)).

```sql
CREATE TABLE _shardlite_reminders (
  key TEXT PRIMARY KEY,  -- the app's name for the reminder: 'upgrade:barracks'
  due INTEGER NOT NULL   -- Unix seconds
) STRICT;
CREATE INDEX _shardlite_reminders_due ON _shardlite_reminders (due);
CREATE TABLE _shardlite_delete (
  marked INTEGER PRIMARY KEY CHECK (marked = 1)  -- a row, the only one there can be, marks the shard
) STRICT;
```

### The dispatcher

Every node also does a share of the cluster's own work: the dispatcher wakes the shards whose reminders are due while they are inactive. It needs no setting and serves no route. How it works is in the internals' [dispatcher](internals.md#the-dispatcher).
