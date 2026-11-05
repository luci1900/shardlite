# shardlite guide

For people who build apps on shardlite and people who run it. The [overview](overview.md) introduces the concepts, and the [reference](#reference) at the end lists every header, endpoint, setting and reserved table. How the node does what it does is in [internals](internals.md).

## Serving

The node routes requests and calls for any shard of a kind the app serves. Any id can be called and the first call creates the shard (see [Creating shards](#creating-shards)). A process that keeps a shard's state in memory and serves its clients itself, over any protocol, is a [single writer](#a-single-writer) of that shard: a game server holding a world, for example.

The node sits in front of the app. For each request it works out the shard (see [Routing requests to shards](#routing-requests-to-shards)), activates it if needed and forwards to the app with:

```
Shardlite-Opened: game.users/42
Shardlite-File:   /data/game.users/42.k3x9.db
```

`Shardlite-Opened` is the shard the node opened for this request. It says what the node did, not what the request is for. The app works out which shard a request is for from the request itself (the path, the payload, the first gRPC message). If that is the opened shard, the app uses the open file. If not, or if the node opened nothing, the app **relays** the request to the right shard through the [control API](#localhost-control-api) with `Shardlite-Relay: true` and no `Shardlite-Caller`, passing on the client's credentials and `X-Forwarded-*` headers, and streams the answer back (a WebSocket too). A wrong or missing hint from a client therefore costs a hop and never correctness. The node serves a relay as the client's request it is, with a client's limits: only kinds clients can name, never creating a shard, no caller, and hops counted as a call's are.

The proxy handles HTTP, gRPC, server-sent events and WebSocket, passing requests through unchanged apart from `Shardlite-*` headers, which the node strips from client requests and app responses. Client requests for paths under `/_shardlite/` are refused, except the node's own `GET /_shardlite/ready`. Custom UDP, WebRTC and raw TCP go direct to the app's own servers, such as a game server that is a [single writer](#a-single-writer) of its world.

### What an app must do

The app and node share a local filesystem on one kernel (the same host, with a shared volume in containers). Network filesystems are not supported, since they break SQLite locking. The app:

- opens `Shardlite-File` with any SQLite driver of 3.37.0 (2021) or later (the reserved tables are STRICT, which older versions cannot open), and never builds paths itself;
- uses WAL mode with default locking (not `locking_mode=EXCLUSIVE` or a VFS without shared memory);
- only runs passive checkpoints, as SQLite's automatic ones are. `FULL`, `RESTART` and `TRUNCATE` always find the shard busy, since the node holds a read transaction on it;
- closes its connections to a shard within the kind's idle timeout of last using it (for example a pool with `SetConnMaxIdleTime` in Go). Setting `idle_timeout` on every kind and giving the pool the value from the declaration the app serves keeps the two in step. The node lets go of a shard only once no other process has it open, so a longer-held connection keeps it active and blocks moving it, and after a node restart a shard the app still holds is unavailable until the app closes it;
- treats tables whose names start with `_shardlite_` as reserved (see [Reserved tables](#reserved-tables));
- serves `GET /_shardlite/ready` with its declaration (see [Shard kinds](#shard-kinds)), and `POST /_shardlite/reminders` if it sets reminders;
- follows the [rules for app code](#rules-for-app-code).

### Schema migrations

Migrations belong to the app, with whatever tooling it already uses. The node knows nothing about schemas. Recommendations:

- Check `PRAGMA user_version` when opening a shard and migrate if needed. Migrations are lazy, so only shards opened since a deploy are on the new version. Refuse shards newer than the app can read.
- Set `PRAGMA application_id` in the first migration to a hash of the app's identifier and the kind, and check it on open, so a shard is never opened as the wrong kind.
- Make the tables STRICT, so the save format refuses what it cannot read back.
- Prefer expand and contract, so a rollback is redeploying the previous release. For changes that can't be additive, ship down migrations too.

### Shard kinds

The app declares itself at `GET /_shardlite/ready`, which answers a JSON declaration naming the app, its version and its kinds:

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

The node polls the route, and a 2xx answer that parses as a valid declaration is both the app's health check and the source of its kinds. Keep the declaration in a file in the app's repository and serve it from the route: it is a contract between the app and the cluster, and its diff belongs in version control. The route is under `/_shardlite/`, which the node refuses to clients, so the declaration (which lists kinds clients may not name) reaches only the node. The fields are in the [reference](#declaration).

- The app's name is part of every stored path, so it is chosen once: renaming an app moves its data. A name starting with `_` is the node's own (see [System kinds](#system-kinds)). The version is an opaque string the node logs and does nothing else with.
- A node with `SHARDLITE_APP` serves every kind the app declares. One without serves only the [system kinds](#system-kinds) and answers requests for any other kind as undeclared.
- Clients can only name kinds marked `public`. A client that names any other kind gets the same answer as for an undeclared kind, so it can't tell the kind exists. The node logs why, so a kind that should be `public` is easy to spot in development.
- Requests and calls for undeclared kinds are refused, which catches typos. A kind with no app in its name (`users/42`) is a malformed name and gets `400`.
- A changed declaration takes effect at the next poll, for new activations and responses. A poll that fails changes nothing: the node keeps the last good kinds and reports not ready until a good declaration comes.
- The node reports ready (`GET /_shardlite/ready` on the public listener, which the load balancer polls) only while the app's declaration is current and its lease is good, so a bad deploy stops at the first node.

## Routing requests to shards

Routing is never authorisation. It only decides which shard the node opens for a request. The app checks credentials however it likes, and the node never inspects them.

The node takes the shard a client names from fixed conventions, in this order:

1. **Header:** `Shardlite-Target: game.guildchat/7`, the one `Shardlite-*` header the node accepts from clients (gRPC clients send it as metadata).
2. **Query parameter:** `?shardlite-target=game.guildchat/7`, which the node strips. This is the convention for browser WebSockets, which can't set headers.
3. **No shard.** The node is a plain reverse proxy: login, news, store catalogues and health checks.

The conventions are fixed, not configured per kind or app, so old and new app versions never send the same URL to different shards during a rolling deploy. All three are hints that save a hop. A right hint reaches the owner with the file open. A wrong one costs a hop and an activation of the shard it names, since the node routes by the hint before any app code runs.

### Kinds clients can name

A client that names a shard reaches its activation before any app code runs. Clients can't create shards, but they can activate or restore any existing shard of a kind they can name, which costs blob storage requests even when the app then rejects the request. An id that doesn't exist costs one blob read, and gets the same `404` as a kind that isn't `public`. Recommendations:

- **Make a kind `public` only if clients need to name it,** usually the player's own shard and anything they hold a long-lived connection to. Every other kind is reached through calls from the app after it has authenticated the request.
- **Give kinds clients can name unguessable ids** (random UUIDs), so ids can't be used to force restores, push shards out of the cache or find out which exist. A kind with guessable ids, such as an index keyed by email, must stay non-public.
- **Keep hot shared shards behind the app.** A shard many clients can reach sends all their requests to one node and one writer. Split it from the start and put caching or rate limiting in front of it (see [Recommendations](#recommendations-for-app-code)).

Rate limiting at the load balancer is still the defence against a flood.

### Shardless requests that need a shard

A shardless route is an ordinary app route. When its request belongs to a shard (a login, which has to reach the player's account, see [Logging in](#logging-in)), the app:

- **calls the shard** with `Shardlite-Target: game.users/42`, passing on the client's credentials so the shard authenticates as usual (this works where a redirect doesn't), or
- **tells the client where the shard is:** a URL that names it (`/login?shardlite-target=game.users/42`) or a `307` to it. WebSocket handshakes can't follow redirects, so they get the URL in an ordinary response.

Call a new shard before redirecting to it, since a client can only reach a shard that exists. For ids the client chooses, the client picks the id and a shardless handler calls the shard's init route, which records the creator, answers success to the creator on a retry and `409` to anyone else. Rate-limit that route per user, since each new id it accepts is a new shard in storage. These decisions are app code, so old and new versions can disagree: keep them stable across releases and change one over two releases (first make the new target handle requests sent to the old one, then switch).

### Logging in

Players log in with a provider (Steam, PSN, a username and password) and reach one account however they logged in, as a centralised database would do with an accounts table and a table per provider:

1. **A kind per provider, split at least 64 ways** (`game.steam-ids/<hash % 64>`, non-public). Each row maps an identity to an account id, and a provider's own columns (a password hash) live there.
2. **Login** looks up the row, creating it with a new UUID if it's missing (or after asking the player, if the game does), then calls the account (`game.users/<uuid>`) with the identity.
3. **The account adds** any identity it's called with to its own list.
4. **Linking and relinking** change the row (linking only if it isn't linked yet), then call the new account as in login.
5. **Before the account relies on its list** (to show it, unlink or delete), it drops entries whose row no longer points at it.

The rows are the truth and the account's list is a cache that logins refill, so nothing has to keep them in step. Whether to ask before creating, link codes, merges and a website for relinking are the game's choices on top.

### Connections and home shards

A WebSocket connection is tied to one shard, its client's **home shard** (normally the player shard), and the app calls other shards through the control API. A client that needs a stream to another shard opens a second connection. An open connection (WebSocket, server-sent events, streaming gRPC) keeps its shard active, so authenticate in the handshake (a cookie, or a token in the query string next to `shardlite-target`) and a rejected client gets `401` without holding the shard. The app can close a connection to let the shard deactivate.

### Creating shards

The app creates shards and clients reach them. Any id can be called and the first call creates the shard, empty, before the app sees it. A request a client routes to a shard that doesn't exist gets the same `404` as an undeclared kind.

When a shard deactivates holding nothing of the app's (no table, index, `user_version` or `application_id`), the node deletes it, and a client request for it gets `404` until the app calls it again. The first open migrates the schema, so only an app with no migrations yet leaves an empty shard (the node logs the first such deletion of each kind). Clients need one rule: on `404`, go back through the app rather than retry the same target.

## Durability

Each kind sets `"sync"` in its declaration: `"none"` or `"storage"` (the default). Every commit is shipped to blob storage whatever the tier. The setting only decides where acknowledgements wait. Any other name is refused: a declaration naming it makes the node report not ready, and a `Shardlite-Sync` header naming it is a `400`.

| Tier | Waits for | A crash of the node | Loss of the host | Like |
|---|---|---|---|---|
| `none` | nowhere | can lose the commits not yet in storage, up to a flush interval and a PUT | can lose the same, and every commit storage does not have yet | Postgres with `synchronous_commit = off` |
| `storage` (the default) | blob storage | covered | covered | Postgres with WAL shipping made synchronous |

`none` adds no latency, and is right for output that can be rebuilt or lost (presence, positions, chat). `storage` adds up to `SHARDLITE_FLUSH_MIN` plus one PUT, shared by the answers waiting: roughly 50–200 ms on S3 Standard. To make it faster, lower `SHARDLITE_FLUSH_MIN`: at 20 ms a busy node writes up to 50 journal PUTs a second while output waits, against 10 at the default.

Nothing leaves a shard before what it committed earlier met its tier. The node holds responses, streams and calls to other shards alike (a `wallet` shard can't debit gold, call `game.inventory/42` to grant an item and then lose the debit in a crash). Output from a shard with nothing new to commit passes without waiting on `none`. Every chunk of a stream can wait for its tier, pings included, so real-time kinds should use `none`. The app can override its kind's tier for one response or call with `Shardlite-Sync: none` or `storage`.

`POST /_shardlite/sync` on the control API waits until everything committed on the node is in blob storage and the node has confirmed that it still owns its shards, and fails if it has lost its lease. A process that reads, calls `sync` and then answers its own client is linearizable. It is for side effects shardlite can't see: publishing to Kafka and external HTTP calls.

### Consistency

Each shard has one owner, which serves every request for it, and SQLite orders its commits.

| | Writes | Reads |
|---|---|---|
| `none` | acknowledged a flush and a PUT before they are in storage: a crash of the node can lose them | linearizable, given working clocks, and after a crash can miss the last flush interval |
| `storage` | linearizable and durable once acknowledged | linearizable under any clocks and pauses, like etcd's default reads and Consul's `consistent` reads |

On `storage` a shard behaves as one copy of the database: once a write is acknowledged, every read that starts afterwards sees it, on whichever node the request lands. `none` promises the same only until the node crashes.

A `storage` answer is confirmed before it leaves: the node knows that nobody else has begun to take its shards since the answer was read. A write is confirmed by its own segment reaching storage. A read waits for a segment the node is writing anyway, or for one read of its own lease, shared by every answer waiting, so an idle node pays one blob GET (tens of milliseconds on S3) and a busy node nothing extra. A request is confirmed once, at its first chunk, so a stream is linearizable at its start and then follows the node's copy in order until the node notices it has lost the shard (within a lease renewal, 3 s by default). An app that needs the shard as it is now polls. If the lease has been taken the node stops, and if it cannot be read the answer fails with `503` and a retry hint.

"Working clocks" applies to `none`, which answers without confirming. It means clock rates on different nodes agree to within 20% (they need not be synchronised) and no machine is paused in a way its own clocks don't see, which a hypervisor can do. A hot read endpoint that can live with that sends `Shardlite-Sync: none` on its response to skip the confirmation. The node checks what it can: it compares its clock with the store's `Date` header, warns when the offset passes a second and refuses to start past the lease TTL, but nothing checks the rate and no answer fails on the clock.

The guarantees hold for what goes through the node and what the app reads from the shard's database. An app that answers from its own in-memory copy can serve something older: keep such a cache no longer than an open connection and check `PRAGMA data_version` before using it, or keep a version row that every write bumps. Across shards there are no transactions: two reads of two shards are two separate reads, not a snapshot. Calls happen at most once per attempt, and a [durable send](#durable-send) at least once.

## Localhost control API

The app talks to its node over HTTP on localhost TCP or a Unix socket, on a separate listener from the public one, so apps may send `Shardlite-*` headers to it. Most of the API is headers on the app's normal requests: a request with `Shardlite-Target` is a [call](#calls-between-shards), or a relayed client request if it also has `Shardlite-Relay: true`. The rest is in the [reference](#control-api).

## Deleting shards

A shard deletes itself. When it deactivates holding a table named `_shardlite_delete`, its data goes, and a later call for the same id creates a new, empty shard that never sees the old data. `CREATE TABLE _shardlite_delete (x)` in any transaction marks it. Its due reminders are handled first, and later ones go with it. Until deactivation the shard is served as usual, so the app can check for the table and refuse requests. Uses: matches, trades and lobbies that end.

No call deletes a shard from outside, since a delete would race with requests and the app's open files. For a deletion decided outside the shard (account deletion, say), add a route to the shard that finishes its work and creates the marker. The shard is deleted within its idle timeout. Deleting a shard that isn't active is left to the operator, and no command does it yet.

## Calls between shards

| | Calls | Durable send (reminders and calls) |
|---|---|---|
| Shape | synchronous request and response | a call made by the sender's wake handler, with the sender's commit behind it |
| Delivery | at most once per attempt, retried by the caller | at least once, with the receiver skipping repeats |
| Tied to the caller's commit | no | yes, recorded in the same transaction |
| For | answers needed now: reads, queries, checks | changes that must happen |

The app always calls its local node, which routes. A shard call is just another HTTP route in the app:

```
app (game.users/42) → POST localhost:2282/invite
                 Shardlite-Target: game.guild/7
                 node: who owns game.guild/7?
                   local  → POST app/invite with Shardlite-* for game.guild/7
                   remote → forward to the owner node → its app/invite
```

- **The target is explicit.** The path, method and body pass through unchanged, so this works for REST, GraphQL or gRPC. A call from a shard can't be upgraded to a WebSocket (`400`), so use server-sent events or a gRPC stream for subscriptions. Calls from the app with no shard behind them can upgrade. A local call costs about 0.1 ms and a remote one adds 0.5–2 ms.
- **The callee knows the caller.** The app sets `Shardlite-Caller: game.users/42` (copied from the `Shardlite-Opened` it received), or `app` for a call with no shard behind it. The node strips the header from external requests, so it grants access to internal routes, which require it. A route that only takes calls may take its identity from `Shardlite-Opened`. A route clients can reach authenticates every request and works out the shard from the request, whoever sends it.
- **Gates apply on both sides.** The caller's gate holds the call until the caller's kind's tier has its commits, and the callee's holds the answer.
- **Calls activate and create the target,** unless the call sends `Shardlite-If-Active: true` (answer `404` rather than activate, for presence checks and fan-out to members who may be offline) or `Shardlite-If-Exists: true` (answer `404` rather than create). Use `Shardlite-If-Exists` for any call whose id came from a client, so random ids don't create shards through app code.
- **Hops are limited.** The app passes `Shardlite-Hops` on as it would `traceparent`. The node increments it and refuses past `SHARDLITE_MAX_HOPS` with `508`, which stops runaway recursion, and a relay counts hops the same way.
- **Cancellation propagates.** When a client disconnects or a caller gives up, the node cancels the request on every hop it controls (writes already committed are not rolled back). Timeouts are the app's, set on its own HTTP or gRPC client.
- **The node never retries a request the app may have seen,** so calls that change state should carry an idempotency key.

**gRPC** works in both directions: the app adds `shardlite-target` metadata (usually with a client interceptor) and the node proxies HTTP/2 without TLS, trailers and streams as they are. Its own errors are gRPC status responses. It never parses messages and doesn't read `grpc-timeout`.

**Reaching many shards.** For guild chat or a group invite, call each member's shard in parallel with `Shardlite-If-Active: true` so offline members aren't woken. The app keeps control of retries and timeouts.

## Reminders

A reminder is a row a shard sets for itself, with the time it is due: an upgrade finishing, an auction closing, a lobby that must end even if nobody is connected. The node wakes the shard's app when the earliest one is due, as a Durable Objects alarm does. The node creates the table at activation:

```sql
_shardlite_reminders (key TEXT PRIMARY KEY, due INTEGER NOT NULL) STRICT  -- due in Unix seconds
```

- **The rows are the app's.** It inserts, reschedules and deletes them with plain SQL in its own transactions (`INSERT … ON CONFLICT (key) DO UPDATE SET due = excluded.due`, and `DELETE` to cancel). Anything a reminder needs lives in the app's own tables under the same key, so the reminder and its data commit together. The table is STRICT, so a value of the wrong type is refused (a date string in `due` would otherwise wake nobody, ever), though a numeric string or a whole float still converts to an integer.
- **The wake.** When the earliest due time has come, the node calls `POST /_shardlite/reminders` on the shard's app (no body, `Shardlite-Opened` and `Shardlite-Caller` naming the shard) and gives it 30 seconds to answer. The handler takes its identity from `Shardlite-Opened`, and can trust that the call came from its own node, which answers `404` to every other request under `/_shardlite/`. A wake never acts on a commit that could still be lost.
- **The handler** selects `WHERE due <= unixepoch()` and handles each row in its own transaction, so a row's work and its deletion (or next due time) commit together. It answers `2xx` once everything due is done, and `503` if a row can't be finished.
- **Retries.** Any other status, no answer in time or rows still due after a `2xx` is a failure, and the next wake waits one second, then twice as long for each failure in a row up to a minute.
- **Stale reminders.** A reminder due more than a day ago is ignored: it doesn't wake the shard or keep it active. It stays in the table for the app to clean up (`DELETE FROM _shardlite_reminders WHERE due < unixepoch() - 86400`).
- **Repeats are the app's.** A handler that wants a recurring reminder sets the next `due` in the same transaction as its work. Resolution is a second.
- **Timing.** A shard that is active is woken within about a second of its time. An inactive one is woken by the cluster, usually within a few seconds of the minute the reminder is due in, later when many shards are due in the same minute or a node has failed. Rely on the minute only. A reminder due now keeps the shard active, so the app closes its connections after handling.

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

- **At least once, with the work done once.** A failed call is made again at the next wake. A lost acknowledgement makes the call twice, and the receiver's record makes the work happen once. A sender stops trying once its reminder is stale, so a `seen` entry older than a day can be dropped.
- **Calls that can never succeed** (a `4xx` that retrying won't change) should be dropped or marked failed by the handler, or they are retried until the reminder goes stale.
- **No ordering.** An app that needs order adds a sequence number per sender and receiver, and the receiver answers `503` to a number that isn't next.
- **One handler serves every reminder the app sets.** It looks at which of the app's tables has a row under each due key.

Uses: fan-out in one transaction (rewards for 100 players at the end of a match), transfers and trades by custody, and calls that must follow a commit.

## Rules for app code

These are part of the contract with shardlite. Apps that break them stall or fail.

- **Requests for the same shard run concurrently,** as on any database, and the shard's transactions are the concurrency control. Do a read and the write that depends on it in one transaction (`BEGIN IMMEDIATE`) or one statement (`UPDATE … SET balance = balance - 10 WHERE balance >= 10`). State kept in app memory per shard isn't protected by SQLite. Orleans and Durable Objects run one turn at a time, and an app that expects that loses updates here.
- **Handle concurrent requests, including calls back into the app itself.** A call to a shard on the same node arrives at the same app as a new request while the calling request waits. A single-threaded synchronous server deadlocks, and thread pools can run out when calls nest, so async servers are the safer choice.
- **Don't hold a write transaction while calling another shard.** If `game.users/42` is mid-transaction, calls `game.guild/7` and the guild calls back, the callback can't get the write lock. Call first and then write, or commit and then call.
- **Start write transactions with `BEGIN IMMEDIATE`, or retry them.** A deferred transaction that reads and then writes fails with `SQLITE_BUSY` if another connection committed in between, and `busy_timeout` doesn't retry that case. The node's bookkeeping commits now and then, so this is rare but happens.
- **Expect calls to fail and set timeouts.** A call can arrive while its target is activating or moving, and its node can fail. Retry where it makes sense.
- **Work out the shard from the request and relay on a mismatch** (see [Serving](#serving)). Don't answer from the open file for another shard's request, and don't skip authentication because a request arrived as a call.

## Recommendations for app code

- **Choose ids that need no lookup.** A client can choose a new shard's id (a UUIDv7), which makes creating it idempotent. An id built from other data, such as a trade between two players, can be a name-based UUID (v5) of that data.
- **Call `sync` before acknowledging writes that must not be lost** on a `none` kind. A small commit wrapper on the SQLite driver makes this hard to forget. Use a durable send for calls that must happen.
- **Drive time-based work from requests.** Energy, cooldowns, deadlines and daily resets can be worked out from timestamps whenever a request touches the shard, so a player who never returns costs nothing. Set a reminder only for things that must happen on time while nobody is connected, and spread reminders that many shards share (add a random delay of a few minutes), since the cluster paces them at about 1,000 a second.
- **Warm shards a player is about to use** by calling them in the background at login or matchmaking: a cheap route, with `Shardlite-If-Exists: true` so a missing one isn't created, and a cap on how many one request warms. The call activates each shard on its owner (or claims it, if nobody owns it), so the player's first request finds it open.
- **Keep indexes in index shards, updated by durable sends** (`game.lobbies-index/eu`). Handlers upsert, and readers skip entries older than a timeout.
- **Delete short-lived shards with the marker,** and a reminder for when an abandoned one should be gone at the latest.
- **Never give shared state a single shard id.** Split anything many players write (a leaderboard, a queue, a global counter, the [login tables](#logging-in)) at least 64 ways from the start, by meaning first (season, region, mode) and then by hash, with an aggregator shard the buckets report to by durable send. An unused bucket costs nothing, while splitting later is a migration. Cache shared data (a message of the day, a guild's member list) in the app rather than reading its shard every time.
- **Publish by recording events** in the topic's shard with a sequence number: connected clients get them pushed, and subscribers catch up when next touched.
- **Keep large data in object storage.** Upload it under a unique name, then commit the reference in the shard. A crash in between leaves an orphan, never a missing object.

## A single writer

Some processes hold a shard's state in memory for a long time and write it back: a game server running a survival world or an MMO realm, a client syncing a cloud save, a worker that owns a job. Two of them must never both write the same shard, and whatever starts them can't promise that, since a process it thinks is dead (hung, or on a partitioned host) can still be running when its replacement starts. The shard fences them itself with an **epoch**, the same idea the node uses for its own journal:

- **Open.** The process's first call (`POST /open` on the shard, say) adds one to an epoch the shard keeps in a table of its own and answers it. From then on only the newest process to open the shard can change it.
- **Save.** Every write carries the epoch, and the shard refuses one that isn't current (`409`, say). An older process finds out at its next save and stops. Its players may see a moment of rollback, and nothing it did after the newer open is kept.
- **Nothing else.** There is no lease to renew and no close. The next open fences the old writer whether it closed or not, and a crash would skip a close anyway. Which process runs a world and where its players connect is for whatever allocates them to answer, since it knows when one dies.

Keep the kind on `storage` (the default): an open lost in a crash could hand the same epoch to two processes.

Keep one save in flight and coalesce the rest, as game servers already do: while one waits for its answer, the next gathers every change since. That keeps saves in order without sequence numbers, and the answer to the save that included a change is the signal that it is stored. The process keeps running while it waits, and waits only where a player is told something happened (a trade, loot).

### Writing to the file directly

A process on a host that runs a node can open the shard through the node's control API, so the shard activates on its own host, and then write the shard's file itself instead of saving through calls. Open answers the file's path (its request's `Shardlite-File`) and the node's name (from `GET /_shardlite/node`), and the process uses the file only if the name is its own node's. Then:

- **each save is the process's own transaction** (`BEGIN IMMEDIATE`), which rolls back unless the epoch is still its own and adds one to a save counter in the same table;
- **a call confirms it** (`POST /confirm`, say) by answering the epoch and the counter from the shard as it is now. The same epoch and a counter at least the saved one mean the save is in the shard, and since the answer passes the gate, on `storage` it is durable. The confirm is the save in flight.

The confirm is what makes this safe. After a node restart the process's connection is to a file the node has let go of: its commits land there and are lost, and the confirm finds the counter short (or the shard unavailable while the process still holds the old file). The process then closes the file, opens again and writes everything since its last confirmed save again from memory. A direct commit is never saved before its confirm.

An open connection keeps the shard active and stops the node moving it (see [What an app must do](#what-an-app-must-do)), so close the file when the world empties, and the shard hibernates after its idle timeout. A drain leaves shards held open to adoption, so a host whose processes hold shards drains more slowly. A process that only loads from the file and saves through calls closes it once it has loaded.

### Showing it to players

While a world runs, its players can ask its process. Other requests for it (a character's belongings, how many are on it) call it with `Shardlite-If-Active: true`, which answers `404` instead of activating it, so a large hibernated world isn't restored to answer one. Keep the kind out of `public` so that clients can't name it, and have the world publish what players need to see while it is hibernated by calls to other shards, such as a character summary in the player's shard.

## Running shardlite

Nodes are disposable. Blob storage holds the data, and local disks are caches that the node evicts least recently used first when full (`SHARDLITE_CACHE_LIMIT` sets a limit). Local NVMe is fastest and ephemeral disks are fine.

- **Kubernetes:** the node as a DaemonSet, with `SHARDLITE_APP` pointing at a Service with `internalTrafficPolicy: Local`, and the app pods and node sharing the data directory and the control socket through a `hostPath` volume. A sidecar per pod also works, but every app deploy then restarts the node and loses its cache. Elsewhere the node runs as a systemd unit or similar.
- **Development and single machines:** one node on RustFS in Docker (the Compose file the tests use). For production on one machine, keep RustFS's buckets in strict durability mode, since a store that acknowledges a write before it is on disk breaks the gates' promise.
- **Scaling:** add nodes, and new connections and shards land on them (scale by CPU and memory). To remove one, drain it: it fails its readiness check, stops serving, evicts its shards and exits.
- **Restarts and upgrades:** drain the same way. A restart is treated as a crash and the node starts with an empty cache, so shards are restored as they activate. During a rolling upgrade, old and new nodes forward to each other and read each other's data.
- **Failure:** requests for a dead node's shards get a quick `503` with `Retry-After` until its lease has expired and another node has taken over (10–20 s with the default lease TTL). A crash loses a `none` kind's last flush and PUT.

### Load balancer

Clients reach nodes through the deployment's own load balancer, which terminates TLS and absorbs hostile traffic. Nodes listen only on the private network. The load balancer sets `X-Forwarded-For`, `X-Forwarded-Proto` and `X-Forwarded-Host` on every request (overwriting what clients send), the node adds the load balancer's address to the end of `X-Forwarded-For`, and the load balancer checks `GET /_shardlite/ready`.

A request that lands on a node that doesn't own its shard is forwarded to the owner (0.5–2 ms). With a sticky load balancer an unowned shard is activated on the node that received the request. With a controllable one (HAProxy, nginx, Envoy), route by hashing `Shardlite-Target` (header or query parameter) to reach the owner directly.

### Blob storage

The supported stores are AWS S3 and, for development, tests and self-hosting, RustFS. A store must support linearizable conditional writes (`If-None-Match` and `If-Match` on writes, `If-Match` on deletes) and custom metadata on conditional writes, returned on `HEAD`. Cloudflare R2 doesn't document conditional deletes, so it fails the startup check until it does. The node tests its store at startup and refuses to start if conditional writes misbehave.

`SHARDLITE_PREFIX` puts every object under a prefix (`staging/`), so several environments can share one bucket. No lifecycle rule is needed: each node deletes its own expired journal segments. Shardlite keeps no history of old files, so a deployment that wants point-in-time restore can turn on bucket versioning. Deleting players who never return is the app's decision.

### Hosts

The node targets Linux only, and apps that use it must run on the same Linux machine (or VM), since they share SQLite files. Game clients and editors can run anywhere. On macOS, run the node, backend and game server in Docker with shard files in a Docker volume (not a bind mount, since SQLite locking isn't reliable across file sharing). On Windows, use WSL2 or Docker with shard files on the Linux filesystem (not under `/mnt/c`).

- **Open files and memory mappings.** Each active shard takes 8 file descriptors and about 7 memory mappings, plus one more descriptor for each segment waiting to be written, which a slow blob store lets pile up. Raise the hard `ulimit -n` and `vm.max_map_count` (65,530 by default, reached at about 10,000 active shards). The node warns at startup if they are too low.
- **Data directory.** `SHARDLITE_DATA` must be on a local filesystem (the node refuses to start on NFS, SMB or FUSE), with room for the largest shards being restored or rebased (at most 4 rebases at once) and about twice the size of any commit that rewrites a whole database. A shard's tail lives in unnamed files in `{data}/.tmp`, which show in `df` and not in `du`, and a slow blob store keeps the space of an old tail until it catches up.
- **Clocks.** Run NTP. Leases need only clock rates that agree, but a clock that is far off delays failover. Clocks matter for failover speed and for `none` reads, not for `storage`. `GET /_shardlite/node` shows the offset against the store's and whether the node is warning, and the node logs a store that stops sending a `Date` header, since the check is then blind.

### Traffic between nodes

A node has three listeners: public (clients, through the load balancer), peer (other nodes) and control (the app on the same host). The peer listener speaks HTTP without TLS and is trusted to carry node-only headers, so it must be reachable only from other nodes, on a trusted private network or an encrypted one (WireGuard, Tailscale). Never put it behind the load balancer or expose it to clients or the app. Service meshes are not a target, since they add two proxy hops to every call between nodes.

### Load

- **Rebalancing.** New nodes only receive new connections and shards, so the node moves busy shards from overloaded nodes to less loaded ones, rarely and with a cap per interval. It closes those shards' WebSocket connections with a reconnect close code, or sends HTTP/2 `GOAWAY` (`Connection: close` on HTTP/1.1) to HTTP clients, and in-flight requests finish. Clients must reconnect on that close code.
- **Load shedding.** Above a CPU or memory threshold, a node replies `503` with `Retry-After` and refuses new activations. Active shards keep working.
- **Hot shards.** Each shard has a cap on requests in flight. Past it, the node replies `429`.

### Cost and latency

Rough estimates, not measurements. On S3 Standard a busy node writes 4 journal PUTs a second at the default flush interval, about 50 USD a month whatever the number of active shards (idle nodes write nothing). A shard deactivating costs one PUT and a manifest write, about 10 USD per million sessions, and a 100 KB player shard is 100 GB of storage per million players. A request for an active shard adds about 0.1 ms, a cached one about 1 ms, and a shard owned by another node 0.5–2 ms plus the first request's manifest read (10–30 ms). Claiming a shard takes 20–60 ms plus a snapshot download. If an owner crashes, its shards wait up to twice the lease TTL, then claim and restore.

### Observability

The node passes W3C trace context (`traceparent`) through every hop and adds spans of its own (routing, activation, restore, gate waits). It logs an alert if a shard's commits have gone longer than 3× `SHARDLITE_DELTA_AGE` without a snapshot, which means its snapshots are failing. `SHARDLITE_LISTEN_PPROF` serves Go's pprof handlers.

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
| `Shardlite-Relay` | app on the control API | `true`: the request is a client's, relayed (see [Serving](#serving)). With `Shardlite-Caller` it is a `400` |
| `Shardlite-Hops` | app on calls, node to app | calls along one chain |
| `Shardlite-If-Active` | app on calls | `true`: answer `404` rather than activate |
| `Shardlite-If-Exists` | app on calls | `true`: answer `404` rather than create |
| `Shardlite-Sync` | app on a response or call | `none` or `storage`, overriding the kind's tier once |

### Control API

On `SHARDLITE_LISTEN_CONTROL` (localhost TCP) and `SHARDLITE_LISTEN_CONTROL_SOCKET` (a Unix socket):

| Request | Meaning |
|---|---|
| `ANY /{path}` with `Shardlite-Target` | call `{path}` on the shard's owner |
| `ANY /{path}` with `Shardlite-Target` and `Shardlite-Relay: true` | relay a client's request for `{path}` to the shard's owner, with a client's limits |
| `POST /_shardlite/sync` | wait until everything committed before the call is in blob storage (fails if the node has lost its lease) |
| `GET /_shardlite/node` | who this node is and how its clock stands against the store's |

The node polls the app's `GET /_shardlite/ready` and calls `POST /_shardlite/reminders`. The public listener serves `GET /_shardlite/ready` to the load balancer. A path under `/_shardlite/` that a listener doesn't serve itself is refused with `404`.

### Status codes

Errors the node generates itself (sent as gRPC status for gRPC requests):

| Code | When |
|---|---|
| `400` | a malformed shard name or `Shardlite-*` header, or a relay that sets `Shardlite-Caller` |
| `404` | an undeclared kind, a kind that isn't `public` named by a client or in a relay, a client request or a relay for a shard that doesn't exist, `Shardlite-If-Active: true` on an inactive shard, `Shardlite-If-Exists: true` on a missing one, or a path under `/_shardlite/` the node doesn't serve |
| `429` | the shard's cap on requests in flight |
| `503` | the owner doesn't answer (with `Retry-After`), the shard moved since the node last forwarded to it (`Retry-After: 0`: try again at once), the node is shedding load, or the shard's files failed their checksums |
| `504` | a timeout inside the node ended the request |
| `508` | hops past `SHARDLITE_MAX_HOPS` |

### Declaration

`"app"` names the app the kinds belong to, and the node qualifies each kind with it (a change of it is a change of every shard's name). `"version"` is the app's own, an opaque string, optional for now. Each kind under `"kinds"` takes:

| Field | Default | Meaning |
|---|---|---|
| `sync` | `"storage"` | `"storage"` or `"none"` (see [Durability](#durability)) |
| `idle_timeout` | `SHARDLITE_IDLE_TIMEOUT` | how long a shard stays active without requests, as a Go duration (`"10m"`) |
| `public` | `false` | clients may name the kind |

### Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `SHARDLITE_NODE` | the host name | the node's name (1 to 128 ASCII letters, digits, `.`, `-` and `_`), which names its lease and journal |
| `SHARDLITE_ADVERTISE_PEER` | the peer listener's address | how other nodes reach the peer listener (`host:port`), required when it listens on a wildcard address (the default) |
| `SHARDLITE_BLOB` | required | `s3://bucket` |
| `SHARDLITE_S3_ENDPOINT` | AWS | the URL of an S3-compatible store |
| `SHARDLITE_PREFIX` | | prefix of every object path, for example `staging/` |
| `SHARDLITE_APP` | | the app's base URL, polled for its declaration. Without it the node serves only the system kinds |
| `SHARDLITE_LISTEN_PUBLIC` | `:2280` | the public listener |
| `SHARDLITE_LISTEN_PEER` | `:2281` | the listener for traffic from other nodes |
| `SHARDLITE_LISTEN_CONTROL` | `127.0.0.1:2282` | the control API on localhost TCP |
| `SHARDLITE_LISTEN_CONTROL_SOCKET` | | the control API on a Unix socket |
| `SHARDLITE_DATA` | required | the node's directory in the host's disk: shard files (the local cache) and the cache index's database |
| `SHARDLITE_CACHE_LIMIT` | no limit | bytes of deactivated shards to keep |
| `SHARDLITE_FLUSH` | `250ms` | flush interval |
| `SHARDLITE_FLUSH_MIN` | `100ms` | the shortest time between two flushes, which bounds the wait at a gate |
| `SHARDLITE_IDLE_TIMEOUT` | built in | idle timeout for kinds that don't set one |
| `SHARDLITE_LEASE_TTL` | `10s` | node lease TTL |
| `SHARDLITE_DELTA_AGE` | `15m` | how old a shard's unsnapshotted commits get before a snapshot is written. The tail alert (3×) and journal expiry follow from it |
| `SHARDLITE_DELTA_SIZE` | `256KB` | how many bytes of unsnapshotted commits make a snapshot due, whatever their age |
| `SHARDLITE_MAX_HOPS` | `32` | the hop limit for calls |
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

### System kinds

The node's own kinds, served in process with no app behind them, under the node's own app `_system` (`_system.sweep`, `_system.dispatch`). Clients can't name them, and the app (or an operator with `curl` on a node's control listener) can call their routes. Each has fixed ids kept active on one node of the cluster. How they work is in [system kinds](internals.md#system-kinds).

| Kind | Ids | What it does |
|---|---|---|
| `_system.sweep` | `_cluster` | deletes LTX files that no manifest lists and that are older than a day |
| `_system.dispatch` | `_cluster` | wakes the shards whose reminders are due while they are inactive |
