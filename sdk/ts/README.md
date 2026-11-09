# shardlite for TypeScript

The TypeScript SDK for apps served by shardlite: the part of the [app contract](../../docs/guide.md) that is the same in every app. The same shape as the Go SDK at [`../go`](../go), in plain TypeScript with no runtime dependencies, written against the web-standard `Headers`, `Request`, `Response` and `fetch`, so it runs on Node, Bun and Deno and under any framework that hands those down. It never reads the context header's format, so there is no copy of it in JavaScript to keep in step with the node's.

The source is plain `.ts` with no build step: `npm test` runs the tests, `npm run lint` type-checks with `tsc`, and the dev dependencies are `typescript` and `@types/node`. The tests need Node 22.18 or later (the first version that runs TypeScript unflagged), which package.json's `engines` with `.npmrc`'s `engine-strict` makes npm enforce. The examples import the source where it sits, as `./shardlite.ts`. The package name is a placeholder until an app outside the repository wants it published (docs/roadmap.md).

The SDK returns promises that matter: a forgotten `await` on `opened.withDb(...)` lets the request end with its database still open, and one on `node.sync()` or `nodeError(res)` drops its failure. Turn on [`@typescript-eslint/no-floating-promises`](https://typescript-eslint.io/rules/no-floating-promises/) in the app, which catches both.

## The app's node

An app builds one `Node` from a config and keeps it for its lifetime. The config holds the opener (the app's driver), the app's version, its kinds and optionally the address of the node's control API (`127.0.0.1:2282` unless told otherwise) and two hooks. Declare the kinds as values and keep them together in one file, since their diff is the declaration's diff:

```ts
import { DatabaseSync } from "node:sqlite";
import { kind, shardlite } from "./shardlite.ts";

export const users = kind("users", { public: true, idleTimeoutMs: 600_000 });
export const guilds = kind("guild");
export const chat = kind("chat", { public: true, sync: "none" });

export const node = shardlite({
  open: ({ uri }) => new DatabaseSync(uri),
  version: "2026.10.0",
  kinds: [users, guilds, chat],
  onOpen: (db, shard) => migrate(db, shard), // optional, after the pragmas
  health: () => checkDependencies(), // optional, throws while the app is not ready
});
```

`kind(name, { public, sync, idleTimeoutMs })` describes one kind (the guide's [Shard kinds](../../docs/guide.md#shard-kinds)). Each setting left out is the node's default, so `public` is false, `sync` is the node's tier (`"none"` or `"storage"`) and `idleTimeoutMs` is the node's idle timeout. `shardlite` throws at startup on a missing opener, an invalid kind name, a kind named twice, an unknown `sync` and a negative idle timeout, so a declaration the node would refuse never leaves the app. A library exports a function that takes the kind's name, so the app picks it as the guide says (`matchmakerQueue("matchmaker.queue")`).

`node.ready()` answers the route the node polls, `GET READY_PATH`, with the declaration built from the version and the kinds. It answers `503` with the message of `health`'s error while `health` throws or rejects, so the node sends the app no traffic during a migration or with a dependency down.

`kind.shard(id)` names a shard of that kind: a `ShardName`, which is a string at runtime (it stores and logs as text, and `users.shard("42")` is `"users/42"`) but a distinct type, so a plain string cannot be passed where a name is wanted. `parseShard(s)` reads one back from the app's own tables (the target of a durable send) and throws when the string does not fit the grammar of the guide's [Shard names](../../docs/guide.md#shard-names). The SDK checks the grammar and never whether a kind is declared: the node answers a call for an undeclared kind with `404`, which keeps an app whose kinds change while it runs working.

## A handler

Every server runs the same handler, a `Request` in and a `Response` out: Bun's `Bun.serve({ fetch: handler })` and Deno's `Deno.serve(handler)` take it as is, and under `node:http` it takes a few lines to wrap (read the body, build `new Request` with `req.headers`, run the handler, answer from the `Response`). Three routes belong to shardlite: the declaration at `GET READY_PATH`, the wake at `POST REMINDERS_PATH` if the app sets reminders, and the app's own routes, which are for a shard.

`node.request(req.headers)` returns the request's scope, which holds its one database handle (the next section). The headers object stands for the request: the same object gives the same scope, and any other object gives a new scope with a handle and busy guard of its own. A framework that rebuilds headers between layers (Hono, `new Request(req, ...)`, an adapter that copies them) hands out a second scope for one request, and the guard no longer sees both. Make the scope once at the edge of the handler and pass it down. A helper takes a `Scope`, never headers.

A route that belongs to a shard works out which shard the request is for (from the path, the payload, the first gRPC message) and passes its name to `expect`. If that is the shard the node opened, `expect` returns an `Opened`. If not, it returns a `Refusal` with status `400` and a message, which the app answers in its own format. The SDK writes no responses on the app's routes:

```ts
import { Refusal, READY_PATH } from "./shardlite.ts";
import { node, users } from "./kinds.ts";

const handler = async (req: Request): Promise<Response> => {
  const url = new URL(req.url);
  if (url.pathname === READY_PATH) {
    return node.ready();
  }
  const scope = node.request(req.headers); // once, here, then passed down
  const visit = /^\/users\/([^/]+)\/visit$/.exec(url.pathname);
  if (req.method === "POST" && visit) {
    const opened = scope.expect(users.shard(visit[1]!));
    if (opened instanceof Refusal) {
      return new Response(opened.message, { status: opened.status });
    }
    return await opened.withDb((db) => {
      db.exec("INSERT INTO visits DEFAULT VALUES");
      return new Response(`hello from ${opened.name}`);
    });
  }
  return new Response("not found", { status: 404 });
};
```

`Refusal` extends `Error` with `status` and `message`, so an app with one central error handler can `throw opened` and answer it there with `err instanceof Refusal`. A wrong shard is a stale client or a bug, so the status is `400`, not the `404` a client reads as a shard to drop, nor a `421`. `expect` is not authentication: an app that has users authenticates every request on a route clients can reach, and `Shardlite-Caller` is never a reason to skip that.

There are three ways in, one for each kind of route:

- `scope.expect(name)` is for a route clients can reach that belongs to a shard.
- `scope.expectCall()` is for a route that only takes calls and for the wake at `REMINDERS_PATH`. It takes the shard from what the node opened and refuses a client's request (no `Shardlite-Caller`) with `403`, and a request with no `Shardlite-Opened` with `400`. It returns an `Opened | Refusal` like `expect`.
- `scope` itself is for a route that belongs to no shard (a login, a webhook). It can `call` and read the `caller()`, and has no database. `scope.opened()` gives the name of the shard the node opened, or `undefined`, for logging and metrics only.

An `Opened` has `name`, `caller()`, `call(...)` and `withDb(fn)`. A database comes only from an `Opened`, so a handler cannot use a shard's file without checking what the request is for.

## The database, one handle per request

`opened.withDb(fn)` checks that the shard's file exists, opens it through the app's opener, runs `PRAGMAS` (`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;`) and then `onOpen(db, shard)` if the config has one, calls `fn` with the handle, and closes the handle once `fn` settles, whether it returned or threw. `onOpen` is the place to migrate: the busy timeout is set, so a migration waits for the node's own bookkeeping commits and does not fail on them. Nothing outlives the request: a connection that does pins its shard on the node, and a commit through one left open when the node stops is lost. Opening an existing file costs tens of microseconds, about what a local proxy hop costs.

It rejects, and never calls `fn`, for a missing file (the path is no longer the shard's), a failed open, a failed pragma or an `onOpen` that throws (the handle closes first). A close that fails after `fn` settled rejects with the close error, and when `fn` failed its error wins.

A `withDb` inside `fn`, or alongside it for the same request, rejects with an error saying the database is already open: open it once and pass the handle down. A request therefore holds one connection and cannot wait on its own write lock for the whole busy timeout. If `fn` returns a `ReadableStream` or an async iterable, `withDb` rejects with an error that says so, since the database would be read after the close: read it inside `fn` and return the result.

### The openers

The opener is the app's, since the driver and its options are. It receives `{ path, uri }`: the file's path, and the same path as a URI that opens it read-write and never creates it (`file:` then the path with `%`, `?` and `#` escaped, then `?mode=rw`). The contract asks the open to be read-write and never create: the node created the file before handing the path out, so a failed open means the path is no longer the shard's, and a driver that creates would make a new empty database that nothing captures. `withDb`'s existence check makes a driver with no refuse-to-create option safe too. Each driver's way, and how it runs a statement (the reminders' and the app's):

| Driver | Opener, never creating a file | Run `stmt.sql` with `stmt.params` | Read the due keys |
|---|---|---|---|
| [`node:sqlite`](https://nodejs.org/api/sqlite.html) | `({ uri }) => new DatabaseSync(uri)` | `db.prepare(stmt.sql).run(...stmt.params)` | `db.prepare(dueReminders().sql).all()` |
| [`better-sqlite3`](https://github.com/WiseLibs/better-sqlite3) | `({ path }) => new Database(path, { fileMustExist: true })` | as above | as above |
| `bun:sqlite` (Bun) | `({ path }) => new Database(path, { create: false })` | as above | as above |
| [`sqlite`](https://www.npmjs.com/package/sqlite), the promise wrapper over `sqlite3` | `({ path }) => open({ filename: path, driver: sqlite3.Database, mode: sqlite3.OPEN_READWRITE })`, fits `Database` as is | `db.run(stmt.sql, ...stmt.params)` | `await db.all(dueReminders().sql)` |
| [`sqlite3`](https://github.com/mapbox/node-sqlite3), callbacks through `fromCallbacks` | `({ path }) => { const raw = new sqlite3.Database(path, sqlite3.OPEN_READWRITE); return { ...fromCallbacks(raw), raw }; }`, so `fn` has the raw handle for binding | `raw.run(stmt.sql, stmt.params, cb)` | `raw.all(dueReminders().sql, cb)` |
| [`@libsql/client`](https://docs.turso.tech/sdk/ts/reference) | `({ path }) => { const client = createClient({ url: \`file:${path}\` }); return { client, exec: (sql) => client.executeMultiple(sql), close: () => client.close() }; }`. It cannot refuse to create, `withDb`'s check covers it | `db.client.execute({ sql: stmt.sql, args: stmt.params })` | `(await db.client.execute(stmt.sql)).rows` |

The type the opener returns is what `fn` and `onOpen` receive, so `db.prepare(...)` is typed for `node:sqlite` and `raw` is there for `sqlite3`. All the SDK asks of it is the `Database` interface: `exec(sql)` and `close()`, either one returning a promise if the driver's does.

## The caller

`opened.caller()` and `scope.caller()` return who made the request: `{ kind: "client" }`, `{ kind: "app" }` or `{ kind: "shard"; shard: ShardName }`. A route that grants access by caller switches over it, and TypeScript (with `@typescript-eslint/switch-exhaustiveness-check`) reports a case left out:

```ts
const caller = opened.caller();
switch (caller.kind) {
  case "client": // authenticate it as any web app does
  case "app":
  case "shard": // caller.shard is who called
}
```

A `Shardlite-Caller` value the node would never write reads as the client, the least trusted.

## Calls

`scope.call(target, path, init)` calls `target` (a `ShardName`, say `guilds.shard("7")`) at a path of the app's protocol through the node's control API, and `opened.call` is the same from an `Opened`. It sends at once and returns fetch's `Response`, which the app reads or cancels as with any fetch. It carries the request's `Shardlite-Context` and `traceparent`, so the node knows the caller and how long the chain of calls is and holds the call for the commits of the shard that made it. Make the call while serving the request, since one made after the response was sent is not held. The SDK never reads the context, so it has no copy of its format to drift.

```ts
const res = await opened.call(guilds.shard(id), "/invite", {
  method: "POST",
  body: JSON.stringify({ from: opened.name }),
  headers: { "Idempotency-Key": key }, // the app's own protocol
  ifExists: true, // the id came from a client: refuse a wrong one
  signal: AbortSignal.timeout(1000), // timeouts are the app's
});
```

The init is `fetch`'s (method, body, headers, signal) with three options beside it: `ifActive: true` answers `404` rather than activate the target, `ifExists: true` answers `404` rather than create it, and `sync: "none"` or `"storage"` overrides the target kind's durability tier for the call. `init.headers` is for the app's own protocol (`Idempotency-Key`, `Content-Type`, a client's credentials passed on) and refuses any `Shardlite-*` name, in any case and with `_` for `-`, since the options and the scope set those. A target that is not a valid shard name, a path without a leading slash and a refused header all reject before anything is sent.

A call with no request behind it (at startup, from a worker or a timer) is `node.callAsApp(target, path, init)`. It sends no context, so the node takes the call as the app's, restarts the hops and does not hold it for any shard's commits. That is wrong for a call made while serving a request, which is why it has the long name and `node` has no plain `call`. It is also the one call that may upgrade to a WebSocket.

### Node errors

A node's own error (the guide's [Status codes](../../docs/guide.md#status-codes)) carries `Shardlite-Error`, and the app's never does. `await nodeError(res)` returns the node's error as a `NodeError` when the response is one, and `undefined` when it is the app's answer, so a caller can tell a `404` from the node ("no such shard") from a `404` of the callee's:

```ts
const res = await opened.call(guilds.shard(id), "/invite", { method: "POST" });
const nerr = await nodeError(res);
if (nerr?.reason === "unknown-shard") {
  return new Response("no such guild", { status: 404 });
}
if (nerr !== undefined) {
  // the node could not deliver the call: pass its hint on, or retry
  return new Response(nerr.body, { status: nerr.status });
}
// not the node's: res is the callee's own answer
```

`NodeError` extends `Error` (it is not called `Error`, so it does not hide the global) with `status`, `reason`, `rawReason`, `body` and `retryAfterMs`. `reason` is one of the `Reason` strings, the guide's status codes bar `not-owner`, which never leaves the cluster. A node newer than the SDK may send a reason it does not know: `reason` is then `undefined` and `rawReason` has the value, so keep a default case in a switch over `reason`. `body` is the start of the response's body (up to 512 bytes, trimmed), which is how the node explains itself, and `message` reads as the Go SDK's error does: the status, the reason, the body and the retry hint.

`retryAfterMs` is the node's `Retry-After` hint in milliseconds, as the node writes it (whole seconds). `0` means try again at once (a shard that moved), and `undefined` means the node gave none, so the error says nothing about whether another attempt can succeed (the callee may have acted on the request). The SDK has no retry policy of its own: the node's hint is the signal, and an idempotent call is the app's to retry.

`nodeError` reads up to 512 bytes of the body, and only when it returns an error: that body is spent afterwards, so do not read it again. When it returns `undefined` it has not touched the body, which the app reads or cancels as with any fetch.

### Durability

`node.sync(signal)` waits until everything committed on the node is in blob storage and the node still owns its shards, and rejects if the node lost its lease. It is for a side effect the node cannot see (publishing to a queue, an external call): call it right before the side effect. It rejects with the node's `NodeError` for an error of the node's own, with a plain `Error` holding the status and the start of the body for any other non-2xx (a proxy in the way), and with an `Error` whose `cause` is the failure when the node cannot be reached.

A write on a `none` kind that must not be lost is acknowledged differently: `syncHeader("storage")` on the response makes it wait for the shard's own commits to reach blob storage, without waiting for the whole node. The kinds that are `storage` already wait.

```ts
return new Response("saved", { headers: { ...syncHeader("storage") } });
```

`syncHeader` returns the one header to spread into the response's headers beside the app's own. A tier other than `"none"` or `"storage"` is a type error and throws if it gets past the types. The same tier on a call is the `sync` option.

A callee reads the caller with `caller()` and takes its own shard from `expectCall()`. gRPC calls are the app's own: it sends the `Shardlite-Context` it was given as metadata under `shardlite-context`, and `shardlite-target`.

## Reminders, the wake, deleting a shard

`setReminder(key, due)`, `cancelReminder(key)` and `dueReminders()` hand back a `Statement`, `{ sql, params }` with read-only parameters, to run through the app's driver (per the openers table) in the transaction of the work each belongs with, so a reminder commits with its work. Bindings are plain values only, strings and integers (`due` as Unix seconds), which every driver binds the same way. A `Statement<Row>` carries the type of the rows it selects, so `dueReminders()` is a `Statement<{ key: string }>` and a helper that runs it can return `{ key: string }[]` (a `Statement<unknown>` parameter accepts any statement).

`setReminder` throws for an invalid `Date`, which the reserved table would refuse anyway. The node ignores a reminder due more than a day ago, and `setReminder` does not refuse one: it returns the statement as usual and calls `console.warn` naming the key and the due time. A mistake in the app's due computation then costs a reminder and not the transaction it runs in. A handler that repeats a reminder sets the next due time from now, not from the old one. The wake is a request like any other, with the shard opened and named as the caller, so its handler takes its shard from `expectCall`:

```ts
// with the work that needs it, in one transaction:
db.run(setReminder("auction:123", new Date(Date.now() + 60_000)).sql);

// the wake, at POST REMINDERS_PATH:
const opened = node.request(req.headers).expectCall();
if (opened instanceof Refusal) {
  return new Response(opened.message, { status: opened.status });
}
return await opened.withDb(async (db) => {
  const rows = await db.all(dueReminders().sql); // [{ key }], earliest first
  for (const { key } of rows) {
    // the key's work, then cancelReminder(key) (done) or setReminder(key, next),
    // in its own transaction. Answer 2xx when all are done, 503 when one
    // could not be finished, and the node wakes again with a backoff.
  }
  return new Response(null, { status: 204 });
});
```

`markDeleted()` is the statement that marks the shard for deletion: run it in the transaction that decides the shard is finished, and a marked shard is served as usual until it deactivates and its data goes. `unmarkDeleted()` takes the marker back, and `isMarkedDeleted()` is a `Statement<{ marked: 0 | 1 }>` (an `EXISTS` select aliased as `marked`) that a handler runs to refuse requests for a finished shard that is still served. The guide's [durable send](../../docs/guide.md#durable-send) builds on the wake: record a send and a due-now reminder in the transaction that causes it, and make each call from the wake with the key under `Idempotency-Key`.

## A test

`shardlitetest.ts` runs the node itself for a test, on its in-memory store (`SHARDLITE_BLOB=memory://`: one node, with everything gone when it stops). `startNode(config, app)` builds the app's `Node` from the config with `addr` set to the node's control address, serves `app(node)` (a function from the `Node` to the handler above) on a local `node:http` server, runs the `shardlite` binary against it and resolves once the node's public ready route answers `200`. So a test sees what the app sees in production (the declaration, the hop limit, the gates, real wakes) and nothing stands in for it. It uses `node:child_process`, which Node, Bun and Deno all provide.

```ts
import { startNode } from "./shardlitetest.ts";

test("a visit", async (t) => {
  const started = await startNode(config, app);
  t.after(() => started.close()); // SIGTERM, then SIGKILL if the node is slow
  const user = users.shard("visit-test-42"); // an id of this test's own

  // A client can reach a shard once it exists, which a call creates.
  await started.node.callAsApp(user, "/users/visit-test-42/visit", { method: "POST" });

  // A client's request: the public URL and Shardlite-Target.
  const res = await fetch(`${started.url}/users/visit-test-42/visit`, {
    method: "POST",
    headers: { [HEADER_TARGET]: user },
  });
});
```

`started.url` is the public base URL for clients' requests, `started.node` is the app's `Node` and calls through it reach the node's control API, `started.control` is its address and `started.output()` is the node's log so far, for a failure. `close()` stops the node and the app's server, and can be called twice. A call's effect shows at its callee's own route (its response, or what the route records). A wake is tested by setting a reminder that is due now and waiting, with a deadline of a few seconds, for the wake handler to run.

The binary is the path in `SHARDLITE_BIN`, else `shardlite` on `PATH`. Build it with `go build -o <dir>/shardlite ./cmd/shardlite` in the shardlite repository. Without one `startNode` rejects and says so. A node takes a fraction of a second to start. Tests that share one node (started once in a parent test, say) give each test shard ids of its own, so state never leaks from one to the next: the node has no way to clear itself. `shardlitetest.test.ts` is an app and its tests in full.

## What it leaves out

Routing (the one `expect` check above), gRPC (the app copies the `shardlite-context` and `shardlite-target` metadata in an interceptor), retries, durable send helpers and the single writer: the guide's sections carry the patterns, and a helper would be a second source of truth for them.
