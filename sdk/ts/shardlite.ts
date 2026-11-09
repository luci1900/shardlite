// The TypeScript SDK for apps served by shardlite: the same contract as the Go
// SDK at sdk/go, in plain TypeScript with no runtime dependencies. It is
// written against the web-standard Headers, fetch and Response, so it runs on
// Node, Bun and Deno and under any framework. The contract is the guide
// (docs/guide.md) and sdk/ts/README.md is this SDK's guide. The one thing it
// reads beyond the web standards is node:fs, to check that the shard's file
// exists before opening it.

import { accessSync, constants as fsConstants } from "node:fs";

// # Shard names and kinds

/**
 * The durability tier of a kind. Leaving it out is the node's default,
 * "storage".
 */
export type Sync = "none" | "storage";

declare const shardNameBrand: unique symbol;

/**
 * The name of a shard, kind/id. It is a string at runtime, so it stores and
 * logs as text, but only Kind.shard and parseShard make one.
 */
export type ShardName = string & { readonly [shardNameBrand]: true };

const MAX_KIND_LEN = 64;
const MAX_ID_LEN = 128;
// A kind is segments joined by dots, each a lowercase letter then lowercase
// letters, digits, - or _. A leading underscore marks the node's own kinds.
const KIND_NAME = /^[a-z][a-z0-9_-]*(\.[a-z][a-z0-9_-]*)*$/;
const ID_NAME = /^[A-Za-z0-9_-]{1,128}$/;

const validKind = (kind: string): boolean => kind.length <= MAX_KIND_LEN && KIND_NAME.test(kind);

/**
 * shardError says why s does not fit the grammar of a shard name, or returns
 * undefined when it does.
 */
function shardError(s: string): string | undefined {
	const slash = s.indexOf("/");
	if (slash < 0) {
		return `${JSON.stringify(s)} is not kind/id`;
	}
	if (!validKind(s.slice(0, slash))) {
		return `${JSON.stringify(s)} has no valid kind (lowercase letters, digits, - and _, starting with a letter, in segments joined by dots, up to ${MAX_KIND_LEN} characters)`;
	}
	if (!ID_NAME.test(s.slice(slash + 1))) {
		return `${JSON.stringify(s)} needs an id of 1 to ${MAX_ID_LEN} letters, digits, - or _`;
	}
	return undefined;
}

/**
 * parseShard reads a shard name, kind/id, from a string the app kept (the
 * target of a durable send, say). It throws when the name does not fit the
 * grammar of the guide's Shard names, and checks nothing else: whether the kind
 * is declared is the node's to answer, with a 404.
 */
export function parseShard(s: string): ShardName {
	const err = shardError(s);
	if (err !== undefined) {
		throw new Error(`shardlite: ${err}`);
	}
	return s as ShardName;
}

/**
 * The settings of a kind's declaration. Each one left out is the node's
 * default.
 */
export interface KindOptions {
	/** Lets clients name the kind in Shardlite-Target. */
	public?: boolean;
	sync?: Sync;
	/**
	 * How long a shard of this kind stays active without a request, in
	 * milliseconds.
	 */
	idleTimeoutMs?: number;
}

/**
 * One kind of shard the app serves. Make it with kind and list it in the
 * config's kinds.
 */
export interface Kind {
	/**
	 * The whole name of the kind, with no app in front: users, or
	 * matchmaker.queue for a library's.
	 */
	readonly name: string;
	readonly public: boolean;
	readonly sync?: Sync;
	/** Zero is the node's default. */
	readonly idleTimeoutMs: number;
	/**
	 * shard names the shard of this kind with id. The id is not checked here: a
	 * call to a bad name rejects before it is sent, and a request never matches
	 * one.
	 */
	shard(id: string): ShardName;
}

/**
 * kind describes a kind of shard. shardlite checks the name and the options
 * when it gets the kind.
 */
export function kind(name: string, options: KindOptions = {}): Kind {
	return {
		name,
		public: options.public ?? false,
		...(options.sync === undefined ? {} : { sync: options.sync }),
		idleTimeoutMs: options.idleTimeoutMs ?? 0,
		shard: (id) => `${name}/${id}` as ShardName,
	};
}

// # The request

// The headers and paths of the contract (the guide's reference has them all).
// The options of a call and the scope set the Shardlite-* headers, so only the
// two an app copies into gRPC metadata are exported, with the paths it serves.
const HEADER_OPENED = "Shardlite-Opened";
const HEADER_FILE = "Shardlite-File";
export const HEADER_CONTEXT = "Shardlite-Context";
const HEADER_CALLER = "Shardlite-Caller";
export const HEADER_TARGET = "Shardlite-Target";
const HEADER_IF_ACTIVE = "Shardlite-If-Active";
const HEADER_IF_EXISTS = "Shardlite-If-Exists";
const HEADER_SYNC = "Shardlite-Sync";
const HEADER_ERROR = "Shardlite-Error";

// The value of Shardlite-Caller for a call with no shard behind it.
const CALLER_APP = "app";

export const READY_PATH = "/_shardlite/ready";
export const REMINDERS_PATH = "/_shardlite/reminders";
const SYNC_PATH = "/_shardlite/sync";

/**
 * DEFAULT_ADDR is where the node's control API listens unless it is told
 * otherwise (SHARDLITE_LISTEN_CONTROL).
 */
export const DEFAULT_ADDR = "127.0.0.1:2282";

/**
 * What the SDK reads a header from: web Headers, anything shaped like it (as
 * most frameworks have), or a plain object of header names to values, as Node's
 * http module gives in req.headers.
 */
export type HeadersLike = Headers | { get(name: string): string | null } | Record<string, string | string[] | undefined>;

/**
 * header reads one header, by its case-insensitive name, from any HeadersLike.
 * Array values (as Node's req.headers gives for a header sent twice) read their
 * first.
 */
function header(h: HeadersLike, name: string): string | undefined {
	const get = (h as { get?: unknown }).get;
	if (typeof get === "function") {
		const v = (get as (n: string) => string | null).call(h, name);
		return v === null || v === undefined ? undefined : v;
	}
	const rec = h as Record<string, string | string[] | undefined>;
	const lower = name.toLowerCase();
	for (const key of Object.keys(rec)) {
		if (key.toLowerCase() === lower) {
			const v = rec[key];
			return Array.isArray(v) ? v[0] : v;
		}
	}
	return undefined;
}

/** Who made a request. */
export type Caller = { kind: "client" } | { kind: "app" } | { kind: "shard"; shard: ShardName };

/**
 * callerOf reads the caller the node set on a request. A value the node would
 * never write reads as the client, the least trusted.
 */
function callerOf(h: HeadersLike): Caller {
	const v = header(h, HEADER_CALLER);
	if (v === undefined || v === "") {
		return { kind: "client" };
	}
	if (v === CALLER_APP) {
		return { kind: "app" };
	}
	return shardError(v) === undefined ? { kind: "shard", shard: v as ShardName } : { kind: "client" };
}

/**
 * openedOf reads the shard the node opened and its file from a request's
 * headers.
 */
function openedOf(h: HeadersLike): { name: ShardName; file: string } | undefined {
	const name = header(h, HEADER_OPENED);
	const file = header(h, HEADER_FILE);
	if (name === undefined || !file || shardError(name) !== undefined) {
		return undefined;
	}
	return { name: name as ShardName, file };
}

/**
 * Refusal is why a request is not for the shard the node opened. expect and
 * expectCall return one in place of an Opened and write nothing: the app
 * answers in its own protocol and format, with the status the refusal carries.
 * A handler narrows with instanceof, and an app with a central error handler
 * can throw it:
 *
 *     const opened = scope.expect(users.shard(id));
 *     if (opened instanceof Refusal) {
 *       return new Response(opened.message, { status: opened.status });
 *     }
 *
 * Expect is not authentication: an app that has users authenticates every
 * request on a route clients can reach.
 */
export class Refusal extends Error {
	readonly status: number;

	constructor(status: number, message: string) {
		super(message);
		this.name = "Refusal";
		this.status = status;
	}
}

// # The database, opened once per request

/**
 * PRAGMAS is what withDb runs on a request's handle. The guide asks for WAL
 * mode (the node sets it when it creates the file, so this only checks) and a
 * busy timeout, since the node's own bookkeeping commits now and then and a
 * writer then waits for it rather than failing.
 */
const PRAGMAS = "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;";

/**
 * Database is all the SDK asks of a driver: run SQL, and close. Either method
 * may return a promise, which the SDK awaits. The synchronous drivers
 * (node:sqlite, better-sqlite3, bun:sqlite) and the promise wrapper around
 * sqlite3 satisfy it as they are; a callback driver goes through fromCallbacks.
 */
export interface Database {
	exec(sql: string): unknown;
	close(): unknown;
}

/** The shard's file as the opener gets it. */
export interface ShardFile {
	/**
	 * The path, for drivers that take one (better-sqlite3 with fileMustExist,
	 * bun:sqlite with create: false).
	 */
	path: string;
	/**
	 * The path as a URI that opens the file read-write and never creates it:
	 * file: then the path with %, ? and # escaped, then ?mode=rw.
	 */
	uri: string;
}

/**
 * Opener opens the shard's database file. It must open the file read-write and
 * never create it: the node creates the file before it hands the path out, so
 * an open that fails means the path is no longer the shard's, and a driver that
 * creates would make a new empty database that nothing captures. withDb checks
 * the file exists first, so a driver that cannot refuse to create is still
 * safe. The README has an opener for each driver style.
 */
export type Opener<D extends Database> = (file: ShardFile) => D | Promise<D>;

/**
 * fromCallbacks adapts a driver whose exec and close report through a
 * Node-style callback (as sqlite3's do) into a Database. A callback's error
 * becomes the promise's rejection.
 */
export function fromCallbacks(
	db: {
		exec(sql: string, cb: (err: Error | null) => void): unknown;
		close(cb: (err: Error | null) => void): unknown;
	},
): Database {
	return {
		exec: (sql) =>
			new Promise((resolve, reject) => {
				db.exec(sql, (err) => (err ? reject(err) : resolve(undefined)));
			}),
		close: () =>
			new Promise((resolve, reject) => {
				db.close((err) => (err ? reject(err) : resolve(undefined)));
			}),
	};
}

/**
 * fileURI is the URI that opens file read-write without creating it. The
 * characters a SQLite URI gives meaning to are escaped.
 */
const fileURI = (file: string): string =>
	`file:${file.replaceAll("%", "%25").replaceAll("?", "%3f").replaceAll("#", "%23")}?mode=rw`;

/**
 * checkFile refuses a file that is not there before the app's opener runs, for
 * the drivers that would create it. node:fs is the one thing the SDK reads
 * beyond the web standards, and every runtime where an app can open a SQLite
 * file (Node, Bun, Deno) implements it.
 */
function checkFile(name: ShardName, file: string): void {
	try {
		accessSync(file, fsConstants.F_OK);
	} catch {
		throw new Error(`shardlite: opening ${name} at ${file}: the file does not exist, so the path is no longer the shard's`);
	}
}

async function quietClose(db: Database): Promise<void> {
	try {
		await db.close();
	} catch {
		// the open's caller has the error that matters
	}
}

/** What a Node hands to the scopes of its requests. */
interface Core<D extends Database> {
	open: Opener<D>;
	onOpen: ((db: D, shard: ShardName) => void | Promise<void>) | undefined;
	addr: string;
}

/**
 * Handle opens a request's database for one withDb at a time. A second withDb
 * while one is open is refused: a request holds one connection, and a second
 * would wait on the first's write lock for the whole busy timeout.
 */
class Handle<D extends Database> {
	private readonly core: Core<D>;
	private readonly name: ShardName;
	private readonly file: string;
	private busy = false;

	constructor(core: Core<D>, name: ShardName, file: string) {
		this.core = core;
		this.name = name;
		this.file = file;
	}

	async use<T>(fn: (db: D) => T | Promise<T>): Promise<T> {
		// Set before the first await, so a withDb alongside this one sees it.
		if (this.busy) {
			throw new Error(`shardlite: ${this.name}'s database is already open for this request: pass the handle down`);
		}
		this.busy = true;
		try {
			const db = await this.openDb();
			let settled = false;
			try {
				const out = await fn(db);
				refuseStream(out);
				settled = true;
				return out;
			} finally {
				await closeAfter(db, settled);
			}
		} finally {
			this.busy = false;
		}
	}

	private async openDb(): Promise<D> {
		checkFile(this.name, this.file);
		const db = await this.core.open({ path: this.file, uri: fileURI(this.file) });
		try {
			await db.exec(PRAGMAS);
			await this.core.onOpen?.(db, this.name);
		} catch (e) {
			await quietClose(db);
			throw e;
		}
		return db;
	}
}

/**
 * closeAfter closes db. A close that fails rejects only when the function
 * settled, so the function's own error wins.
 */
async function closeAfter(db: Database, settled: boolean): Promise<void> {
	try {
		await db.close();
	} catch (e) {
		if (settled) {
			throw e;
		}
	}
}

/** refuseStream rejects what would be read after the handle closes. */
function refuseStream(out: unknown): void {
	const stream = out instanceof ReadableStream || (typeof out === "object" && out !== null && Symbol.asyncIterator in out);
	if (stream) {
		throw new Error("shardlite: withDb's function returned a stream, which would be read after the database closes: read it inside the function and return the result");
	}
}

// # The scope of a request

/** The request a scope is for. */
class Scope<D extends Database> {
	private readonly core: Core<D>;
	private readonly headers: HeadersLike;
	private handle: Handle<D> | undefined;

	constructor(core: Core<D>, headers: HeadersLike) {
		this.core = core;
		this.headers = headers;
	}

	/** caller returns who made the request. */
	caller(): Caller {
		return callerOf(this.headers);
	}

	/**
	 * opened returns the shard the node opened for the request, if any. It says
	 * what the node did, not what the request is for, and gives a name and
	 * never a database: it is for logging and metrics.
	 */
	opened(): ShardName | undefined {
		return openedOf(this.headers)?.name;
	}

	/**
	 * call calls the shard target at path through the node, from the request
	 * being served, and returns fetch's Response. It sends at once, carrying
	 * the request's Shardlite-Context and traceparent, so the node knows the
	 * caller and how long the chain of calls is and holds the call for the
	 * commits of the shard that made it. Make it while serving the request: one
	 * made after the response is sent is not held. Timeouts are the app's: pass
	 * init.signal, as in AbortSignal.timeout(1000).
	 *
	 * The init is fetch's, with the options ifActive and ifExists (answer 404
	 * rather than activate or create the target) and sync (overrides the target
	 * kind's durability tier for this call). Headers of the app's own protocol
	 * (Idempotency-Key, Content-Type) go in init.headers, which refuses any
	 * Shardlite-* name, since the options and the scope set those. A target
	 * that is not a valid shard name, or a path without a leading slash,
	 * rejects before anything is sent.
	 */
	call(target: ShardName, path: string, init?: CallInit): Promise<Response> {
		return send(this.core.addr, this.headers, target, path, init);
	}

	/**
	 * expect is for a route clients can reach that belongs to a shard. The app
	 * works out which shard the request is for (from the path, the payload or
	 * the first gRPC message) and passes its name. If that is the shard the
	 * node opened, expect returns it. Otherwise (another shard, or none) it
	 * returns a Refusal with status 400 and a message saying so (the client
	 * fixes its Shardlite-Target). A wrong shard is a stale client or a bug, so
	 * not a 404, which a client reads as a shard to drop, nor a 421.
	 */
	expect(name: ShardName): Opened<D> | Refusal {
		const got = openedOf(this.headers);
		if (got === undefined) {
			return new Refusal(400, `the request is for ${name} but names no shard to the node: send Shardlite-Target naming the shard`);
		}
		if (got.name !== name) {
			return new Refusal(400, `the request is for ${name} but the node opened ${got.name}: send Shardlite-Target naming the shard`);
		}
		return this.forShard(got);
	}

	/**
	 * expectCall is for a route that only takes calls and for the wake at
	 * REMINDERS_PATH. The shard is the one the node opened (Shardlite-Opened)
	 * and the route needs a caller: a client's request is refused with 403,
	 * since the node sets Shardlite-Caller on calls only and strips it from
	 * clients.
	 */
	expectCall(): Opened<D> | Refusal {
		if (this.caller().kind === "client") {
			return new Refusal(403, "this route is for calls from the app and its shards, not for clients");
		}
		const got = openedOf(this.headers);
		if (got === undefined) {
			return new Refusal(400, "this route is for a shard: send it through the node, which sets Shardlite-Opened");
		}
		return this.forShard(got);
	}

	private forShard(got: { name: ShardName; file: string }): Opened<D> {
		this.handle ??= new Handle(this.core, got.name, got.file);
		return new Opened(this, got.name, this.handle);
	}
}

/**
 * Opened is the shard the node opened for a request, which the app has checked
 * the request is for. Only expect and expectCall return one.
 */
class Opened<D extends Database> {
	/** The shard the node opened. */
	readonly name: ShardName;
	private readonly scope: Scope<D>;
	private readonly handle: Handle<D>;

	constructor(scope: Scope<D>, name: ShardName, handle: Handle<D>) {
		this.scope = scope;
		this.name = name;
		this.handle = handle;
	}

	/** caller returns who made the request. */
	caller(): Caller {
		return this.scope.caller();
	}

	/**
	 * call calls the shard target from the request being served, as the scope's
	 * call does.
	 */
	call(target: ShardName, path: string, init?: CallInit): Promise<Response> {
		return this.scope.call(target, path, init);
	}

	/**
	 * withDb opens the shard's database through the app's opener, runs PRAGMAS
	 * and then the config's onOpen on it, calls fn with the handle, and closes
	 * the handle once fn settles, whether it returned or threw. Await it, so
	 * the handle closes before the request ends: a connection kept open pins
	 * its shard on the node, and a commit through one that is open when the
	 * node stops is lost. Opening costs tens of microseconds, about what a
	 * local proxy hop costs.
	 *
	 * A withDb inside fn, or alongside it for the same request, rejects: pass
	 * the handle down instead. It rejects when the shard's file does not exist (the path is no longer the shard's), when the
	 * open fails, or when PRAGMAS or onOpen fail, and it never calls fn then. A
	 * close that fails once fn has settled rejects with the close error; if fn
	 * failed, its error wins. It also rejects when fn returns a ReadableStream
	 * or an async iterable, which would be read after the close: read it inside
	 * fn and return the result.
	 */
	withDb<T>(fn: (db: D) => T | Promise<T>): Promise<T> {
		return this.handle.use(fn);
	}
}

export type { Opened, Scope };

// # Calls and durability

/** The init of a call: fetch's, with the options of a call beside it. */
export interface CallInit extends RequestInit {
	/**
	 * Answer 404 rather than activate the target (Shardlite-If-Active), for
	 * presence checks and fan-out to members who may be offline.
	 */
	ifActive?: boolean;
	/**
	 * Answer 404 rather than create the target (Shardlite-If-Exists). Use it
	 * for any call whose id came from a client.
	 */
	ifExists?: boolean;
	/**
	 * Overrides the target kind's durability tier for this call
	 * (Shardlite-Sync).
	 */
	sync?: Sync;
}

/**
 * send makes a call and sends it. from is the headers of the request being
 * served, undefined for a call from the app.
 */
async function send(addr: string, from: HeadersLike | undefined, target: ShardName, path: string, init: CallInit = {}): Promise<Response> {
	if (typeof target !== "string" || target === "") {
		throw new Error("shardlite: a call needs a target");
	}
	if (shardError(target) !== undefined) {
		throw new Error(`shardlite: ${JSON.stringify(target)} is not a valid shard name to call`);
	}
	if (!path.startsWith("/")) {
		throw new Error(`shardlite: the path of a call starts with a slash: ${JSON.stringify(path)}`);
	}
	const { ifActive, ifExists, sync, headers: given, ...rest } = init;
	const headers = new Headers(given);
	for (const name of headers.keys()) {
		if (name.replaceAll("_", "-").startsWith("shardlite-")) {
			throw new Error(`shardlite: init.headers cannot set ${name}: the options of a call and its request's scope set the Shardlite-* headers`);
		}
	}
	if (sync !== undefined && sync !== "none" && sync !== "storage") {
		throw new Error(`shardlite: ${JSON.stringify(sync)} is not a Sync tier`);
	}
	headers.set(HEADER_TARGET, target);
	if (ifActive) {
		headers.set(HEADER_IF_ACTIVE, "true");
	}
	if (ifExists) {
		headers.set(HEADER_IF_EXISTS, "true");
	}
	if (sync !== undefined) {
		headers.set(HEADER_SYNC, sync);
	}
	if (from !== undefined) {
		const context = header(from, HEADER_CONTEXT);
		if (context !== undefined) {
			headers.set(HEADER_CONTEXT, context);
		}
		const traceparent = header(from, "traceparent");
		if (traceparent !== undefined && !headers.has("traceparent")) {
			headers.set("traceparent", traceparent);
		}
	}
	return fetch(`http://${addr}${path}`, { ...rest, headers });
}

// # Node errors and the response's tier

/**
 * Why the node answered with an error of its own (the Shardlite-Error header
 * and the guide's Status codes). Switch over it to tell causes apart. A node
 * newer than the SDK may send a reason that is not here: NodeError.reason is
 * then undefined and rawReason has it.
 *
 * - "bad-request": a malformed shard name or Shardlite-* header, or a call the
 *   node refuses as made.
 * - "unknown-shard": an undeclared kind, a kind a client may not name, or a
 *   shard that does not exist and may not be created.
 * - "not-active": a call with ifActive to a shard that is not active.
 * - "no-endpoint": a path the listener does not serve.
 * - "hops": calls chained past the node's hop limit.
 * - "app-unreachable": the node failing to reach the callee's app, which
 *   never had the request.
 * - "app-failed": the callee's app having the request and the connection
 *   failing.
 * - "owner-unreachable": the shard's owner node not answering, or being dead
 *   and not adopted yet.
 * - "owner-failed": the owner node having the request and the connection
 *   failing.
 * - "moved": the shard moving or being claimed elsewhere meanwhile.
 * - "draining": the node stopping and unable to serve or place the request.
 * - "lease-lost": the node's lease being lost or expired.
 * - "bad-file": the shard's files failing their checksums.
 * - "sync-unmet": the node being unable to meet the durability tier asked
 *   for.
 * - "not-ready": the node's readiness check failing.
 * - "timeout": a timeout inside the node ending the request.
 * - "internal": any other failure of the node itself (storage, say).
 */
export type Reason =
	| "bad-request"
	| "unknown-shard"
	| "not-active"
	| "no-endpoint"
	| "hops"
	| "app-unreachable"
	| "app-failed"
	| "owner-unreachable"
	| "owner-failed"
	| "moved"
	| "draining"
	| "lease-lost"
	| "bad-file"
	| "sync-unmet"
	| "not-ready"
	| "timeout"
	| "internal";

// An entry for every Reason, which tsc keeps complete.
const REASONS: Record<Reason, true> = {
	"bad-request": true,
	"unknown-shard": true,
	"not-active": true,
	"no-endpoint": true,
	hops: true,
	"app-unreachable": true,
	"app-failed": true,
	"owner-unreachable": true,
	"owner-failed": true,
	moved: true,
	draining: true,
	"lease-lost": true,
	"bad-file": true,
	"sync-unmet": true,
	"not-ready": true,
	timeout: true,
	internal: true,
};

const reasonOf = (raw: string): Reason | undefined => (Object.hasOwn(REASONS, raw) ? (raw as Reason) : undefined);

/**
 * NodeError is an error the node answered with, as opposed to the callee app's
 * own. nodeError builds it from a call's response and node.sync throws it, so
 * find it with instanceof. It is called NodeError, not Error, so it does not
 * hide the global.
 *
 *     const res = await opened.call(guilds.shard(id), "/invite");
 *     const nerr = await nodeError(res);
 *     if (nerr?.reason === "lease-lost") {
 *       ...
 *     }
 *
 * The SDK has no retry policy of its own: the node's hint is the signal.
 */
export class NodeError extends Error {
	/** The HTTP status of the response. */
	readonly status: number;
	/**
	 * The node's reason, undefined if the SDK does not know it.
	 */
	readonly reason: Reason | undefined;
	/**
	 * The value of Shardlite-Error as the node sent it, the only record of a
	 * reason that is undefined.
	 */
	readonly rawReason: string;
	/**
	 * The node's Retry-After hint in milliseconds: how long to wait before the
	 * next attempt, which may be zero (try again at once). Undefined when the
	 * node gave none, and then the error says nothing about whether another
	 * attempt can succeed.
	 */
	readonly retryAfterMs: number | undefined;
	/**
	 * The start of the response's body (up to 512 bytes), with surrounding
	 * space trimmed. The message reads as the Go SDK's error does: the
	 * status, the reason, this body and the hint.
	 */
	readonly body: string;

	constructor(init: { status: number; rawReason: string; body: string; retryAfterMs: number | undefined }) {
		let message = `shardlite: the node answered ${init.status} (${init.rawReason})`;
		if (init.body !== "") {
			message += `: ${init.body}`;
		}
		if (init.retryAfterMs !== undefined) {
			message += ` (retry after ${goSeconds(init.retryAfterMs / 1000)})`;
		}
		super(message);
		this.name = "NodeError";
		this.status = init.status;
		this.rawReason = init.rawReason;
		this.reason = reasonOf(init.rawReason);
		this.retryAfterMs = init.retryAfterMs;
		this.body = init.body;
	}
}

/** goSeconds writes whole seconds as Go's time.Duration does (90 is 1m30s). */
function goSeconds(s: number): string {
	const h = Math.floor(s / 3600);
	const m = Math.floor((s % 3600) / 60);
	const rest = `${s % 60}s`;
	if (h > 0) {
		return `${h}h${m}m${rest}`;
	}
	return m > 0 ? `${m}m${rest}` : rest;
}

/**
 * readHead reads up to 512 bytes of a response's body as trimmed text and
 * cancels the rest, so the body is spent. A body that is gone, locked or
 * already read reads as empty.
 */
async function readHead(res: Response): Promise<string> {
	if (res.body === null || res.bodyUsed || res.body.locked) {
		return "";
	}
	const chunks: Uint8Array[] = [];
	let n = 0;
	try {
		const reader = res.body.getReader();
		while (n < 512) {
			const { done, value } = await reader.read();
			if (done) {
				break;
			}
			chunks.push(value);
			n += value.length;
		}
		await reader.cancel();
	} catch {
		// what was read is the message
	}
	const bytes = new Uint8Array(Math.min(n, 512));
	let at = 0;
	for (const c of chunks) {
		const part = c.subarray(0, bytes.length - at);
		bytes.set(part, at);
		at += part.length;
	}
	return new TextDecoder().decode(bytes).trim();
}

/**
 * nodeError reads the node's error from the response of a call. It returns it
 * when the response carries Shardlite-Error, which the node sets on every error
 * of its own and strips from every app response, so undefined means the
 * response is the app's answer.
 *
 * When it returns an error it has read up to 512 bytes of the body into the
 * error's body and cancelled the rest, so the body is spent: do not read it again.
 * When it returns undefined it has not touched the body, which the app reads as
 * it likes.
 *
 * Retry-After is read as the node writes it, a whole number of seconds. A
 * response without one, or with one in another form (a date, say), has no
 * hint.
 */
export async function nodeError(res: Response): Promise<NodeError | undefined> {
	const rawReason = res.headers.get(HEADER_ERROR);
	if (rawReason === null) {
		return undefined;
	}
	const body = await readHead(res);
	const secs = res.headers.get("Retry-After")?.trim() ?? "";
	const ms = /^\d+$/.test(secs) ? Number(secs) * 1000 : NaN;
	return new NodeError({
		status: res.status,
		rawReason,
		body,
		retryAfterMs: Number.isSafeInteger(ms) ? ms : undefined,
	});
}

/**
 * syncHeader returns the header that sets a response's durability tier
 * (Shardlite-Sync), to spread into the headers of its init. On a "none" kind,
 * "storage" makes this acknowledgement wait for the shard's commits to reach
 * blob storage:
 *
 *     new Response("saved", { headers: { ...syncHeader("storage") } })
 *
 * It throws for anything but "none" or "storage", which the types already
 * refuse.
 */
export function syncHeader(s: Sync): Record<string, string> {
	if (s !== "none" && s !== "storage") {
		throw new Error(`shardlite: ${JSON.stringify(s)} is not a Sync tier: use "none" or "storage"`);
	}
	return { [HEADER_SYNC]: s };
}

// # The app's Node

/** Config is what an app tells the SDK about itself, once. */
export interface Config<D extends Database> {
	/** Opens a shard's database. It is required. */
	open: Opener<D>;
	/** host:port of the node's control API, DEFAULT_ADDR if left out. */
	addr?: string;
	/**
	 * The app's own version, opaque to the node, which shows it in its logs.
	 */
	version?: string;
	/** The kinds the app serves. node.ready declares them to the node. */
	kinds?: readonly Kind[];
	/**
	 * Runs once on a request's database after it opens and PRAGMAS run, before
	 * the handler sees it. It is the place to migrate: the busy timeout is set,
	 * so it waits for the node's own bookkeeping commits and does not fail on
	 * them. If it throws, withDb rejects with its error.
	 */
	onOpen?: (db: D, shard: ShardName) => void | Promise<void>;
	/**
	 * Asked by node.ready on every poll. An error makes the app answer not
	 * ready (503) with the error's message, so the node sends it no traffic
	 * during a migration or with a dependency down.
	 */
	health?: () => void | Promise<void>;
}

/**
 * The declaration of one kind, as the node parses it (the guide's Declaration).
 */
interface DeclaredKind {
	public?: true;
	sync?: Sync;
	idle_timeout?: string;
}

/**
 * The app's handle on its node, made by shardlite. It serves the declaration,
 * gives each request a scope to open its database and call from, and makes the
 * calls an app makes without a request.
 */
class Node<D extends Database> {
	/** host:port of the node's control API. */
	readonly addr: string;
	private readonly core: Core<D>;
	private readonly health: (() => void | Promise<void>) | undefined;
	private readonly declaration: string;
	private readonly scopes = new WeakMap<object, Scope<D>>();

	constructor(core: Core<D>, health: (() => void | Promise<void>) | undefined, declaration: string) {
		this.core = core;
		this.addr = core.addr;
		this.health = health;
		this.declaration = declaration;
	}

	/**
	 * ready answers the app's declaration for GET READY_PATH, the route the
	 * node polls for the app's health and kinds, built from the version and the
	 * kinds. It answers 503 with the message of the health function's error
	 * while that throws or rejects:
	 *
	 *     if (url.pathname === READY_PATH) return node.ready();
	 */
	async ready(): Promise<Response> {
		if (this.health !== undefined) {
			try {
				await this.health();
			} catch (e) {
				return new Response(e instanceof Error ? e.message : String(e), {
					status: 503,
					headers: { "content-type": "text/plain; charset=utf-8" },
				});
			}
		}
		return new Response(this.declaration, { headers: { "content-type": "application/json" } });
	}

	/**
	 * request returns the scope of the request whose headers are headers
	 * (req.headers or request.headers). The headers object stands for the
	 * request: the same object gives the same scope, any other gives a new one
	 * with its own handle and busy guard, so a framework that rebuilds headers
	 * splits one request in two. Ask once at the edge of the handler and pass
	 * the scope down. It is called request, not Request, so it does not hide
	 * the global one.
	 *
	 * Use expect or expectCall on it for a route that belongs to a shard. A
	 * route that belongs to none (a login, a webhook) calls and reads the
	 * caller from the scope itself, which has no database.
	 */
	request(headers: HeadersLike): Scope<D> {
		let scope = this.scopes.get(headers);
		if (scope === undefined) {
			scope = new Scope(this.core, headers);
			this.scopes.set(headers, scope);
		}
		return scope;
	}

	/**
	 * callAsApp calls the shard target from the app with no request behind it
	 * (at startup, from a worker or a timer) and sends at once. It carries no
	 * request's context, so the node takes the call as the app's, restarts the
	 * hops and does not hold it for any shard's commits. A call made while
	 * serving a request goes through its scope, which carries the context. The
	 * init, the checks and the return are those of the scope's call.
	 *
	 * It is the one call that may upgrade to a WebSocket.
	 */
	callAsApp(target: ShardName, path: string, init?: CallInit): Promise<Response> {
		return send(this.core.addr, undefined, target, path, init);
	}

	/**
	 * sync waits until everything committed on the node is in blob storage and
	 * the node has confirmed that it still owns its shards, and fails if it has
	 * lost its lease. It is for side effects the node can't see (publishing to
	 * a queue, an external call), called right before them. A write on a kind
	 * that does not wait for storage waits through syncHeader("storage") on its
	 * response instead. Timeouts are the app's: pass a signal.
	 *
	 * It rejects with the node's NodeError for an error of the node's own, with
	 * a plain Error holding the status and the start of the body for any other
	 * non-2xx (a proxy in the way, say), and with an Error that has the fetch
	 * failure as its cause when the node cannot be reached.
	 */
	async sync(signal?: AbortSignal): Promise<void> {
		let res: Response;
		try {
			res = await fetch(`http://${this.core.addr}${SYNC_PATH}`, { method: "POST", ...(signal === undefined ? {} : { signal }) });
		} catch (e) {
			throw new Error(`shardlite: sync: ${(e as Error).message}`, { cause: e });
		}
		if (200 <= res.status && res.status < 300) {
			await res.body?.cancel();
			return;
		}
		const nerr = await nodeError(res);
		if (nerr !== undefined) {
			throw nerr;
		}
		throw new Error(`shardlite: sync: ${res.status} ${res.statusText}: ${await readHead(res)}`);
	}
}

export type { Node };

/**
 * shardlite checks the config and builds the app's Node, which the app keeps
 * for its lifetime. It throws on a missing opener, an invalid kind name (or one
 * with the node's leading underscore), a kind named twice, an unknown sync tier
 * and a negative idle timeout, so a declaration the node would refuse never
 * leaves the app.
 */
export function shardlite<D extends Database>(config: Config<D>): Node<D> {
	if (typeof config.open !== "function") {
		throw new Error("shardlite: config.open is required");
	}
	const kinds = new Map<string, DeclaredKind>();
	for (const k of config.kinds ?? []) {
		if (k.name.startsWith("_")) {
			throw new Error(`shardlite: the kind ${JSON.stringify(k.name)} starts with "_", which is the node's own`);
		}
		if (!validKind(k.name)) {
			throw new Error(`shardlite: ${JSON.stringify(k.name)} is not a valid kind name (lowercase letters, digits, - and _, starting with a letter, in segments joined by dots, up to ${MAX_KIND_LEN} characters)`);
		}
		if (kinds.has(k.name)) {
			throw new Error(`shardlite: the kind ${JSON.stringify(k.name)} is declared twice`);
		}
		if (k.sync !== undefined && k.sync !== "none" && k.sync !== "storage") {
			throw new Error(`shardlite: the kind ${JSON.stringify(k.name)} has the unknown sync ${JSON.stringify(k.sync)}`);
		}
		if (!(k.idleTimeoutMs >= 0) || !Number.isFinite(k.idleTimeoutMs)) {
			throw new Error(`shardlite: the kind ${JSON.stringify(k.name)} needs an idleTimeoutMs of zero or more`);
		}
		const declared: DeclaredKind = {};
		if (k.public) {
			declared.public = true;
		}
		if (k.sync !== undefined) {
			declared.sync = k.sync;
		}
		if (k.idleTimeoutMs > 0) {
			declared.idle_timeout = `${k.idleTimeoutMs}ms`;
		}
		kinds.set(k.name, declared);
	}
	const declaration = JSON.stringify({
		...(config.version ? { version: config.version } : {}),
		kinds: Object.fromEntries(kinds),
	});
	const core: Core<D> = { open: config.open, onOpen: config.onOpen, addr: config.addr ?? DEFAULT_ADDR };
	return new Node(core, config.health, declaration);
}

// # Reminders and deleting a shard

declare const rowBrand: unique symbol;

/**
 * A statement to run in the app's own transaction, with its parameters:
 * positional ? placeholders and plain values (strings and integers), which every
 * SQLite driver binds the same way. It follows Drizzle's toSQL(). Row is the
 * type of a row the statement selects, never at runtime: a statement that
 * selects nothing leaves it as never, and Statement<unknown> accepts any.
 */
export interface Statement<Row = never> {
	readonly sql: string;
	readonly params: readonly (string | number)[];
	readonly [rowBrand]?: Row;
}

/** A day, how long past its due time the node still wakes for a reminder. */
const STALE_REMINDER_MS = 24 * 3600_000;

/**
 * setReminder sets the reminder key to be due at due, or moves it. The
 * statement writes the shard's own reserved table, so run it in the transaction
 * of the work that needs it and it commits with that work. The node wakes the
 * app at REMINDERS_PATH when the reminder is due (to the second). Anything the
 * wake needs is in the app's own tables under the same key.
 *
 * It throws for an invalid Date, which the table would refuse anyway. The node
 * ignores a reminder due more than a day ago. setReminder still returns the
 * statement, so a mistake in the due time does not fail the transaction, and
 * calls console.warn naming the key and the due time. A handler that repeats a
 * reminder sets the next due time from now, not from the old one.
 */
export function setReminder(key: string, due: Date): Statement {
	const ms = due.getTime();
	if (Number.isNaN(ms)) {
		throw new Error(`shardlite: setReminder(${JSON.stringify(key)}): the due time is an invalid Date`);
	}
	if (ms < Date.now() - STALE_REMINDER_MS) {
		console.warn(`shardlite: the reminder ${JSON.stringify(key)} is due at ${due.toISOString()}, more than a day ago, and the node ignores a reminder that old`);
	}
	return {
		sql: "INSERT INTO _shardlite_reminders (key, due) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET due = excluded.due",
		params: [key, Math.floor(ms / 1000)],
	};
}

/**
 * cancelReminder deletes the reminder key. A wake's handler runs it in the
 * transaction of the work the reminder was for, once that work is done, so the
 * reminder goes only with its work.
 */
export function cancelReminder(key: string): Statement {
	return { sql: "DELETE FROM _shardlite_reminders WHERE key = ?", params: [key] };
}

/**
 * dueReminders selects the keys of the reminders that are due now, the earliest
 * first (ties by key), each row an object with a key column. A wake's handler
 * runs it first, then does each key's work and its cancelReminder (or next
 * setReminder) in one transaction. Reading the rows is the app's, since that is
 * the other place drivers differ.
 */
export function dueReminders(): Statement<{ key: string }> {
	return { sql: "SELECT key FROM _shardlite_reminders WHERE due <= unixepoch() ORDER BY due, key", params: [] };
}

/**
 * markDeleted marks the shard for deletion: when it next deactivates, its data
 * goes, and a later call for the same id creates a new, empty shard. Run the
 * statement in the transaction that decides the shard is finished (the last
 * player leaving a lobby) and it commits with that decision. The shard is
 * served as usual until it deactivates.
 */
export function markDeleted(): Statement {
	return { sql: "INSERT OR IGNORE INTO _shardlite_delete VALUES (1)", params: [] };
}

/**
 * unmarkDeleted takes the marker back, so the shard is kept when it
 * deactivates. It does nothing for a shard that is not marked.
 */
export function unmarkDeleted(): Statement {
	return { sql: "DELETE FROM _shardlite_delete", params: [] };
}

/**
 * isMarkedDeleted selects whether the shard is marked for deletion: one row
 * with a marked column, 1 or 0. A handler asks it to refuse requests for a
 * finished shard that is still served.
 */
export function isMarkedDeleted(): Statement<{ marked: 0 | 1 }> {
	return { sql: "SELECT EXISTS (SELECT 1 FROM _shardlite_delete) AS marked", params: [] };
}
