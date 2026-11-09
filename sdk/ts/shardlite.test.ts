// The SDK's tests, on node:sqlite alone (no driver from npm): the config's
// refusals, the declaration, shard names, the scope of a request and what it
// expects, withDb over the three driver shapes the Database interface admits
// (synchronous, promise-returning and callback-style through fromCallbacks),
// calls against a real node:http server, the node's errors and the response's
// tier, and the statements against the reserved tables as the node creates
// them.

import { test } from "node:test";
import type { TestContext } from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { createServer } from "node:http";
import type { IncomingHttpHeaders, Server } from "node:http";
import type { AddressInfo } from "node:net";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import {
	DEFAULT_ADDR,
	HEADER_CONTEXT,
	HEADER_TARGET,
	READY_PATH,
	REMINDERS_PATH,
	NodeError,
	Refusal,
	cancelReminder,
	dueReminders,
	fromCallbacks,
	isMarkedDeleted,
	kind,
	markDeleted,
	nodeError,
	parseShard,
	setReminder,
	shardlite,
	syncHeader,
	unmarkDeleted,
} from "./shardlite.ts";
import type { Caller, Database, Node, Opened, Opener, Reason, Scope, ShardName, Statement, Sync } from "./shardlite.ts";

// The pragmas the SDK runs, written out here since the SDK keeps them private.
const PRAGMAS = "PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;";

const users = kind("users", { public: true });
const guild = kind("guild");

// The reserved tables exactly as the node creates them at activation.
const RESERVED = `
CREATE TABLE _shardlite_reminders (
  key TEXT PRIMARY KEY,
  due INTEGER NOT NULL
) STRICT;
CREATE INDEX _shardlite_reminders_due ON _shardlite_reminders (due);
CREATE TABLE _shardlite_delete (marked INTEGER PRIMARY KEY CHECK (marked = 1)) STRICT;
`;

// A shard's file as the node hands its path out, with the reserved tables.
function shardFile(name = "users-42.k3x9.7f3a9c1e5b20.db"): string {
	const dir = fs.mkdtempSync(path.join(os.tmpdir(), "shardlite-ts-"));
	const file = path.join(dir, name);
	const db = new DatabaseSync(file);
	db.exec(RESERVED);
	db.close();
	return file;
}

const CONTEXT = 'shard="users/42", hops=2';
const TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

// The headers the node sends the app with a shard opened. A null caller is a
// client's request, which the node sets none on.
function nodeHeaders(file: string, caller: string | null = "users/42"): Headers {
	const h = new Headers({
		"Shardlite-Opened": "users/42",
		"Shardlite-File": file,
		"Shardlite-Context": CONTEXT,
		traceparent: TRACEPARENT,
	});
	if (caller !== null) {
		h.set("Shardlite-Caller", caller);
	}
	return h;
}

// The file: URI that opens a file read-write without creating it, written here
// for the tests that open the file themselves.
const fileUri = (file: string): string =>
	`file:${file.replaceAll("%", "%25").replaceAll("?", "%3f").replaceAll("#", "%23")}?mode=rw`;

// node:sqlite returns rows as objects with no prototype, which deepEqual keeps
// apart from object literals, so the row assertions read them through this.
const plain = (rows: unknown[]): unknown[] => rows.map((r) => ({ ...(r as object) }));

// A refusal is a result and not an exception, so the tests that need the
// opened shard narrow it here.
function must<T extends object>(result: T | Refusal): T {
	if (result instanceof Refusal) {
		throw result;
	}
	return result;
}

const openSqlite: Opener<DatabaseSync> = ({ uri }) => new DatabaseSync(uri);

// A node whose handler is the first argument of each test's server.
const nodeFor = (addr = DEFAULT_ADDR): Node<DatabaseSync> => shardlite({ open: openSqlite, addr });

// The three driver shapes withDb asks so little of, each wrapped around
// node:sqlite and recording what the SDK did with it.
interface Kit {
	open: Opener<Database>;
	opens(): number;
	execs(): string[];
	last(): DatabaseSync; // the underlying handle of the last open
}

function kit(label: string, wrap: (db: DatabaseSync, execs: string[]) => Database): { label: string; make: () => Kit } {
	return {
		label,
		make: () => {
			const dbs: DatabaseSync[] = [];
			const execs: string[] = [];
			return {
				open: ({ uri }) => {
					const db = new DatabaseSync(uri);
					dbs.push(db);
					return wrap(db, execs);
				},
				opens: () => dbs.length,
				execs: () => execs,
				last: () => dbs[dbs.length - 1] as DatabaseSync,
			};
		},
	};
}

const shapes = [
	kit("node:sqlite, synchronous", (db, execs) => {
		const orig = db.exec.bind(db);
		db.exec = (sql: string): void => {
			execs.push(sql);
			orig(sql);
		};
		return db;
	}),
	kit("a promise wrapper, as an async driver", (db, execs) => {
		const orig = db.exec.bind(db);
		db.exec = (sql: string): Promise<void> => {
			execs.push(sql);
			orig(sql);
			return Promise.resolve();
		};
		return db;
	}),
	kit("a callback wrapper, through fromCallbacks", (db, execs) =>
		fromCallbacks({
			exec(sql, cb) {
				execs.push(sql);
				try {
					db.exec(sql);
					cb(null);
				} catch (e) {
					cb(e as Error);
				}
			},
			close(cb) {
				try {
					db.close();
					cb(null);
				} catch (e) {
					cb(e as Error);
				}
			},
		}),
	),
];

// The config.
test("the node's address defaults to the control API's", () => {
	assert.equal(DEFAULT_ADDR, "127.0.0.1:2282");
	assert.equal(shardlite({ open: openSqlite }).addr, DEFAULT_ADDR);
	assert.equal(shardlite({ open: openSqlite, addr: "10.0.0.1:9" }).addr, "10.0.0.1:9");
});

test("shardlite refuses what the node would refuse, at startup", () => {
	const withKinds = (...kinds: ReturnType<typeof kind>[]) => (): unknown => shardlite({ open: openSqlite, kinds });
	assert.throws(() => shardlite({} as never), /config\.open is required/);
	assert.throws(() => shardlite({ open: "no" } as never), /config\.open is required/);
	for (const bad of ["Users", "1users", "", "a b", "users/x", "a..b", "a.", ".a", "a.1b", "x".repeat(65)]) {
		assert.throws(withKinds(kind(bad)), /is not a valid kind name/, bad);
	}
	assert.throws(withKinds(kind("_shardlite")), /starts with "_"/);
	assert.throws(withKinds(kind("users"), kind("guild"), kind("users", { public: true })), /"users" is declared twice/);
	assert.throws(withKinds(kind("users", { sync: "disk" as Sync })), /unknown sync "disk"/);
	assert.throws(withKinds(kind("users", { idleTimeoutMs: -1 })), /idleTimeoutMs of zero or more/);
	assert.throws(withKinds(kind("users", { idleTimeoutMs: Number.NaN })), /idleTimeoutMs of zero or more/);

	// and what it accepts: the longest kind, dotted kinds, and none at all
	shardlite({ open: openSqlite, kinds: [kind("a".repeat(64)), kind("matchmaker.queue"), kind("a-b_c.d9")] });
	shardlite({ open: openSqlite });
});

test("ready serves the declaration built from the version and the kinds", async () => {
	const node = shardlite({
		open: openSqlite,
		version: "2026.10.0",
		kinds: [
			kind("users", { public: true, idleTimeoutMs: 600_000 }),
			kind("guild"),
			kind("chat", { public: true, sync: "none" }),
			kind("matchmaker.queue", { sync: "storage" }),
		],
	});
	const res = await node.ready();
	assert.equal(res.status, 200);
	assert.equal(res.headers.get("content-type"), "application/json");
	assert.deepEqual(await res.json(), {
		version: "2026.10.0",
		kinds: {
			users: { public: true, idle_timeout: "600000ms" },
			guild: {},
			chat: { public: true, sync: "none" },
			"matchmaker.queue": { sync: "storage" },
		},
	});

	// a field at its default is left out, so the node applies its own
	assert.equal(await (await shardlite({ open: openSqlite }).ready()).text(), '{"kinds":{}}');
	assert.equal(await (await shardlite({ open: openSqlite, kinds: [kind("a", { public: false, idleTimeoutMs: 0 })] }).ready()).text(), '{"kinds":{"a":{}}}');
});

test("ready answers 503 with the health function's message while it fails", async () => {
	let failure: unknown = new Error("migrating");
	const node = shardlite({
		open: openSqlite,
		kinds: [users],
		health: async () => {
			if (failure !== undefined) {
				throw failure;
			}
		},
	});
	let res = await node.ready();
	assert.equal(res.status, 503);
	assert.equal(await res.text(), "migrating");

	failure = "the cache is down"; // not an Error
	res = await node.ready();
	assert.equal(res.status, 503);
	assert.equal(await res.text(), "the cache is down");

	failure = undefined;
	res = await node.ready();
	assert.equal(res.status, 200);
	assert.deepEqual(await res.json(), { kinds: { users: { public: true } } });

	// a health function that throws synchronously is the same
	const sync = shardlite({
		open: openSqlite,
		health: () => {
			throw new Error("down");
		},
	});
	assert.equal((await sync.ready()).status, 503);
});

// Shard names.
test("kind.shard and parseShard build and read names of the grammar", () => {
	assert.equal(users.shard("42"), "users/42");
	assert.equal(kind("matchmaker.queue").shard("eu_1-b"), "matchmaker.queue/eu_1-b");
	for (const good of ["users/42", "a/B", "a-b_c.d9/Id_-9", "matchmaker.queue/0", `${"a".repeat(64)}/${"1".repeat(128)}`]) {
		assert.equal(parseShard(good), good);
	}
	for (const bad of [
		"users", // no id
		"users/", // an empty id
		"/42", // no kind
		"Users/42", // a capital
		"_node/42", // the node's own
		"users/4 2",
		"users/4/2",
		"users/42\n", // a newline after a good name
		"users/é",
		`${"a".repeat(65)}/1`,
		`users/${"1".repeat(129)}`,
		"a..b/1",
		"",
	]) {
		assert.throws(() => parseShard(bad), /shardlite: .*(is not kind\/id|has no valid kind|needs an id)/, JSON.stringify(bad));
	}
	// kind.shard checks nothing: a call to a bad name rejects before it is sent
	assert.equal(users.shard("a/b"), "users/a/b");
});

// The scope of a request.
test("a scope is shared for one headers object", () => {
	const node = nodeFor();
	const file = shardFile();
	const headers = nodeHeaders(file);
	assert.equal(node.request(headers), node.request(headers));
	assert.notEqual(node.request(headers), node.request(nodeHeaders(file)));
	const plainObject = { "shardlite-opened": "users/42", "shardlite-file": file };
	assert.equal(node.request(plainObject), node.request(plainObject));
	assert.notEqual(nodeFor().request(headers), node.request(headers)); // another node, another scope
});

test("caller reads the client, the app and a shard", () => {
	const node = nodeFor();
	const file = shardFile();
	const client: Caller = { kind: "client" };
	assert.deepEqual(node.request(nodeHeaders(file, null)).caller(), client);
	assert.deepEqual(node.request(new Headers()).caller(), client);
	assert.deepEqual(node.request(nodeHeaders(file, "app")).caller(), { kind: "app" });
	assert.deepEqual(node.request(nodeHeaders(file, "guild/7")).caller(), { kind: "shard", shard: guild.shard("7") });
	assert.deepEqual(node.request({ "shardlite-caller": "users/42" }).caller(), { kind: "shard", shard: "users/42" }); // Node's header objects

	// a value the node would never write reads as the client
	for (const odd of ["", "App", "users", "Users/1", "users/", "users/1/2", "_node/1"]) {
		assert.deepEqual(node.request(nodeHeaders(file, odd)).caller(), client, JSON.stringify(odd));
	}
});

test("opened names the opened shard for logging, and is undefined for a shardless request", () => {
	const node = nodeFor();
	const file = shardFile();
	assert.equal(node.request(nodeHeaders(file)).opened(), "users/42");
	assert.equal(node.request(nodeHeaders(file, null)).opened(), "users/42"); // a client's request has one too
	assert.equal(node.request({ "shardlite-opened": ["users/42"], "shardlite-file": [file] }).opened(), "users/42");
	assert.equal(node.request(new Headers()).opened(), undefined);
	assert.equal(node.request(new Headers({ "Shardlite-Opened": "users/42" })).opened(), undefined); // no file
	assert.equal(node.request(new Headers({ "Shardlite-File": file })).opened(), undefined); // no shard
	assert.equal(node.request(new Headers({ "Shardlite-Opened": "nonsense", "Shardlite-File": file })).opened(), undefined);
});

test("expect returns the opened shard on a match", () => {
	const node = nodeFor();
	const file = shardFile();
	const scope = node.request(nodeHeaders(file, null));
	const opened = must(scope.expect(users.shard("42")));
	assert.equal(opened.name, "users/42");
	assert.deepEqual(opened.caller(), { kind: "client" });
});

test("expect refuses another shard, another kind and a shardless request with a 400", () => {
	const node = nodeFor();
	const file = shardFile();
	const scope = node.request(nodeHeaders(file, null));

	for (const other of [users.shard("43"), guild.shard("42")]) {
		const ref = scope.expect(other);
		assert.ok(ref instanceof Refusal);
		assert.equal(ref.status, 400);
		assert.equal(ref.message, `the request is for ${other} but the node opened users/42: send Shardlite-Target naming the shard`);
	}

	for (const headers of [new Headers(), new Headers({ "Shardlite-Opened": "users/42" }), { "x-other": "1" }]) {
		const ref = node.request(headers).expect(users.shard("42"));
		assert.ok(ref instanceof Refusal);
		assert.equal(ref.status, 400);
		assert.match(ref.message, /^the request is for users\/42 but names no shard to the node/);
	}
});

test("a Refusal is an Error with a status, which an app may throw to its own handler", () => {
	const ref = nodeFor().request(new Headers()).expect(users.shard("42"));
	assert.ok(ref instanceof Refusal);
	assert.ok(ref instanceof Error);
	assert.equal(ref.name, "Refusal");
	assert.equal(ref.status, 400);
	assert.throws(() => {
		throw ref;
	}, (e: unknown) => e instanceof Refusal && e.status === 400);
	assert.equal(new Refusal(409, "taken").status, 409);
});

test("expectCall refuses a client with 403 and takes the shard from what the node opened", () => {
	const node = nodeFor();
	const file = shardFile();

	const client = node.request(nodeHeaders(file, null)).expectCall();
	assert.ok(client instanceof Refusal);
	assert.equal(client.status, 403);
	assert.match(client.message, /for calls from the app and its shards, not for clients/);

	const app = must(node.request(nodeHeaders(file, "app")).expectCall());
	assert.equal(app.name, "users/42");
	assert.deepEqual(app.caller(), { kind: "app" });

	const shard = must(node.request(nodeHeaders(file, "guild/7")).expectCall());
	assert.equal(shard.name, "users/42");
	assert.deepEqual(shard.caller(), { kind: "shard", shard: "guild/7" });

	// a call with no shard opened is a 400, and a client is refused first
	const shardless = node.request(new Headers({ "Shardlite-Caller": "app" })).expectCall();
	assert.ok(shardless instanceof Refusal);
	assert.equal(shardless.status, 400);
	assert.match(shardless.message, /Shardlite-Opened/);
	const clientShardless = node.request(new Headers()).expectCall();
	assert.ok(clientShardless instanceof Refusal);
	assert.equal(clientShardless.status, 403);
});

// withDb, over each driver shape.
for (const shape of shapes) {
	test(`withDb (${shape.label}) opens once, runs the pragmas and closes when fn returns`, async () => {
		const file = shardFile();
		const k = shape.make();
		const node = shardlite({ open: k.open });
		const opened = must(node.request(nodeHeaders(file)).expect(users.shard("42")));
		const got = await opened.withDb((db) => {
			assert.ok(db);
			assert.equal(k.opens(), 1);
			return "value";
		});
		assert.equal(got, "value");
		assert.equal(k.opens(), 1);
		assert.deepEqual(k.execs(), [PRAGMAS]);
		await assert.rejects(async () => k.last().exec("SELECT 1"), /database is not open/);

		// the pragmas took effect: the file is in WAL mode
		const raw = new DatabaseSync(fileUri(file));
		const mode = raw.prepare("PRAGMA journal_mode").get() as { journal_mode: string };
		assert.equal(mode.journal_mode, "wal");
		raw.close();
	});

	test(`withDb (${shape.label}) closes when fn throws, and fn's error wins`, async () => {
		const file = shardFile();
		const k = shape.make();
		const opened = must(shardlite({ open: k.open }).request(nodeHeaders(file)).expectCall());
		await assert.rejects(
			opened.withDb(async () => {
				throw new Error("boom");
			}),
			/boom/,
		);
		await assert.rejects(async () => k.last().exec("SELECT 1"), /database is not open/);
	});

	test(`withDb (${shape.label}) refuses a missing file and creates nothing`, async () => {
		const file = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "shardlite-ts-")), "gone.db");
		const k = shape.make();
		const opened = must(shardlite({ open: k.open }).request(nodeHeaders(file)).expectCall());
		await assert.rejects(
			opened.withDb(() => assert.fail("fn must not run")),
			/does not exist, so the path is no longer the shard's/,
		);
		assert.equal(k.opens(), 0);
		assert.ok(!fs.existsSync(file));
	});
}

const closeFails: Database = {
	exec: () => undefined,
	close: () => {
		throw new Error("close failed");
	},
};

test("withDb rejects with the close error once fn has settled", async () => {
	const opened = must(shardlite({ open: () => closeFails }).request(nodeHeaders(shardFile())).expectCall());
	await assert.rejects(opened.withDb(() => "ok"), /close failed/);
});

test("withDb keeps fn's error over the close error", async () => {
	const opened = must(shardlite({ open: () => closeFails }).request(nodeHeaders(shardFile())).expectCall());
	await assert.rejects(
		opened.withDb(async () => {
			throw new Error("boom");
		}),
		/boom/,
	);
});

test("withDb closes the handle when the pragmas fail, and never calls fn", async () => {
	let closed = false;
	const db: Database = {
		exec: () => {
			throw new Error("the driver refused the pragmas");
		},
		close: () => {
			closed = true;
		},
	};
	const opened = must(shardlite({ open: () => db }).request(nodeHeaders(shardFile())).expectCall());
	await assert.rejects(opened.withDb(() => assert.fail("fn must not run")), /refused the pragmas/);
	assert.ok(closed);
});

test("fromCallbacks passes a callback's error on as a rejection", async () => {
	const db = fromCallbacks({
		exec(_sql, cb) {
			cb(new Error("exec failed"));
		},
		close(cb) {
			cb(null);
		},
	});
	await assert.rejects(async () => {
		await db.exec("SELECT 1");
	}, /exec failed/);
	await db.close(); // a null error resolves
});

test("onOpen runs once per open, after the pragmas, with the handle and the shard", async () => {
	const file = shardFile();
	const seen: { timeout: unknown; name: ShardName }[] = [];
	const node = shardlite({
		open: openSqlite,
		onOpen: (db, name) => {
			// the busy timeout is the pragmas', so a migration waits for the node
			const row = db.prepare("PRAGMA busy_timeout").get() as { timeout: unknown };
			seen.push({ timeout: row.timeout, name });
		},
	});
	const opened = must(node.request(nodeHeaders(file)).expect(users.shard("42")));
	await opened.withDb(() => {
		assert.equal(seen.length, 1); // before the handler sees the handle
	});
	assert.deepEqual(seen, [{ timeout: 5000, name: "users/42" }]);

	// an async onOpen is awaited
	let migrated = false;
	const slow = shardlite({
		open: openSqlite,
		onOpen: async () => {
			await Promise.resolve();
			migrated = true;
		},
	});
	await must(slow.request(nodeHeaders(file)).expectCall()).withDb(() => assert.ok(migrated));
});

test("an error from onOpen rejects withDb, closes the handle and never calls fn", async () => {
	let handle: DatabaseSync | undefined;
	const node = shardlite({
		open: openSqlite,
		onOpen: (db) => {
			handle = db;
			throw new Error("migration failed");
		},
	});
	const opened = must(node.request(nodeHeaders(shardFile())).expectCall());
	await assert.rejects(opened.withDb(() => assert.fail("fn must not run")), /migration failed/);
	assert.ok(handle);
	assert.throws(() => handle?.exec("SELECT 1"), /database is not open/);
});

test("a nested withDb is refused, and the outer one keeps its handle", async () => {
	let opens = 0;
	const node = shardlite({
		open: (file) => {
			opens++;
			return openSqlite(file) as DatabaseSync;
		},
	});
	const headers = nodeHeaders(shardFile());
	const outer = must(node.request(headers).expect(users.shard("42")));
	const again = must(node.request(headers).expectCall()); // the same scope
	const result = await outer.withDb(async (db) => {
		await assert.rejects(again.withDb(() => assert.fail("fn must not run")), /already open for this request/);
		await assert.rejects(outer.withDb(() => assert.fail("fn must not run")), /already open for this request/);
		db.exec("SELECT 1"); // the refusals did not close it
		return "outer";
	});
	assert.equal(result, "outer");
	assert.equal(opens, 1);

	// once the first has settled, a withDb opens afresh
	let kept: DatabaseSync | undefined;
	await outer.withDb((db) => {
		kept = db;
	});
	assert.equal(opens, 2);
	assert.throws(() => kept?.exec("SELECT 1"), /database is not open/);
});

test("a withDb alongside another is refused", async () => {
	let opens = 0;
	const node = shardlite({
		open: (file) => {
			opens++;
			return openSqlite(file) as DatabaseSync;
		},
	});
	const opened = must(node.request(nodeHeaders(shardFile())).expectCall());
	const [a, b] = await Promise.allSettled([opened.withDb(() => "a"), opened.withDb(() => "b")]);
	assert.deepEqual(a, { status: "fulfilled", value: "a" });
	assert.equal(b.status, "rejected");
	assert.match(String((b as PromiseRejectedResult).reason), /already open for this request/);
	assert.equal(opens, 1);
});

test("a withDb whose open fails leaves the scope free for the next", async () => {
	const file = shardFile();
	const opened = must(nodeFor().request(nodeHeaders(file)).expectCall());
	fs.rmSync(file);
	await assert.rejects(opened.withDb(() => assert.fail("fn must not run")), /does not exist/);
	await assert.rejects(opened.withDb(() => assert.fail("fn must not run")), /does not exist/); // not "already open"
});

test("withDb rejects when fn returns a stream, and closes the handle", async () => {
	let handle: DatabaseSync | undefined;
	const node = shardlite({
		open: (file) => (handle = openSqlite(file) as DatabaseSync),
	});
	const opened = must(node.request(nodeHeaders(shardFile())).expectCall());

	await assert.rejects(opened.withDb(() => new ReadableStream()), /returned a stream.*read it inside/);
	assert.throws(() => handle?.exec("SELECT 1"), /database is not open/);

	async function* rows(): AsyncGenerator<number> {
		yield 1;
	}
	await assert.rejects(opened.withDb(() => rows()), /returned a stream/);
	await assert.rejects(opened.withDb(async () => new ReadableStream()), /returned a stream/);
	await assert.rejects(opened.withDb(async (db) => ({ rows: [db], stream: new ReadableStream() }).stream), /returned a stream/);

	// what was read inside fn is fine, and so is a sync iterable
	assert.deepEqual(await opened.withDb(() => [1, 2, 3]), [1, 2, 3]);
	assert.equal(await opened.withDb(() => new Response("body").text()), "body");
});

test("the opener's uri opens a path with %, ? and # in it", async () => {
	const file = shardFile("odd%name?x#y.db");
	let seen: { path: string; uri: string } | undefined;
	const node = shardlite({
		open: (f) => {
			seen = f;
			return new DatabaseSync(f.uri);
		},
	});
	const opened = must(node.request(nodeHeaders(file)).expectCall());
	const count = await opened.withDb((db) => (db.prepare("SELECT count(*) AS n FROM _shardlite_reminders").get() as { n: number }).n);
	assert.equal(count, 0); // the reserved table is there, so it is the shard's file
	assert.ok(seen);
	assert.equal(seen.path, file);
	assert.ok(seen.uri.startsWith("file:/"));
	assert.ok(seen.uri.endsWith("/odd%25name%3fx%23y.db?mode=rw"), seen.uri);

	// mode=rw: a path that is not there fails to open and creates nothing
	const missing = path.join(path.dirname(file), "missing.db");
	assert.throws(() => new DatabaseSync(`file:${missing}?mode=rw`));
	assert.ok(!fs.existsSync(missing));
});

// Calls and sync, against a real server as the node's control API is one.
interface Seen {
	method: string | undefined;
	url: string | undefined;
	headers: IncomingHttpHeaders;
	body: string;
}

// A server that records each request and answers 200, so a test reads what the
// SDK sent once the call has resolved.
function startServer(): Promise<{ server: Server; addr: string; seen: Seen[] }> {
	const seen: Seen[] = [];
	const server = createServer((req, res) => {
		const chunks: Buffer[] = [];
		req.on("data", (c) => chunks.push(c));
		req.on("end", () => {
			seen.push({ method: req.method, url: req.url, headers: req.headers, body: Buffer.concat(chunks).toString() });
			res.writeHead(200, { "content-type": "text/plain" });
			res.end("ok");
		});
	});
	return new Promise((resolve) => {
		server.listen(0, "127.0.0.1", () => {
			const ai = server.address() as AddressInfo;
			resolve({ server, addr: `${ai.address}:${ai.port}`, seen });
		});
	});
}

const stop = (server: Server): Promise<void> =>
	new Promise<void>((resolve) => {
		server.closeAllConnections();
		server.close(() => resolve());
	});

test("a scope's call sets the target and carries the context and traceparent", async () => {
	const { server, addr, seen } = await startServer();
	try {
		const node = nodeFor(addr);
		const file = shardFile();
		const scope = node.request(nodeHeaders(file));
		const res = await scope.call(guild.shard("7"), "/invite?src=ui", {
			method: "POST",
			body: "hello",
			headers: { "Idempotency-Key": "k1" },
			ifExists: true,
		});
		assert.equal(res.status, 200);
		assert.equal(await res.text(), "ok");

		const first = seen[0];
		assert.ok(first);
		assert.equal(first.method, "POST");
		assert.equal(first.url, "/invite?src=ui");
		assert.equal(first.headers["shardlite-target"], "guild/7");
		assert.equal(first.headers["shardlite-context"], CONTEXT);
		assert.equal(first.headers["traceparent"], TRACEPARENT);
		assert.equal(first.headers["shardlite-if-exists"], "true");
		assert.equal(first.headers["idempotency-key"], "k1");
		assert.equal(first.body, "hello");
		assert.equal(first.headers["shardlite-caller"], undefined); // the caller comes from the context, which the app never sets by hand

		// the same from the opened shard, with the method defaulting to GET
		const opened = must(scope.expect(users.shard("42")));
		await opened.call(guild.shard("8"), "/info");
		const second = seen[1];
		assert.ok(second);
		assert.equal(second.method, "GET");
		assert.equal(second.headers["shardlite-target"], "guild/8");
		assert.equal(second.headers["shardlite-context"], CONTEXT);
		assert.equal(second.headers["traceparent"], TRACEPARENT);

		// from Node's header objects, as an app under node:http has them
		await node.request({ "shardlite-context": CONTEXT, traceparent: TRACEPARENT }).call(guild.shard("7"), "/invite");
		const third = seen[2];
		assert.ok(third);
		assert.equal(third.headers["shardlite-context"], CONTEXT);
		assert.equal(third.headers["traceparent"], TRACEPARENT);

		// a client's request has no context of its own, so the call has none
		await node.request(new Headers()).call(guild.shard("7"), "/invite");
		const fourth = seen[3];
		assert.ok(fourth);
		assert.equal(fourth.headers["shardlite-context"], undefined);
		assert.equal(fourth.headers["traceparent"], undefined);

		// the app's own traceparent on the call wins, as the app's header does
		await scope.call(guild.shard("7"), "/invite", { headers: { traceparent: "mine" } });
		assert.equal(seen[4]?.headers["traceparent"], "mine");
	} finally {
		await stop(server);
	}
});

test("callAsApp carries neither the context nor a traceparent", async () => {
	const { server, addr, seen } = await startServer();
	try {
		const res = await nodeFor(addr).callAsApp(guild.shard("7"), "/tick", { method: "POST", ifActive: true, signal: AbortSignal.timeout(5000) });
		assert.equal(res.status, 200);
		const req = seen[0];
		assert.ok(req);
		assert.equal(req.method, "POST");
		assert.equal(req.url, "/tick");
		assert.equal(req.headers["shardlite-target"], "guild/7");
		assert.equal(req.headers["shardlite-if-active"], "true");
		assert.equal(req.headers["shardlite-context"], undefined);
		assert.equal(req.headers["traceparent"], undefined);
	} finally {
		await stop(server);
	}
});

test("each call option sets its header, and an option left out sets none", async () => {
	const { server, addr, seen } = await startServer();
	try {
		const scope = nodeFor(addr).request(new Headers());
		const target = guild.shard("7");
		const headerOf = (name: string, i: number): string | string[] | undefined => seen[i]?.headers[name];

		await scope.call(target, "/x", { ifActive: true });
		assert.equal(headerOf("shardlite-if-active", 0), "true");
		assert.equal(headerOf("shardlite-if-exists", 0), undefined);
		assert.equal(headerOf("shardlite-sync", 0), undefined);

		await scope.call(target, "/x", { ifExists: true });
		assert.equal(headerOf("shardlite-if-exists", 1), "true");
		assert.equal(headerOf("shardlite-if-active", 1), undefined);

		await scope.call(target, "/x", { sync: "none" });
		assert.equal(headerOf("shardlite-sync", 2), "none");
		await scope.call(target, "/x", { sync: "storage" });
		assert.equal(headerOf("shardlite-sync", 3), "storage");

		await scope.call(target, "/x", { ifActive: false, ifExists: false });
		assert.equal(headerOf("shardlite-if-active", 4), undefined);
		assert.equal(headerOf("shardlite-if-exists", 4), undefined);

		// none of the options reaches the node as a field of its own
		assert.equal(seen.length, 5);
	} finally {
		await stop(server);
	}
});

test("a call refuses a Shardlite-* header, in any case and with _ for -, before sending", async () => {
	const { server, addr, seen } = await startServer();
	try {
		const scope = nodeFor(addr).request(nodeHeaders(shardFile()));
		const target = guild.shard("7");
		for (const headers of [
			{ "Shardlite-Caller": "app" },
			{ "shardlite-context": 'shard="users/1", hops=0' },
			{ SHARDLITE_TARGET: "users/1" },
			{ Shardlite_Caller: "app" },
			{ "shardlite_if_active": "yes" },
			{ "Shardlite-Sync": "disk" },
			new Headers([["Shardlite-If-Active", "yes"]]),
			[["Shardlite-File", "/etc/passwd"]] as [string, string][],
		]) {
			await assert.rejects(scope.call(target, "/x", { headers }), /init\.headers cannot set shardlite[-_]/i);
		}
		await assert.rejects(nodeFor(addr).callAsApp(target, "/x", { headers: { "Shardlite-Context": "x" } }), /cannot set/);
		assert.equal(seen.length, 0);

		// a header that only starts like it is the app's
		await scope.call(target, "/x", { headers: { "Shardlite": "1", "X-Shardlite-Id": "2" } });
		assert.equal(seen.length, 1);
	} finally {
		await stop(server);
	}
});

test("a call rejects an invalid target, path or sync tier before sending", async () => {
	const { server, addr, seen } = await startServer();
	try {
		const node = nodeFor(addr);
		const scope = node.request(new Headers());
		const target = guild.shard("7");
		await assert.rejects(scope.call("" as ShardName, "/x"), /needs a target/);
		await assert.rejects(scope.call(undefined as never, "/x"), /needs a target/);
		for (const bad of ["guild", "guild/", "Guild/7", "guild/a/b", "guild/a b", `guild/${"1".repeat(129)}`, `${"a".repeat(65)}/1`]) {
			await assert.rejects(scope.call(bad as ShardName, "/x"), /is not a valid shard name to call/, bad);
			await assert.rejects(node.callAsApp(bad as ShardName, "/x"), /is not a valid shard name to call/, bad);
		}
		await assert.rejects(scope.call(target, "invite"), /starts with a slash/);
		await assert.rejects(node.callAsApp(target, ""), /starts with a slash/);
		await assert.rejects(scope.call(target, "/x", { sync: "disk" as Sync }), /not a Sync tier/);
		assert.equal(seen.length, 0);
	} finally {
		await stop(server);
	}
});

// A server that answers every request with the given response, for the tests
// of what sync and nodeError read.
async function answerWith(status: number, headers: Record<string, string>, body: string): Promise<{ server: Server; addr: string }> {
	const server = createServer((_req, res) => {
		res.writeHead(status, headers);
		res.end(body);
	});
	const addr = await new Promise<string>((resolve) => {
		server.listen(0, "127.0.0.1", () => {
			const ai = server.address() as AddressInfo;
			resolve(`${ai.address}:${ai.port}`);
		});
	});
	return { server, addr };
}

test("sync resolves on 2xx", async () => {
	const { server, addr } = await answerWith(204, {}, "");
	try {
		await nodeFor(addr).sync();
	} finally {
		await stop(server);
	}
});

test("sync throws the node's error, with its reason and retry hint", async () => {
	const { server, addr } = await answerWith(503, { "Shardlite-Error": "lease-lost", "Retry-After": "1" }, "the lease is gone\n");
	try {
		const err = await nodeFor(addr).sync(AbortSignal.timeout(5000)).then(
			() => undefined,
			(e: unknown) => e,
		);
		assert.ok(err instanceof NodeError, String(err));
		assert.ok(err instanceof Error);
		assert.equal(err.status, 503);
		assert.equal(err.reason, "lease-lost");
		assert.equal(err.rawReason, "lease-lost");
		assert.equal(err.body, "the lease is gone");
		assert.equal(err.message, "shardlite: the node answered 503 (lease-lost): the lease is gone (retry after 1s)");
		assert.equal(err.retryAfterMs, 1000);
	} finally {
		await stop(server);
	}
});

test("sync without the node's error is a plain error with the status and the body", async () => {
	// A proxy in the way answers, not the node.
	const { server, addr } = await answerWith(502, {}, "bad gateway\n");
	try {
		const err = await nodeFor(addr).sync().then(
			() => undefined,
			(e: unknown) => e,
		);
		assert.ok(err instanceof Error);
		assert.ok(!(err instanceof NodeError));
		assert.match(err.message, /502/);
		assert.match(err.message, /bad gateway$/);
	} finally {
		await stop(server);
	}
});

test("sync wraps a fetch failure, keeping it as the cause", async () => {
	// The port of a server that has stopped refuses the connection.
	const { server, addr } = await answerWith(204, {}, "");
	await stop(server);
	const err = await nodeFor(addr).sync().then(
		() => undefined,
		(e: unknown) => e,
	);
	assert.ok(err instanceof Error);
	assert.ok(!(err instanceof NodeError));
	assert.match(err.message, /^shardlite: sync: /);
	assert.ok(err.cause instanceof Error, "the fetch failure is the cause");
});

// The reasons the SDK knows, as a literal that tsc keeps complete: it names
// every member of Reason, and a member added to the type without an entry
// fails the lint. The fixture's reasons are the node's, which the node's own
// tests pin to the node, so this pins the type to the node.
const known: Record<Reason, true> = {
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

const fixtureReasons = (): string[] => {
	const file = path.join(import.meta.dirname, "..", "testdata", "conformance.json");
	return (JSON.parse(fs.readFileSync(file, "utf8")) as { reasons: string[] }).reasons;
};

test("the Reason type and the fixture's reasons agree, both ways", async () => {
	assert.deepEqual(Object.keys(known).sort(), [...fixtureReasons()].sort());
	// and the SDK reads each of them as itself
	for (const raw of fixtureReasons()) {
		const nerr = await nodeError(new Response("", { status: 503, headers: { "Shardlite-Error": raw } }));
		assert.equal(nerr?.reason, raw);
		assert.equal(nerr?.rawReason, raw);
	}
});

test("nodeError reads the node's error from a response", async () => {
	const res = new Response("no such kind: ghost\n", { status: 404, headers: { "Shardlite-Error": "unknown-shard" } });
	const nerr = await nodeError(res);
	assert.ok(nerr instanceof NodeError);
	assert.ok(nerr instanceof Error);
	assert.equal(nerr.name, "NodeError");
	assert.equal(nerr.status, 404);
	assert.equal(nerr.reason, "unknown-shard");
	assert.equal(nerr.rawReason, "unknown-shard");
	assert.equal(nerr.body, "no such kind: ghost");
	assert.equal(nerr.message, "shardlite: the node answered 404 (unknown-shard): no such kind: ghost");
	assert.equal(nerr.retryAfterMs, undefined);
});

test("nodeError leaves an app's error, and its body, alone", async () => {
	const res = new Response("no such guild member\n", { status: 409 });
	assert.equal(await nodeError(res), undefined);
	assert.equal(res.bodyUsed, false);
	assert.equal((await res.text()).trim(), "no such guild member");

	// a response with a body already read is not the node's either
	assert.equal(await nodeError(res), undefined);
});

test("nodeError spends the body of the node's error", async () => {
	const res = new Response("the quota is out\n", { status: 503, headers: { "Shardlite-Error": "quota" } });
	assert.ok(await nodeError(res));
	assert.equal(res.bodyUsed, true);
});

test("nodeError with a reason the SDK does not know keeps the raw value", async () => {
	const nerr = await nodeError(new Response("the quota is out\n", { status: 503, headers: { "Shardlite-Error": "quota" } }));
	assert.ok(nerr);
	assert.equal(nerr.reason, undefined);
	assert.equal(nerr.rawReason, "quota");
	assert.equal(nerr.status, 503);
	assert.equal(nerr.body, "the quota is out");

	// a name on Object.prototype is not a reason either
	assert.equal((await nodeError(new Response("", { status: 500, headers: { "Shardlite-Error": "constructor" } })))?.reason, undefined);

	// an empty value is still the node's error
	const empty = await nodeError(new Response(null, { status: 500, headers: { "Shardlite-Error": "" } }));
	assert.ok(empty);
	assert.equal(empty.reason, undefined);
	assert.equal(empty.rawReason, "");
	assert.equal(empty.body, "");
	assert.equal(empty.message, "shardlite: the node answered 500 ()");
});

test("nodeError reads at most 512 bytes of the message", async () => {
	const headers = { "Shardlite-Error": "internal" };
	const long = await nodeError(new Response("x".repeat(2000), { status: 502, headers }));
	assert.equal(long?.body.length, 512);

	// the same across chunks of a stream
	const chunk = new TextEncoder().encode("y".repeat(300));
	const stream = new ReadableStream<Uint8Array>({
		pull(controller) {
			controller.enqueue(chunk);
		},
	});
	const streamed = await nodeError(new Response(stream, { status: 502, headers }));
	assert.equal(streamed?.body, "y".repeat(512));

	// a body of exactly 512 bytes, and a short one
	assert.equal((await nodeError(new Response("z".repeat(512), { status: 502, headers })))?.body.length, 512);
	assert.equal((await nodeError(new Response("  short \n", { status: 502, headers })))?.body, "short");
});

test("nodeError reads Retry-After as the node writes it, whole seconds", async () => {
	const retry = async (value: string | undefined): Promise<number | undefined> => {
		const headers: Record<string, string> = { "Shardlite-Error": "moved" };
		if (value !== undefined) {
			headers["Retry-After"] = value;
		}
		const nerr = await nodeError(new Response("", { status: 503, headers }));
		assert.ok(nerr);
		return nerr.retryAfterMs;
	};
	assert.equal(await retry(undefined), undefined);
	assert.equal(await retry("0"), 0); // try again at once, which is not no hint
	assert.equal(await retry("3"), 3000);
	assert.equal(await retry(" 1 "), 1000);
	for (const bad of ["Wed, 21 Oct 2026 07:28:00 GMT", "-1", "1.5", "soon", "", "99999999999999999999"]) {
		assert.equal(await retry(bad), undefined, JSON.stringify(bad));
	}
});

test("a node's error reads as the Go SDK's does, the hint as a Go duration", async () => {
	const text = async (retryAfter: string): Promise<string | undefined> =>
		(await nodeError(new Response("", { status: 503, headers: { "Shardlite-Error": "moved", "Retry-After": retryAfter } })))?.message;
	assert.equal(await text("0"), "shardlite: the node answered 503 (moved) (retry after 0s)");
	assert.equal(await text("90"), "shardlite: the node answered 503 (moved) (retry after 1m30s)");
	assert.equal(await text("3600"), "shardlite: the node answered 503 (moved) (retry after 1h0m0s)");
});

test("syncHeader names the response's tier, and throws for any other value", () => {
	assert.deepEqual(syncHeader("storage"), { "Shardlite-Sync": "storage" });
	assert.deepEqual(syncHeader("none"), { "Shardlite-Sync": "none" });

	// it spreads into a Response's headers beside the app's own
	const res = new Response("saved", { headers: { "content-type": "text/plain", ...syncHeader("storage") } });
	assert.equal(res.headers.get("Shardlite-Sync"), "storage");
	assert.equal(res.headers.get("content-type"), "text/plain");

	for (const bad of ["disk", "", "None"]) {
		assert.throws(() => syncHeader(bad as Sync), /is not a Sync tier/);
	}
});

test("the paths and the headers for gRPC metadata are the node's", () => {
	assert.equal(HEADER_TARGET, "Shardlite-Target");
	assert.equal(HEADER_CONTEXT, "Shardlite-Context");
	assert.equal(READY_PATH, "/_shardlite/ready");
	assert.equal(REMINDERS_PATH, "/_shardlite/reminders");
});

// rowsOf is what a driver's helper looks like to the types: the rows a
// statement selects.
const rowsOf = <Row>(_stmt: Statement<Row>): Row[] => [];

// The types, which tsc checks and nothing runs. Each line below a directive
// comment must fail to type-check, or the lint fails.
function typeLevelCases(node: Node<DatabaseSync>, scope: Scope<DatabaseSync>, result: Opened<DatabaseSync> | Refusal): void {
	// @ts-expect-error a plain string is not a ShardName
	void scope.call("users/42", "/x");
	// @ts-expect-error a plain string is not a ShardName
	void node.callAsApp("users/42", "/x");
	// @ts-expect-error a plain string is not a ShardName
	void scope.expect("users/42");
	// @ts-expect-error a sync tier is none or storage
	kind("users", { sync: "disk" });
	// @ts-expect-error a sync tier is none or storage
	void scope.call(users.shard("1"), "/x", { sync: "disk" });
	// @ts-expect-error a Refusal has no withDb: narrow it first
	void result.withDb(() => 1);
	// @ts-expect-error a request's scope has no database until expect
	void scope.withDb(() => 1);
	// @ts-expect-error a call needs a target
	void scope.call(undefined, "/x");
	// @ts-expect-error a call needs a path
	void scope.call(users.shard("1"));
	// @ts-expect-error dueReminders' rows have a key and nothing else
	void rowsOf(dueReminders())[0]?.due;
	void rowsOf(dueReminders())[0]?.key;
	// @ts-expect-error marked is 0 or 1
	void (rowsOf(isMarkedDeleted())[0]?.marked === 2);
	// @ts-expect-error the tier of a response is none or storage
	void syncHeader("disk");
	// @ts-expect-error the params of a statement are not for the app to change
	dueReminders().params.push(1);
	// @ts-expect-error the caller of a shard names it
	const bad: Caller = { kind: "shard" };
	void bad;
	if (!(result instanceof Refusal)) {
		void result.withDb((db) => db.exec("SELECT 1")); // narrowed, so this is fine
	}
}

test("the type-level cases are checked by tsc", () => {
	assert.equal(typeof typeLevelCases, "function");
});

// The statements, run against the reserved tables on a real file.
function run(db: DatabaseSync, stmt: Statement<unknown>): void {
	db.prepare(stmt.sql).run(...stmt.params);
}

test("setReminder and cancelReminder write the reserved table", () => {
	const file = shardFile();
	const db = new DatabaseSync(fileUri(file));
	const due = new Date(Math.floor(Date.now() / 1000) * 1000 + 3600_000);

	const stmt = setReminder("upgrade:barracks", due);
	assert.match(stmt.sql, /^INSERT INTO _shardlite_reminders/);
	assert.ok(stmt.sql.includes("?"));
	assert.deepEqual(stmt.params, ["upgrade:barracks", due.getTime() / 1000]); // due as an integer of Unix seconds
	run(db, stmt);

	// a second set moves the reminder, on the conflict
	run(db, setReminder("upgrade:barracks", new Date(due.getTime() + 60_000)));
	assert.deepEqual(
		plain([db.prepare("SELECT due FROM _shardlite_reminders WHERE key = ?").get("upgrade:barracks")]),
		[{ due: due.getTime() / 1000 + 60 }],
	);

	run(db, setReminder("daily", new Date(due.getTime() - 3600_000)));
	run(db, cancelReminder("upgrade:barracks"));
	assert.deepEqual(plain(db.prepare("SELECT key FROM _shardlite_reminders ORDER BY key").all()), [{ key: "daily" }]);

	const cancel = cancelReminder("gone");
	assert.match(cancel.sql, /^DELETE FROM _shardlite_reminders WHERE key = \?$/);
	assert.deepEqual(cancel.params, ["gone"]);
	db.close();
});

test("dueReminders selects the keys due now, the earliest first", () => {
	const file = shardFile();
	const db = new DatabaseSync(fileUri(file));
	const now = Math.floor(Date.now() / 1000);
	run(db, setReminder("b-past", new Date((now - 60) * 1000)));
	run(db, setReminder("a-tie", new Date((now - 60) * 1000)));
	run(db, setReminder("c-now", new Date(now * 1000)));
	run(db, setReminder("d-future", new Date((now + 3600) * 1000)));

	const stmt = dueReminders();
	assert.match(stmt.sql, /ORDER BY due, key/);
	assert.deepEqual(stmt.params, []);
	assert.deepEqual(plain(db.prepare(stmt.sql).all()), [{ key: "a-tie" }, { key: "b-past" }, { key: "c-now" }]);
	db.close();
});

test("markDeleted writes the row, and twice is still one row", () => {
	const file = shardFile();
	const db = new DatabaseSync(fileUri(file));
	const stmt = markDeleted();
	assert.match(stmt.sql, /^INSERT OR IGNORE INTO _shardlite_delete/);
	assert.deepEqual(stmt.params, []);
	run(db, stmt);
	run(db, stmt);
	assert.deepEqual(plain(db.prepare("SELECT marked FROM _shardlite_delete").all()), [{ marked: 1 }]);

	db.close();
});

// Captures console.warn for one test and restores it after. The tests that
// warn run one at a time (node:test runs a file's tests in turn).
function captureWarnings(t: TestContext): string[] {
	const seen: string[] = [];
	t.mock.method(console, "warn", (...args: unknown[]) => {
		seen.push(args.join(" "));
	});
	return seen;
}

test("setReminder throws for an invalid Date", (t) => {
	const warnings = captureWarnings(t);
	assert.throws(() => setReminder("tick", new Date(NaN)), /setReminder\("tick"\).*invalid Date/);
	assert.throws(() => setReminder("tick", new Date("soon")), /invalid Date/);
	assert.deepEqual(warnings, []);
});

test("setReminder warns about a due time more than a day ago, and still returns the statement", (t) => {
	const warnings = captureWarnings(t);
	const due = new Date(Date.now() - 25 * 3600_000);
	const stmt = setReminder("stale", due);
	assert.match(stmt.sql, /^INSERT INTO _shardlite_reminders/);
	assert.deepEqual(stmt.params, ["stale", Math.floor(due.getTime() / 1000)]);
	assert.equal(warnings.length, 1);
	const warning = warnings[0] ?? "";
	assert.ok(warning.includes('"stale"'), warning);
	assert.ok(warning.includes(due.toISOString()), warning);
	assert.match(warning, /more than a day ago/);
	assert.match(warning, /node ignores/);
});

test("setReminder does not warn for a due time within a day, past or future", (t) => {
	const warnings = captureWarnings(t);
	setReminder("recent", new Date(Date.now() - 23 * 3600_000));
	setReminder("now", new Date());
	setReminder("later", new Date(Date.now() + 30 * 24 * 3600_000));
	assert.deepEqual(warnings, []);
});

test("isMarkedDeleted and unmarkDeleted read and clear the marker", () => {
	const file = shardFile();
	const db = new DatabaseSync(fileUri(file));
	const marked = (): unknown[] => plain(db.prepare(isMarkedDeleted().sql).all(...isMarkedDeleted().params));

	const check = isMarkedDeleted();
	assert.match(check.sql, /_shardlite_delete/);
	assert.deepEqual(check.params, []);
	assert.deepEqual(marked(), [{ marked: 0 }]);

	run(db, markDeleted());
	assert.deepEqual(marked(), [{ marked: 1 }]);

	const clear = unmarkDeleted();
	assert.match(clear.sql, /^DELETE FROM _shardlite_delete/);
	assert.deepEqual(clear.params, []);
	run(db, clear);
	assert.deepEqual(marked(), [{ marked: 0 }]);
	assert.deepEqual(plain(db.prepare("SELECT marked FROM _shardlite_delete").all()), []);

	// unmarking a shard that is not marked does nothing
	run(db, clear);
	assert.deepEqual(marked(), [{ marked: 0 }]);
	db.close();
});

test("a wake handler: expectCall, withDb, the due keys, and each one's work and cancel in one transaction", async () => {
	const file = shardFile();
	const db0 = new DatabaseSync(fileUri(file));
	run(db0, setReminder("tick", new Date(Date.now() - 1000)));
	run(db0, setReminder("later", new Date(Date.now() + 3600_000)));
	db0.close();

	// the wake: the node's headers name the shard as the caller
	const wake = nodeFor().request(nodeHeaders(file, "users/42"));
	const opened = must(wake.expectCall());
	await opened.withDb((db) => {
		const due = db.prepare(dueReminders().sql).all() as { key: string }[];
		assert.deepEqual(due.map((r) => r.key), ["tick"]);
		db.exec("BEGIN IMMEDIATE");
		run(db, cancelReminder("tick"));
		db.exec("COMMIT");
	});

	const db1 = new DatabaseSync(fileUri(file));
	assert.deepEqual(plain(db1.prepare("SELECT key FROM _shardlite_reminders").all()), [{ key: "later" }]);
	db1.close();
});
