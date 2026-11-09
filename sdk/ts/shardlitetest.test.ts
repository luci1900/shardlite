// The test helper against a real node: it needs the shardlite binary (the path
// in SHARDLITE_BIN, else on PATH), which the fast suite builds. The tests share
// one node, since starting one takes a moment, and use ids of their own.

import { test } from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import {
	HEADER_TARGET,
	READY_PATH,
	REMINDERS_PATH,
	Refusal,
	cancelReminder,
	dueReminders,
	kind,
	setReminder,
} from "./shardlite.ts";
import type { Caller, Node, ShardName } from "./shardlite.ts";
import { startNode } from "./shardlitetest.ts";
import type { AppHandler } from "./shardlitetest.ts";

const users = kind("users", { public: true });
const guilds = kind("guild");

const config = {
	open: ({ uri }: { uri: string }) => new DatabaseSync(uri),
	version: "test",
	kinds: [users, guilds],
	onOpen: (db: DatabaseSync) => {
		db.exec("CREATE TABLE IF NOT EXISTS visits (id INTEGER PRIMARY KEY AUTOINCREMENT)");
	},
};

type N = Node<DatabaseSync>;

const who = (c: Caller): string => (c.kind === "shard" ? c.shard : c.kind === "app" ? "the app" : "a client");

const text = (body: string, status = 200) => new Response(body, { status });

// The wakes the app has handled, by shard.
const woken = new Map<string, number>();

// app is the README's handler, with a route to call a guild, one to set a
// reminder that is due now and the wake.
const app: AppHandler<DatabaseSync> = (node: N) => async (req: Request): Promise<Response> => {
	const url = new URL(req.url);
	if (url.pathname === READY_PATH) {
		return node.ready();
	}
	const route = /^\/users\/([^/]+)\/(visit|invite|remind)$/.exec(url.pathname);
	if (req.method === "POST" && route) {
		const opened = node.request(req.headers).expect(users.shard(route[1]!));
		if (opened instanceof Refusal) {
			return text(opened.message, opened.status);
		}
		switch (route[2]) {
			case "visit":
				return await opened.withDb((db) => {
					db.exec("INSERT INTO visits DEFAULT VALUES");
					const row = db.prepare("SELECT count(*) AS n FROM visits").get() as { n: number };
					return text(`hello from ${opened.name}: ${row.n}`);
				});
			case "invite": {
				const res = await opened.call(guilds.shard(url.searchParams.get("guild") ?? ""), "/join", { method: "POST" });
				return text(await res.text(), res.status);
			}
			default:
				return await opened.withDb((db) => {
					const stmt = setReminder("now", new Date());
					db.prepare(stmt.sql).run(...stmt.params);
					return text("ok");
				});
		}
	}
	if (req.method === "POST" && (url.pathname === "/join" || url.pathname === "/hello")) {
		const opened = node.request(req.headers).expectCall();
		if (opened instanceof Refusal) {
			return text(opened.message, opened.status);
		}
		return text(`${opened.name} called by ${who(opened.caller())}`);
	}
	if (req.method === "POST" && url.pathname === REMINDERS_PATH) {
		const opened = node.request(req.headers).expectCall();
		if (opened instanceof Refusal) {
			return text(opened.message, opened.status);
		}
		return await opened.withDb((db) => {
			const keys = db.prepare(dueReminders().sql).all() as { key: string }[];
			for (const { key } of keys) {
				const stmt = cancelReminder(key);
				db.prepare(stmt.sql).run(...stmt.params);
			}
			woken.set(opened.name, (woken.get(opened.name) ?? 0) + keys.length);
			return new Response(null, { status: 204 });
		});
	}
	return text("not found", 404);
};

let ids = 0;
const newId = (): string => `t${++ids}`;
const idOf = (shard: ShardName): string => shard.slice(shard.indexOf("/") + 1);

test("startNode runs a node that serves the app", async (t) => {
	const started = await startNode(config, app);
	t.after(() => started.close());
	assert.ok(started.url.startsWith("http://127.0.0.1:"));
	assert.ok(started.control.startsWith("127.0.0.1:"));
	assert.equal(started.node.addr, started.control);
	assert.match(started.output(), /memory/);

	const post = async (shard: ShardName, path: string): Promise<{ status: number; body: string }> => {
		const res = await fetch(started.url + path, { method: "POST", headers: { [HEADER_TARGET]: shard } });
		return { status: res.status, body: (await res.text()).trim() };
	};
	// A client can reach a shard once it exists, which a call creates.
	const create = async (shard: ShardName): Promise<void> => {
		const res = await started.node.callAsApp(shard, "/hello", { method: "POST" });
		assert.equal(res.status, 200, await res.text());
	};

	await t.test("a client's request reaches the app with the shard opened", async () => {
		const shard = users.shard(newId());
		await create(shard);
		for (const want of [1, 2]) {
			const { status, body } = await post(shard, `/users/${idOf(shard)}/visit`);
			assert.deepEqual([status, body], [200, `hello from ${shard}: ${want}`]);
		}
		// The route names another shard than the one the node opened.
		const other = await post(shard, `/users/${newId()}/visit`);
		assert.equal(other.status, 400);
	});

	await t.test("a call through the app's node reaches the callee with its caller", async () => {
		const guild = guilds.shard(newId());
		const res = await started.node.callAsApp(guild, "/join", { method: "POST" });
		assert.deepEqual([res.status, await res.text()], [200, `${guild} called by the app`]);

		// A call made on a request is from the shard that request opened.
		const user = users.shard(newId());
		await create(user);
		const invited = await post(user, `/users/${idOf(user)}/invite?guild=${idOf(guild)}`);
		assert.deepEqual(invited, { status: 200, body: `${guild} called by ${user}` });
	});

	await t.test("a reminder that is due is woken", async () => {
		const user = users.shard(newId());
		await create(user);
		assert.equal((await post(user, `/users/${idOf(user)}/remind`)).status, 200);
		const deadline = Date.now() + 15_000;
		while ((woken.get(user) ?? 0) === 0) {
			assert.ok(Date.now() < deadline, `no wake for ${user} within 15 s:\n${started.output()}`);
			await new Promise((resolve) => setTimeout(resolve, 20));
		}
	});
});

test("close stops the node and the app, and can be called again", async () => {
	const started = await startNode(config, app);
	assert.equal((await fetch(started.url + READY_PATH)).status, 200);
	await started.close();
	await started.close();
	await assert.rejects(fetch(started.url + READY_PATH));
});

test("a missing binary rejects with how to build one", async (t) => {
	const bin = process.env["SHARDLITE_BIN"];
	const path = process.env["PATH"];
	t.after(() => {
		for (const [name, value] of [["SHARDLITE_BIN", bin], ["PATH", path]] as const) {
			if (value === undefined) {
				delete process.env[name];
			} else {
				process.env[name] = value;
			}
		}
	});
	process.env["SHARDLITE_BIN"] = "/nonexistent/nowhere/shardlite";
	await assert.rejects(startNode(config, app), (err: Error) => {
		assert.match(err.message, /SHARDLITE_BIN/);
		assert.match(err.message, /nowhere/);
		assert.match(err.message, /go build -o <dir>\/shardlite \.\/cmd\/shardlite/);
		return true;
	});

	// Not set, and not on PATH.
	delete process.env["SHARDLITE_BIN"];
	process.env["PATH"] = "/nonexistent";
	await assert.rejects(startNode(config, app), /go build -o <dir>\/shardlite \.\/cmd\/shardlite/);
});

test("a handler's response is passed on as it is", async (t) => {
	// The adapter between node:http and the web handler, through a node that
	// forwards a client's request to the app.
	const seen: string[] = [];
	const started = await startNode(config, (node: N) => async (req: Request) => {
		const url = new URL(req.url);
		if (url.pathname === READY_PATH) {
			return node.ready();
		}
		seen.push(`${req.method} ${url.pathname}${url.search} ${await req.text()}`);
		return new Response("a body", { status: 202, headers: { "x-app": "yes" } });
	});
	t.after(() => started.close());
	const guild = guilds.shard(newId());
	const res = await started.node.callAsApp(guild, "/echo?q=1", { method: "PUT", body: "hi" });
	assert.deepEqual([res.status, res.headers.get("x-app"), await res.text()], [202, "yes", "a body"]);
	assert.deepEqual(seen, ["PUT /echo?q=1 hi"]);
});
