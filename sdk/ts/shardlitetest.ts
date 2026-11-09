// Tests an app built on shardlite.ts against a real node. startNode runs the
// shardlite binary on its in-memory store (SHARDLITE_BLOB=memory://), with the
// app's handler behind it, so a test sees everything the node does (the
// declaration, the hop limit, the gates, real wakes) and no copy of it has to
// be kept in step. It runs on node:child_process and node:http, which Node, Bun
// and Deno all provide.
//
// The binary is the path in SHARDLITE_BIN, else shardlite on PATH. Without one
// startNode rejects, with how to build it.

import { spawn } from "node:child_process";
import type { ChildProcess } from "node:child_process";
import { accessSync, constants, mkdtempSync, rmSync, statSync } from "node:fs";
import { createServer } from "node:http";
import type { IncomingMessage, Server, ServerResponse } from "node:http";
import { createServer as createTcpServer } from "node:net";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { READY_PATH, shardlite } from "./shardlite.ts";
import type { Config, Database, Node } from "./shardlite.ts";

const UP_TIMEOUT_MS = 30_000;
const READY_TIMEOUT_MS = 20_000;
const STOP_TIMEOUT_MS = 15_000;

/** A running node and the app behind it. */
export interface StartedNode<D extends Database> {
	/** The node's public base URL, where a client's requests go. */
	readonly url: string;
	/** host:port of the node's control API, which `node` calls. */
	readonly control: string;
	/** The app's Node, built from the config and pointed at the node. */
	readonly node: Node<D>;
	/** What the node has written so far (its log, on stderr). */
	output(): string;
	/**
	 * Stops the node (SIGTERM, then SIGKILL if it does not exit in time) and the
	 * app's server. Calling it again does nothing.
	 */
	close(): Promise<void>;
	[Symbol.asyncDispose](): Promise<void>;
}

/** The app's handler, built from the app's Node. */
export type AppHandler<D extends Database> = (node: Node<D>) => (req: Request) => Response | Promise<Response>;

/**
 * startNode runs a node and returns once its public ready route answers 200.
 * It builds the app's Node from config with `addr` set to the node's control
 * address, serves `app(node)` on a local port as the app, and takes a
 * directory for the node's data of its own. The caller closes it, or uses
 * `await using`.
 *
 * The node keeps everything in memory: one node, with nothing left when it
 * stops. Tests that share a node should use shard ids of their own.
 */
export async function startNode<D extends Database>(config: Config<D>, app: AppHandler<D>): Promise<StartedNode<D>> {
	const bin = binary();
	const [publicPort, controlPort] = [await freePort(), await freePort()];
	const publicAddr = `127.0.0.1:${publicPort}`;
	const control = `127.0.0.1:${controlPort}`;
	const node = shardlite({ ...config, addr: control });
	const server = await serve(app(node));
	const dataDir = mkdtempSync(path.join(tmpdir(), "shardlitetest-"));

	let output = "";
	const child = spawn(bin, [], {
		env: {
			...process.env,
			SHARDLITE_BLOB: "memory://",
			SHARDLITE_NODE: "shardlitetest",
			SHARDLITE_DATA: path.join(dataDir, "data"),
			SHARDLITE_APP: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
			SHARDLITE_LISTEN_PUBLIC: publicAddr,
			SHARDLITE_LISTEN_CONTROL: control,
			SHARDLITE_LISTEN_PEER: "127.0.0.1:0",
			SHARDLITE_FLUSH: "50ms",
		},
		stdio: ["ignore", "pipe", "pipe"],
	});
	const exited = new Promise<number | null>((resolve) => {
		child.once("close", (code) => resolve(code));
		child.once("error", () => resolve(null));
	});
	const onOutput = (chunk: Buffer) => {
		output += chunk.toString();
	};
	child.stdout.on("data", onOutput);
	child.stderr.on("data", onOutput);
	child.once("error", (err) => {
		output += `${err}\n`;
	});

	let closed: Promise<void> | undefined;
	const close = () => {
		closed ??= (async () => {
			await stop(child, exited);
			await new Promise<void>((resolve) => {
				server.close(() => resolve());
				server.closeAllConnections();
			});
			rmSync(dataDir, { recursive: true, force: true });
		})();
		return closed;
	};

	try {
		await waitFor(UP_TIMEOUT_MS, "the node to log that it is up", exited, () => output.includes("shardlite is up"));
		const url = `http://${publicAddr}`;
		await waitFor(READY_TIMEOUT_MS, "the node to answer ready", exited, async () => {
			try {
				return (await fetch(url + READY_PATH)).status === 200;
			} catch {
				return false;
			}
		});
		return {
			url,
			control,
			node,
			output: () => output,
			close,
			[Symbol.asyncDispose]: close,
		};
	} catch (err) {
		await close();
		throw new Error(`${err instanceof Error ? err.message : err}\nthe node's output:\n${output}`);
	}
}

/** The node's binary: SHARDLITE_BIN, else shardlite on PATH. */
function binary(): string {
	const howTo =
		'build it with "go build -o <dir>/shardlite ./cmd/shardlite" from the shardlite repository and set SHARDLITE_BIN=<dir>/shardlite (or put <dir> on PATH)';
	const bin = process.env["SHARDLITE_BIN"];
	if (bin) {
		try {
			statSync(bin);
		} catch (err) {
			throw new Error(`shardlitetest: SHARDLITE_BIN is "${bin}", which cannot be read (${err instanceof Error ? err.message : err}): ${howTo}`);
		}
		return bin;
	}
	for (const dir of (process.env["PATH"] ?? "").split(path.delimiter)) {
		if (dir === "") {
			continue;
		}
		const candidate = path.join(dir, "shardlite");
		try {
			accessSync(candidate, constants.X_OK);
			if (statSync(candidate).isFile()) {
				return candidate;
			}
		} catch {
			// not here
		}
	}
	throw new Error(`shardlitetest: no shardlite binary: SHARDLITE_BIN is not set and shardlite is not on PATH: ${howTo}`);
}

/** A loopback port nothing listens on at the moment. */
function freePort(): Promise<number> {
	return new Promise((resolve, reject) => {
		const s = createTcpServer();
		s.once("error", reject);
		s.listen(0, "127.0.0.1", () => {
			const { port } = s.address() as AddressInfo;
			s.close(() => resolve(port));
		});
	});
}

/** Polls cond until it holds, and rejects when the node exits or on timeout. */
async function waitFor(
	timeoutMs: number,
	what: string,
	exited: Promise<number | null>,
	cond: () => boolean | Promise<boolean>,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	let code: number | null | undefined;
	void exited.then((c) => {
		code = c;
	});
	while (!(await cond())) {
		if (code !== undefined) {
			throw new Error(`shardlitetest: the node exited (${code}) before ${what}`);
		}
		if (Date.now() > deadline) {
			throw new Error(`shardlitetest: timed out after ${timeoutMs} ms waiting for ${what}`);
		}
		await new Promise((resolve) => setTimeout(resolve, 10));
	}
}

/** Stops the node on SIGTERM, and kills it if it does not exit in time. */
async function stop(child: ChildProcess, exited: Promise<number | null>): Promise<void> {
	if (child.exitCode !== null || child.signalCode !== null) {
		return;
	}
	child.kill("SIGTERM");
	const timer = new Promise<"late">((resolve) => setTimeout(() => resolve("late"), STOP_TIMEOUT_MS).unref());
	if ((await Promise.race([exited, timer])) === "late") {
		child.kill("SIGKILL");
		await exited;
		throw new Error(`shardlitetest: the node did not stop on SIGTERM within ${STOP_TIMEOUT_MS} ms`);
	}
}

/** Serves a web handler on a local node:http server. */
function serve(handler: (req: Request) => Response | Promise<Response>): Promise<Server> {
	const server = createServer((req, res) => {
		void adapt(handler, req, res);
	});
	return new Promise((resolve, reject) => {
		server.once("error", reject);
		server.listen(0, "127.0.0.1", () => resolve(server));
	});
}

async function adapt(
	handler: (req: Request) => Response | Promise<Response>,
	req: IncomingMessage,
	res: ServerResponse,
): Promise<void> {
	try {
		const headers = new Headers();
		for (const [name, value] of Object.entries(req.headers)) {
			for (const v of Array.isArray(value) ? value : value === undefined ? [] : [value]) {
				headers.append(name, v);
			}
		}
		const method = req.method ?? "GET";
		const chunks: Buffer[] = [];
		for await (const chunk of req) {
			chunks.push(chunk as Buffer);
		}
		const hasBody = method !== "GET" && method !== "HEAD";
		const request = new Request(`http://${req.headers.host ?? "127.0.0.1"}${req.url ?? "/"}`, {
			method,
			headers,
			...(hasBody ? { body: Buffer.concat(chunks) } : {}),
		});
		const response = await handler(request);
		const out = new Headers(response.headers);
		const cookies = out.getSetCookie();
		out.delete("set-cookie");
		res.statusCode = response.status;
		for (const [name, value] of out) {
			res.setHeader(name, value);
		}
		if (cookies.length > 0) {
			res.setHeader("set-cookie", cookies);
		}
		if (response.body === null) {
			res.end();
			return;
		}
		const reader = response.body.getReader();
		for (;;) {
			const { done, value } = await reader.read();
			if (done) {
				break;
			}
			res.write(value);
		}
		res.end();
	} catch (err) {
		if (!res.headersSent) {
			res.statusCode = 500;
		}
		res.end(`${err}`);
	}
}
