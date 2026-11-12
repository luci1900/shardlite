# shardlite

SQLite databases served as actors, with blob storage underneath.

Give each player, match, guild or document a small SQLite database of its own: a shard. The node routes each request to the right shard, keeps it backed up in S3 (or similar) and moves it between nodes. The app is any program (in any language) that opens the shard's SQLite file.

## Good at

- **Launch spikes.** Add nodes while the traffic climbs and remove them afterwards. Nodes are disposable and an idle shard costs only its storage.
- **A database per entity.** A player shard is a save file that is always written and backed up, its schema the save format.
- **Any stack.** The node proxies HTTP, gRPC, server-sent events and WebSocket. A game server can hold a shard and serve its players over UDP.
- **Durability you choose.** Each kind either waits for blob storage before it answers or doesn't, and a shard's reads and writes are linearizable while its owner holds it.
- **Timers and messages.** A shard sets reminders for itself and calls other shards, with durable sends that survive a crash.

## Try it

A counter on your machine in Docker: a node, a small app and RustFS for storage. You need Docker with Compose and a clone of this repository. Save the two files below next to each other at the repository root.

<details>
<summary><code>app.js</code>, the app</summary>

```js
const http = require('node:http');
const { DatabaseSync } = require('node:sqlite');

http.createServer(async (req, res) => {
  try {
    if (req.url === '/_shardlite/ready') {
      // The node asks which kinds of shard the app serves.
      res.setHeader('content-type', 'application/json');
      return res.end('{"kinds": {"counter": {}}}');
    }
    const file = req.headers['shardlite-file'];
    if (req.url === '/add' && file) {
      // A call to a counter: the node has opened its shard and names the file.
      const db = new DatabaseSync(`file:${file}?mode=rw`);
      try {
        db.exec('CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)');
        const { n } = db.prepare('INSERT INTO counter VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET n = n + 1 RETURNING n').get();
        return res.end(`${n}\n`);
      } finally {
        db.close();
      }
    }
    const id = req.method === 'POST' && req.url.match(/^\/counter\/([\w-]{1,128})$/)?.[1];
    if (id) {
      // A client's request: call the counter's shard through the node.
      const answer = await fetch('http://127.0.0.1:2282/add', { method: 'POST', headers: { 'shardlite-target': `counter/${id}` } });
      res.writeHead(answer.status);
      return res.end(await answer.text());
    }
    res.writeHead(404).end();
  } catch (err) {
    res.writeHead(500).end(`${err.message}\n`);
  }
}).listen(3000);
```

A client asks for `/counter/alice`, and the app calls the shard `counter/alice` through the node's control API, which creates the shard on its first call. The node sends the call back to the app with the shard's file open, and the app adds one. A real app also checks `Shardlite-Opened` and keeps a connection pool (see [What an app must do](docs/guide.md#what-an-app-must-do)).

</details>

<details>
<summary><code>compose.yaml</code>, the node, the app and the store</summary>

```yaml
services:
  rustfs:
    image: rustfs/rustfs:1.0.1
    command: /data
    environment:
      RUSTFS_ACCESS_KEY: shardlite
      RUSTFS_SECRET_KEY: shardlite-secret
      RUSTFS_NEW_BUCKET_DURABILITY_MODE: strict # a write is on disk before it is acknowledged

  bucket: # creates the bucket once, since the node never does
    image: amazon/aws-cli:2.27.50
    entrypoint: ["/bin/sh", "-c"]
    command: ["until aws s3api head-bucket --bucket shardlite || aws s3api create-bucket --bucket shardlite; do sleep 1; done"]
    environment:
      AWS_ENDPOINT_URL: http://rustfs:9000
      AWS_ACCESS_KEY_ID: shardlite
      AWS_SECRET_ACCESS_KEY: shardlite-secret
      AWS_DEFAULT_REGION: us-east-1
    depends_on: [rustfs]

  app:
    image: node:24
    command: ["node", "/app.js"]
    ports: ["127.0.0.1:2280:2280"] # the node's public port, on the network the two share
    volumes:
      - ./app.js:/app.js:ro
      - data:/data

  shardlite:
    build: { context: ., dockerfile: docker/Dockerfile }
    command: ["shardlite"]
    network_mode: service:app # the node and the app share localhost, as in a pod
    environment:
      SHARDLITE_BLOB: s3://shardlite
      SHARDLITE_S3_ENDPOINT: http://rustfs:9000
      AWS_ACCESS_KEY_ID: shardlite
      AWS_SECRET_ACCESS_KEY: shardlite-secret
      SHARDLITE_DATA: /data
      SHARDLITE_APP: http://127.0.0.1:3000
    volumes: ["data:/data"] # the node and the app share the shard files
    depends_on:
      app: { condition: service_started }
      bucket: { condition: service_completed_successfully }

volumes:
  data: # a volume, not a bind mount, so SQLite's locking works on macOS too
```

</details>

Start it (the first build takes a few minutes):

```sh
docker compose up --build
```

Then, from another terminal, count:

```sh
curl -X POST localhost:2280/counter/alice
curl -X POST localhost:2280/counter/alice
```

The count goes up by one each time. Restart the node, which comes back with no shard files and restores `alice` from storage on the next request (give it a few seconds to be ready):

```sh
docker compose restart shardlite
curl -X POST localhost:2280/counter/alice
```

The count carries on where it was. Another id is another database, starting at one:

```sh
curl -X POST localhost:2280/counter/bob
```

`docker compose down -v` removes everything, the stored shards included.

## Read on

- [Overview](docs/overview.md): the concepts and goals.
- [Guide](docs/guide.md): writing an app.
- [Deployment](docs/deployment.md): running it.
- [Internals](docs/internals.md): how the node works.

## License

Copyright (C) 2026 Luci Brănescu

Shardlite is licensed under the GNU Affero General Public License, version 3 only. See [LICENSE](LICENSE).
