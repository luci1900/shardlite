# Deploying shardlite

For whoever runs the cluster. The [guide](guide.md) is for people who build apps on shardlite, the [overview](overview.md) introduces the concepts, and how the node does what it does is in [internals](internals.md).

## The unit

A unit is one node and its app on one Linux kernel, sharing a local disk, with the app on localhost. Nodes are disposable: blob storage holds the data and a node's disk holds only the working copies of its active shards. One unit per machine is the natural size, since a second node on a machine adds no capacity (nothing enforces it). To try it on one machine, follow the quickstart in the [README](../README.md).

## What it needs

- **A local disk** for the data directory (not a network filesystem, which the node refuses). Local NVMe is fastest and ephemeral disks are fine.
- **A peer port** (`:2281` by default) that the other units reach directly, on a private network (see [Traffic between nodes](#traffic-between-nodes)).
- **A load balancer** that checks `GET /_shardlite/ready` (see [Load balancer](#load-balancer)).
- **`SIGTERM` and a grace period of at least 60 seconds.** A stop waits up to 10 seconds for requests in flight and up to 30 for the shards to be let go of while blob storage is slow, and a node killed before it finishes is a crash.
- **A store that passes the startup check** (see [Stores](#stores)).

## Stop order

Either side may stop first. The app, told to stop, finishes the requests in flight, as any web app behind a load balancer does. A node that stops first lets go of its shards over any file the app still holds. A node whose app has answered and then gone without any HTTP answer for 30 seconds stops by itself and exits non-zero, so a supervisor that restarts it on failure brings it back to wait for its app. A node that has never heard from its app waits and stays up, as does one whose app answers badly (a failing declaration), not ready.

## On each platform

| | Kubernetes | ECS | Plain host |
|---|---|---|---|
| Unit | a StatefulSet pod (`podManagementPolicy: Parallel`, no volume claims), with the node as a native sidecar | a task, with both containers essential | the machine, with systemd units or any supervisor |
| Data directory | `emptyDir` | a shared volume | a local directory |
| Stop order | kubelet stops the sidecar after the app | the app `dependsOn` the node | either |
| Readiness | a readiness probe on the node | the target group's health check | the load balancer's check |
| Grace period (60 s) | `terminationGracePeriodSeconds` | `stopTimeout` | the supervisor's timeout |

Node names (the host name by default, so a StatefulSet's ordinal on Kubernetes) and the peer address (detected, see the `SHARDLITE_ADVERTISE_PEER` row of the [table](#environment-variables)) need no setting.

On Kubernetes the node is not a DaemonSet, which would need a `hostPath` volume shared with the app pods, and the baseline Pod Security Standard forbids that. A StatefulSet keeps names stable, so a restarted pod takes over its old lease and journal at once, and the data directory needs no volume claim since a restart starts empty.

## What doesn't fit

- **Serverless platforms.** They have no long-lived unit and no network between instances, and shardlite is itself that layer.
- **A data directory on a network filesystem,** which breaks SQLite's locking.

## Stores

The tested stores are AWS S3 and RustFS (for development, tests and self-hosting), and any other store is decided by the startup check: the node refuses to start if its conditional writes misbehave. A store must support linearizable conditional writes (`If-None-Match` and `If-Match` on writes, `If-Match` on deletes) and custom metadata on conditional writes, returned on `HEAD`. Cloudflare R2 doesn't document conditional deletes, so it fails the check. GCS and Azure Blob have no backend yet (see the [roadmap](roadmap.md)), so a deployment on GCP or Azure runs its own RustFS. Keep RustFS's buckets in strict durability mode, since a store that acknowledges a write before it is on disk breaks the gates' promise.

`SHARDLITE_PREFIX` puts every object under a prefix (`staging/`), so several environments can share one bucket. No lifecycle rule is needed: each node deletes its own expired journal segments. Shardlite keeps no history of old files, so a deployment that wants point-in-time restore can turn on bucket versioning. Deleting players who never return is the app's decision.

## Running shardlite

- **Scaling:** add nodes, and new connections and shards land on them (scale by CPU and memory). To remove one, stop it (`SIGTERM` or `SIGINT`) and it drains: its readiness check fails so the load balancer takes it out, other nodes place nothing on it, and it lets go of its shards and releases its lease (see [internals](internals.md#drain)). A shard with open connections is let go of at the stop, and its clients reconnect through the load balancer to another node.
- **Restarts and upgrades:** stop the node the same way. The new process is treated as a crash and starts with nothing on disk, deleting any shard file left behind, and shards are restored as they activate. During a rolling upgrade, old and new nodes forward to each other and read each other's data. A node that loses its app is covered in [Stop order](#stop-order).
- **Failure:** while its app is gone, a node answers `502` with `Retry-After` to the requests for its shards until the 30 seconds are up, then stops. Requests for the shards a dead node was serving get a quick `503` with `Retry-After: 1` until it has been adopted and another node has taken over (10–20 s with the default lease TTL). Its idle shards had been released already and are claimable at once.
- **A store outage:** a store no node can reach stops the cluster. Each node's lease lives in blob storage and expires when it can't be renewed (8 s at the default 10 s TTL), so every node stops and its supervisor restarts it until the store answers (the startup check fails until then). The cluster is as available as the store, less the lease TTL, which is also the outage a node rides out: a longer TTL absorbs a blip and costs failover speed. A store back in time costs a burst of `503`s and nothing else. Nothing acknowledged on `storage` is lost, and `none` kinds lose what was acknowledged since the last segment landed. One node cut off from the store is an ordinary failover. A warning and metric for it are on the [roadmap](roadmap.md).

### Load balancer

Clients reach nodes through the deployment's own load balancer, which terminates TLS and absorbs hostile traffic. Nodes listen only on the private network. The load balancer sets `X-Forwarded-For`, `X-Forwarded-Proto` and `X-Forwarded-Host` on every request (overwriting what clients send), the node adds the load balancer's address to the end of `X-Forwarded-For`, and the load balancer checks `GET /_shardlite/ready`.

A request that lands on a node that doesn't own its shard is forwarded to the owner, so a load balancer with session affinity (a cookie, which clients must keep) gives the lowest latency: a client's home shard is activated on its node and later requests find it open. Shards that many clients share (a guild, a match) are reached through forwards whatever the load balancer does. With a controllable one (HAProxy, nginx, Envoy), hashing `Shardlite-Target` (header or query parameter) sends every request for a shard to one node, shared shards included, though a node joining or leaving moves part of the hash and those shards are forwarded until they are next released.

### Hosts

The node targets Linux only. Game clients and editors can run anywhere. On macOS and Windows, run the node and the app in Docker (or WSL2) with shard files in a Docker volume or on the Linux filesystem, not a bind mount or `/mnt/c`, since SQLite locking isn't reliable across file sharing.

- **Open files and memory mappings.** Each active shard takes 8 file descriptors and about 7 memory mappings. Raise the hard `ulimit -n` and `vm.max_map_count` (65,530 by default, reached at about 10,000 active shards). The node warns at startup if they are too low.
- **Data directory.** `SHARDLITE_DATA` must be on a local filesystem (the node refuses to start on NFS, SMB or FUSE), with room for the largest shards being restored or rebased and about twice the size of any commit that rewrites a whole database. A shard's tail lives in unnamed files in `{data}/.tmp`, which show in `df` and not in `du`.
- **Clocks.** Run NTP. Leases need only clock rates that agree, but a clock far off delays failover. Clocks matter for failover speed and for `none` reads, not for `storage`. The node warns when its clock is more than a second from the store's.

### Traffic between nodes

A node has three listeners: public (clients, through the load balancer), peer (other nodes) and control (the app on the same host). The peer listener speaks HTTP without TLS and is trusted to carry node-only headers, so it must be reachable only from other nodes, on a trusted private network or an encrypted one (WireGuard, Tailscale). Never put it behind the load balancer or expose it to clients or the app. Service meshes are not a target, since they add two proxy hops to every call between nodes.

### Load

- **Rebalancing.** New nodes only receive new connections and shards, so shards move themselves, when they are released: at their idle timeout, at the stop of their node and at a failover. The next request for a released shard claims it where it arrives, so with the load balancers above a shard ends up where its clients or its hash are, and calls from a shard are claimed at random among the live nodes. A shard with open connections is not idle: it moves only at a stop or a failover, and the node never closes a connection for a move, so close long-lived connections at natural breaks (the end of a match, a scene change), never on a timer.

### Cost and latency

Rough estimates, not measurements. On S3 Standard a busy node writes 4 journal PUTs a second at the default flush interval, about 50 USD a month whatever the number of active shards (idle nodes write nothing). A shard deactivating costs one PUT and a manifest write, about 10 USD per million sessions. A request for an active shard adds about 0.1 ms, and a shard owned by another node 0.5–2 ms plus a manifest read (10–30 ms) on the first request. A shard that has gone idle is claimed and restored by its next request: a claim (20–60 ms) and a snapshot download.

### Observability

The node passes W3C trace context (`traceparent`) through every hop unchanged, and adds no spans of its own yet (tracing is on the [roadmap](roadmap.md)). It logs an alert if a shard's commits have gone longer than 3× `SHARDLITE_DELTA_AGE` without a snapshot (its snapshots are failing), and a warning naming a shard whose file the app has kept open past the shard's idle timeout. `SHARDLITE_LISTEN_PPROF` serves Go's pprof handlers.

## Reference

### Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `SHARDLITE_NODE` | the host name | the node's name (1 to 128 ASCII letters, digits, `.`, `-` and `_`), which names its lease and journal |
| `SHARDLITE_ADVERTISE_PEER` | detected | how other nodes reach the peer listener (`host:port`). Unset, the node uses the listener's address if it names a host, or else the host's one private IPv4 address. With none, several or only IPv6 it is required |
| `SHARDLITE_BLOB` | required | `s3://bucket`, or `memory://` for an app's tests and local development only: one node on an in-memory store, lost when it stops, and the node warns at startup |
| `SHARDLITE_S3_ENDPOINT` | AWS | the URL of an S3-compatible store |
| `SHARDLITE_PREFIX` | | prefix of every object path, for example `staging/` |
| `SHARDLITE_APP` | | the app's base URL, polled for its declaration. Without it the node serves no kinds |
| `SHARDLITE_LISTEN_PUBLIC` | `:2280` | the public listener |
| `SHARDLITE_LISTEN_PEER` | `:2281` | the listener for traffic from other nodes |
| `SHARDLITE_LISTEN_CONTROL` | `127.0.0.1:2282` | the control API on localhost TCP |
| `SHARDLITE_DATA` | required | the node's directory in the host's disk, for the files of its active shards |
| `SHARDLITE_FLUSH` | `250ms` | flush interval |
| `SHARDLITE_FLUSH_MIN` | `100ms` | the shortest time between two flushes, which bounds the wait at a gate |
| `SHARDLITE_IDLE_TIMEOUT` | built in | idle timeout for kinds that don't set one |
| `SHARDLITE_LEASE_TTL` | `10s` | node lease TTL, also how long a store outage rides out before every node stops (see [Running shardlite](#running-shardlite)) |
| `SHARDLITE_DELTA_AGE` | `15m` | how old a shard's unsnapshotted commits get before a snapshot is written. The tail alert (3×) and journal expiry follow from it |
| `SHARDLITE_DELTA_SIZE` | `256KB` | how many bytes of unsnapshotted commits make a snapshot due, whatever their age |
| `SHARDLITE_MAX_HOPS` | `32` | the hop limit for calls |
| `SHARDLITE_LISTEN_PPROF` | off | address for Go's pprof handlers |
