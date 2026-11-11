# shardlite roadmap

## Before a launch

What is left to make v1, in order, riskiest parts first. The one node and the cluster (leases, fencing, adoption, forwarding, reminders, drains) are built. An item's design is worked out in the plan of the stage that builds it.

### Ready for a launch

- Load shedding with `503` and `GOAWAY` for shedding HTTP/2 clients.
- A cap on activations in flight, answering `503` beyond it, against floods of requests for existing shards that each cost a restore.
- LTX files over S3's 5 GB limit for a single PUT, with multipart uploads.
- Distributed tracing with W3C trace context, and counting shardless requests that lead to calls to a shard.
- Metrics for everything the node does. Shards the app holds open past their idle timeout need a gauge beside the count of holds warned, so a leak shows as a level and not only in the logs.
- Protocol versions for rolling node upgrades: each node publishes the range it speaks in its lease, and every request between nodes names the highest version both sides speak.
- Format versions for everything shardlite stores, with a marker per deployment that a node from an older release refuses.
- The `shardlite` command for operators: `ls`, `export`, `import`, `verify` and `gc`. `gc` deletes the LTX files no manifest lists, past the journal's expiry, with the walk in `internal/audit`, and `verify` reports them.
- A soak test: the linearizability check (`TestFaultCluster`) run for hours, with slow storage beside its kills, pauses, cuts and drains.
- Checking the cluster and dispatch models against the code, as capture's are by replay: the fault runs log the nodes' events (leases, fences, journal segments, manifest swaps, reminder entries) and a checker confirms each run's log is a behaviour of `cluster.qnt` or `dispatch.qnt`. Quint has no built-in support for checking a log, so this starts with a prototype on one fault scenario.
- What a store outage does to the cluster: the lease lives in blob storage, so a store no node can reach stops every node until it returns. What is left: how fast the cluster comes back, what the operator sees, and whether a degraded mode is worth having. The fault rig should have a scenario for it.

## Later

- Large shards: a rebase uploads the whole database, so shards of gigabytes may want a higher file limit or a middle level of merged deltas.
- Refusing commits through a connection an app kept open past a stop's let-go, which are lost today, by holding SQLite's WAL write lock on the deleted file until the app closes it. Blocked on making a lost POSIX lock visible.
- Encryption and authentication of [traffic between nodes](#securing-traffic-between-nodes).
- Checking tokens at the node, which closes the gap the [overview](overview.md#goal) names. It rewords "the node never inspects credentials" in the [guide](guide.md#routing-requests-to-shards), so it is a design decision of its own.
- A journal store of its own, a tier that waits for a fast store instead of blob storage, behind the `journal.Store` interface (`internal/journal`) and its conformance suite. Candidates: Postgres with synchronous replication, S3 Express One Zone.
- Blob backends for GCS and Azure Blob behind `blob.Backend`, passing the conformance suite and the startup check. Until then, GCP or Azure runs its own RustFS (see [deployment](deployment.md#stores)).

## Later designs

Features from [Later](#later) that are designed in more detail.

### Securing traffic between nodes

v1 assumes the nodes are on a trusted private network. The options: built-in mTLS from a shared cluster secret, your own certificates from cert-manager, Vault or step-ca, or an encrypted network (WireGuard, Tailscale) with shardlite staying unencrypted.

Service meshes are not a target, since they add two proxy hops to every call between nodes. In a cluster that already runs one, exclude shardlite's traffic from it.

## Open questions

- **Flush interval.** It sets the loss window on a node crash (for `none` kinds), the latency of `sync` and the PUT rate per node. 250 ms for now.
- **WAL capture correctness.** The riskiest part, already checked by the model, the replay of its traces and the fault tests.
- Tail age limit and file limit: shorter tails mean faster recovery after a crash at the cost of more deltas, and rarer rebases mean more files per restore.
- Node lease TTL: short enough for fast failover, long enough to survive VM pauses and network blips, and the same margin is how long a store outage the cluster rides out.
- **Regions.** v1 is one region; region-locked games run a cluster per region. Global games would need a home region in the manifest, forwarded requests, explicit moves and data residency pinning.
- **The cost of an idle return.** Every return after an idle timeout pays a claim and a restore, and a return through a node whose owner cache names the old owner pays a client retry first. To measure with the bench: restore time against shard size, the retries. Mitigations: a longer `idle_timeout`, or keeping a released shard's files for reuse while the manifest is still this node's version.
- **Throughput of a hot shard's wake.** One wake at a time per shard, each due reminder in its own transaction, so a hot shard with many due together may do better batching its rows. To measure with the bench: 1,000 due at once, one row per transaction and in batches.
