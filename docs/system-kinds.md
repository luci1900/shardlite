# shardlite system kinds

The node's own shards, and how to use them for work that one node should do for the whole cluster. The rest of the node is in [internals](internals.md), and the app-facing summary is in the [guide](guide.md#system-kinds).

Some of the node's own work needs one runner, and somewhere to keep state if it needs any. A shard already has both (one owner, fencing, adoption and a SQLite file), so the node keeps shards of its own under reserved kinds, each served by a system app built into the node. **A system kind may be late, never wrong:** while one is unavailable things get slower or messier but nothing becomes incorrect, so nothing a shard needs in order to activate can be a system kind.

A system kind behaves as an app kind wherever practical, and differs in three ways:

- **Reserved names, declared in code.** `_sweep` and `_dispatch`, with ids such as `_cluster`. The list is the same on every node of a version, and adding a kind in a rolling deploy is safe since old nodes never claim it. No system kind is public, and only its fixed ids exist.
- **Its app is in the node.** An `http.Handler` per shard, reached through the same routing as an app's calls, with its responses gated the same way. It sees only its shard (a transaction on capture's connection) and other system shards through calls, and gets whatever else it needs when it is built. Routes only the node may send live under `/_shardlite/`, which every listener refuses before routing.
- **Fixed ids are kept claimed.** The claimer acquires each fixed id that isn't active here: at startup, when a node dies or is adopted, and every 10 minutes as a safety net. The claim rule makes this idempotent, so one node ends up with each. At activation the node applies the kind's migrations and seeds a reminder due now (and makes the one it has due now if an outage longer than reminders are kept left it stale, which would otherwise end the shard's runs for good). Fixed ids never go idle and set no `next_reminder`, since the claimer brings them back.

Schemas are embedded migrations, and a system shard's `user_version` is its version. A node finding a shard migrated by a newer release gives it up unchanged, so a newer node claims it.

| Kind | Ids | What it does |
|---|---|---|
| `_sweep` | `_cluster` | deletes the LTX files no manifest lists (see [Garbage](internals.md#garbage)) |
| `_dispatch` | `_cluster` | wakes the shards whose reminders are due while they are inactive (see [`_dispatch`](#_dispatch)) |

## Cluster-wide coordination

A system kind is how the node does work that exactly one node should do at a time, without a leader election or a coordination service. A shard already gives the pieces: one owner at any moment (the claim rule), a fence against a stale owner, adoption when the owner dies and a SQLite file for the state the work needs. A fixed id is a singleton job, and the claimer keeps it running somewhere. `_dispatch` and `_sweep` are two uses of this.

What the pattern gives:

- **One runner.** The claim rule picks one node per id. After a crash or a move, another node claims the shard and the claimer brings it back, so the job needs no election and no handover.
- **State that moves with the job.** The sweeper's cursor lives in its shard's tables, so a new owner carries on from the last committed batch. Its state is as durable as any shard's.
- **A schedule.** The job's reminder is its timer. A wake runs one pass, and the pass sets the next reminder.
- **Fencing.** A zombie owner can't commit past its fence, and a job that deletes or sends checks the lease first.

What a job has to be:

- **Late, never wrong.** A move or a crash can repeat the last pass, or skip a few seconds of passes. Every step is safe to redo, as the sweeper's batches and the dispatcher's moves are, and a job that is down only makes things slower.
- **Off the critical path.** Nothing a shard needs to activate, serve or be released can wait on a system kind, since the kind itself needs activating.
- **Not a hot path.** One shard is one writer on one node. The work per pass is bounded (a page of 1,000 keys, at most 256 calls at once, 25 seconds) and a job that outgrows one node splits by id (`_dispatch/_0` to `_dispatch/_n`) rather than by being made faster.
- **Built into the node.** The list of kinds is code, the same on every node of a version, and its routes live under `/_shardlite/`.

Work that fits: sweeping garbage, waking shards from an index and other cleanup (retiring leases, on the roadmap). Work that doesn't: anything an app should own (use an app kind) and anything that needs an answer in the request path.

## `_dispatch`

`_dispatch/_cluster` lists the reminder index up to the current minute and sends each due shard a **dispatch**: a call that makes the shard active on its owner, or claims it on a node picked for load. The shard's reminders loop then wakes the app as it does for any active shard.

- **One shard.** Its database holds only its `run` reminder, and its kind has sync off. One owner's death leaves inactive reminders late by a lease TTL and a claim, which minute resolution already allows. The work per entry is a manifest read and a call, so one node should handle thousands a second, and the listing is one paginated listing a minute. Splitting it later needs no migration, since the shard holds nothing: `_dispatch/_0` to `_dispatch/_n` would each take some `{p}` values.
- **A pass per wake.** It lists `index/reminders/` from the start up to the current minute, one page of 1,000 keys. Processed entries leave the head of the listing, so there is no cursor, and a cold start, a move and catching up after an outage are the same case. For each entry, at most 256 at once (an `errgroup` inside the handler), it reads the manifest. An entry the manifest doesn't back is deleted if the pruning rule allows it and moved forward otherwise. A backed entry gets its shard dispatched and is then moved forward whatever the answer. The next pass is a second later if the page held due entries, and otherwise at the start of the next minute. A pass stops within 25 s and leaves the rest to the next.
- **Moving forward** creates the entry `{base}.{n+1}` at the minute `min(2^n, 16)` minutes from now, then deletes the old one. An entry whose shard was active already goes straight to 16 minutes, since its reminders loop covers it and the entry is only there for a crash with nothing of the shard in the journal. An entry whose dispatch keeps failing (a kind removed from the kinds file, a restore that fails) backs off the same way without blocking the head. The index carries the backoff, so the shard stays stateless and a stuck backlog is visible in the bucket. A failed dispatch is logged and counted.
- **Where a dispatch goes.** To the owner if the lease scan's last listing has it alive. Otherwise (no owner, a dead owner, an owner adopted up to its epoch) to the better of two live nodes picked at random, the one with fewer of this dispatcher's dispatches in flight. If the owner doesn't take it (a node that stopped is listed alive until its lease is a TTL old) it is tried once on a node picked that way. Lease counts are seconds old and would send a herd to the same node, so they aren't used, and the dispatcher's own node gets no preference.
- **The call.** `POST /_shardlite/dispatch` on the peer listener, or in process when the target is this node. The receiver acquires the shard and never creates it: active here answers at once, cached here is reactivated, and an unowned or claimable shard is claimed here and restored. The answer is `200` once the shard is active, saying whether it was active already or the dispatch activated it. A shard another live node owns answers `421` naming it, one whose owner awaits adoption answers `503`, and no manifest answers `404`.
- **Herds.** The in-flight limit and a rate limit (1,000 dispatches a second) bound how fast a minute with many entries drains, so a daily reset at midnight spreads over minutes.

