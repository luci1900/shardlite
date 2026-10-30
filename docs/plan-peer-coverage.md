# Plan: peer answers on coverage

A `peer` answer waits today for the peer's frontier, the peer's own unbroken run of segments. This plan makes it wait for coverage instead: every segment up to the answer's in the record or on the published peer. It builds on the peer as it stands (the fan-out, the fetch before the fence and the read at every fence, none of which change) and removes the seed, which exists only because the frontier has to be unbroken. It sits before any further work on the peer because it shrinks the repair to choosing a node, and the repair is the part of the peer that costs most to reason about.

## The problem

The `peer` tier's gate (`waitPeer` in `internal/node/flush.go`) waits on `Writer.PeerReached`, the entries the peer has taken in order (`mirrorOut.take` in `internal/segment/fanout.go`). The record's copy never counts toward it, even though a segment in the record is held more strongly than one on the peer.

So a replacement peer is no use until it holds everything the old one held. The repair (`internal/node/repair.go`) therefore:

- waits for `Writer.Settled`, every segment the writer built in the record, so the seed has no hole;
- seeds the replacement with this life's whole landed run, from `Writer.StartSeq` up, segment by segment (`repairLoop.seed`), which is work proportional to the life of the journal;
- swaps it in only if no segment was built meanwhile (`Writer.Repair` answers false and the pass starts again), and drains the queue under `peerMu` so the seed and the queue neither overlap nor leave a hole (`queuePeer`, `TestRepairDrainsTheQueuedBatches`).

`specs/mirrors.qnt` writes the same rule as `covered`: the peer holds every number built, no holes. The model's record lands every segment in the step that builds it, so it never sees the record lag behind the peer, which is the case the seed exists for.

### A gap found while reading for this plan

An adoption fetches from the peer the dead node's lease names (`l.Peer` in `internal/node/leasescan.go`), and the lease names a repaired peer only from the next renewal (`NodeInfo` in `internal/node/node.go` reads `currentPeer`). `Writer.Repair` counts the replacement's takes as soon as it swaps. A node that answers `peer` writes on the replacement and dies before the renewal leaves an adopter fetching from the old peer, which never had them: acknowledged commits lost outside the window the guide states. The window is one renewal interval after each repair. The first part checks this with a test before anything changes, since this plan's fix for it is the publication rule below.

## The design

A `peer` answer for a chunk may leave once every segment up to the one carrying it is **covered**: in the record's landed run, or taken by the **published peer**, the peer the latest renewed lease names. The two runs are both in number order (the record's uploader and the peer's send are serial), so coverage is the record's landed run followed by the published peer's run from where the record's ends. That is exactly the run the adoption's fetch takes back ("what the peer holds above the run the record landed", [Takeover](internals.md#takeover)), so the fetch needs no change.

The repair becomes:

1. Notice the peer does not stand, as now.
2. Choose a replacement from the lease listing, as now.
3. Swap it into the writer at once, starting at the lowest number still queued (or the next number to build). Nothing is copied into it.
4. The replacement's takes join coverage only once a renewal naming it has landed, and only once the record's landed run reaches the old peer's frontier, so the old peer never holds something coverage relies on that the record lacks.

Below the replacement's first number the record alone covers, which it will within a PUT, so a swap costs at most one record write of latency on the answers in flight and nothing else.

### Decisions

- **Coverage counts the record.** A segment in the record is held more strongly than one on the peer, so letting it count never weakens an answer. Chosen over a frontier the seed keeps unbroken, which costs a copy of the life's whole run per repair.
- **Only the published peer counts.** An adopter fetches from the peer the lease names, so a copy anywhere else is one no adoption reads. Chosen over counting the replacement from the swap, which is the gap above. The renewal after a repair can be asked for at once rather than waited for, so the wait is a lease write, not an interval.
- **The old peer stops counting only once the record has caught up with it.** Until then the answers it carried are in no other readable place. Chosen over swapping at any moment, which would let an adopter fetch from the replacement and find a hole where the old peer's run was. It is weaker than `Settled` (the record against the old peer's frontier, not against everything built), and the record's uploader retries until it lands, so it always comes.
- **A replacement starts empty.** Its run starts at the lowest number still queued, and the queue is handed over rather than drained. Chosen over seeding, which coverage makes unnecessary.
- **An answer already waiting when the peer falls out completes once the record covers it.** Today it fails at the fall (`PeerOut` in `waitPeer`). The record's coverage is stronger than the tier asked for, so this gives nothing weaker, and it removes a burst of `503`s on every fall. Chosen over failing at the fall.
- **A new `peer` answer still fails while no peer stands.** The tier promises latency as well as durability, and an operator should see a missing peer, not a tier that has quietly become `storage`. Chosen over answering on the record alone. This keeps the guide's rule that a tier the node cannot meet is an error.
- **The diagnostic keeps the frontier.** `GET /_shardlite/node` still reports the peer's frontier and whether it is in step (`PeerCovered`), since that is what an operator wants to see. Only the gate stops reading it.

## Docs

- `docs/internals.md`, Node journals and snapshots: the fan-out keeps the frontier as the peer's own run. The repair loses the seed and the wait for the record to settle, and says when a replacement starts counting. Durability gates: the `peer` tier waits for coverage, defined there.
- `docs/internals.md`, Concurrency: the repair is a choice and a swap. The swap no longer drains the queue.
- `docs/guide.md`, Durability: the repair sentence ("The repair names a peer for a start that ran alone and replaces one that fell out or died") gains that `peer` writes work again once the next renewal names the replacement, and that an answer in flight when the peer falls out completes on the record. The loss window is unchanged.
- `specs/mirrors.qnt`, its header: coverage, the record lagging behind the peer, and replacement.
- While there: internals still says "kinds with sync on" in Consistency and Takeover, from before the tiers. Those become "kinds on the `storage` tier" or the tier each sentence means.

## Open questions

- Whether a renewal should be asked for at once after a swap, or the replacement left to count from the next scheduled one. Asking costs one lease write per repair and shortens the time `peer` answers fail. The plan assumes asking.
- Whether the old peer's copy should be deleted once the record has caught up with it. It is already garbage once the record has every segment, and the peer's handler accepts `DELETE`, so this is tidiness, not correctness.

## Testing

- `specs/mirrors.qnt`: the record lands asynchronously, in order (a `recLand` step, and the death strands what is in flight), and the peer can be replaced (a pairing number on each peer copy, a published pairing the adoption fetches from). `covered` becomes the coverage rule over the record and the published pairing's copies. `noLoss` still fails on `peerAcks` (the tier's price) and `noStrandedAcks` still holds. Two new variants, each expected to fail `noStrandedAcks`: `countBeforePublish` (the replacement counts from the swap, the gap above) and `swapBeforeRecord` (the old peer stops counting before the record catches up with it). A scenario per rule. Checked with `quint run`, as AGENTS.md says.
- `internal/node/mirror_test.go`: first, a test that reproduces the gap on today's code (answer `peer` writes on a replacement, kill the owner before the renewal, check the adopter's record). Then: a repair with the record lagging swaps without seeding and answers once the record covers the gap; a replacement's takes do not count until a renewal names it; an answer in flight when the peer falls out completes on the record. `TestTheRepairWaitsForTheRecord` changes to the new rule: the swap waits for the record to reach the old peer's frontier, not for everything built.
- `internal/segment/fanout_test.go`: coverage follows the record and the peer together, a hole on the peer that the record has does not hold an answer, and the swap hands the queue over. `TestRepairDrainsTheQueuedBatches` goes, replaced by a test that the replacement sends what was queued. `TestPeerReachedFollowsTheTakenRun` stays for the diagnostic.
- The fault rig: a kill of the owner between a repair and the renewal that names it, and a kill while the record lags behind a replacement, checked for no acknowledged loss within the stated window.

## Parts

Stop after each part. Each part has a section in the report beside this plan.

1. **The gap, shown.** The test in `internal/node/mirror_test.go` that reproduces it on today's code, and `specs/mirrors.qnt` with the asynchronous record, replacement and the `countBeforePublish` variant failing as today's code does. No behaviour changes.
2. **Coverage in the model.** `covered` becomes the coverage rule, `swapBeforeRecord` is added, and both new variants fail while `peerAcks` holds `noStrandedAcks`. No code changes.
3. **Coverage in the writer.** `internal/segment`: a covered counter advanced when a batch is in the record or taken by a peer that counts, `Writer.CoverageReached` beside `PeerReached`, the published pairing and the old peer's release as inputs the node sets. `waitPeer` in `internal/node/flush.go` waits on it, and an answer in flight completes on the record when the peer falls out. The repair still seeds, so behaviour only gains.
4. **The repair without a seed.** `internal/node/repair.go` loses `seed` and the `Settled` wait, swaps at once and asks for a renewal. `Writer.Repair` takes the replacement and its first number and hands the queue over. `Settled` and the drain go if nothing else uses them.
5. **Docs.** Everything under Docs above, including the stale "sync on" wording.
