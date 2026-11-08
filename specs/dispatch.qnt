// Who wakes a shard that isn't active: an entry in a reminder index in blob storage, created before every manifest
// write that records when the shard next needs waking, and a dispatcher that lists the index, wakes the shards it
// finds and deletes the entries their manifests don't back.
//
// One shard, whose state is a version and the minute its pending reminder is due (`rem`, -1 for none). The cluster
// is assumed, as cluster.qnt checks it: one live node owns the shard at a time, a node's journal holds what it has
// flushed and nothing a fenced node writes after, and a dead owner's shard can't be claimed until its journal is
// adopted. So here a node is alive or dead (for good), the shard on it is active or not there, with what its file holds
// (`local`) and what its journal holds (`durable`). A crash leaves the tail to an adopter, and an adoption is the two
// steps the index cares about: the entry, then the delta that sets `next_reminder` to the adoption's minute and
// clears the journal. A write the dead node had in flight may still land (a zombie's CAS), since nothing fences a
// manifest. Leases, segments, fences, sweeps, restarts and journal expiry are cluster.qnt's and left out, since none
// changes what a restore sees.
//
// The manifest's `nr` is `next_reminder`: owner writes (deltas and deactivations) set it from the state they
// write (here exactly `rem`), claims keep it, and an adoption's delta sets it to the adoption's minute
// (the adopter can't read the shard's reminders). The rules:
//   - Index first: before a manifest write that sets `nr` to a minute, the writer creates the entry for that minute
//     with its own base (the ETag the write will CAS against). It may pause between the two, and its CAS fails if
//     the manifest has moved on.
//   - Each write has its own entry: one that exists already under another base never counts.
//   - Adoption creates an entry at its minute, with the base its delta will CAS against, before the shard's delta,
//     and the delta sets `nr` to that same minute.
//   - The pruning rule: an entry may be deleted when the manifest doesn't back it (there is none, or `nr` is absent
//     or after the entry's minute) and the manifest's ETag is not the entry's base. A backed entry is dispatched and
//     moved forward, never deleted.
//   - A dispatcher reads the manifest and acts later on what it read. There may be two at once.
//
// ETags change on every manifest write. (On S3, where the ETag is the MD5 of the body, that needs every write's body
// to differ from every earlier body of the same manifest, which the manifest's `rev` counter, up by one on every
// write, makes so.)
//
// The invariant (covered): a shard whose state, as a restore would see it, has a reminder has a backed entry or a
// tail that an adoption will still write a delta for. An active shard needs one too, since its node can die before
// any of it reaches the journal.
//
// Variants (constants below) each drop one rule, to show it is needed. Each has an instance at the end.
//
// A deactivation is one write that records the shard's state and releases it (no owner, no journal), and the node's
// files go. So a shard that isn't active on a node is not there at all, and the next dispatch claims it where it lands.
//
// Left out: lost responses, deletion, rebasing, reads that don't match the files (they set
// `next_reminder` to now, which is no different here), how far a move goes (any later minute is as safe as
// another, so moves go one minute on), where dispatches go and limits.
//
// Run (simulation only: `quint verify` fails on a file with four or more instances before it starts, and a copy
// with the protocol's instance alone verifies 4 steps in seconds but not 6 in 15 minutes):
//   quint typecheck specs/dispatch.qnt
//   quint run specs/dispatch.qnt --main=dispatchProtocol --invariant=safe --max-steps=40 --max-samples=50000
//   quint run specs/dispatch.qnt --main=pruneWithoutBase --invariant=covered --max-steps=40 --max-samples=50000   (should fail)
//   quint run specs/dispatch.qnt --main=deleteOnAnswer --invariant=covered --max-steps=40 --max-samples=50000   (should fail)
//   quint run specs/dispatch.qnt --main=manifestFirst --invariant=covered --max-steps=40 --max-samples=50000   (should fail)
//   quint run specs/dispatch.qnt --main=noAdoptionEntry --invariant=covered --max-steps=40 --max-samples=50000   (should fail)
//   quint test specs/dispatch.qnt --main=dispatchProtocol
//   quint test specs/dispatch.qnt --main=sharedEntry --match=sharedEntryExample   (should fail)
//
// Simulation rarely reaches sharedEntry's counterexample (it needs three writes, a stale entry and a dispatcher's read
// in one trace), so it has a scripted example instead.

module dispatch {
  const NODES: Set[int]
  const DISPATCHERS: Set[int]
  const MAX_COMMITS: int
  const MAX_TIME: int

  // Variants, each false in the protocol:
  //   SHARED_ENTRY: a writer that finds an entry for its minute under another base counts it as its own.
  //   PRUNE_WITHOUT_BASE: the dispatcher deletes an entry the manifest doesn't back without checking its base.
  //   DELETE_ON_ANSWER: the dispatcher deletes a backed entry once the shard's node answers that it is active.
  //   MANIFEST_FIRST: an owner creates its entry after its manifest write rather than before.
  //   NO_ADOPTION_ENTRY: adoption writes the shard's delta without creating an entry.
  const SHARED_ENTRY: bool
  const PRUNE_WITHOUT_BASE: bool
  const DELETE_ON_ANSWER: bool
  const MANIFEST_FIRST: bool
  const NO_ADOPTION_ENTRY: bool

  type St = { v: int, rem: int }

  type Manifest = {
    exists: bool,
    etag: int,          // changes on every write
    owner: int,         // a node, -1 if none
    journal: bool,      // the owner's journal may hold commits after snap
    snap: St,
    nr: int             // next_reminder, -1 if absent
  }

  type Entry = { minute: int, base: int, n: int }   // the key's minute, base and move count

  // An owner's manifest write in progress: its entry created (unless MANIFEST_FIRST) and its CAS still to come.
  // late: with MANIFEST_FIRST, the CAS landed and the entry is still to come.
  type Write = { on: bool, late: bool, snap: St, deact: bool, base: int }

  type NodeState = {
    alive: bool,
    adopted: bool,      // dead, and its journal adopted (or released by a stop)
    active: bool,
    local: St,
    durable: St,        // the last state in its journal (or the snapshot it restored)
    mseen: Manifest,
    w: Write
  }

  // An adoption of a dead node's shard: its entry created, its delta still to come.
  // minute: the adoption's minute, for the entry and for nr. base: the ETag its delta CASes against.
  type Adoption = { on: bool, target: int, minute: int, base: int }

  // phase: 0 idle, 1 read the manifest for the entry, 2 dispatched (answered: the shard was active on the node).
  type Disp = { phase: int, e: Entry, m: Manifest, answered: bool }

  type State = {
    manifest: Manifest,
    entries: Set[Entry],
    nodes: int -> NodeState,
    adoptions: int -> Adoption,
    disps: int -> Disp,
    now: int,
    next: int,
    etags: int,
    // witnesses
    pruned: bool,        // an entry was deleted by the pruning rule
    keptForBase: bool,   // an entry the manifest didn't back was kept because its base was current
    moved: bool,         // a backed entry was moved forward
    claimedByDispatch: bool,
    adoptionEntry: bool, // an adoption created an entry
    casFailed: bool,     // a prepared owner write found the manifest moved on
    raced: bool          // two dispatchers held the same entry at once
  }

  var s: State

  // -- Helpers

  pure val noSt: St = { v: -1, rem: -1 }
  pure val emptySt: St = { v: 0, rem: -1 }
  pure val noManifest: Manifest = { exists: false, etag: 0, owner: -1, journal: false, snap: emptySt, nr: -1 }
  pure val noWrite: Write = { on: false, late: false, snap: noSt, deact: false, base: -1 }
  pure val noEntry: Entry = { minute: -1, base: -1, n: -1 }
  pure val noAdoption: Adoption = { on: false, target: -1, minute: -1, base: -1 }
  pure val idle: Disp = { phase: 0, e: noEntry, m: noManifest, answered: false }
  pure val fresh: NodeState = { alive: true, adopted: false, active: false, local: emptySt,
                                durable: emptySt, mseen: noManifest, w: noWrite }

  // The owner's journal holds commits past the manifest's snapshot.
  def tailed(st: State): bool = {
    val mf = st.manifest
    mf.exists and mf.journal and mf.owner >= 0 and st.nodes.get(mf.owner).durable.v > mf.snap.v
  }

  // What a restore gives.
  def truth(st: State): St =
    if (not(st.manifest.exists)) emptySt
    else if (tailed(st)) st.nodes.get(st.manifest.owner).durable
    else st.manifest.snap

  def claimable(st: State): bool =
    if (not(st.manifest.exists) or st.manifest.owner < 0) true
    else {
      val o = st.nodes.get(st.manifest.owner)
      not(o.alive) and (not(st.manifest.journal) or o.adopted)
    }

  // The manifest backs the entry: it has a next_reminder no later than the entry's minute.
  pure def backs(m: Manifest, e: Entry): bool = m.exists and m.nr >= 0 and m.nr <= e.minute

  // The pruning rule, on the manifest as the dispatcher read it.
  pure def prunable(m: Manifest, e: Entry): bool =
    not(backs(m, e)) and (PRUNE_WITHOUT_BASE or not(m.exists) or m.etag != e.base)

  def withNode(st: State, n: int, ns: NodeState): State = { ...st, nodes: st.nodes.put(n, ns) }
  def withAdoption(st: State, n: int, a: Adoption): State = { ...st, adoptions: st.adoptions.put(n, a) }
  def withDisp(st: State, d: int, x: Disp): State = { ...st, disps: st.disps.put(d, x) }

  // A manifest write, with a new ETag.
  def writeManifest(st: State, m: Manifest): State = { ...st, manifest: { ...m, etag: st.etags + 1 }, etags: st.etags + 1 }

  def addEntry(st: State, e: Entry): State = { ...st, entries: st.entries.union(Set(e)) }

  // Claim (or create) the shard on node n.
  def claimed(st: State, n: int): State = {
    val ns = st.nodes.get(n)
    val content = truth(st)
    val st1 = writeManifest(st, { ...st.manifest, exists: true, owner: n, journal: true })
    withNode(st1, n, { ...ns, active: true, local: content, durable: content, mseen: st1.manifest })
  }

  def canClaim(st: State, n: int): bool = {
    val ns = st.nodes.get(n)
    ns.alive and not(ns.active) and claimable(st)
  }

  action init = s' = {
    manifest: noManifest,
    entries: Set(),
    nodes: NODES.mapBy(n => fresh),
    adoptions: NODES.mapBy(n => noAdoption),
    disps: DISPATCHERS.mapBy(d => idle),
    now: 0,
    next: 1,
    etags: 0,
    pruned: false,
    keptForBase: false,
    moved: false,
    claimedByDispatch: false,
    adoptionEntry: false,
    casFailed: false,
    raced: false
  }

  action tick = all {
    s.now < MAX_TIME,
    s' = { ...s, now: s.now + 1 }
  }

  // -- Owning and serving

  // Claim (or create) the shard, by a request.
  action claim(n) = all {
    canClaim(s, n),
    s' = claimed(s, n)
  }

  // A commit, which may set a reminder (due now or later) or run the pending one.
  action commit(n, rem) = all {
    val ns = s.nodes.get(n)
    all {
      ns.active,
      not(ns.w.on),
      s.next <= MAX_COMMITS,
      rem < 0 or rem >= s.now,
      s' = { ...withNode(s, n, { ...ns, local: { v: s.next, rem: rem } }), next: s.next + 1 }
    }
  }

  action flush(n) = all {
    val ns = s.nodes.get(n)
    all {
      ns.active,
      ns.local != ns.durable,
      s' = withNode(s, n, { ...ns, durable: ns.local })
    }
  }

  // The first half of a delta (deact false) or of a deactivation (deact true), which always writes the manifest
  // (even with nothing new to record, since it releases the shard): the entry for the state's reminder, with the
  // ETag the write will CAS against as its base.
  action prepare(n, deact) = all {
    val ns = s.nodes.get(n)
    val snap = ns.durable
    val base = ns.mseen.etag
    val e = { minute: snap.rem, base: base, n: 0 }
    val shared = SHARED_ENTRY and s.entries.exists(x => x.minute == snap.rem)
    all {
      ns.active,
      not(ns.w.on),
      if (deact) ns.local == ns.durable else ns.mseen.snap.v < ns.durable.v,
      val st = if (snap.rem >= 0 and not(MANIFEST_FIRST) and not(shared)) addEntry(s, e) else s
      s' = withNode(st, n, { ...ns, w: { on: true, late: false, snap: snap, deact: deact, base: base } })
    }
  }

  // The second half: the CAS, which may land after its node has died. A deactivation's also releases the shard (no
  // owner, no journal) and the node's files go. The code asks again until it learns the outcome and never serves
  // the shard on a guess, so a node that dies or stops first just leaves the write to land or not (a crash). A
  // manifest that has moved on fails the CAS, and the node no longer owns the shard.
  action cas(n) = all {
    val ns = s.nodes.get(n)
    val w = ns.w
    all {
      w.on,
      not(w.late),
      if (s.manifest.exists and s.manifest.etag == w.base) {
        val m = if (w.deact) { ...s.manifest, owner: -1, journal: false, snap: w.snap, nr: w.snap.rem }
                else { ...s.manifest, snap: w.snap, nr: w.snap.rem }
        val st = writeManifest(s, m)
        val late = MANIFEST_FIRST and w.snap.rem >= 0 and ns.alive
        val w2 = if (late) { ...w, late: true } else noWrite
        s' = withNode(st, n, if (not(ns.alive)) { ...ns, w: noWrite }
                             else if (w.deact) { ...ns, active: false, local: emptySt, durable: emptySt, mseen: noManifest, w: w2 }
                             else { ...ns, mseen: st.manifest, w: w2 })
      } else
        s' = { ...withNode(s, n, { ...ns, active: false, w: noWrite }), casFailed: true }
    }
  }

  // MANIFEST_FIRST: the entry, after the manifest write.
  action lateEntry(n) = all {
    val ns = s.nodes.get(n)
    all {
      ns.alive,
      ns.w.late,
      s' = withNode(addEntry(s, { minute: ns.w.snap.rem, base: ns.w.base, n: 0 }), n, { ...ns, w: noWrite })
    }
  }

  // -- Crashes and stops

  // Some other node stays alive, so a crash or a stop leaves someone to adopt and to claim.
  def survivor(st: State, n: int): bool = NODES.exists(o => o != n and st.nodes.get(o).alive)

  // A crash: the file and any adoption in progress are lost, the journal stays for an adopter, and a write in
  // flight may still land.
  action crash(n) = all {
    val ns = s.nodes.get(n)
    all {
      ns.alive,
      survivor(s, n),
      s' = withAdoption(withNode(s, n, { ...ns, alive: false, active: false, local: emptySt,
                                         w: if (ns.w.late) noWrite else ns.w }), n, noAdoption)
    }
  }

  // A stop: nothing active, so every shard has been let go of, and the released lease counts as adopted. Nothing
  // about reminders.
  action stop(n) = all {
    val ns = s.nodes.get(n)
    all {
      ns.alive,
      survivor(s, n),
      not(ns.active),
      not(ns.w.on),
      not(s.adoptions.get(n).on),
      s' = withNode(s, n, { ...ns, alive: false, adopted: true, local: emptySt })
    }
  }

  // -- Adoption

  // Whether an adoption of node x writes a delta for the shard's tail.
  def writesTail(st: State, x: int): bool = tailed(st) and st.manifest.owner == x

  // The entry before the delta: the adoption's minute, with the base the delta will CAS against.
  action adoptEntry(m, x) = all {
    val e = { minute: s.now, base: s.manifest.etag, n: 0 }
    all {
      m != x,
      s.nodes.get(m).alive,
      not(s.adoptions.get(m).on),
      not(s.nodes.get(x).alive),
      not(s.nodes.get(x).adopted),
      val st = if (writesTail(s, x) and not(NO_ADOPTION_ENTRY)) { ...addEntry(s, e), adoptionEntry: true } else s
      s' = withAdoption(st, m, { on: true, target: x, minute: s.now, base: s.manifest.etag })
    }
  }

  // The delta, by CAS against the entry's base, with next_reminder at the adoption's minute and the journal
  // cleared, and the journal adopted. A manifest that has moved on sends the adoption back to its entry.
  action adoptDelta(m) = all {
    val ad = s.adoptions.get(m)
    val x = ad.target
    val adopted = { ...s.nodes.get(x), adopted: true }
    all {
      ad.on,
      if (not(writesTail(s, x)))
        s' = withAdoption(withNode(s, x, adopted), m, noAdoption)
      else if (s.manifest.etag == ad.base)
        s' = withAdoption(withNode(writeManifest(s, { ...s.manifest, snap: s.nodes.get(x).durable, journal: false, nr: ad.minute }),
                                   x, adopted), m, noAdoption)
      else
        s' = withAdoption(s, m, noAdoption)
    }
  }

  // -- Dispatchers

  // A pass lists a due entry and reads the shard's manifest.
  action dRead(d, e) = all {
    s.disps.get(d).phase == 0,
    s.entries.contains(e),
    e.minute <= s.now,
    s' = { ...withDisp(s, d, { phase: 1, e: e, m: s.manifest, answered: false }),
           raced: s.raced or DISPATCHERS.exists(o => o != d and s.disps.get(o).phase > 0 and s.disps.get(o).e == e) }
  }

  // A dispatch of a backed entry to node n, which acquires the shard without creating it: active there answers,
  // claimable is claimed there. Otherwise it answers with an error.
  action dispatchTo(d, n) = all {
    val x = s.disps.get(d)
    val ns = s.nodes.get(n)
    all {
      x.phase == 1,
      backs(x.m, x.e),
      if (ns.active)
        s' = withDisp(s, d, { ...x, phase: 2, answered: true })
      else if (s.manifest.exists and canClaim(s, n))
        s' = { ...withDisp(claimed(s, n), d, { ...x, phase: 2, answered: true }), claimedByDispatch: true }
      else
        s' = withDisp(s, d, { ...x, phase: 2, answered: false })
    }
  }

  // After the read (and the dispatch of a backed entry): delete the entry if the pruning rule allows it, and
  // otherwise move it forward (a new entry with the same base, then the old one deleted).
  action dSettle(d) = all {
    val x = s.disps.get(d)
    val e = x.e
    val moveTo = { minute: s.now + 1, base: e.base, n: e.n + 1 }
    val isBacked = backs(x.m, e)
    val drop = if (isBacked) DELETE_ON_ANSWER and x.answered else prunable(x.m, e)
    all {
      if (isBacked) x.phase == 2 else x.phase == 1,
      if (drop)
        s' = { ...withDisp(s, d, idle), entries: s.entries.exclude(Set(e)), pruned: s.pruned or not(isBacked) }
      else
        s' = { ...withDisp(s, d, idle), entries: s.entries.exclude(Set(e)).union(Set(moveTo)),
               moved: s.moved or isBacked, keptForBase: s.keptForBase or not(isBacked) }
    }
  }

  // A dispatcher stops partway (a stop, a crash): it forgets its pass.
  action dStop(d) = all {
    s.disps.get(d).phase > 0,
    s' = withDisp(s, d, idle)
  }

  action step = {
    nondet n = NODES.oneOf()
    nondet x = NODES.oneOf()
    nondet d = DISPATCHERS.oneOf()
    nondet rem = Set(-1).union(0.to(MAX_TIME)).oneOf()
    nondet deact = Set(true, false).oneOf()
    nondet e = s.entries.union(Set(noEntry)).oneOf()
    any {
      tick,
      claim(n),
      commit(n, rem),
      flush(n),
      prepare(n, deact),
      cas(n),
      lateEntry(n),
      crash(n),
      stop(n),
      adoptEntry(n, x),
      adoptDelta(n),
      dRead(d, e),
      dispatchTo(d, n),
      dSettle(d),
      dStop(d)
    }
  }

  // -- Invariants

  // The owner's tail is in a journal an adoption will still read.
  val tailPending = tailed(s) and not(s.nodes.get(s.manifest.owner).adopted)

  val covered = (s.manifest.exists and truth(s).rem >= 0) implies
    (s.entries.exists(e => backs(s.manifest, e)) or tailPending)

  // The manifest's next_reminder is no later than its snapshot's reminder, or already due (an adoption sets it to its
  // minute, which is later than a reminder that was overdue by then, and the dispatcher lists everything due).
  val nrCovers = (s.manifest.exists and s.manifest.snap.rem >= 0) implies
    (s.manifest.nr >= 0 and (s.manifest.nr <= s.manifest.snap.rem or s.manifest.nr <= s.now))

  val safe = covered and nrCovers

  // Witnesses: each should be violated, which shows the state is reachable.
  val neverPruned = not(s.pruned)
  val neverKeptForBase = not(s.keptForBase)
  val neverMoved = not(s.moved)
  val neverClaimedByDispatch = not(s.claimedByDispatch)
  val neverAdoptionEntry = not(s.adoptionEntry)
  val neverCasFailed = not(s.casFailed)
  val neverRaced = not(s.raced)
  // A shard with a reminder whose manifest's next_reminder came from an adoption.
  val neverCoveredByAdoption = not(s.adoptionEntry and s.manifest.exists and truth(s).rem >= 0 and not(tailPending))

  // -- Scenarios

  // A shard deactivated with a reminder: its entry is dispatched when due, the shard is claimed again where the
  // dispatch lands and the entry moved forward.
  run deactivateTest =
    init
      .then(claim(0)).then(commit(0, 1)).then(flush(0)).then(prepare(0, true)).then(cas(0))
      .then(tick)
      .then(dRead(0, { minute: 1, base: 1, n: 0 })).then(dispatchTo(0, 0)).then(dSettle(0))
      .expect(s.nodes.get(0).active and s.entries == Set({ minute: 2, base: 1, n: 1 }) and safe)

  // A deactivated shard is claimed by the node its dispatch went to.
  run dispatchClaimTest =
    init
      .then(claim(0)).then(commit(0, 0)).then(flush(0)).then(prepare(0, true)).then(cas(0))
      .then(dRead(0, { minute: 0, base: 1, n: 0 })).then(dispatchTo(0, 1)).then(dSettle(0))
      .expect(s.nodes.get(1).active and s.claimedByDispatch and safe)

  // A shard whose reminder ran: the next delta sets no next_reminder and the old entry is pruned.
  run pruneTest =
    init
      .then(claim(0)).then(commit(0, 0)).then(flush(0)).then(prepare(0, false)).then(cas(0))
      .then(commit(0, -1)).then(flush(0)).then(prepare(0, true)).then(cas(0))
      .then(dRead(0, { minute: 0, base: 1, n: 0 })).then(dSettle(0))
      .expect(s.entries == Set() and s.pruned and safe)

  // The race the base is for: the dispatcher reads the manifest before an owner's write lands. The entry stays,
  // and the write finds it.
  run raceTest =
    init
      .then(claim(0)).then(commit(0, 0)).then(flush(0)).then(prepare(0, true))
      .then(dRead(0, { minute: 0, base: 1, n: 0 }))
      .then(cas(0))
      .then(dSettle(0))
      .expect(s.entries == Set({ minute: 1, base: 1, n: 1 }) and s.keptForBase and safe)

  // An active shard whose owner crashes after committing a reminder: the adopter's entry covers it.
  run activeCrashTest =
    init
      .then(claim(0)).then(commit(0, 1)).then(flush(0))
      .then(crash(0))
      .then(adoptEntry(1, 0)).then(adoptDelta(1))
      .expect(s.manifest.nr == 0 and s.entries == Set({ minute: 0, base: 1, n: 0 }) and truth(s).rem == 1 and safe)

  // A shard dispatched and active whose node dies before anything reaches the journal: adoption finds no tail,
  // and the moved entry still wakes it.
  run dispatchedCrashTest =
    init
      .then(claim(0)).then(commit(0, 0)).then(flush(0)).then(prepare(0, true)).then(cas(0))
      .then(dRead(0, { minute: 0, base: 1, n: 0 })).then(dispatchTo(0, 1)).then(dSettle(0))
      .then(crash(1))
      .then(adoptEntry(2, 1)).then(adoptDelta(2))
      .expect(s.entries == Set({ minute: 1, base: 1, n: 1 }) and claimable(s) and safe)

  // A zombie's write: the owner crashes with its delta's CAS in flight, the adopter's delta lands first and the
  // zombie's CAS fails. The adopter's entry covers the shard.
  run zombieTest =
    init
      .then(claim(0)).then(commit(0, 1)).then(flush(0)).then(prepare(0, false))
      .then(crash(0))
      .then(adoptEntry(1, 0)).then(adoptDelta(1))
      .then(cas(0))
      .expect(s.casFailed and s.manifest.nr == 0 and claimable(s) and safe)

  // A stop does nothing about reminders: the shard's entry covers it.
  run stopTest =
    init
      .then(claim(0)).then(commit(0, 1)).then(flush(0)).then(prepare(0, true)).then(cas(0))
      .then(stop(0))
      .then(tick)
      .then(dRead(0, { minute: 1, base: 1, n: 0 })).then(dispatchTo(0, 1)).then(dSettle(0))
      .expect(s.nodes.get(1).active and safe)

  // SHARED_ENTRY's counterexample: a write that set a reminder and one that cleared it leave a stale entry, a third
  // write for the same minute counts it, and the dispatcher, having read the manifest between, prunes it.
  run sharedEntryExample =
    init
      .then(claim(0)).then(commit(0, 0)).then(flush(0)).then(prepare(0, false)).then(cas(0))
      .then(commit(0, -1)).then(flush(0)).then(prepare(0, false)).then(cas(0))
      .then(commit(0, 0)).then(flush(0)).then(prepare(0, true))
      .then(dRead(0, { minute: 0, base: 1, n: 0 })).then(dSettle(0))
      .then(cas(0))
      .expect(covered)
}

module dispatchProtocol {
  import dispatch(NODES = Set(0, 1, 2), DISPATCHERS = Set(0, 1), MAX_COMMITS = 3, MAX_TIME = 3,
                  SHARED_ENTRY = false, PRUNE_WITHOUT_BASE = false, DELETE_ON_ANSWER = false, MANIFEST_FIRST = false,
                  NO_ADOPTION_ENTRY = false).*
}

module sharedEntry {
  import dispatch(NODES = Set(0, 1, 2), DISPATCHERS = Set(0, 1), MAX_COMMITS = 3, MAX_TIME = 3,
                  SHARED_ENTRY = true, PRUNE_WITHOUT_BASE = false, DELETE_ON_ANSWER = false, MANIFEST_FIRST = false,
                  NO_ADOPTION_ENTRY = false).*
}

module pruneWithoutBase {
  import dispatch(NODES = Set(0, 1, 2), DISPATCHERS = Set(0, 1), MAX_COMMITS = 3, MAX_TIME = 3,
                  SHARED_ENTRY = false, PRUNE_WITHOUT_BASE = true, DELETE_ON_ANSWER = false, MANIFEST_FIRST = false,
                  NO_ADOPTION_ENTRY = false).*
}

module deleteOnAnswer {
  import dispatch(NODES = Set(0, 1, 2), DISPATCHERS = Set(0, 1), MAX_COMMITS = 3, MAX_TIME = 3,
                  SHARED_ENTRY = false, PRUNE_WITHOUT_BASE = false, DELETE_ON_ANSWER = true, MANIFEST_FIRST = false,
                  NO_ADOPTION_ENTRY = false).*
}

module manifestFirst {
  import dispatch(NODES = Set(0, 1, 2), DISPATCHERS = Set(0, 1), MAX_COMMITS = 3, MAX_TIME = 3,
                  SHARED_ENTRY = false, PRUNE_WITHOUT_BASE = false, DELETE_ON_ANSWER = false, MANIFEST_FIRST = true,
                  NO_ADOPTION_ENTRY = false).*
}

module noAdoptionEntry {
  import dispatch(NODES = Set(0, 1, 2), DISPATCHERS = Set(0, 1), MAX_COMMITS = 3, MAX_TIME = 3,
                  SHARED_ENTRY = false, PRUNE_WITHOUT_BASE = false, DELETE_ON_ANSWER = false, MANIFEST_FIRST = false,
                  NO_ADOPTION_ENTRY = true).*
}
