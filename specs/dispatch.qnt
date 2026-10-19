// Who wakes a shard that isn't active: an entry in a reminder index in blob storage, created before every manifest
// write that records when the shard next needs waking, and a dispatcher that lists the index, wakes the shards it
// finds and deletes the entries their manifests don't back.
//
// One shard, whose state is a version and the minute its pending reminder is due (`rem`, -1 for none). Data
// durability is cluster.qnt's: here the leases, journals, fences, adoption and the claim rule are the same,
// simplified (no lost responses, one segment per flush), so that what a crash, a restart, a zombie or a stop leaves
// can be checked for wakes.
//
// The manifest's `nr` is `next_reminder`: owner writes (deltas and deactivations) set it from the state they
// write (here exactly `rem`), claims and releases keep it, and an adoption's delta sets it to the adoption's minute
// (the adopter can't read the shard's reminders). The rules:
//   - Index first: before a manifest write that sets `nr` to a minute, the writer creates the entry for that minute
//     with its own base (the ETag the write will CAS against). It may pause between the two, and its CAS fails if
//     the manifest has moved on.
//   - Each write has its own entry: one that exists already under another base never counts.
//   - Adoption creates an entry at its minute, with the base its delta will CAS against, before each shard's delta,
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
// Left out: lost responses, deletion, handoffs in one write, rebasing, reads that don't match the files (they set
// `next_reminder` to now, which is no different here), how far a move goes (any later minute is as safe as
// another, so moves go one minute on), where dispatches go, limits, and a zombie's earlier life writing after a
// restart (one process per node, as in cluster.qnt).
//
// Run (simulation only, verification doesn't finish at this size):
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
  pure val MAX_EPOCH = 2
  const MAX_COMMITS: int
  const MAX_SEGS: int
  const MAX_TIME: int

  // Variants, each false in the protocol:
  //   SHARED_ENTRY: a writer that finds an entry for its minute under another base counts it as its own.
  //   PRUNE_WITHOUT_BASE: the dispatcher deletes an entry the manifest doesn't back without checking its base.
  //   DELETE_ON_ANSWER: the dispatcher deletes a backed entry once the shard's node answers that it is active.
  //   MANIFEST_FIRST: an owner creates its entry after its manifest write rather than before.
  //   NO_ADOPTION_ENTRY: adoption writes the shards' deltas without creating entries.
  const SHARED_ENTRY: bool
  const PRUNE_WITHOUT_BASE: bool
  const DELETE_ON_ANSWER: bool
  const MANIFEST_FIRST: bool
  const NO_ADOPTION_ENTRY: bool

  type St = { v: int, rem: int }

  type Manifest = {
    exists: bool,
    etag: int,          // changes on every write
    owner: int,         // an incarnation, -1 if none
    journal: bool,      // the owner's journal may hold commits after snap
    snap: St,
    nr: int             // next_reminder, -1 if absent
  }

  type Entry = { minute: int, base: int, n: int }   // the key's minute, base and move count

  type Lease = { epoch: int, expired: bool, released: bool, adoptedBy: int, adopted: int, jnext: int }

  type Seg = {
    inc: int,           // the incarnation that wrote it, -1 for a fence
    has: bool,          // it holds the shard's commits
    st: St              // the shard's state as of the flush
  }

  // An owner's manifest write in progress: its entry created (unless MANIFEST_FIRST) and its CAS still to come.
  // late: with MANIFEST_FIRST, the CAS landed and the entry is still to come.
  type Write = { on: bool, late: bool, snap: St, deact: bool, base: int }

  type NodeState = {
    ready: bool,
    active: bool,
    cached: bool,
    local: St,
    durable: St,        // the last state in its journal (or the snapshot it restored)
    next: int,
    mseen: Manifest,
    w: Write
  }

  // phase: 0 none, 1 marked, 2 fenced, 3 read, 4 entry created, 5 delta written.
  // minute: the adoption's minute, for the entry and for nr. base: the ETag its delta CASes against.
  type Adoption = { phase: int, target: int, epoch: int, lseen: Lease, fence: int, tail: St, minute: int, base: int }

  // phase: 0 idle, 1 read the manifest for the entry, 2 dispatched (answered: the shard was active on the node).
  type Disp = { phase: int, e: Entry, m: Manifest, answered: bool }

  type State = {
    manifest: Manifest,
    entries: Set[Entry],
    leases: int -> Lease,
    journals: int -> List[Seg],
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
  pure val noLease: Lease = { epoch: 0, expired: false, released: false, adoptedBy: -1, adopted: -1, jnext: 0 }
  pure val noWrite: Write = { on: false, late: false, snap: noSt, deact: false, base: -1 }
  pure val noEntry: Entry = { minute: -1, base: -1, n: -1 }
  pure val noAdoption: Adoption = { phase: 0, target: -1, epoch: -1, lseen: noLease, fence: -1, tail: noSt, minute: -1, base: -1 }
  pure val idle: Disp = { phase: 0, e: noEntry, m: noManifest, answered: false }
  pure val down: NodeState = { ready: false, active: false, cached: false, local: emptySt, durable: emptySt, next: 0,
                               mseen: noManifest, w: noWrite }

  pure def nodeOf(inc: int): int = inc / 10
  pure def epochOf(inc: int): int = inc % 10

  def incOf(st: State, n: int): int = n * 10 + st.leases.get(n).epoch
  def leaseOf(st: State, x: int): Lease = if (x < 0) noLease else st.leases.get(x)
  def segsOf(st: State, x: int): List[Seg] = if (x < 0) List() else st.journals.get(x)

  def dead(st: State, inc: int): bool =
    if (inc < 0) true
    else {
      val l = st.leases.get(nodeOf(inc))
      l.epoch > epochOf(inc) or (l.epoch == epochOf(inc) and (l.expired or l.released))
    }

  def adoptable(st: State, x: int): bool = {
    val l = st.leases.get(x)
    l.expired and not(l.released) and l.adopted < l.epoch
  }

  // The latest segment below `upto` that holds the shard and whose incarnation `p` accepts.
  def latestWhere(st: State, x: int, upto: int, p: int => bool): St = {
    val segs = segsOf(st, x)
    val j = segs.indices().filter(k => k < upto and segs[k].has and p(segs[k].inc)).fold(-1, (acc, k) => if (k > acc) k else acc)
    if (j < 0) noSt else segs[j].st
  }

  def latestOf(st: State, inc: int): St =
    latestWhere(st, nodeOf(inc), st.journals.get(nodeOf(inc)).length(), i => i == inc)

  // What a restore gives.
  def truth(st: State): St =
    if (not(st.manifest.exists)) emptySt
    else if (st.manifest.owner < 0) st.manifest.snap
    else {
      val j = latestOf(st, st.manifest.owner)
      if (st.manifest.journal and j.v > st.manifest.snap.v) j else st.manifest.snap
    }

  def claimable(st: State): bool =
    if (not(st.manifest.exists) or st.manifest.owner < 0) true
    else {
      val i = st.manifest.owner
      dead(st, i) and (not(st.manifest.journal) or st.leases.get(nodeOf(i)).adopted >= epochOf(i))
    }

  // The manifest backs the entry: it has a next_reminder no later than the entry's minute.
  pure def backs(m: Manifest, e: Entry): bool = m.exists and m.nr >= 0 and m.nr <= e.minute

  // The pruning rule, on the manifest as the dispatcher read it.
  pure def prunable(m: Manifest, e: Entry): bool =
    not(backs(m, e)) and (PRUNE_WITHOUT_BASE or not(m.exists) or m.etag != e.base)

  def room(st: State, x: int): bool = segsOf(st, x).length() < MAX_SEGS
  // The node's writer may write: nothing (a fence) took its number.
  def canWrite(st: State, n: int): bool = st.nodes.get(n).next == st.journals.get(n).length()

  def withNode(st: State, n: int, ns: NodeState): State = { ...st, nodes: st.nodes.put(n, ns) }
  def withLease(st: State, n: int, l: Lease): State = { ...st, leases: st.leases.put(n, l) }
  def withAdoption(st: State, n: int, a: Adoption): State = { ...st, adoptions: st.adoptions.put(n, a) }
  def withDisp(st: State, d: int, x: Disp): State = { ...st, disps: st.disps.put(d, x) }

  // A manifest write, with a new ETag.
  def writeManifest(st: State, m: Manifest): State = { ...st, manifest: { ...m, etag: st.etags + 1 }, etags: st.etags + 1 }

  def addEntry(st: State, e: Entry): State = { ...st, entries: st.entries.union(Set(e)) }

  // The journal's segments below `upto` are deleted.
  def sweep(st: State, x: int, upto: int): State = {
    val segs = st.journals.get(x)
    { ...st, journals: st.journals.put(x, range(0, segs.length()).foldl(List(), (acc, k) =>
        acc.append(if (k < upto) { ...segs[k], has: false } else segs[k]))) }
  }

  // Claim (or create) the shard on node n.
  def claimed(st: State, n: int): State = {
    val ns = st.nodes.get(n)
    val content = truth(st)
    val st1 = writeManifest(st, { ...st.manifest, exists: true, owner: incOf(st, n), journal: true })
    withNode(st1, n, { ...ns, active: true, local: content, durable: content, mseen: st1.manifest })
  }

  def canClaim(st: State, n: int): bool = {
    val ns = st.nodes.get(n)
    ns.ready and not(ns.active) and not(ns.cached) and claimable(st)
  }

  action init = s' = {
    manifest: noManifest,
    entries: Set(),
    leases: NODES.mapBy(n => noLease),
    journals: NODES.mapBy(n => List()),
    nodes: NODES.mapBy(n => { ...down, ready: true }),
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
      room(s, n),
      if (canWrite(s, n))
        s' = withNode({ ...s, journals: s.journals.put(n, s.journals.get(n).append({ inc: incOf(s, n), has: true, st: ns.local })) },
                      n, { ...ns, durable: ns.local, next: ns.next + 1 })
      else
        s' = withNode(s, n, down)
    }
  }

  // The first half of a delta (deact false) or a deactivation that writes the manifest (deact true): the entry for
  // the state's reminder, with the ETag the write will CAS against as its base.
  action prepare(n, deact) = all {
    val ns = s.nodes.get(n)
    val snap = ns.durable
    val base = ns.mseen.etag
    val e = { minute: snap.rem, base: base, n: 0 }
    val shared = SHARED_ENTRY and s.entries.exists(x => x.minute == snap.rem)
    all {
      ns.active,
      not(ns.w.on),
      if (deact) ns.local == ns.durable and ns.mseen.snap != ns.durable else ns.mseen.snap.v < ns.durable.v,
      val st = if (snap.rem >= 0 and not(MANIFEST_FIRST) and not(shared)) addEntry(s, e) else s
      s' = withNode(st, n, { ...ns, w: { on: true, late: false, snap: snap, deact: deact, base: base } })
    }
  }

  // The second half: the CAS. A manifest that has moved on fails it (a delta tries again later, a deactivation
  // leaves the shard active), and the node no longer owns the shard.
  action cas(n) = all {
    val ns = s.nodes.get(n)
    val w = ns.w
    all {
      w.on,
      not(w.late),
      if (s.manifest.exists and s.manifest.etag == w.base) {
        val st = writeManifest(s, { ...s.manifest, snap: w.snap, nr: w.snap.rem })
        val late = MANIFEST_FIRST and w.snap.rem >= 0
        s' = withNode(st, n, { ...ns, active: not(w.deact), cached: w.deact, mseen: st.manifest,
                               w: if (late) { ...w, late: true } else noWrite })
      } else
        s' = { ...withNode(s, n, { ...ns, active: false, cached: false, local: emptySt, durable: emptySt, w: noWrite }), casFailed: true }
    }
  }

  // MANIFEST_FIRST: the entry, after the manifest write.
  action lateEntry(n) = all {
    val w = s.nodes.get(n).w
    all {
      w.late,
      s' = withNode(addEntry(s, { minute: w.snap.rem, base: w.base, n: 0 }), n, { ...s.nodes.get(n), w: noWrite })
    }
  }

  // Deactivation with nothing to write: the manifest's snapshot (and its next_reminder) is the shard's state.
  action deactivateClean(n) = all {
    val ns = s.nodes.get(n)
    all {
      ns.active,
      not(ns.w.on),
      ns.local == ns.durable,
      ns.mseen.snap == ns.durable,
      s' = withNode(s, n, { ...ns, active: false, cached: true })
    }
  }

  // A cached shard comes back with no blob storage request.
  action reactivate(n) = all {
    s.nodes.get(n).cached,
    s' = withNode(s, n, { ...s.nodes.get(n), active: true, cached: false })
  }

  // Eviction: the manifest names no owner and keeps its next_reminder.
  action release(n) = all {
    val ns = s.nodes.get(n)
    all {
      ns.cached,
      s.manifest == ns.mseen,
      val st = writeManifest(s, { ...s.manifest, owner: -1, journal: false })
      s' = withNode(st, n, { ...ns, cached: false, local: emptySt, durable: emptySt, mseen: st.manifest })
    }
  }

  // Journal expiry: segments go once the shard's files cover them. Nothing checks the lease first, so a zombie
  // expires too.
  action expireOwn(n) = all {
    val segs = s.journals.get(n)
    all {
      s.nodes.get(n).ready,
      segs.indices().exists(k => segs[k].has and segs[k].st.v <= s.manifest.snap.v),
      s' = { ...s, journals: s.journals.put(n, segs.foldl(List(), (acc, g) =>
               acc.append(if (g.has and g.st.v <= s.manifest.snap.v) { ...g, has: false } else g))) }
    }
  }

  // -- Leases, stops and restarts

  action expireLease(n) = all {
    val l = s.leases.get(n)
    all {
      not(l.expired),
      not(l.released),
      s' = withLease(s, n, { ...l, expired: true })
    }
  }

  // A stop: nothing active, so every tail is a delta. The journal's segments go and the lease is released.
  // Nothing about reminders.
  action shutdown(n) = all {
    val ns = s.nodes.get(n)
    val l = s.leases.get(n)
    all {
      ns.ready,
      not(ns.active),
      not(ns.w.on),
      l.adoptedBy < 0,
      val st = sweep(s, n, s.journals.get(n).length())
      s' = withAdoption(withNode(withLease(st, n, { ...l, released: true, adopted: l.epoch, jnext: ns.next }), n, down), n, noAdoption)
    }
  }

  // A new incarnation. With epochs not yet adopted it adopts its own journal first, and its writer starts after.
  action restart(n) = all {
    val l = s.leases.get(n)
    val inc = n * 10 + l.epoch + 1
    val own = l.adopted < l.epoch
    all {
      l.epoch < MAX_EPOCH,
      dead(s, l.adoptedBy),
      val nl = { ...l, epoch: l.epoch + 1, expired: false, released: false, adoptedBy: if (own) inc else -1 }
      val st = withLease(s, n, nl)
      if (own)
        s' = withAdoption(withNode(st, n, down), n, { ...noAdoption, phase: 1, target: n, epoch: l.epoch, lseen: nl })
      else
        s' = withAdoption(withNode(st, n, { ...down, ready: true, next: l.jnext }), n, noAdoption)
    }
  }

  // -- Adoption

  action mark(m, x) = all {
    val l = s.leases.get(x)
    val a = incOf(s, m)
    all {
      m != x,
      s.nodes.get(m).ready,
      s.adoptions.get(m).phase == 0,
      adoptable(s, x),
      dead(s, l.adoptedBy) or l.adoptedBy == a,
      val nl = { ...l, adoptedBy: a }
      s' = withAdoption(withLease(s, x, nl), m, { ...noAdoption, phase: 1, target: x, epoch: l.epoch, lseen: nl })
    }
  }

  action fence(m) = all {
    val ad = s.adoptions.get(m)
    val j = segsOf(s, ad.target)
    all {
      ad.phase == 1,
      room(s, ad.target),
      s' = withAdoption({ ...s, journals: s.journals.put(ad.target, j.append({ inc: -1, has: false, st: noSt })) },
                        m, { ...ad, phase: 2, fence: j.length() })
    }
  }

  action readJournal(m) = all {
    val ad = s.adoptions.get(m)
    all {
      ad.phase == 2,
      s' = withAdoption(s, m, { ...ad, phase: 3, tail: latestWhere(s, ad.target, ad.fence, i => i >= 0 and nodeOf(i) == ad.target) })
    }
  }

  // Whether the adoption writes a delta for the shard's tail.
  def writesTail(st: State, ad: Adoption): bool = {
    val mf = st.manifest
    mf.exists and mf.journal and mf.owner >= 0 and nodeOf(mf.owner) == ad.target and ad.tail.v > mf.snap.v
  }

  // The entry before the delta: the adoption's minute, with the base the delta will CAS against.
  action adoptEntry(m) = all {
    val ad = s.adoptions.get(m)
    val need = writesTail(s, ad)
    val e = { minute: s.now, base: s.manifest.etag, n: 0 }
    all {
      ad.phase == 3,
      val st = if (need and not(NO_ADOPTION_ENTRY)) { ...addEntry(s, e), adoptionEntry: true } else s
      s' = withAdoption(st, m, { ...ad, phase: 4, minute: s.now, base: s.manifest.etag })
    }
  }

  // The delta, by CAS against the entry's base, with next_reminder at the adoption's minute. A manifest that has
  // moved on sends the adoption back to read it again.
  action adoptDelta(m) = all {
    val ad = s.adoptions.get(m)
    all {
      ad.phase == 4,
      if (not(writesTail(s, ad)))
        s' = withAdoption(s, m, { ...ad, phase: 5 })
      else if (s.manifest.etag == ad.base)
        s' = withAdoption(writeManifest(s, { ...s.manifest, snap: ad.tail, journal: false, nr: ad.minute }), m, { ...ad, phase: 5 })
      else
        s' = withAdoption(s, m, { ...ad, phase: 3 })
    }
  }

  // Finish by CAS on the lease, then sweep the dead journal below the fence. A restart's writer starts after it.
  action finish(m) = all {
    val ad = s.adoptions.get(m)
    val l = leaseOf(s, ad.target)
    all {
      ad.phase == 5,
      if (l == ad.lseen) {
        val st1 = sweep(withLease(s, ad.target, { ...l, adopted: ad.epoch, adoptedBy: -1, jnext: ad.fence + 1 }), ad.target, ad.fence)
        if (ad.target == m)
          s' = withAdoption(withNode(st1, m, { ...st1.nodes.get(m), ready: true, next: ad.fence + 1 }), m, noAdoption)
        else
          s' = withAdoption(st1, m, noAdoption)
      } else
        s' = withAdoption(s, m, noAdoption)
    }
  }

  action abandon(m) = all {
    val ad = s.adoptions.get(m)
    all {
      ad.phase > 0,
      ad.target != m,
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
  // cached there is reactivated, claimable is claimed there. Otherwise it answers with an error.
  action dispatchTo(d, n) = all {
    val x = s.disps.get(d)
    val ns = s.nodes.get(n)
    all {
      x.phase == 1,
      backs(x.m, x.e),
      if (ns.ready and ns.active)
        s' = withDisp(s, d, { ...x, phase: 2, answered: true })
      else if (ns.ready and ns.cached)
        s' = withDisp(withNode(s, n, { ...ns, active: true, cached: false }), d, { ...x, phase: 2, answered: true })
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

  // A dispatcher stops partway (a move of `_dispatch`, a crash): it forgets its pass.
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
      deactivateClean(n),
      reactivate(n),
      release(n),
      expireOwn(n),
      expireLease(n),
      shutdown(n),
      restart(n),
      mark(n, x),
      fence(n),
      readJournal(n),
      adoptEntry(n),
      adoptDelta(n),
      finish(n),
      abandon(n),
      dRead(d, e),
      dispatchTo(d, n),
      dSettle(d),
      dStop(d)
    }
  }

  // -- Invariants

  // The owner's tail is in a journal an adoption will still read.
  val tailPending = {
    val mf = s.manifest
    mf.exists and mf.journal and mf.owner >= 0 and s.leases.get(nodeOf(mf.owner)).adopted < epochOf(mf.owner)
      and latestOf(s, mf.owner).v > mf.snap.v
  }

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

  def only(st: State): Entry = st.entries.fold(noEntry, (acc, e) => e)

  // A shard deactivated with a reminder: its entry is dispatched when due, the shard is reactivated and the entry
  // moved forward.
  run deactivateTest =
    init
      .then(claim(0)).then(commit(0, 1)).then(flush(0)).then(prepare(0, true)).then(cas(0))
      .then(tick)
      .then(dRead(0, { minute: 1, base: 1, n: 0 })).then(dispatchTo(0, 0)).then(dSettle(0))
      .expect(s.nodes.get(0).active and s.entries == Set({ minute: 2, base: 1, n: 1 }) and safe)

  // An evicted shard is claimed by the node its dispatch went to.
  run evictTest =
    init
      .then(claim(0)).then(commit(0, 0)).then(flush(0)).then(prepare(0, true)).then(cas(0)).then(release(0))
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
      .then(expireLease(0))
      .then(mark(1, 0)).then(fence(1)).then(readJournal(1)).then(adoptEntry(1)).then(adoptDelta(1)).then(finish(1))
      .expect(s.manifest.nr == 0 and s.entries == Set({ minute: 0, base: 1, n: 0 }) and truth(s).rem == 1 and safe)

  // A shard dispatched and active whose node dies before anything reaches the journal: adoption finds no tail,
  // and the moved entry still wakes it.
  run dispatchedCrashTest =
    init
      .then(claim(0)).then(commit(0, 0)).then(flush(0)).then(prepare(0, true)).then(cas(0)).then(release(0))
      .then(dRead(0, { minute: 0, base: 1, n: 0 })).then(dispatchTo(0, 1)).then(dSettle(0))
      .then(expireLease(1))
      .then(mark(2, 1)).then(fence(2)).then(readJournal(2)).then(adoptEntry(2)).then(adoptDelta(2)).then(finish(2))
      .expect(s.entries == Set({ minute: 1, base: 1, n: 1 }) and claimable(s) and safe)

  // A restart adopts its own journal with an entry, and its writer starts after.
  run restartTest =
    init
      .then(claim(0)).then(commit(0, 1)).then(flush(0))
      .then(restart(0)).then(fence(0)).then(readJournal(0)).then(adoptEntry(0)).then(adoptDelta(0)).then(finish(0))
      .expect(s.nodes.get(0).ready and s.nodes.get(0).next == 2 and s.adoptionEntry and safe)

  // A stop does nothing about reminders: the shard's entry covers it.
  run stopTest =
    init
      .then(claim(0)).then(commit(0, 1)).then(flush(0)).then(prepare(0, true)).then(cas(0))
      .then(shutdown(0))
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
  import dispatch(NODES = Set(0, 1, 2), DISPATCHERS = Set(0, 1), MAX_COMMITS = 3, MAX_SEGS = 6, MAX_TIME = 3,
                  SHARED_ENTRY = false, PRUNE_WITHOUT_BASE = false, DELETE_ON_ANSWER = false, MANIFEST_FIRST = false,
                  NO_ADOPTION_ENTRY = false).*
}

module sharedEntry {
  import dispatch(NODES = Set(0, 1, 2), DISPATCHERS = Set(0, 1), MAX_COMMITS = 3, MAX_SEGS = 6, MAX_TIME = 3,
                  SHARED_ENTRY = true, PRUNE_WITHOUT_BASE = false, DELETE_ON_ANSWER = false, MANIFEST_FIRST = false,
                  NO_ADOPTION_ENTRY = false).*
}

module pruneWithoutBase {
  import dispatch(NODES = Set(0, 1, 2), DISPATCHERS = Set(0, 1), MAX_COMMITS = 3, MAX_SEGS = 6, MAX_TIME = 3,
                  SHARED_ENTRY = false, PRUNE_WITHOUT_BASE = true, DELETE_ON_ANSWER = false, MANIFEST_FIRST = false,
                  NO_ADOPTION_ENTRY = false).*
}

module deleteOnAnswer {
  import dispatch(NODES = Set(0, 1, 2), DISPATCHERS = Set(0, 1), MAX_COMMITS = 3, MAX_SEGS = 6, MAX_TIME = 3,
                  SHARED_ENTRY = false, PRUNE_WITHOUT_BASE = false, DELETE_ON_ANSWER = true, MANIFEST_FIRST = false,
                  NO_ADOPTION_ENTRY = false).*
}

module manifestFirst {
  import dispatch(NODES = Set(0, 1, 2), DISPATCHERS = Set(0, 1), MAX_COMMITS = 3, MAX_SEGS = 6, MAX_TIME = 3,
                  SHARED_ENTRY = false, PRUNE_WITHOUT_BASE = false, DELETE_ON_ANSWER = false, MANIFEST_FIRST = true,
                  NO_ADOPTION_ENTRY = false).*
}

module noAdoptionEntry {
  import dispatch(NODES = Set(0, 1, 2), DISPATCHERS = Set(0, 1), MAX_COMMITS = 3, MAX_SEGS = 6, MAX_TIME = 3,
                  SHARED_ENTRY = false, PRUNE_WITHOUT_BASE = false, DELETE_ON_ANSWER = false, MANIFEST_FIRST = false,
                  NO_ADOPTION_ENTRY = true).*
}
