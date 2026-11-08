// How nodes agree on who owns one shard, and what they keep when they disagree.
//
// There is no consensus protocol here. The only agreement is blob storage's linearizable conditional
// writes, so the store is modelled as shared state that every write changes atomically. Each node claims
// the shard, serves it, ships its commits, lets go of it when it goes idle, adopts dead nodes' journals, and the
// store decides whether each of its writes succeeds. Nodes never check their own standing before writing:
// a node whose lease has expired but which hasn't noticed (a zombie) attempts everything a healthy node
// does, adoptions included, and fencing and compare-and-swap have to stop it.
//
// Histories are lists of unique commit ids, not positions, so a fork (two nodes committing on top of the
// same prefix, and only one surviving) loses an id and is caught.
//
// Lost responses. Every write can land while its writer never hears (each action that writes takes a
// `learn` flag). blob.Store reads back its own write id on a retry, so a retry of a write that landed counts
// as success: the model lets a writer redo a step whose earlier attempt landed. A manifest handle remembers
// an update whose outcome stayed unknown and settles it before its next write (`seen`).
//
// Journals. Each node has one journal, a list of numbered segments shared by its incarnations, as in the
// code: a writer writes at its next number and is fenced if anything (a fence, or another incarnation's
// segment) took that number. An adopter fences at the first free number, which is the end of the list.
//
// Compare-and-swap is by contents: a writer's CAS succeeds if the object still holds what the writer last
// saw. That is S3's behaviour, whose ETags are the contents' MD5 (a store whose ETags change on every write
// refuses strictly more).
//
// Bounds: one restart per node and at most MAX_SEGS segments per journal (a fence or a flush that would go past
// it is not taken, which only cuts off adopters that give up and retry for ever). The instances at the end of
// the file set the rest.
//
// Tiers. The acknowledgement rides on one store: syncOn names which one the shard's kind said. With it on
// (storage, the default), the gate holds an answer until the journal in storage has everything, and ackedKept
// says what that is worth. The answer is also confirmed: `read` takes it from the node's file, and `confirm`
// lets it go once a read of the node's lease shows its incarnation unmarked, or a segment of its journal has
// landed since (confirmedFresh says what that is worth). With it off (the none tier) an acknowledgement sits on
// the node alone and `respond` answers with nothing to confirm it, so the model says nothing about its loss and
// the exhaustive check fixes sync on.
//
// Variants (constants below) switch a rule to a rejected alternative, to show its counterexample. Each has an
// instance at the end of the file.
//
// Restarts. A restarting node adopts its own journal and starts its writer when the adoption finishes.
//
// The lifecycle of one node's shard (activation steps, the durability gate itself, the app holding the file
// open) is shard.qnt. Here a node is active or idle,
// and the gate is a guard on `respond`. Also left out: the interior of the database, deletion, messages,
// forwarding, rebasing, renewals and sweeping the journal.
//
// Run:
//   quint typecheck specs/cluster.qnt
//   quint run specs/cluster.qnt --main=protocol --invariant=safe --max-steps=40 --max-samples=50000
//   quint run specs/cluster.qnt --main=protocol --invariant=liveNeverFenced --max-steps=40 --max-samples=50000
//   quint run specs/cluster.qnt --main=protocol --invariant=claimRulesAgree --max-steps=40 --max-samples=50000
//   quint test specs/cluster.qnt --main=uncheckedConfirm --match=staleConfirm   (the same test fails under protocol)
//   quint run specs/cluster.qnt --main=claimAfterLease --invariant=ackedKept --max-steps=40   (should fail)
//   quint run specs/cluster.qnt --main=resumeAfterUnknown --invariant=ackedKept --max-steps=40   (should fail)
//   quint run specs/cluster.qnt --main=restartOverrides --invariant=liveNeverFenced --max-steps=40   (should fail)
//   quint test specs/cluster.qnt --main=protocol
//   quint test specs/cluster.qnt --main=claimNeedsCleared --match=namedJournalClaim   (should fail)
//   quint test specs/cluster.qnt --main=claimNeedsAdopted --match=earlyClaim   (should fail)
//
// Simulation is the check on protocol: three nodes are what makes the interesting traces reachable, and
// bounded verification (quint verify) doesn't get to an adoption's five steps in useful time at that size.
// verifySmall, with two nodes and two commits, is the instance to try verification on.

module cluster {
  const NODES: Set[int]
  // An incarnation (a node's process with one lease epoch) is node * 10 + epoch.
  pure val MAX_EPOCH = 1
  // Commit ids are 1 to MAX_COMMITS, handed out as they are made.
  const MAX_COMMITS: int
  // At most this many segments per journal.
  const MAX_SEGS: int
  // Whether the shard's kind has sync on. With it off ackedKept says nothing, so the exhaustive check fixes it on.
  const SYNC: Set[bool]

  // Variants, each switching one rule to a rejected alternative (false in the protocol). Each is rejected by a
  // counterexample, which its instance at the end of the file reaches.
  //   CLAIM_AFTER_LEASE: claim a dead owner's shard as soon as its lease is invalid (the code's earlier rule).
  //   RESTART_OVERRIDES: a restarting node takes its lease even while another node is adopting it.
  //   RESUME_AFTER_UNKNOWN: a node whose deactivation's outcome is unknown resumes the shard as active, as its own.
  //   CLAIM_NEEDS_ADOPTED: a dead owner's shard is claimed only once its adoption has finished, even if the
  //     adopter has already cleared its journal.
  //   CLAIM_NEEDS_CLEARED: a dead owner's shard is claimed only once its manifest names no journal, whether or
  //     not its adoption has finished.
  //   CLAIM_ON_EXPIRY: claim a dead owner's shard whose manifest names no journal as soon as its lease has
  //     expired, without a newer epoch or another incarnation's mark (the rule this model had before it took the
  //     code's, which Handle.CheckClaimable states as ownerDead).
  //   UNCHECKED_CONFIRM: a storage answer is confirmed without evidence (the clocks alone, as before).
  const CLAIM_AFTER_LEASE: bool
  const CLAIM_ON_EXPIRY: bool
  const UNCHECKED_CONFIRM: bool
  const CLAIM_NEEDS_ADOPTED: bool
  const CLAIM_NEEDS_CLEARED: bool
  const RESTART_OVERRIDES: bool
  const RESUME_AFTER_UNKNOWN: bool

  type Owner =
    | NoOwner
    | Held(int)   // an incarnation

  type Manifest = {
    exists: bool,
    owner: Owner,
    journal: bool,      // the owner's node journal may hold commits after snap (the manifest's journal field)
    snap: List[int]     // the commits in the shard's LTX files
  }

  type Lease = {
    epoch: int,
    expired: bool,      // the lease has run out and the node has not renewed it
    released: bool,     // a graceful shutdown
    adoptedBy: int,     // the incarnation adopting the node's journal, -1 if none
    adopted: int,       // every epoch up to this one has had its journal adopted, -1 if none
    jnext: int          // where the next incarnation's journal starts, recorded when an adoption finishes
  }

  type Segment = {
    inc: int,           // the incarnation that wrote it, -1 for a fence
    has: bool,          // it holds the shard (a fence doesn't)
    content: List[int]  // the shard's commits as of the flush
  }

  type NodeState = {
    ready: bool,        // the process is up and its own adoption is done
    active: bool,
    local: List[int],   // the commits in the node's copy of the file
    durable: int,       // how many of them are in the node's journal (or the snapshot)
    base: int,          // how many are in the snapshot as far as the node knows
    next: int,          // the segment number its writer writes next
    pend: int,          // a segment that landed and whose answer hasn't come: its length, -1 if none
    mseen: Manifest,    // the manifest its handle holds
    hasUnk: bool,       // it made an update whose outcome it doesn't know
    unk: Manifest,      // that update
    rd: bool,           // a storage answer was read and waits for its confirmation
    ans: List[int],     // that answer: the commits in the node's file when it was read
    rdseg: int,         // the length of the node's journal then
    rdacked: Set[int]   // the commits acknowledged then
  }

  type Adoption = {
    phase: int,         // 0 none, 1 marked, 2 fenced, 3 read, 4 deltas written
    target: int,        // the node being adopted
    epoch: int,         // the epoch up to which it adopts
    lseen: Lease,       // the target lease as the adopter last wrote it
    landed: bool,       // its finish landed, unheard
    fence: int,         // the fence's number
    tail: List[int]     // what the journal holds for the shard
  }

  type State = {
    // blob storage
    manifest: Manifest,
    leases: int -> Lease,          // by node
    journals: int -> List[Segment],  // by node
    // nodes
    nodes: int -> NodeState,
    adoptions: int -> Adoption,    // by adopting node
    // ghosts
    acked: Set[int],     // commits some client has seen acknowledged
    next: int,           // the next commit id
    syncOn: bool,
    rejected: bool,      // the store refused a write, for the witnesses
    forks: bool,         // an adoption found a tail not continuing the snapshot
    readopted: bool,     // an adoption was taken over from a dead adopter
    lateClaims: bool,    // a dead owner's shard was claimed
    namedClaims: bool,   // a dead owner's shard was claimed while its manifest still named the owner's journal
    earlyClaims: bool,   // a dead owner's shard was claimed before its node's adoption finished
    deactivated: bool,   // a deactivation's write landed
    staleAnswers: bool,  // a node that isn't the manifest's owner responded
    staleReads: bool,    // a response left out a commit some client had seen acknowledged
    staleConfirms: bool, // a confirmed answer left out a commit acknowledged before it was read
    liveStale: bool,     // a node whose lease is good responded for a shard the manifest says it doesn't own
    liveFenced: bool     // an adopter with a good lease fenced the journal of a node with one
  }

  var s: State

  // -- Helpers

  pure def listToSet(l: List[int]): Set[int] = l.indices().map(i => l[i])

  pure def isPrefix(a: List[int], b: List[int]): bool =
    a.length() <= b.length() and a.indices().forall(i => a[i] == b[i])

  pure def nodeOf(inc: int): int = inc / 10
  pure def epochOf(inc: int): int = inc % 10

  def incOf(st: State, n: int): int = n * 10 + st.leases.get(n).epoch

  // Lookups that an empty adoption (target -1) can reach. TLC evaluates more eagerly than the simulator.
  def leaseOf(st: State, x: int): Lease = if (x < 0) noLease else st.leases.get(x)
  def segsOf(st: State, x: int): List[Segment] = if (x < 0) List() else st.journals.get(x)

  // An incarnation is dead once its node has a newer epoch, or its lease has expired or been released. No
  // incarnation (-1, an empty adoptedBy) counts as dead: there is nobody to wait for.
  def dead(st: State, inc: int): bool =
    if (inc < 0) true
    else {
      val l = st.leases.get(nodeOf(inc))
      l.epoch > epochOf(inc) or (l.epoch == epochOf(inc) and (l.expired or l.released))
    }

  // A node's lease may be adopted: expired, not given up gracefully, and not adopted up to its epoch.
  def adoptable(st: State, x: int): bool = {
    val l = st.leases.get(x)
    l.expired and not(l.released) and l.adopted < l.epoch
  }

  // The latest segment in a node's journal, below `upto`, that `p` accepts. List() if none.
  def latestWhere(st: State, x: int, upto: int, p: int => bool): List[int] = {
    val segs = segsOf(st, x)
    val j = segs.indices().filter(k => k < upto and segs[k].has and p(segs[k].inc)).fold(-1, (acc, k) => if (k > acc) k else acc)
    if (j < 0) List() else segs[j].content
  }

  def latestOf(st: State, inc: int): List[int] =
    latestWhere(st, nodeOf(inc), st.journals.get(nodeOf(inc)).length(), i => i == inc)

  // What a restore gives: the snapshot, plus the owner's journal if the manifest names it and it holds more.
  def truth(st: State): List[int] =
    if (not(st.manifest.exists)) List()
    else match st.manifest.owner {
      | NoOwner => st.manifest.snap
      | Held(i) =>
          val j = latestOf(st, i)
          if (st.manifest.journal and j.length() > st.manifest.snap.length()) j else st.manifest.snap
    }

  // The claim rule: a shard with no owner, or one whose owner is dead and whose tail is known to be in the
  // snapshot (the manifest names no journal, or the owner's node has had every epoch up to the owner's
  // adopted).
  // The owner is dead as the code sees it without comparing clocks: its node has a newer epoch, or another
  // incarnation has marked the lease (ownerDead in internal/shardfile/manifest.go). An expired lease alone is not.
  def codeDead(st: State, inc: int): bool = {
    val l = st.leases.get(nodeOf(inc))
    l.epoch > epochOf(inc) or (l.adoptedBy >= 0 and l.adoptedBy != inc)
  }

  def claimable(st: State): bool =
    if (not(st.manifest.exists)) true
    else match st.manifest.owner {
      | NoOwner => true
      | Held(i) => dead(st, i) and (CLAIM_AFTER_LEASE
                                    or (not(st.manifest.journal) and not(CLAIM_NEEDS_ADOPTED) and (CLAIM_ON_EXPIRY or codeDead(st, i)))
                                    or (st.leases.get(nodeOf(i)).adopted >= epochOf(i) and not(CLAIM_NEEDS_CLEARED)))
    }

  // The manifest a node's handle would CAS against, after settling an unknown outcome: its own update if
  // that is what is stored.
  def seen(st: State, ns: NodeState): Manifest = if (ns.hasUnk and ns.unk == st.manifest) st.manifest else ns.mseen
  def casOK(st: State, ns: NodeState): bool = seen(st, ns) == st.manifest

  def withNode(st: State, n: int, ns: NodeState): State = { ...st, nodes: st.nodes.put(n, ns) }
  def withLease(st: State, n: int, l: Lease): State = { ...st, leases: st.leases.put(n, l) }
  def withAdoption(st: State, n: int, a: Adoption): State = { ...st, adoptions: st.adoptions.put(n, a) }

  val noManifest: Manifest = { exists: false, owner: NoOwner, journal: false, snap: List() }
  val noLease: Lease = { epoch: 0, expired: false, released: false, adoptedBy: -1, adopted: -1, jnext: 0 }
  val noAdoption: Adoption = { phase: 0, target: -1, epoch: -1, lseen: noLease, landed: false, fence: -1, tail: List() }
  val down: NodeState = { ready: false, active: false, local: List(), durable: 0, base: 0, next: 0, pend: -1,
                          mseen: noManifest, hasUnk: false, unk: noManifest,
                          rd: false, ans: List(), rdseg: 0, rdacked: Set() }

  def setManifest(st: State, m: Manifest): State = { ...st, manifest: m }
  pure def max(a: int, b: int): int = if (a > b) a else b
  def room(st: State, x: int): bool = segsOf(st, x).length() < MAX_SEGS

  action init = {
    nondet sync = SYNC.oneOf()
    initWith(sync)
  }

  action initWith(sync) = all {
    s' = {
      manifest: noManifest,
      leases: NODES.mapBy(n => noLease),
      journals: NODES.mapBy(n => List()),
      nodes: NODES.mapBy(n => { ...down, ready: true }),
      adoptions: NODES.mapBy(n => noAdoption),
      acked: Set(),
      next: 1,
      syncOn: sync,
      rejected: false,
      forks: false,
      readopted: false,
      lateClaims: false,
      namedClaims: false,
      earlyClaims: false,
      deactivated: false,
      staleAnswers: false,
      staleReads: false,
      staleConfirms: false,
      liveStale: false,
      liveFenced: false
    }
  }

  // -- Claiming and serving

  // Claim the shard by CAS on the manifest, or create it with a conditional create, which claims it too. The
  // claim names the node's journal; the snapshot is unchanged. The node restores the snapshot plus whatever
  // the previous owner's journal holds, if the manifest named it (truth).
  action claim(n, learn) = all {
    val ns = s.nodes.get(n)
    val inc = incOf(s, n)
    val content = truth(s)
    val late = s.manifest.exists and s.manifest.owner != NoOwner
    val m = { ...s.manifest, exists: true, owner: Held(inc), journal: true,
              snap: if (s.manifest.exists) s.manifest.snap else List() }
    val early = late and (match s.manifest.owner { | NoOwner => false | Held(i) => s.leases.get(nodeOf(i)).adopted < epochOf(i) })
    val st = { ...setManifest(s, m), lateClaims: s.lateClaims or late, namedClaims: s.namedClaims or (late and s.manifest.journal),
               earlyClaims: s.earlyClaims or early }
    all {
      ns.ready,
      not(ns.active),
      claimable(s),
      if (learn)
        s' = withNode(st, n, { ...ns, active: true, local: content, durable: content.length(),
                               base: m.snap.length(), mseen: m, hasUnk: false })
      else
        // The claim landed, the node never heard. Its next activation reads the manifest afresh.
        s' = st
    }
  }

  action commit(n) = all {
    s.nodes.get(n).active,
    s.next <= MAX_COMMITS,
    val ns = s.nodes.get(n)
    s' = { ...withNode(s, n, { ...ns, local: ns.local.append(s.next) }), next: s.next + 1 }
  }

  // A client gets a response reflecting everything in the node's file, with nothing to confirm it: the none
  // tier, which trusts the clocks. (With sync on the answer is read and confirm below.)
  action respond(n) = all {
    val ns = s.nodes.get(n)
    all {
      ns.active,
      ns.local.length() > 0,
      not(s.syncOn),
      s' = { ...s, acked: s.acked.union(listToSet(ns.local)),
             staleAnswers: s.staleAnswers or s.manifest.owner != Held(incOf(s, n)),
             staleReads: s.staleReads or not(s.acked.subseteq(listToSet(ns.local))),
             liveStale: s.liveStale or (not(dead(s, incOf(s, n))) and s.manifest.owner != Held(incOf(s, n))) }
    }
  }

  // A storage answer is read from the node's file, and leaves once the node knows it still owned the shard
  // after the read. The gate also holds it until everything it reflects is in the journal (shard.qnt).
  action read(n) = all {
    val ns = s.nodes.get(n)
    all {
      s.syncOn,
      ns.active,
      not(ns.rd),
      ns.local.length() > 0,
      s' = withNode(s, n, { ...ns, rd: true, ans: ns.local, rdseg: s.journals.get(n).length(), rdacked: s.acked })
    }
  }

  // The node's own lease still shows its incarnation: not released, not marked by another incarnation, not
  // adopted up to the epoch. Expiry doesn't matter, it is the clocks' business.
  def leaseHolds(st: State, n: int): bool = {
    val l = st.leases.get(n)
    not(l.released) and (l.adoptedBy < 0 or l.adoptedBy == incOf(st, n)) and l.adopted < l.epoch
  }

  // A segment of the node's journal, from this incarnation, created since the read.
  def segmentSince(st: State, n: int, from: int): bool = {
    val segs = st.journals.get(n)
    segs.indices().exists(k => k >= from and segs[k].has and segs[k].inc == incOf(st, n))
  }

  // Confirmation by a read of the lease or by a segment landing, whichever comes first. The answer goes out.
  action confirm(n) = all {
    val ns = s.nodes.get(n)
    all {
      ns.rd,
      ns.durable >= ns.ans.length(),
      UNCHECKED_CONFIRM or leaseHolds(s, n) or segmentSince(s, n, ns.rdseg),
      s' = { ...withNode(s, n, { ...ns, rd: false }),
             acked: s.acked.union(listToSet(ns.ans)),
             staleConfirms: s.staleConfirms or not(ns.rdacked.subseteq(listToSet(ns.ans))),
             liveStale: s.liveStale or (not(dead(s, incOf(s, n))) and s.manifest.owner != Held(incOf(s, n))) }
    }
  }

  // The lease read finds the node's lease taken: the node stops and restarts as from a crash.
  action confirmFail(n) = all {
    val ns = s.nodes.get(n)
    all {
      ns.rd,
      not(leaseHolds(s, n)),
      s' = withNode(s, n, down)
    }
  }

  // -- Shipping

  // The node writes a segment at its next number. If the number is taken (a fence) the node has been fenced
  // and its process exits. A segment can land without the writer hearing: it retries with the same write id
  // until it hears, and the retry finds its own segment (flushAck).
  action flush(n) = all {
    val ns = s.nodes.get(n)
    val inc = incOf(s, n)
    all {
      ns.active,
      ns.pend < 0,
      ns.local.length() > ns.durable,
      ns.next != s.journals.get(n).length() or room(s, n),
      if (ns.next == s.journals.get(n).length())
        s' = { ...withNode(s, n, { ...ns, next: ns.next + 1, pend: ns.local.length() }),
               journals: s.journals.put(n, s.journals.get(n).append({ inc: inc, has: true, content: ns.local })) }
      else
        s' = { ...withNode(s, n, down), rejected: true }
    }
  }

  action flushAck(n) = all {
    val ns = s.nodes.get(n)
    all {
      ns.pend >= 0,
      s' = withNode(s, n, { ...ns, durable: ns.pend, pend: -1 })
    }
  }

  // A delta of what is in the journal, added to the manifest by CAS.
  action delta(n, learn) = all {
    val ns = s.nodes.get(n)
    val content = ns.local.slice(0, ns.durable)
    all {
      ns.active,
      ns.base < ns.durable,
      if (casOK(s, ns)) {
        val m = { ...s.manifest, snap: content }
        if (learn) s' = withNode(setManifest(s, m), n, { ...ns, base: ns.durable, mseen: m, hasUnk: false })
        else s' = withNode(setManifest(s, m), n, { ...ns, hasUnk: true, unk: m })
      } else
        s' = { ...s, rejected: true }
    }
  }

  // Deactivation: once its last commit is in the journal, the node writes its tail as the snapshot and
  // releases the shard (no owner, no journal), both by one CAS, and drops its files. If the CAS fails the
  // shard stays active (capture resumes, as the code does). The code asks again until it learns the outcome,
  // and a node that stops first abandons the shard without serving it, so an unknown outcome drops the files
  // too and the node never serves on a guess, unless RESUME_AFTER_UNKNOWN.
  action deactivate(n, learn) = all {
    val ns = s.nodes.get(n)
    all {
      ns.active,
      ns.pend < 0,
      ns.durable == ns.local.length(),
      if (casOK(s, ns)) {
        val m = { ...s.manifest, owner: NoOwner, journal: false, snap: ns.local }
        val st = { ...setManifest(s, m), deactivated: true }
        if (learn or not(RESUME_AFTER_UNKNOWN))
          s' = withNode(st, n, { ...ns, active: false, local: List(), durable: 0, base: 0, hasUnk: false, rd: false })
        else
          s' = withNode(st, n, { ...ns, hasUnk: true, unk: m })
      } else
        s' = { ...s, rejected: true }
    }
  }

  // -- Leases and restarts

  // The node's lease runs out: its holder's deadline has passed. The process carries on until it notices (or
  // never does). In the code this is established without comparing clocks: the holder's deadline is by its own
  // clocks, and an adopter marks a lease only after seeing it unchanged for a TTL on its own monotonic clock.
  action expireLease(n) = all {
    val l = s.leases.get(n)
    all {
      not(l.expired),
      not(l.released),
      s' = withLease(s, n, { ...l, expired: true })
    }
  }

  // A graceful shutdown: nothing active, so every shard has been let go of. The lease is released by CAS, recording that
  // every epoch up to this one needs no adoption. A lease being adopted has changed, and the CAS fails. An
  // adoption of another node that is in progress is cancelled first (the lease scan stops early).
  action shutdown(n) = all {
    val ns = s.nodes.get(n)
    val l = s.leases.get(n)
    all {
      ns.ready,
      not(ns.active),
      l.adoptedBy < 0,
      s' = withAdoption(withNode(withLease(s, n, { ...l, released: true, adopted: l.epoch, jnext: ns.next }), n, down), n, noAdoption)
    }
  }

  // The process dies (or exits after being fenced, or after a shutdown) and starts again with a new epoch.
  // What was in memory is gone. Acquire marks the lease as adopted by the new incarnation if any epoch is not
  // adopted yet, and the node adopts its own journal before it serves. If a live node is adopting the lease,
  // Acquire waits (unless RESTART_OVERRIDES).
  action restart(n) = all {
    val l = s.leases.get(n)
    val inc = n * 10 + l.epoch + 1
    val own = l.adopted < l.epoch
    all {
      l.epoch < MAX_EPOCH,
      RESTART_OVERRIDES or dead(s, l.adoptedBy),
      val nl = { ...l, epoch: l.epoch + 1, expired: false, released: false, adoptedBy: if (own) inc else -1 }
      val st = withLease(s, n, nl)
      if (own)
        s' = withAdoption(withNode(st, n, down), n, { ...noAdoption, phase: 1, target: n, epoch: l.epoch, lseen: nl })
      else
        s' = withAdoption(withNode(st, n, { ...down, ready: true, next: l.jnext }), n, noAdoption)
    }
  }

  // -- Adoption, in steps

  // Mark a dead lease as adopted by CAS. A lease whose adopter is dead can be marked again, and a retry of the
  // node's own mark counts as success.
  action mark(m, x, learn) = all {
    val l = s.leases.get(x)
    val a = incOf(s, m)
    all {
      m != x,
      s.nodes.get(m).ready,
      s.adoptions.get(m).phase == 0,
      adoptable(s, x),
      dead(s, l.adoptedBy) or l.adoptedBy == a,
      val nl = { ...l, adoptedBy: a }
      val st = { ...withLease(s, x, nl), readopted: s.readopted or (l.adoptedBy >= 0 and l.adoptedBy != a) }
      if (learn) s' = withAdoption(st, m, { ...noAdoption, phase: 1, target: x, epoch: l.epoch, lseen: nl })
      else s' = st
    }
  }

  // Fence the journal at the first free number, above everything in it. A retry of a fence whose answer was
  // lost finds its own write id, so the adopter always learns the number. (An adopter that gives up and starts
  // again fences again, which is harmless.)
  action fence(m) = all {
    val ad = s.adoptions.get(m)
    val j = segsOf(s, ad.target)
    val live = ad.target >= 0 and ad.target != m and not(dead(s, incOf(s, m))) and not(dead(s, ad.target * 10 + leaseOf(s, ad.target).epoch))
    all {
      ad.phase == 1,
      room(s, ad.target),
      s' = withAdoption({ ...s,
                          journals: s.journals.put(ad.target, j.append({ inc: -1, has: false, content: List() })),
                          liveFenced: s.liveFenced or live }, m, { ...ad, phase: 2, fence: j.length() })
    }
  }

  // Read the journal below the fence: the shard's latest segment from any incarnation of the node.
  action readJournal(m) = all {
    val ad = s.adoptions.get(m)
    all {
      ad.phase == 2,
      s' = withAdoption(s, m, { ...ad, phase: 3, tail: latestWhere(s, ad.target, ad.fence, i => i >= 0 and nodeOf(i) == ad.target) })
    }
  }

  // A delta for the tail, if the manifest still names the journal and the tail holds more than the snapshot,
  // added by CAS with the journal cleared. Redoing it is harmless: the journal is cleared or the CAS fails.
  action adoptDelta(m, learn) = all {
    val ad = s.adoptions.get(m)
    val mf = s.manifest
    val names = mf.exists and mf.journal and (match mf.owner { | NoOwner => false | Held(i) => nodeOf(i) == ad.target })
    val more = ad.tail.length() > mf.snap.length()
    all {
      ad.phase == 3,
      if (names and more and not(isPrefix(mf.snap, ad.tail)))
        s' = withAdoption({ ...s, forks: true }, m, noAdoption)
      else {
        val st = if (names and more) setManifest(s, { ...mf, snap: ad.tail, journal: false }) else s
        s' = withAdoption(st, m, if (learn) { ...ad, phase: 4 } else ad)
      }
    }
  }

  // Finish by CAS on the lease: every epoch up to the adopted one is done, and the node's next incarnation
  // starts its journal after the fence. A retry of a finish that landed counts as success. A conflict means
  // the node restarted or another adopter took over: the adoption ends. The node's own adoption makes it
  // ready.
  action finish(m, learn) = all {
    val ad = s.adoptions.get(m)
    val l = leaseOf(s, ad.target)
    all {
      ad.phase == 4,
      if (l == ad.lseen) {
        val st = withLease(s, ad.target, { ...l, adopted: ad.epoch, adoptedBy: -1, jnext: ad.fence + 1 })
        if (not(learn)) s' = withAdoption(st, m, { ...ad, landed: true })
        else if (ad.target == m) s' = withAdoption(withNode(st, m, { ...s.nodes.get(m), ready: true, next: ad.fence + 1 }), m, noAdoption)
        else s' = withAdoption(st, m, noAdoption)
      } else if (ad.landed and l.adopted >= ad.epoch) {
        // The finish landed earlier and the retry recognises it.
        if (ad.target == m) s' = withAdoption(withNode(s, m, { ...s.nodes.get(m), ready: true, next: ad.fence + 1 }), m, noAdoption)
        else s' = withAdoption(s, m, noAdoption)
      } else
        s' = withAdoption({ ...s, rejected: true }, m, noAdoption)
    }
  }

  // An adopter of another node gives up (an unknown outcome, its context ending). Its mark stays until it
  // dies or retries.
  action abandon(m) = all {
    val ad = s.adoptions.get(m)
    all {
      ad.phase > 0,
      ad.target != m,
      s' = withAdoption(s, m, noAdoption)
    }
  }

  action step = {
    nondet n = NODES.oneOf()
    nondet x = NODES.oneOf()
    nondet learn = Set(true, false).oneOf()
    any {
      claim(n, learn),
      commit(n),
      respond(n),
      read(n),
      confirm(n),
      confirmFail(n),
      flush(n),
      flushAck(n),
      delta(n, learn),
      deactivate(n, learn),
      expireLease(n),
      shutdown(n),
      restart(n),
      mark(n, x, learn),
      fence(n),
      readJournal(n),
      adoptDelta(n, learn),
      finish(n, learn),
      abandon(n)
    }
  }

  // -- Scenarios

  // A zombie answers a read that leaves out a commit the shard's new owner acknowledged, on a tier with
  // nothing to confirm it (sync off: none). Node 0 owns the shard, commits and flushes, then its lease
  // runs out while it carries on. Node 1 adopts it (fence, read, delta) and claims the shard, which the cleared
  // journal allows before the adoption finishes, then commits, flushes and acknowledges commit 2. Node 0 answers
  // from its file, which only has commit 1.
  run staleReadTest =
    initWith(false)
      .then(claim(0, true)).then(commit(0)).then(flush(0)).then(flushAck(0)).then(respond(0))
      .then(expireLease(0))
      .then(mark(1, 0, true)).then(fence(1)).then(readJournal(1)).then(adoptDelta(1, true))
      .then(claim(1, true)).then(commit(1)).then(flush(1)).then(flushAck(1)).then(respond(1))
      .then(respond(0))
      .expect(s.staleReads and safe)

  // The same on the storage tier. Node 0 reads its answer, but the adopter's mark is on its lease, so no
  // confirmation is enabled for it: the lease check fails and so does the node. (No segment of node 0's is
  // created after the read: the journal was fenced.) Under CLAIM_ON_EXPIRY, UNCHECKED_CONFIRM this test fails.
  run confirmedReadTest =
    initWith(true)
      .then(claim(0, true)).then(commit(0)).then(flush(0)).then(flushAck(0)).then(read(0)).then(confirm(0))
      .then(expireLease(0))
      .then(mark(1, 0, true)).then(fence(1)).then(readJournal(1)).then(adoptDelta(1, true))
      .then(claim(1, true)).then(commit(1)).then(flush(1)).then(flushAck(1)).then(read(1)).then(confirm(1))
      .then(read(0))
      .expect(not(leaseHolds(s, 0)) and not(segmentSince(s, 0, s.nodes.get(0).rdseg)))
      .then(confirmFail(0))
      .expect(not(s.staleConfirms) and s.acked == Set(1, 2) and not(s.nodes.get(0).ready) and safe)

  // The same trace with the confirmation going through, which only UNCHECKED_CONFIRM allows: the stale answer
  // leaves, and confirmedFresh is violated.
  run staleConfirmTest =
    initWith(true)
      .then(claim(0, true)).then(commit(0)).then(flush(0)).then(flushAck(0)).then(read(0)).then(confirm(0))
      .then(expireLease(0))
      .then(mark(1, 0, true)).then(fence(1)).then(readJournal(1)).then(adoptDelta(1, true))
      .then(claim(1, true)).then(commit(1)).then(flush(1)).then(flushAck(1)).then(read(1)).then(confirm(1))
      .then(read(0)).then(confirm(0))
      .expect(s.staleConfirms)

  // The same, until the end: node 1's claim needs only the cleared journal, and the adoption finishes later.
  run earlyClaimTest =
    initWith(true)
      .then(claim(0, true)).then(commit(0)).then(flush(0)).then(flushAck(0))
      .then(expireLease(0))
      .then(mark(1, 0, true)).then(fence(1)).then(readJournal(1)).then(adoptDelta(1, true))
      .then(claim(2, true))
      .then(finish(1, true))
      .expect(s.earlyClaims and s.leases.get(0).adopted == 0 and safe)

  // A dead node's active shard whose tail its delta has already written: the manifest still names the
  // journal, which holds nothing beyond the snapshot, so the adopter writes nothing for it. Only the
  // finished adoption lets node 2 claim it (it fails under CLAIM_NEEDS_CLEARED: the claim is never enabled).
  run namedJournalClaimTest =
    initWith(true)
      .then(claim(0, true)).then(commit(0)).then(flush(0)).then(flushAck(0)).then(delta(0, true))
      .then(expireLease(0))
      .then(mark(1, 0, true)).then(fence(1)).then(readJournal(1)).then(adoptDelta(1, true)).then(finish(1, true))
      .then(claim(2, true))
      .expect(s.namedClaims and s.nodes.get(2).local == List(1) and safe)

  // -- Invariants

  // With sync on, no client has seen a commit acknowledged that a restore would now lose. A fork shows up
  // here: a commit written by a node that wasn't the owner is not in the chain the owner's successor reads.
  val ackedKept = s.syncOn implies s.acked.forall(c => listToSet(truth(s)).contains(c))

  // The owner's journal and the snapshot are one history: one is a prefix of the other.
  val ownerChain = match s.manifest.owner {
    | NoOwner => true
    | Held(i) =>
        val j = latestOf(s, i)
        not(s.manifest.journal) or isPrefix(s.manifest.snap, j) or isPrefix(j, s.manifest.snap)
  }

  // A node working on the shard under its current incarnation's ownership has the snapshot as a prefix of its
  // file, so a delta from it (or from its journal) continues the history.
  val ownerFileExtendsSnap = NODES.forall(n => {
    val ns = s.nodes.get(n)
    (ns.active and s.manifest.owner == Held(incOf(s, n))) implies isPrefix(s.manifest.snap, ns.local)
  })

  // A node's journal is its file as of some flush.
  val journalBehindFile = NODES.forall(n => s.nodes.get(n).durable <= s.nodes.get(n).local.length())

  // Commit ids are unique, so the chain never repeats one.
  val noRepeats = truth(s).indices().forall(i => truth(s).indices().forall(j => (truth(s)[i] == truth(s)[j]) implies i == j))

  // An adoption never finds a tail that doesn't continue the snapshot.
  val noForks = not(s.forks)

  // Once a node's epoch is adopted (or released), its journal holds nothing for the shard beyond the snapshot.
  val adoptedCovered = match s.manifest.owner {
    | NoOwner => true
    | Held(i) =>
        (s.manifest.journal and s.leases.get(nodeOf(i)).adopted >= epochOf(i)) implies isPrefix(latestOf(s, i), s.manifest.snap)
  }

  // A node whose lease is good answers only for a shard it owns. (A zombie can answer for one it lost, and its
  // answer can leave out commits the new owner acknowledged: staleReadTest shows it. Durability doesn't
  // depend on it, since a zombie acknowledges nothing that fencing doesn't keep.)
  val liveOwnerAnswers = not(s.liveStale)

  // A confirmed answer holds every commit acknowledged before it was read.
  val confirmedFresh = not(s.staleConfirms)

  val safe = and {
    ackedKept,
    ownerChain,
    ownerFileExtendsSnap,
    journalBehindFile,
    noRepeats,
    noForks,
    adoptedCovered,
    liveOwnerAnswers,
    confirmedFresh
  }

  // The looser claim rule (CLAIM_ON_EXPIRY) never differs from the code's here: a manifest names no journal
  // only after an adopter has cleared it, and the adopter's mark stays on the lease until a newer epoch or the
  // adoption's end. So no reachable state has an owner dead only by expiry and a journal-less manifest.
  val claimRulesAgree = match s.manifest.owner {
    | NoOwner => true
    | Held(i) => not(s.manifest.exists and not(s.manifest.journal) and dead(s, i)) or codeDead(s, i) or s.leases.get(nodeOf(i)).adopted >= epochOf(i)
  }

  // Not safety, but what keeps a restart from being undone: an adopter whose lease is good never fences the
  // journal of a node whose lease is good. (A paused adopter that wakes after its lease ran out can, and the
  // node it fences restarts.)
  val liveNeverFenced = not(s.liveFenced)

  // Witnesses: each should be violated, which shows the state is reachable.
  val neverReading = NODES.forall(n => not(s.nodes.get(n).rd))
  val neverBActive = not(s.nodes.get(1).active)
  val neverAdopted = NODES.forall(n => s.leases.get(n).adopted < 0)
  val neverAdoptedByOther = NODES.forall(n => not(s.leases.get(n).adopted == s.leases.get(n).epoch and not(s.leases.get(n).released)))
  val neverRejected = not(s.rejected)
  val neverReadopted = not(s.readopted)
  val neverLateClaim = not(s.lateClaims)
  val neverLateClaimWithAck = not(s.lateClaims and s.acked.size() > 1)
  val neverStaleAnswer = not(s.staleAnswers)
  // A dead owner's shard whose manifest still names the journal, claimed after the adoption finished.
  val neverNamedClaim = not(s.namedClaims)
  // A shard whose tail an adopter wrote, claimed before the adoption as a whole finished.
  val neverEarlyClaim = not(s.earlyClaims)
  // A shard let go of at deactivation and owned again since.
  val neverReclaimed = not(s.deactivated and s.manifest.owner != NoOwner)
  val neverZombieAck = not(s.acked.size() > 0 and NODES.exists(n => dead(s, incOf(s, n)) and s.nodes.get(n).active))
  val neverReleased = NODES.forall(n => not(s.leases.get(n).released))
  // A shard let go of with data in it.
  val neverDeactivatedWithData = not(s.manifest.exists and s.manifest.owner == NoOwner and s.manifest.snap.length() > 0)
}

// The protocol, for simulation: three nodes (a third is needed to take over a dead adopter's work).
module protocol {
  import cluster(NODES = Set(0, 1, 2), MAX_COMMITS = 3, MAX_SEGS = 4, SYNC = Set(true, false),
                 CLAIM_AFTER_LEASE = false, RESTART_OVERRIDES = false, RESUME_AFTER_UNKNOWN = false,
                 CLAIM_NEEDS_ADOPTED = false, CLAIM_NEEDS_CLEARED = false,
                 CLAIM_ON_EXPIRY = false, UNCHECKED_CONFIRM = false).*
}

// The smallest instance worth verifying: two nodes, sync on (with it off ackedKept says nothing), room for
// one adoption on each side.
module verifySmall {
  import cluster(NODES = Set(0, 1), MAX_COMMITS = 2, MAX_SEGS = 2, SYNC = Set(true),
                 CLAIM_AFTER_LEASE = false, RESTART_OVERRIDES = false, RESUME_AFTER_UNKNOWN = false,
                 CLAIM_NEEDS_ADOPTED = false, CLAIM_NEEDS_CLEARED = false,
                 CLAIM_ON_EXPIRY = false, UNCHECKED_CONFIRM = false).*
}

module claimAfterLease {
  import cluster(NODES = Set(0, 1, 2), MAX_COMMITS = 3, MAX_SEGS = 4, SYNC = Set(true, false),
                 CLAIM_AFTER_LEASE = true, RESTART_OVERRIDES = false, RESUME_AFTER_UNKNOWN = false,
                 CLAIM_NEEDS_ADOPTED = false, CLAIM_NEEDS_CLEARED = false,
                 CLAIM_ON_EXPIRY = false, UNCHECKED_CONFIRM = false).*
}

module restartOverrides {
  import cluster(NODES = Set(0, 1, 2), MAX_COMMITS = 3, MAX_SEGS = 4, SYNC = Set(true, false),
                 CLAIM_AFTER_LEASE = false, RESTART_OVERRIDES = true, RESUME_AFTER_UNKNOWN = false,
                 CLAIM_NEEDS_ADOPTED = false, CLAIM_NEEDS_CLEARED = false,
                 CLAIM_ON_EXPIRY = false, UNCHECKED_CONFIRM = false).*
}

module resumeAfterUnknown {
  import cluster(NODES = Set(0, 1, 2), MAX_COMMITS = 3, MAX_SEGS = 4, SYNC = Set(true, false),
                 CLAIM_AFTER_LEASE = false, RESTART_OVERRIDES = false, RESUME_AFTER_UNKNOWN = true,
                 CLAIM_NEEDS_ADOPTED = false, CLAIM_NEEDS_CLEARED = false,
                 CLAIM_ON_EXPIRY = false, UNCHECKED_CONFIRM = false).*
}

module claimNeedsAdopted {
  import cluster(NODES = Set(0, 1, 2), MAX_COMMITS = 3, MAX_SEGS = 4, SYNC = Set(true, false),
                 CLAIM_AFTER_LEASE = false, RESTART_OVERRIDES = false, RESUME_AFTER_UNKNOWN = false,
                 CLAIM_NEEDS_ADOPTED = true, CLAIM_NEEDS_CLEARED = false,
                 CLAIM_ON_EXPIRY = false, UNCHECKED_CONFIRM = false).*
}

module claimNeedsCleared {
  import cluster(NODES = Set(0, 1, 2), MAX_COMMITS = 3, MAX_SEGS = 4, SYNC = Set(true, false),
                 CLAIM_AFTER_LEASE = false, RESTART_OVERRIDES = false, RESUME_AFTER_UNKNOWN = false,
                 CLAIM_NEEDS_ADOPTED = false, CLAIM_NEEDS_CLEARED = true,
                 CLAIM_ON_EXPIRY = false, UNCHECKED_CONFIRM = false).*
}

module claimOnExpiry {
  import cluster(NODES = Set(0, 1, 2), MAX_COMMITS = 3, MAX_SEGS = 4, SYNC = Set(true, false),
                 CLAIM_AFTER_LEASE = false, RESTART_OVERRIDES = false, RESUME_AFTER_UNKNOWN = false,
                 CLAIM_NEEDS_ADOPTED = false, CLAIM_NEEDS_CLEARED = false,
                 CLAIM_ON_EXPIRY = true, UNCHECKED_CONFIRM = false).*
}

module uncheckedConfirm {
  import cluster(NODES = Set(0, 1, 2), MAX_COMMITS = 3, MAX_SEGS = 4, SYNC = Set(true, false),
                 CLAIM_AFTER_LEASE = false, RESTART_OVERRIDES = false, RESUME_AFTER_UNKNOWN = false,
                 CLAIM_NEEDS_ADOPTED = false, CLAIM_NEEDS_CLEARED = false,
                 CLAIM_ON_EXPIRY = false, UNCHECKED_CONFIRM = true).*
}
