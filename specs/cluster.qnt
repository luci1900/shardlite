// How nodes agree on who owns one shard, and what they keep when they disagree.
//
// There is no consensus protocol here. The only agreement is blob storage's linearizable conditional
// writes, so the store is modelled as shared state that every write changes atomically. Each node claims
// the shard, serves it, ships its commits and lets go of it, and the store decides whether each of its
// writes succeeds. Nodes never check their own standing before writing: a node whose lease has expired but
// which hasn't noticed (a zombie) attempts everything a healthy node does, and fencing and
// compare-and-swap have to stop it.
//
// Histories are lists of unique commit ids, not positions, so a fork (two nodes committing on top of the
// same prefix, and only one surviving) loses an id and is caught.
//
// The lifecycle of one node's shard (activation steps, the cache, deactivation, the app holding the file
// open, the durability gate itself) is shard.qnt. Here a node is just inactive or active, and the gate is
// a guard on `respond`. Also left out: the interior of the database, deletion, messages (messages.qnt),
// forwarding, rebasing, lost responses, and adoption as several steps (the fence and the read of the
// journal are one atomic step).
//
// Run:
//   quint typecheck specs/cluster.qnt
//   quint run specs/cluster.qnt --invariant=safe --max-steps=40 --max-samples=50000
//   quint verify specs/cluster.qnt --invariant=safe --max-steps=8

module cluster {
  pure val NODES = Set(0, 1)
  // An incarnation (a node's process with one lease epoch) is node * 10 + epoch.
  pure val MAX_EPOCH = 1
  pure val INCS = Set(0, 1, 10, 11)
  // Commit ids are 1 to MAX_COMMITS, handed out as they are made.
  pure val MAX_COMMITS = 3

  type Owner =
    | NoOwner
    | Held(int)   // an incarnation

  type Manifest = {
    exists: bool,
    owner: Owner,
    snap: List[int]   // the commits in the shard's LTX files
  }

  type Lease = {
    epoch: int,
    expired: bool   // the lease has run out and the node has not renewed it
  }

  type NodeState = {
    active: bool,
    local: List[int],   // the commits in the node's copy of the file
    durable: int,       // how many of them are in the node's journal (or the snapshot)
    base: int           // how many are in the snapshot as far as the node knows (its own view, not the manifest)
  }

  type State = {
    // blob storage
    manifest: Manifest,
    leases: int -> Lease,        // by node
    journals: int -> List[int],  // by incarnation: the shard's commits since the snapshot, as of the last segment
    fenced: Set[int],            // incarnations whose journal an adopter has fenced
    // nodes
    nodes: int -> NodeState,
    // ghosts
    acked: Set[int],     // commits some client has seen acknowledged
    next: int,           // the next commit id
    syncOn: bool,
    rejected: int        // writes the store refused (a stale CAS or a fenced journal), for the witnesses
  }

  var s: State

  // -- Helpers

  pure def listToSet(l: List[int]): Set[int] = l.indices().map(i => l[i])

  pure def isPrefix(a: List[int], b: List[int]): bool =
    a.length() <= b.length() and a.indices().forall(i => a[i] == b[i])

  def incOf(st: State, n: int): int = n * 10 + st.leases.get(n).epoch

  // An incarnation is dead once its node has a newer epoch or its lease has expired. Anyone may then adopt it.
  def dead(st: State, inc: int): bool = {
    val l = st.leases.get(inc / 10)
    l.epoch > inc % 10 or (l.epoch == inc % 10 and l.expired)
  }

  // What a restore gives: the snapshot, plus the owner's journal if it holds more.
  def truth(st: State): List[int] =
    match st.manifest.owner {
      | NoOwner => st.manifest.snap
      | Held(i) =>
          if (st.journals.get(i).length() > st.manifest.snap.length()) st.journals.get(i)
          else st.manifest.snap
    }

  def withNode(st: State, n: int, ns: NodeState): State =
    { ...st, nodes: st.nodes.put(n, ns) }

  // The process dies, or exits after a write told it that it was fenced, and restarts with a new epoch.
  // What was in memory is gone.
  def restarted(st: State, n: int): State = {
    val l = st.leases.get(n)
    val after = withNode(st, n, idle)
    { ...after, leases: st.leases.put(n, { epoch: l.epoch + 1, expired: false }) }
  }

  val idle: NodeState = { active: false, local: List(), durable: 0, base: 0 }

  action init = {
    nondet sync = Set(true, false).oneOf()
    s' = {
      manifest: { exists: false, owner: NoOwner, snap: List() },
      leases: NODES.mapBy(n => { epoch: 0, expired: false }),
      journals: INCS.mapBy(i => List()),
      fenced: Set(),
      nodes: NODES.mapBy(n => idle),
      acked: Set(),
      next: 1,
      syncOn: sync,
      rejected: 0
    }
  }

  // -- Claiming and serving

  // Claim an unowned shard by CAS on the manifest, or create it with a conditional create, which claims it
  // too. The node restores the snapshot (nothing, for a new shard), and its journal starts empty on top of it.
  action claim(n) = all {
    not(s.nodes.get(n).active),
    (not(s.manifest.exists)) or s.manifest.owner == NoOwner,
    val inc = incOf(s, n)
    val content = if (s.manifest.exists) s.manifest.snap else List()
    s' = { ...withNode(s, n, { active: true, local: content, durable: content.length(), base: content.length() }),
           manifest: { exists: true, owner: Held(inc), snap: content },
           journals: s.journals.put(inc, content) }
  }

  action commit(n) = all {
    s.nodes.get(n).active,
    s.next <= MAX_COMMITS,
    val ns = s.nodes.get(n)
    s' = { ...withNode(s, n, { ...ns, local: ns.local.append(s.next) }), next: s.next + 1 }
  }

  // A client gets a response reflecting everything in the node's file. With sync on the gate holds it until
  // all of that is in the journal (shard.qnt checks the gate itself).
  action respond(n) = all {
    s.nodes.get(n).active,
    s.nodes.get(n).local.length() > 0,
    (not(s.syncOn)) or s.nodes.get(n).durable == s.nodes.get(n).local.length(),
    s' = { ...s, acked: s.acked.union(listToSet(s.nodes.get(n).local)) }
  }

  // -- Shipping

  // The node writes a segment to its journal. If an adopter has fenced the journal the write fails and the
  // node, having been fenced, stops for good.
  action flush(n) = all {
    val ns = s.nodes.get(n)
    val inc = incOf(s, n)
    all {
      ns.active,
      ns.local.length() > ns.durable,
      if (not(s.fenced.contains(inc)))
        s' = { ...withNode(s, n, { ...ns, durable: ns.local.length() }),
               journals: s.journals.put(inc, ns.local) }
      else
        s' = { ...restarted(s, n), rejected: s.rejected + 1 }
    }
  }

  // A delta of what is in the journal, added to the manifest by CAS. The node decides to write it from its own
  // view of the snapshot. A stale owner's CAS fails.
  action delta(n) = all {
    val inc = incOf(s, n)
    val ns = s.nodes.get(n)
    all {
      ns.active,
      ns.base < ns.durable,
      if (s.manifest.owner == Held(inc))
        s' = { ...withNode(s, n, { ...ns, base: ns.durable }),
               manifest: { ...s.manifest, snap: s.journals.get(inc) } }
      else
        s' = { ...s, rejected: s.rejected + 1 }
    }
  }

  // Letting go (idle deactivation, a handoff, a drain): once its last commit is in the journal the node writes
  // the tail as the snapshot and releases the shard, both by one CAS. If the CAS fails the node no longer owns
  // the shard, and it stays as it was.
  action release(n) = all {
    val ns = s.nodes.get(n)
    all {
      ns.active,
      ns.durable == ns.local.length(),
      if (s.manifest.owner == Held(incOf(s, n)))
        s' = { ...withNode(s, n, idle),
               manifest: { ...s.manifest, owner: NoOwner, snap: ns.local } }
      else
        s' = { ...s, rejected: s.rejected + 1 }
    }
  }

  // -- Time, crashes and takeover

  // The node's lease runs out. The process carries on until it notices (or never does).
  action expireLease(n) = all {
    not(s.leases.get(n).expired),
    s' = { ...s, leases: s.leases.put(n, { ...s.leases.get(n), expired: true }) }
  }

  action restart(n) = all {
    s.leases.get(n).epoch < MAX_EPOCH,
    s' = restarted(s, n)
  }

  // A node notices a dead incarnation holding the shard. It fences the incarnation's journal, reads what is in
  // it and writes it to the shard's files, then releases the shard. The fence and the read are one step here.
  action adopt = {
    nondet inc = INCS.oneOf()
    all {
      dead(s, inc),
      s.manifest.owner == Held(inc),
      s' = { ...s,
        fenced: s.fenced.union(Set(inc)),
        manifest: { ...s.manifest, owner: NoOwner, snap: truth(s) } }
    }
  }

  action step = {
    nondet n = NODES.oneOf()
    any {
      claim(n),
      commit(n),
      respond(n),
      flush(n),
      delta(n),
      release(n),
      expireLease(n),
      restart(n),
      adopt
    }
  }

  // -- Invariants

  // With sync on, no client has seen a commit acknowledged that a restore would now lose. A fork shows up
  // here: a commit written by a node that wasn't the owner is not in the chain the owner's successor reads.
  val ackedKept = s.syncOn implies s.acked.forall(c => listToSet(truth(s)).contains(c))

  // The owner's journal extends the snapshot, so the snapshot and the journal are one history.
  val ownerChain = match s.manifest.owner {
    | NoOwner => true
    | Held(i) => isPrefix(s.manifest.snap, s.journals.get(i))
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

  val safe = and {
    ackedKept,
    ownerChain,
    ownerFileExtendsSnap,
    journalBehindFile,
    noRepeats
  }

  // Witnesses: each should be violated, which shows the state is reachable.
  val neverBActive = not(s.nodes.get(1).active)
  val neverAdopted = s.fenced.size() == 0
  val neverRejected = s.rejected == 0
  val neverHandoff = not(s.nodes.get(1).active and s.nodes.get(1).local.length() > 0 and s.manifest.owner == Held(incOf(s, 1)) and s.acked.size() > 0)
  val neverZombieAck = not(s.acked.size() > 0 and NODES.exists(n => dead(s, incOf(s, n)) and s.nodes.get(n).active))
}
