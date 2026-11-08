// The lifecycle of one shard, as seen from one node.
//
// This models what the node does with a shard: claiming it, restoring it, serving requests, holding
// responses at the durability gate, shipping commits, and letting go of it again. It does not model what
// is in the SQLite file. A position stands for "the file as of the Nth commit".
//
// The rest of the world is nondeterministic: other nodes claim and release the shard, the node's lease
// expires, the process crashes, the app opens and closes the file. See docs/internals.md (Activation,
// Letting go of a shard, Fencing, Takeover) and internal/node/actor.go and shardset.go.
//
// Run:
//   quint typecheck specs/shard.qnt
//   quint run specs/shard.qnt --invariant=safe --max-steps=40 --max-samples=20000

module shard {
  // Positions stop here so the state space stays finite.
  pure val MAX_POS = 3
  pure val MAX_REFS = 2

  type Phase =
    | Inactive      // nothing open on this node and no files: the shard is only in blob storage
    | Activating    // claimed, being restored or created
    | Active        // open in the app, WAL captured, serving
    | Deactivating  // refusing new requests, shipping its tail and letting go

  type Owner =
    | Nobody  // the manifest names no owner
    | Me      // this node, with a valid lease
    | Dead    // a node epoch whose lease has expired, not yet adopted (this node's own old epoch or this node as a zombie)
    | Other   // another node with a valid lease

  // Where an activation gets the shard from.
  type Source =
    | FromNew
    | FromSnapshot

  type Manifest = {
    exists: bool,
    owner: Owner,
    snap: int,          // last position in the shard's LTX files
    autoDelete: bool    // set at creation
  }

  type State = {
    phase: Phase,
    source: Source,       // meaningful while Activating
    capturing: bool,      // Capture has the file open. False in Deactivating once the node has let go
    manifest: Manifest,
    local: int,           // last commit in the file this node has (meaningful while working)
    durable: int,         // last commit of this process in its node journal
    acked: int,           // highest position reflected in any response a client has received
    refs: int,            // requests and connections holding the shard
    held: int,            // responses waiting at the gate
    need: int,            // the highest position any of them needs to be durable
    idle: bool,           // the idle timeout has passed since the last use
    appOpen: bool,        // some process other than the node has the file open
    msgs: bool,           // undelivered messages keep the shard active
    syncOn: bool,         // the kind has sync on
    kindAutoDelete: bool, // the kind has auto_delete now
    fenced: bool,         // this process has lost its lease but hasn't exited yet (a zombie)
    journalFenced: bool,  // an adopter has fenced this process's journal: its flushes fail
    deletes: int          // ghost: shards deleted so far, for the witnesses
  }

  var s: State

  def working(st: State): bool =
    st.phase == Activating or st.phase == Active or st.phase == Deactivating

  // What blob storage decides, not the node. Every manifest write is a compare-and-swap on the node epoch the
  // manifest names, so it succeeds only if that is this process's. A journal segment is created with
  // If-None-Match, so it succeeds only until an adopter has fenced the journal. A node never checks these
  // itself: it attempts the write and the store answers, so a zombie's attempts are in the model.
  def casOk(st: State): bool = st.manifest.owner == Me
  def journalOk(st: State): bool = not(st.journalFenced)

  // The process dies, or exits after a write told it that it has been fenced, and restarts. Everything in
  // memory is gone and the files are discarded at the next activation.
  def restarted(st: State): State = { ...st,
    phase: Inactive,
    capturing: false,
    manifest: { ...st.manifest, owner: if (st.manifest.owner == Me) Dead else st.manifest.owner },
    refs: 0,
    held: 0,
    need: 0,
    idle: true,
    msgs: false,
    fenced: false,
    journalFenced: false
  }

  // What blob storage has for the shard, however the owner changes.
  def stored(st: State): int = if (st.manifest.snap > st.durable) st.manifest.snap else st.durable

  pure def maxOf(a: int, b: int): int = if (a > b) a else b

  // -- Initial state

  action init = {
    nondet sync = Set(true, false).oneOf()
    nondet auto = Set(true, false).oneOf()
    s' = {
      phase: Inactive,
      source: FromNew,
      capturing: false,
      manifest: { exists: false, owner: Nobody, snap: 0, autoDelete: false },
      local: 0,
      durable: 0,
      acked: 0,
      refs: 0,
      held: 0,
      need: 0,
      idle: true,
      appOpen: false,
      msgs: false,
      syncOn: sync,
      kindAutoDelete: auto,
      fenced: false,
      journalFenced: false,
      deletes: 0
    }
  }

  // -- Activation

  // No manifest: create it with a conditional create, which claims the shard, and start empty.
  action activateNew = all {
    s.phase == Inactive,
    not(s.manifest.exists),
    not(s.fenced),
    not(s.appOpen),
    s' = { ...s,
      phase: Activating,
      source: FromNew,
      manifest: { exists: true, owner: Me, snap: 0, autoDelete: s.kindAutoDelete },
      local: 0,
      durable: 0,
      acked: 0     // a new shard is a new generation: nothing of an earlier one carries over
    }
  }

  // Unowned (or left claimed by an activation that failed): claim by CAS and restore from the snapshot.
  action activateFromSnapshot = all {
    s.phase == Inactive,
    s.manifest.exists,
    s.manifest.owner == Nobody or s.manifest.owner == Me,
    not(s.fenced),
    not(s.appOpen),
    s' = { ...s,
      phase: Activating,
      source: FromSnapshot,
      manifest: { ...s.manifest, owner: Me },
      durable: s.manifest.snap
    }
  }

  action finishActivation = {
    nondet hasMsgs = Set(true, false).oneOf()
    all {
      s.phase == Activating,
      val pos = match s.source {
        | FromNew => 0
        | FromSnapshot => s.manifest.snap
      }
      s' = { ...s,
        phase: Active,
        capturing: true,
        local: pos,
        durable: pos,
        idle: false,
        msgs: hasMsgs
      }
    }
  }

  // A restore or a blob storage request fails. A claim stays (the next request claims it again).
  action failActivation = all {
    s.phase == Activating,
    s' = { ...s, phase: Inactive }
  }

  // -- Serving

  action acquire = all {
    s.phase == Active,
    s.refs < MAX_REFS,
    s' = { ...s, refs: s.refs + 1, idle: false }
  }

  // The app commits. It needs a request or a connection that is still open, and it keeps the file open.
  // A connection the app kept can commit while the node is deactivating.
  action commit = all {
    (s.phase == Active and (s.refs > 0 or s.appOpen))
      or (s.phase == Deactivating and s.capturing and s.appOpen),
    s.local < MAX_POS,
    s' = { ...s, local: s.local + 1, appOpen: true, idle: false }
  }

  // A response is read at the current position. With sync on it waits at the gate until that position is in
  // the journal. Without sync, or with nothing new to capture, it passes.
  action respond = all {
    s.phase == Active,
    s.refs > s.held,
    if (s.syncOn and s.local > s.durable)
      s' = { ...s, held: s.held + 1, need: maxOf(s.need, s.local), idle: false }
    else
      s' = { ...s, refs: s.refs - 1, acked: maxOf(s.acked, s.local), idle: false }
  }

  // A flush that has completed releases the responses it covers. The model keeps only the highest position
  // any held response needs, so they are released together once that one is durable (group commit).
  action releaseHeld = all {
    s.held > 0,
    s.need <= s.durable,
    s' = { ...s,
      held: 0,
      need: 0,
      refs: s.refs - s.held,
      acked: maxOf(s.acked, s.need),
      idle: false
    }
  }

  // The client goes away or the connection closes.
  action cancelRunning = all {
    s.phase == Active,
    s.refs > s.held,
    s' = { ...s, refs: s.refs - 1 }
  }

  action cancelHeld = all {
    s.held > 0,
    // need stays as it was, which can only make the remaining responses wait longer
    s' = { ...s, held: s.held - 1, need: if (s.held == 1) 0 else s.need, refs: s.refs - 1 }
  }

  action idleTimeout = all {
    s.phase == Active,
    s.refs == 0,
    not(s.idle),
    s' = { ...s, idle: true }
  }

  action toggleMessages = {
    nondet b = Set(true, false).oneOf()
    all {
      s.phase == Active,
      s' = { ...s, msgs: b }
    }
  }

  // -- Shipping

  // The flush loop writes a segment to the node journal. If an adopter has fenced the journal the write
  // fails, and the node, having been fenced, stops for good (it exits and restarts).
  action flush = all {
    s.phase == Active or (s.phase == Deactivating and s.capturing),
    s.local > s.durable,
    if (journalOk(s)) s' = { ...s, durable: s.local }
    else s' = restarted(s)
  }

  // A delta from pages already captured, added to the manifest by CAS. A stale owner's CAS fails and nothing
  // changes.
  action delta = all {
    s.phase == Active,
    s.manifest.snap < s.durable,
    if (casOk(s)) s' = { ...s, manifest: { ...s.manifest, snap: s.durable } }
    else s' = s
  }

  // -- Deactivation

  action startDeactivation = all {
    s.phase == Active,
    s.refs == 0,
    s.idle,
    not(s.msgs),
    s' = { ...s, phase: Deactivating, capturing: true }
  }

  // The node waits for the last commit to be in the journal, closes capture's connection and checks that
  // the app has the file closed.
  action releaseFile = all {
    s.phase == Deactivating,
    s.capturing,
    s.local == s.durable,
    if (s.appOpen)
      // The app still has the shard open: capture resumes and the shard stays active, to be tried later.
      s' = { ...s, phase: Active }
    else
      s' = { ...s, capturing: false }
  }

  // The shard is let go of by one CAS on the manifest, which records the tail as the snapshot and gives the
  // shard up (no owner), and the files go. If the CAS fails (the node no longer owns the shard) capture
  // resumes and the shard stays active.
  action finishDeactivation = all {
    s.phase == Deactivating,
    not(s.capturing),
    if (not(casOk(s)))
      s' = { ...s, phase: Active, capturing: true }
    else if (s.local == 0 or (s.kindAutoDelete and s.manifest.autoDelete))
      // The app never wrote to it, or the kind is auto_delete and so was the shard when created:
      // the manifest and the files go.
      s' = { ...s,
        phase: Inactive,
        manifest: { exists: false, owner: Nobody, snap: 0, autoDelete: false },
        local: 0,
        durable: 0,
        acked: 0,
        deletes: s.deletes + 1
      }
    else
      // The tail becomes the snapshot, the shard is released and the files go.
      s' = { ...s,
        phase: Inactive,
        manifest: { ...s.manifest, owner: Nobody, snap: s.local },
        local: 0,
        durable: 0
      }
  }

  // The write fails for another reason (a blob storage error whose outcome is known): capture resumes and the
  // shard stays active. (One whose outcome is unknown is asked again until it is known, which the model leaves
  // out, since it only ends in one of the other two.)
  action abortDeactivation = all {
    s.phase == Deactivating,
    not(s.capturing),
    s' = { ...s, phase: Active, capturing: true }
  }

  // -- The world

  action toggleAppOpen = {
    nondet b = Set(true, false).oneOf()
    all {
      // The app can only open files it was given, and one it opens it has to commit to (or hold) in this model.
      b == false or s.appOpen,
      s' = { ...s, appOpen: b }
    }
  }

  action reloadKinds = {
    nondet b = Set(true, false).oneOf()
    all { s' = { ...s, kindAutoDelete: b } }
  }

  // The node's lease expires while the process runs on (a long pause). Its ownership is invalid at once.
  action leaseLost = all {
    not(s.fenced),
    casOk(s),
    s' = { ...s, fenced: true, manifest: { ...s.manifest, owner: Dead } }
  }

  action crash = all { s' = restarted(s) }

  // Another node notices the expired lease and adopts it: it fences the journal, writes a delta for the tail
  // and releases the shard. A zombie's journal writes fail from here on.
  action adopt = all {
    s.manifest.owner == Dead,
    s' = { ...s,
      manifest: { ...s.manifest, owner: Nobody, snap: stored(s) },
      journalFenced: s.fenced or s.journalFenced
    }
  }

  action otherClaims = all {
    s.manifest.exists,
    s.manifest.owner == Nobody,
    s' = { ...s, manifest: { ...s.manifest, owner: Other } }
  }

  // Another node writes some commits and lets go.
  action otherReleases = {
    nondet more = Set(0, 1).oneOf()
    all {
      s.manifest.owner == Other,
      s.manifest.snap + more <= MAX_POS,
      s' = { ...s, manifest: { ...s.manifest, owner: Nobody, snap: s.manifest.snap + more } }
    }
  }

  action step = any {
    activateNew,
    activateFromSnapshot,
    finishActivation,
    failActivation,
    acquire,
    commit,
    respond,
    releaseHeld,
    cancelRunning,
    cancelHeld,
    idleTimeout,
    toggleMessages,
    flush,
    delta,
    startDeactivation,
    releaseFile,
    finishDeactivation,
    abortDeactivation,
    toggleAppOpen,
    reloadKinds,
    leaseLost,
    crash,
    adopt,
    otherClaims,
    otherReleases
  }

  // -- Invariants

  // With sync on, no client has seen a response that depends on a commit blob storage doesn't have.
  val ackedIsDurable = s.syncOn implies s.acked <= stored(s)

  // A node that hasn't lost its lease is the manifest's owner whenever it works on the shard, so there is
  // one writer. A zombie keeps working until it exits, but its manifest writes and (after the fence) its
  // journal writes fail.
  val oneWriter = (working(s) and not(s.fenced)) implies s.manifest.owner == Me

  // A shard that is being worked on has a manifest.
  val manifestExists = working(s) implies s.manifest.exists

  // A deleted shard leaves nothing behind in the manifest.
  val deletedIsClean = not(s.manifest.exists) implies
    (s.manifest.owner == Nobody and s.manifest.snap == 0)

  // Requests only hold an active shard, and deactivation waits for them.
  val refsOnlyWhenActive = (s.refs > 0) implies s.phase == Active
  val heldAreRefs = s.held <= s.refs

  // The journal is never behind the snapshot of the owner that wrote it, and never ahead of the file.
  val journalOrdered =
    (s.manifest.owner == Me or s.manifest.owner == Dead) implies s.manifest.snap <= s.durable
  val journalBehindFile = (s.phase == Active or s.phase == Deactivating) implies s.durable <= s.local

  // An unwritten shard is deleted, and a shard that has let go of its file has nothing left to ship.
  val letGoIsShipped = (s.phase == Deactivating and not(s.capturing)) implies s.durable == s.local

  val safe = and {
    ackedIsDurable,
    oneWriter,
    manifestExists,
    deletedIsClean,
    refsOnlyWhenActive,
    heldAreRefs,
    journalOrdered,
    journalBehindFile,
    letGoIsShipped
  }

  // Witnesses: each of these should be violated, which shows the state is reachable.
  val neverHeld = s.held == 0
  val neverDeleted = s.deletes == 0
  val neverZombieAdopted = not(s.fenced and s.journalFenced)
}
