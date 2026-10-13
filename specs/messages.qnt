// The outbox and inbox: how a message gets from one shard to another, and what stops it being lost.
//
// A sender shard (S) inserts an outbox row in the same transaction as its change. Once that commit is
// durable the sender's node sends the row to the receiver's node, which inserts an inbox row in a commit
// of its own and answers once that commit is durable. The sender's node then deletes the outbox row. The
// receiver's handler consumes the inbox row in the same transaction as its work.
//
// Assumptions taken from cluster.qnt, which is not modelled here: one node owns a shard at a time, and
// adoption of a crashed node writes a wake hint for exactly the shards it replayed a tail for. A shard is
// simply active, or orphaned by a crash until it is adopted, or inactive. "Tailed" means its journal holds
// commits beyond its last delta. Durability here is the same idea as in shard.qnt: the file is what the
// node has, durable is what survives a crash.
//
// Left out: hop limits, retries and giving up, keyed rows, the receiver's own app calls, ordering, and
// everything about which node a shard is on.
//
// Run:
//   quint typecheck specs/messages.qnt
//   quint run specs/messages.qnt --invariant=safe --max-steps=40 --max-samples=50000
//   quint verify specs/messages.qnt --invariant=safe --max-steps=10

module messages {
  pure val S = 0
  pure val R = 1
  pure val SHARDS = Set(0, 1)
  pure val MAX_MSGS = 2
  pure val MAX_ROWS = 4

  // What a shard's tables hold. Messages are ids, inbox rows are ids of their own.
  type Content = {
    born: Set[int],       // messages whose outbox row has been inserted (never shrinks)
    outbox: Set[int],     // messages still to be sent
    inbox: Set[int],      // rows due now
    later: Set[int],      // rows due later
    effects: List[int]    // rows whose handler has committed, in order
  }

  type Shard = {
    active: bool,
    orphan: bool,   // crashed while active and not yet adopted
    tailed: bool,   // the journal holds commits beyond the last delta
    file: Content,
    durable: Content
  }

  type State = {
    shards: int -> Shard,
    hints: Set[int],            // shards with a wake hint
    inflight: int -> int,       // intake requests on the network, by message
    waiting: Set[int],          // messages the sender's node has an attempt out for (its memory)
    pendingRows: Set[int],      // rows the receiver's node will answer for once durable (its memory)
    acks: Set[int],             // answers on the network, by message
    rowMsg: int -> int,         // the message each inbox row came from, 0 for the shard's own rows
    nextMsg: int,
    nextRow: int,
    sends: int                  // ghost
  }

  var s: State

  pure def listToSet(l: List[int]): Set[int] = l.indices().map(i => l[i])

  val empty: Content = { born: Set(), outbox: Set(), inbox: Set(), later: Set(), effects: List() }

  def withShard(st: State, i: int, sh: Shard): State = { ...st, shards: st.shards.put(i, sh) }

  def hasWork(c: Content): bool = c.outbox.size() > 0 or c.inbox.size() > 0 or c.later.size() > 0

  // Memory the node loses when it lets go of a shard or crashes.
  def forgotten(st: State, i: int): State =
    if (i == S) { ...st, waiting: Set() } else { ...st, pendingRows: Set() }

  action init = s' = {
    shards: SHARDS.mapBy(i => { active: true, orphan: false, tailed: false, file: empty, durable: empty }),
    hints: Set(),
    inflight: 1.to(MAX_MSGS).mapBy(m => 0),
    waiting: Set(),
    pendingRows: Set(),
    acks: Set(),
    rowMsg: 1.to(MAX_ROWS).mapBy(r => 0),
    nextMsg: 1,
    nextRow: 1,
    sends: 0
  }

  // -- The apps

  // The sender's app commits a change and an outbox row together.
  action createMsg = all {
    s.shards.get(S).active,
    s.nextMsg <= MAX_MSGS,
    val sh = s.shards.get(S)
    s' = { ...withShard(s, S, { ...sh, file: { ...sh.file,
                                  born: sh.file.born.union(Set(s.nextMsg)),
                                  outbox: sh.file.outbox.union(Set(s.nextMsg)) } }),
           nextMsg: s.nextMsg + 1 }
  }

  // The receiver's app sets a row in its own inbox for later.
  action setTimer = all {
    s.shards.get(R).active,
    s.nextRow <= MAX_ROWS,
    val sh = s.shards.get(R)
    s' = { ...withShard(s, R, { ...sh, file: { ...sh.file, later: sh.file.later.union(Set(s.nextRow)) } }),
           nextRow: s.nextRow + 1 }
  }

  // Time passes and a row comes due. It is the clock that changes, so a row moves in the file and in what is
  // durable alike.
  action tick = {
    nondet r = 1.to(MAX_ROWS).oneOf()
    def due(c: Content): Content =
      if (c.later.contains(r)) { ...c, later: c.later.exclude(Set(r)), inbox: c.inbox.union(Set(r)) } else c
    val sh = s.shards.get(R)
    all {
      sh.file.later.contains(r) or sh.durable.later.contains(r),
      s' = withShard(s, R, { ...sh, file: due(sh.file), durable: due(sh.durable) })
    }
  }

  // The handler is called for a row that is due and durable, and consumes it in the transaction that does its
  // work. (A handler that fails changes nothing, so it is not modelled.)
  action handle(r) = all {
    val sh = s.shards.get(R)
    all {
      sh.active,
      sh.file.inbox.contains(r),
      sh.durable.inbox.contains(r),
      s' = withShard(s, R, { ...sh, file: { ...sh.file,
                                 inbox: sh.file.inbox.exclude(Set(r)),
                                 effects: sh.file.effects.append(r) } })
    }
  }

  // -- Sending

  // Only commits that are durable are sent, so nothing is sent for a write that is later lost.
  action send(m) = all {
    val sh = s.shards.get(S)
    all {
      sh.active,
      sh.file.outbox.contains(m),
      sh.durable.born.contains(m),
      not(s.waiting.contains(m)),
      s' = { ...s, inflight: s.inflight.put(m, s.inflight.get(m) + 1), waiting: s.waiting.union(Set(m)), sends: s.sends + 1 }
    }
  }

  // The sender gives up waiting (a timeout) and will send again. The request may still arrive.
  action timeout(m) = all {
    s.waiting.contains(m),
    s' = { ...s, waiting: s.waiting.exclude(Set(m)) }
  }

  action dropRequest(m) = all {
    s.inflight.get(m) > 0,
    s' = { ...s, inflight: s.inflight.put(m, s.inflight.get(m) - 1) }
  }

  // The receiver's node inserts the row in a commit of its own. A message that arrives twice is two rows.
  action deliver(m) = all {
    val sh = s.shards.get(R)
    all {
      s.inflight.get(m) > 0,
      sh.active,
      s.nextRow <= MAX_ROWS,
      s' = { ...withShard(s, R, { ...sh, file: { ...sh.file, inbox: sh.file.inbox.union(Set(s.nextRow)) } }),
             inflight: s.inflight.put(m, s.inflight.get(m) - 1),
             rowMsg: s.rowMsg.put(s.nextRow, m),
             pendingRows: s.pendingRows.union(Set(s.nextRow)),
             nextRow: s.nextRow + 1 }
    }
  }

  // The answer goes out once the row's commit is durable.
  action ack(r) = all {
    val sh = s.shards.get(R)
    all {
      s.pendingRows.contains(r),
      sh.active,
      sh.durable.inbox.contains(r) or listToSet(sh.durable.effects).contains(r),
      s' = { ...s, acks: s.acks.union(Set(s.rowMsg.get(r))), pendingRows: s.pendingRows.exclude(Set(r)) }
    }
  }

  action loseAck(m) = all {
    s.acks.contains(m),
    s' = { ...s, acks: s.acks.exclude(Set(m)) }
  }

  // The sender's node deletes the row once it hears. An answer that finds the shard inactive is lost.
  action receiveAck(m) = all {
    s.acks.contains(m),
    val sh = s.shards.get(S)
    if (sh.active)
      s' = { ...withShard(s, S, { ...sh, file: { ...sh.file, outbox: sh.file.outbox.exclude(Set(m)) } }),
             acks: s.acks.exclude(Set(m)),
             waiting: s.waiting.exclude(Set(m)) }
    else
      s' = s
  }

  // -- Shipping and life cycle

  action flush(i) = all {
    val sh = s.shards.get(i)
    all {
      sh.active,
      sh.file != sh.durable,
      s' = withShard(s, i, { ...sh, durable: sh.file, tailed: true })
    }
  }

  // A delta turns the tail into files. The tail is the only sign an adopter has that a shard may have
  // work, so a shard with a live outbox row gets its hint before the tail goes (and the delta waits if the hint
  // can't be written, which is the same thing as the hint and the delta being one step here).
  action delta(i) = all {
    val sh = s.shards.get(i)
    all {
      sh.active,
      sh.tailed,
      // The hint for inbox rows is written moments after their flush and a delta needs a tail minutes old, so
      // the model has the hint first. (If the hint write keeps failing the code could be caught out here.)
      (sh.durable.later.size() == 0 and sh.durable.inbox.size() == 0) or s.hints.contains(i),
      s' = { ...withShard(s, i, { ...sh, tailed: false }),
             hints: if (sh.file.outbox.size() > 0) s.hints.union(Set(i)) else s.hints }
    }
  }

  action activate(i) = all {
    val sh = s.shards.get(i)
    all {
      not(sh.active),
      not(sh.orphan),
      s' = withShard(s, i, { ...sh, active: true, file: sh.durable })
    }
  }

  // Idle deactivation: everything is durable, and rows to send or due now keep the shard active. Before the
  // shard is released a wake hint is written if it has rows due later.
  action deactivate(i) = all {
    val sh = s.shards.get(i)
    all {
      sh.active,
      sh.file == sh.durable,
      sh.file.outbox.size() == 0,
      sh.file.inbox.size() == 0,
      s' = { ...forgotten(withShard(s, i, { ...sh, active: false, tailed: false }), i),
             hints: if (sh.file.later.size() > 0) s.hints.union(Set(i)) else s.hints }
    }
  }

  // A node scaling down delivers for a few seconds, then gives each shard that still has work a wake hint
  // and lets it go.
  action drain(i) = all {
    val sh = s.shards.get(i)
    all {
      sh.active,
      sh.file == sh.durable,
      s' = { ...forgotten(withShard(s, i, { ...sh, active: false, tailed: false }), i),
             hints: if (hasWork(sh.file)) s.hints.union(Set(i)) else s.hints }
    }
  }

  // While a shard is active the node writes a hint for its earliest inbox row, after the flush that made it
  // durable (the code does this for any inbox row, due now or later).
  action writeHint(i) = all {
    val sh = s.shards.get(i)
    all {
      sh.active,
      sh.durable.later.size() > 0 or sh.durable.inbox.size() > 0,
      s' = { ...s, hints: s.hints.union(Set(i)) }
    }
  }

  // The hint goes once the shard has nothing left to deliver.
  action dropHint(i) = all {
    val sh = s.shards.get(i)
    all {
      s.hints.contains(i),
      sh.active,
      not(hasWork(sh.file)),
      not(hasWork(sh.durable)),
      s' = { ...s, hints: s.hints.exclude(Set(i)) }
    }
  }

  // -- Crashes

  // The node dies. The file is gone and so is the node's memory. The shard cannot be activated until
  // an adopter has replayed its tail.
  action crash(i) = all {
    val sh = s.shards.get(i)
    all {
      sh.active,
      s' = forgotten(withShard(s, i, { ...sh, active: false, orphan: true, file: sh.durable }), i)
    }
  }

  // The adopter writes a delta for every shard it replayed a tail for, and a wake hint for it.
  action adopt(i) = all {
    val sh = s.shards.get(i)
    all {
      sh.orphan,
      s' = { ...withShard(s, i, { ...sh, orphan: false, tailed: false }),
             hints: if (sh.tailed) s.hints.union(Set(i)) else s.hints }
    }
  }

  action step = {
    nondet i = SHARDS.oneOf()
    nondet m = 1.to(MAX_MSGS).oneOf()
    nondet r = 1.to(MAX_ROWS).oneOf()
    any {
      createMsg,
      setTimer,
      tick,
      handle(r),
      send(m),
      timeout(m),
      dropRequest(m),
      deliver(m),
      ack(r),
      loseAck(m),
      receiveAck(m),
      flush(i),
      delta(i),
      activate(i),
      deactivate(i),
      drain(i),
      writeHint(i),
      dropHint(i),
      crash(i),
      adopt(i)
    }
  }

  // -- Invariants

  def rowsOf(c: Content): Set[int] =
    c.inbox.union(c.later).union(listToSet(c.effects))

  def received(st: State): Set[int] =
    rowsOf(st.shards.get(R).file).union(rowsOf(st.shards.get(R).durable))

  // Nothing is sent for a write that is later lost: every message in the receiver came from an outbox row that
  // is durable at the sender.
  val noPhantom = 1.to(MAX_ROWS).forall(r =>
    (received(s).contains(r) and s.rowMsg.get(r) != 0) implies
      s.shards.get(S).durable.born.contains(s.rowMsg.get(r)))

  // Nothing is lost once the write is durable: the message is still in the sender's outbox or the receiver has
  // it durably, waiting or handled.
  def heldByReceiver(st: State, m: int): bool =
    1.to(MAX_ROWS).exists(r => st.rowMsg.get(r) == m and rowsOf(st.shards.get(R).durable).contains(r))

  val noLoss = s.shards.get(S).durable.born.forall(m =>
    s.shards.get(S).durable.outbox.contains(m) or heldByReceiver(s, m))

  // An answer is only sent for a row that is durable, so the sender deleting its row is safe.
  val ackedIsDurable = s.acks.forall(m => heldByReceiver(s, m))

  // What a handler commits for a row happens once: the row is not handled twice, and a handled row is gone.
  def noRepeats(l: List[int]): bool =
    l.indices().forall(i => l.indices().forall(j => (l[i] == l[j]) implies i == j))

  val handledOnce = SHARDS.forall(i => {
    val sh = s.shards.get(i)
    noRepeats(sh.file.effects) and noRepeats(sh.durable.effects) and
      listToSet(sh.file.effects).intersect(sh.file.inbox.union(sh.file.later)).size() == 0 and
      listToSet(sh.durable.effects).intersect(sh.durable.inbox.union(sh.durable.later)).size() == 0
  })

  // This was violated until a delta wrote the hint: a shard with a durable outbox row whose tail had become a
  // delta and which then crashed was adopted with no tail to replay, so no hint was written and the shard was
  // inactive with work and no way to be woken except a request that happened to reach it. Removing the
  // hint from `delta` makes this fail again (a durable outbox row, a delta, a crash, an adoption).
  // A shard that is not active and not waiting for an adopter, with work in its tables, has a wake hint, so
  // the work is not stranded.
  val notStranded = SHARDS.forall(i => {
    val sh = s.shards.get(i)
    (not(sh.active) and not(sh.orphan) and hasWork(sh.durable)) implies s.hints.contains(i)
  })

  // Everything except notStranded, for checking the delivery properties on their own.
  val core = and { noPhantom, noLoss, ackedIsDurable, handledOnce }

  val safe = and { noPhantom, noLoss, ackedIsDurable, handledOnce, notStranded }

  // Witnesses: each should be violated.
  val neverHandled = s.shards.get(R).file.effects.length() == 0
  val neverResent = s.sends <= s.nextMsg - 1
  val neverDuplicated = not(1.to(MAX_ROWS).exists(r => 1.to(MAX_ROWS).exists(q =>
    r != q and s.rowMsg.get(r) != 0 and s.rowMsg.get(r) == s.rowMsg.get(q))))
  val neverHinted = s.hints.size() == 0
}
