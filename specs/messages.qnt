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
// commits beyond its last delta. A shard's manifest records when it next needs waking (its wake time), and
// the node writes the hint for that time before every manifest write that records one, so the model has the
// hint appear with the delta that ends a tail. Durability here is the same idea as in shard.qnt: the file is what the
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

  // The flush makes the commits durable and extends the tail. The messages owner writes no hint: while a
  // commit is in the tail, adoption covers it after a crash.
  action flush(i) = all {
    val sh = s.shards.get(i)
    all {
      sh.active,
      sh.file != sh.durable,
      s' = withShard(s, i, { ...sh, durable: sh.file, tailed: true })
    }
  }

  // A delta turns the tail into files and records the shard's wake time in the same manifest write, with its
  // hint written first. The shard has a wake time when it has work of any kind (rows to send, rows due now or
  // rows due later), so from here a crash leaves a hint whatever the tail held. (Rows given up on are not
  // modelled. The code leaves them without a wake time.) The wake time is read through the shard's connection,
  // which can be ahead of the files: a commit that consumes a row but isn't captured yet would hide the row. So
  // the read counts only if nothing is uncaptured (here: the file is all durable), and otherwise the wake time is
  // now, with its hint.
  action delta(i) = all {
    val sh = s.shards.get(i)
    all {
      sh.active,
      sh.tailed,
      s' = { ...withShard(s, i, { ...sh, tailed: false }),
             hints: if (sh.file != sh.durable or hasWork(sh.durable)) s.hints.union(Set(i)) else s.hints }
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

  // Idle deactivation: everything is durable, and rows to send or due now keep the shard active. The final
  // delta records the wake time, with its hint written first if the shard has rows due later. The release
  // deletes no hints, so the existing one stays, and the scan deletes it when it is stale.
  action deactivate(i) = all {
    val sh = s.shards.get(i)
    all {
      sh.active,
      sh.file == sh.durable,
      sh.file.outbox.size() == 0,
      sh.file.inbox.size() == 0,
      s' = { ...forgotten(withShard(s, i, { ...sh, active: false, tailed: false }), i),
             hints: if (hasWork(sh.file)) s.hints.union(Set(i)) else s.hints }
    }
  }

  // A node scaling down delivers for a few seconds, then lets each shard go with a final delta that records the
  // wake time, with its hint written first if the shard still has work.
  action drain(i) = all {
    val sh = s.shards.get(i)
    all {
      sh.active,
      sh.file == sh.durable,
      s' = { ...forgotten(withShard(s, i, { ...sh, active: false, tailed: false }), i),
             hints: if (hasWork(sh.file)) s.hints.union(Set(i)) else s.hints }
    }
  }

  // The wake scan is the only thing that deletes hints. For a shard that is active it waits until the shard has
  // nothing live. For one at rest it reads the manifest (the durable content here) and deletes the hint if the
  // wake time is absent or later than the hint. The model keeps any hint while there is work of any kind, which
  // is stricter than the code, since it doesn't tell the hints of rows due later from the hints for now. A shard
  // that crashed and is not adopted yet belongs to a dead node, so its hint stays.
  action dropHint(i) = all {
    val sh = s.shards.get(i)
    all {
      s.hints.contains(i),
      not(sh.orphan),
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

  // The promise about waking: a shard that is not active and not waiting for an adopter, with rows to send, rows
  // due now or rows due later, has a wake hint, so it is woken when they are due. This holds for outbox rows and
  // due inbox rows that were still failing when a tail became a delta, since the delta's hint covers them.
  // (It subsumes the earlier check for outbox and due inbox rows alone.)
  val notStranded = SHARDS.forall(i => {
    val sh = s.shards.get(i)
    (not(sh.active) and not(sh.orphan) and hasWork(sh.durable)) implies s.hints.contains(i)
  })

  // The delivery properties on their own.
  val core = and { noPhantom, noLoss, ackedIsDurable, handledOnce }

  val safe = and { noPhantom, noLoss, ackedIsDurable, handledOnce, notStranded }

  // Witnesses: each should be violated.
  val neverHandled = s.shards.get(R).file.effects.length() == 0
  val neverResent = s.sends <= s.nextMsg - 1
  val neverDuplicated = not(1.to(MAX_ROWS).exists(r => 1.to(MAX_ROWS).exists(q =>
    r != q and s.rowMsg.get(r) != 0 and s.rowMsg.get(r) == s.rowMsg.get(q))))
  val neverHinted = s.hints.size() == 0
}
