// How one journal's peer holds a mirror of it, and what that is worth.
//
// One writer, one journal, three places that hold its segments: the owner's
// file, the record (blob storage) and the peer (another node's disk, over the
// mirror protocol). The record is the history: the fence is created there, and
// what a restore reads is the record's landed run below the fence. The peer is
// a copy in front of the record, there so an acknowledgement need not wait for
// the record's create. The ownership protocol around the journal (who owns the
// shard, how the lease and the manifest decide) is cluster.qnt; this module is
// only the copies and the adoption's fetch.
//
// Partial presence. Every segment is created once and never modified, so the
// stores never disagree about content, only about how much they hold. Each
// flush lands the segment in the record, and the peer's take is its own:
// it lands or it misses (a hole), independently of the record's take. The
// writer's retry of a lost create is what carries a segment in while the
// process lives, so the record misses a number only past a death
// (flushLost) or while a create sits unresolved (zombieFlush, the writer
// stuck at one number, its record uploader being serial).
//
// Tiers. With acks riding the record (PEER_ACKS false, the storage tier),
// a client's answer waits until the record holds the file,
// so a stranded or un-landed segment holds nothing acknowledged. With
// PEER_ACKS, an answer waits only for the peer to hold every number built:
// the answer is on two machines, the owner's disk and the peer's, and the
// record lands within seconds. That opens a window noLoss prices, not closes:
// once the owner is gone and the adoption runs, the peer's copy is the only
// usable one until the record has it, so losing the peer's host there loses
// the window. And a zombie in its staleness window can hand the peer a
// segment for a commit the record never landed, after which the fence takes
// that number: without a read below the fence, the commit sits on the peer
// below a fence nothing reads again. The adoption's read (READ_FENCES) asks
// the peer at every fence and carries what it holds there into the record's
// history, which is what noStrandedAcks checks; noReadTail is the same
// protocol without the read, and it is where the window loses its commit.
//
// Adoption. An adopter of the dead writer's journal marks the lease, fetches
// the peer's run into the record (one number per step, stopping where the
// peer's run ends), and fences above what landed. Fencing first would take a
// number the peer holds and strand the segment below it, which `stranded`
// witnesses and the NO_PULL variant reaches. The fetch cannot reach a segment
// the zombie hands over after it ran: that number is the fence's own, and
// only the read below the fence can find it.
//
// Bounds: one adoption, at most MAX_SEGS segments, MAX_COMMITS commits.
//
// Run:
//   quint typecheck specs/mirrors.qnt
//   quint run specs/mirrors.qnt --main=mirrorsProtocol --invariant=noLoss --max-steps=40 --max-samples=50000
//   quint run specs/mirrors.qnt --main=mirrorsProtocol --invariant=pullBeforeFence --max-steps=40 --max-samples=50000
//   quint run specs/mirrors.qnt --main=mirrorsProtocol --invariant=noStrandedAcks --max-steps=40 --max-samples=50000
//   quint run specs/mirrors.qnt --main=peerAcks --invariant=noStrandedAcks --max-steps=40 --max-samples=50000
//   quint run specs/mirrors.qnt --main=peerAcks --invariant=noLoss --max-steps=40   (should fail: the tier's price)
//   quint run specs/mirrors.qnt --main=noReadTail --invariant=noStrandedAcks --max-steps=40   (should fail)
//   quint run specs/mirrors.qnt --main=noPull --invariant=pullBeforeFence --max-steps=40   (should fail)
//   quint test specs/mirrors.qnt --main=mirrorsProtocol
//   quint test specs/mirrors.qnt --main=noReadTail --match=zombieWindow
//
// Simulation is the check: the model is small, but the interesting traces
// (a fetch past a death, a zombie's window) want depth more than breadth.

module mirrors {
  const MAX_COMMITS: int  // commit ids are 1 to MAX_COMMITS, handed out as they are made
  const MAX_SEGS: int     // at most this many segments per journal

  // Whether acknowledgements ride on the peer (the tier) instead of the
  // record. Its price is noLoss, and its own check is noStrandedAcks.
  const PEER_ACKS: bool
  // The adoption reads the peer at the fences it finds, carrying what the
  // peer holds there into the record's history.
  const READ_FENCES: bool
  // An adoption fences without fetching the peer first.
  const NO_PULL: bool

  type Segment = {
    has: bool,          // the store holds it (a hole, or a fence, is false)
    content: List[int]  // the commits the segment carries
  }

  type State = {
    file: List[int],     // the owner's file: the commits it holds, in order
    built: int,          // the segments the writer built: the numbers 1..built
    rec: List[Segment],  // the record's copy, by number
    peer: List[Segment], // the peer's copy, by number
    durable: int,        // how much of the file the record holds as the writer heard
    acked: Set[int],     // commits some client has seen acknowledged
    next: int,           // the next commit id
    stale: bool,         // the writer's lease has run out; the process may live on
    stopped: bool,       // the writer's process is gone (a crash, or a death)
    stuck: bool,         // the writer is a zombie parked at one unresolved create
    phase: int,          // the adoption: 0 none, 1 marked, 2 pulled, 3 fenced
    mlen: int,           // how many numbers the peer held when the mark landed
    hostLost: bool,      // the owner's host is gone: the file went with it
    peerLost: bool,      // the peer's host is gone: the peer's copies went with it
    stranded: bool       // the fence took a number the peer holds a segment at
  }

  var s: State

  pure def listToSet(l: List[int]): Set[int] = l.indices().map(i => l[i])

  // The record's landed run holds the commit.
  pure def recHolds(st: State, c: int): bool =
    st.rec.indices().exists(i => st.rec[i].has and listToSet(st.rec[i].content).contains(c))

  pure def peerHolds(st: State, c: int): bool =
    st.peer.indices().exists(i => st.peer[i].has and listToSet(st.peer[i].content).contains(c))

  // What keeps a commit: the record's run keeps it always. The peer keeps it
  // until the journal is fenced (the protocol never reads below a fence, so
  // once there is one, the record's run is the history and the peer's copies
  // decide nothing), and only while the peer's host stands. The owner's file
  // keeps it only before the adoption, and only while its host stands.
  pure def recoverable(st: State, c: int): bool =
    recHolds(st, c)
    or (st.phase < 3 and not(st.peerLost) and peerHolds(st, c))
    or (st.phase == 0 and not(st.hostLost) and listToSet(st.file).contains(c))

  // The loss bound the ladder promises with acks on the record: nothing
  // acknowledged is lost by the loss of one host. An acknowledgement on the
  // record keeps it by the record's run alone, so the owner's host and the
  // peer's host can each go. With acks on the peer the bound is the tier's
  // price: until the record lands, the peer's copy is the only one the
  // cluster can read once the owner is gone, so losing the peer's host there
  // loses the window (peerAcks' counterexample).
  val noLoss = s.acked.forall(c => recoverable(s, c))

  // The fetch precedes the fence: no number the peer holds goes under the
  // fence. noPull's counterexample reaches it, and so does the peer tier's
  // window (a zombie's segment that reached the peer after the fetch ran).
  val pullBeforeFence = not(s.stranded)

  // The fence never leaves an acknowledged commit below it: once the adoption
  // has fenced, the record's history (the segments it landed and the deltas
  // the read writes) holds every commit some client saw acknowledged. A
  // zombie's segment is the case that needs the read: the fetch never saw it,
  // and only the read at the fence's number carries it in. noReadTail reaches
  // the violation; peerAcks holds it.
  val noStrandedAcks = s.phase < 3 or s.acked.forall(c => recHolds(s, c))

  // The coverage rule at the truth level: the peer holds every number built,
  // no holes, and the last of them carries the file as it stands — a commit
  // the last flush did not carry yet is waiting for a segment, and no answer
  // rides on it. The frontier is the code's way of knowing all this.
  pure def covered(st: State): bool =
    st.built == st.peer.length()
    and st.peer.length() > 0
    and st.peer.indices().forall(i => st.peer[i].has)
    and st.peer[st.peer.length() - 1].content == st.file

  action init = all {
    s' = {
      file: List(), built: 0, rec: List(), peer: List(), durable: 0,
      acked: Set(), next: 1, stale: false, stopped: false, stuck: false,
      phase: 0, mlen: 0, hostLost: false, peerLost: false, stranded: false
    }
  }

  action commit = all {
    not(s.stopped),
    not(s.stuck),
    s.next <= MAX_COMMITS,
    s' = { ...s, file: s.file.append(s.next), next: s.next + 1 }
  }

  // The writer builds a segment from the file: the record lands it and hears
  // of it, and the peer's take is its own (partial presence).
  action flush(take) = all {
    not(s.stopped),
    not(s.stuck),
    s.built < MAX_SEGS,
    s' = { ...s, built: s.built + 1, durable: s.file.length(),
           rec: s.rec.append({ has: true, content: s.file }),
           peer: s.peer.append({ has: take, content: s.file }) }
  }

  // The record's create is lost and the writer dies before its retry: the
  // segment is on the peer alone, the only way the record misses a number.
  action flushLost = all {
    not(s.stopped),
    not(s.stuck),
    s.built < MAX_SEGS,
    s' = { ...s, built: s.built + 1, stopped: true,
           peer: s.peer.append({ has: true, content: s.file }) }
  }

  // A zombie in the staleness window hands the peer a segment its record
  // create never landed and never will while it lives: the create is stuck,
  // and the writer builds nothing past it (the record's uploader is serial).
  action zombieFlush = all {
    s.stale,
    not(s.stopped),
    not(s.stuck),
    s.built < MAX_SEGS,
    s' = { ...s, built: s.built + 1, stuck: true,
           peer: s.peer.append({ has: true, content: s.file }) }
  }

  // A client's answer. With acks riding the record, the file is in the
  // record. With the peer tier, the peer holds every number built and the
  // file as it stands: the answer rides on the peer, and the lease is not
  // asked (a zombie in its staleness window answers too, which is the
  // window). The fence ends the writer: its creates are refused there, so no
  // answer comes from it after the adoption has fenced. An answer needs the
  // host of the writer's process and, on the peer tier, the peer's disk: a
  // lost host answers nothing, since its copies are gone (recoverable reads
  // the same flags).
  action respond = all {
    not(s.stopped),
    not(s.hostLost),
    s.phase < 3,
    if (PEER_ACKS) (not(s.peerLost) and covered(s))
    else s.durable == s.file.length(),
    s' = { ...s, acked: s.acked.union(listToSet(s.file)) }
  }

  action expireLease = all {
    not(s.stale),
    s' = { ...s, stale: true }
  }

  // An adopter marks the dead lease. A lease the writer still holds is not
  // adoptable, whatever the process is doing. The fetch is sized by what the
  // peer held at the mark: a segment that reaches it later is the zombie
  // window, stated rather than witnessed here.
  action mark = all {
    s.stale,
    s.phase == 0,
    s' = { ...s, phase: 1, mlen: s.peer.length() }
  }

  // The fetch: the record's landed run is extended with what the peer holds,
  // one number per step, in order, until the peer's run ends. A number the
  // peer missed ends the run (a record without its numbers in order would
  // hide the segments after the gap); a number it already holds would cost a
  // look and copy nothing, which the run's end covers.
  action pull = all {
    s.phase == 1,
    not(NO_PULL),
    s' = (if (s.rec.length() < s.peer.length() and s.peer[s.rec.length()].has)
          { ...s, rec: s.rec.append(s.peer[s.rec.length()]) }
          else
          { ...s, phase: 2 })
  }

  // The fence at the first free number, above what landed. Without the fetch
  // it can take a number the peer held a segment at when the mark landed,
  // and strand it below the fence: `stranded` witnesses it, and noPull
  // reaches it. A zombie's segment that reached the peer after the mark is
  // the one number this can still happen to (the writer is parked at it).
  // The read below the fence (READ_FENCES) asks the peer at the number the
  // fence takes — and at every fence the journal holds — and carries what it
  // finds into the record's history, as the tail the adoption writes as a
  // delta does. Without it the segment stays on the peer below the fence,
  // which nothing reads again, and an acknowledged commit on it is lost.
  action adoptFence = {
    val at = s.rec.length()
    val held = READ_FENCES and at < s.peer.length() and s.peer[at].has
    all {
      s.phase == 2 or (NO_PULL and s.phase == 1),
      at < MAX_SEGS,
      s' = { ...s, phase: 3, stranded: s.stranded or s.mlen > at,
             rec: s.rec.append(if (held) s.peer[at] else { has: false, content: List() }) }
    }
  }

  // The owner's host is lost: the file goes with it. The record and the peer
  // stand, which is what the tiers' ladder promises.
  action ownerHostLost = all {
    not(s.hostLost),
    s' = { ...s, hostLost: true }
  }

  // The peer's host is lost: the peer's copies go with it. The record and
  // the owner's disk stand, which is what the tiers' ladder promises.
  action peerHostLost = all {
    not(s.peerLost),
    s' = { ...s, peerLost: true }
  }

  action step = {
    nondet take = Set(true, false).oneOf()
    any {
      commit,
      flush(take),
      flushLost,
      zombieFlush,
      respond,
      expireLease,
      mark,
      pull,
      adoptFence,
      ownerHostLost,
      peerHostLost
    }
  }

  // -- Scenarios

  // The owner dies with the record's create of its last segment lost: the
  // fetch lands the peer's copy before the fence, and nothing acknowledged
  // is lost, whatever single host is then lost.
  run fetchedTest =
    init
      .then(commit).then(flush(true)).then(respond)
      .then(commit).then(flushLost)
      .then(expireLease).then(mark).then(pull).then(pull).then(adoptFence)
      .then(peerHostLost)
      .expect(s.rec.length() == 3 and not(s.stranded) and noLoss)
}

// The storage tier, for simulation: acks ride on the record.
module mirrorsProtocol {
  import mirrors(MAX_COMMITS = 3, MAX_SEGS = 3, PEER_ACKS = false, READ_FENCES = true, NO_PULL = false).*
}

// Without the fetch, the fence strands what the peer holds.
module noPull {
  import mirrors(MAX_COMMITS = 3, MAX_SEGS = 3, PEER_ACKS = false, READ_FENCES = true, NO_PULL = true).*
}

// The peer tier as it lands: acks ride on the peer, and the adoption's read
// below the fence carries a zombie's last segment into the record's history.
// noStrandedAcks holds here; noLoss does not, which is the tier's price (the
// owner is gone, and the record has not landed the window yet).
module peerAcks {
  import mirrors(MAX_COMMITS = 3, MAX_SEGS = 3, PEER_ACKS = true, READ_FENCES = true, NO_PULL = false).*

  // A zombie acks on the peer a commit the record never landed, and the fence
  // takes the number. The read carries the segment into the record's history,
  // so the acknowledged commit survives the fence.
  run closedTest =
    init
      .then(commit).then(flush(true)).then(commit).then(flush(true)).then(commit)
      .then(expireLease).then(mark).then(pull)
      .then(zombieFlush).then(respond).then(adoptFence)
      .expect(noStrandedAcks and s.rec.length() == 3 and s.acked.size() == 3)
}

// The same protocol without the read: the fence takes the zombie's number and
// the acknowledged commit on it sits on the peer below a fence nothing reads
// again.
module noReadTail {
  import mirrors(MAX_COMMITS = 3, MAX_SEGS = 3, PEER_ACKS = true, READ_FENCES = false, NO_PULL = false).*

  run zombieWindowTest =
    init
      .then(commit).then(flush(true)).then(commit).then(flush(true)).then(commit)
      .then(expireLease).then(mark).then(pull)
      .then(zombieFlush).then(respond).then(adoptFence)
      .expect(not(noStrandedAcks) and s.acked.size() > 0)
}
