// How the node captures one shard's commits by reading SQLite's WAL itself, and why a checkpoint or a WAL restart
// can't drop a frame before it is captured.
//
// The app writes through its own connections. The node reads committed frames from the `-wal` file and holds one read
// transaction on a connection of its own, which is what keeps SQLite from checkpointing and restarting over frames it
// hasn't read. SQLite's rules, as modelled:
//   - The WAL has frames 1..mxFrame (every frame is a commit here). `back` (nBackfill) frames are copied into the
//     database file.
//   - A reader holds a read mark: 0 (it reads the database file only, possible only when back = mxFrame) or a frame
//     count. The node's is always 0 (see below), so marks above 0 only come from the app's own readers, which the model
//     leaves out (they would only lower what a checkpoint copies).
//   - A checkpoint copies frames up to mxFrame and copies nothing while any reader holds mark 0. It may stop short (a
//     partial copy, as when it is interrupted).
//   - The writer restarts the WAL (a new generation, standing for new salts, with no frames and nothing copied) at the
//     start of a write when back = mxFrame and no reader holds a nonzero mark. A reader at mark 0 doesn't stop it.
//   - TRUNCATE restarts at once when its copy was complete. RESTART and FULL wait for readers and fail busy otherwise,
//     so their effect is that of PASSIVE, and the restart is the next write's.
//
// The node's side:
//   - It holds a read transaction on mark 0 whenever the shard is captured. While it does, nothing is copied into the
//     database file and the WAL can't restart once a frame is waiting to be copied, so the WAL only grows.
//   - A sync reads the frames after the ones it has read in the same generation, or all of a new generation (the
//     salts changed), and adds them to the journal. It never waits for anything.
//   - When the WAL is large it takes the write lock, syncs, ends its transaction, runs a PASSIVE checkpoint that copies
//     everything, begins a new transaction (which gets mark 0) and lets go (`lockedRestart`). The app's writes wait
//     for that, and its next write restarts the WAL. A TRUNCATE checkpoint can't be used: it takes the write lock
//     itself.
//   - A snapshot syncs, notes the captured end and builds a page map of the frames in the WAL that the database file
//     doesn't hold. Then it reads one page at a time, while the app goes on, from the WAL or from the database file. The
//     locked restart waits for it.
//   - A release ends the node's transaction. A refused release reopens, takes the write lock, and checks the generation.
//     If it is the same and the WAL still reaches the captured end, it syncs the rest. Otherwise it writes the whole
//     database as one file. Either way it ends as the locked restart does (`resume`).
//
// The database is `PAGES`, each holding the number of the commit that last wrote it (0 for the state the shard was
// restored at). A commit writes one page. `hist` records which, so the model can say what the database was after
// any commit (`truthAt`).
//
// The properties:
//   noLoss: while the shard is captured, every commit that isn't captured is still a frame in the WAL, so no
//     checkpoint and restart took one.
//   replay: the journal (every captured file replayed) is the database as it was after the last captured commit.
//   snapOk: a finished snapshot is the database as it was at the captured end it began at.
//
// Variants (constants below) switch a rule to a rejected alternative, to show its counterexample. Each has an
// instance at the end of the file.
//
// MAP_ALL is a snapshot map that covers every frame in the WAL, the ones the database file holds too. After a locked
// restart every frame is in the file, and the app's next write restarts the WAL under the node's mark 0, which wipes
// what the map points at while the snapshot reads. The protocol maps only the frames after `back`.
//
// An earlier design moved the node's transaction forward after every sync (a ratchet of two overlapping read
// transactions) so that the app's checkpoints could go on. The spike showed it didn't bound the WAL on a busy shard,
// since the transaction it always held stopped the restart, and with the locked restart doing that job the ratchet
// was dropped.
//
// Left out: the app's own read transactions, multi-page commits (a header is only ever written for a whole commit),
// page contents beyond which commit wrote them, the corrupt frame and the torn tail (the WAL reader's, and tested
// there), the locked restart's threshold (here any frames at all) and how the node wins the write lock against a busy
// writer, and liveness.
//
// Run (simulation only):
//   quint typecheck specs/capture.qnt
//   quint run specs/capture.qnt --main=captureProtocol --invariant=safe --max-steps=40 --max-samples=50000
//   quint run specs/capture.qnt --main=lockNoSync --invariant=safe --max-steps=40 --max-samples=50000   (should fail)
//   quint run specs/capture.qnt --main=lockGap --invariant=safe --max-steps=40 --max-samples=50000   (should fail)
//   quint run specs/capture.qnt --main=noSalts --invariant=safe --max-steps=40 --max-samples=50000   (should fail)
//   quint run specs/capture.qnt --main=mapAll --invariant=safe --max-steps=40 --max-samples=50000   (should fail)
//   quint run specs/capture.qnt --main=captureProtocol --invariant=neverRestarts --max-steps=40 --max-samples=50000   (should fail: a restart is reachable)

module capture {
  // At most this many commits.
  const MAX_COMMITS: int

  // Variants, each false in the protocol:
  //   LOCK_NO_SYNC: the locked restart doesn't sync before it checkpoints.
  //   LOCK_GAP: the locked restart syncs and ends the transaction, and checkpoints later without holding the write lock.
  //   NO_SALTS: a resumed release continues if the WAL reaches the captured end, without comparing generations.
  //   MAP_ALL: a snapshot's page map covers every frame in the WAL, the ones in the database file included.
  const LOCK_NO_SYNC: bool
  const LOCK_GAP: bool
  const NO_SALTS: bool
  const MAP_ALL: bool

  pure val PAGES = Set(0, 1)
  pure val NPAGES = 2

  type Frame = { pg: int, sq: int }   // the page it holds and the number of the commit it ends

  // The app's side.
  var hist: List[int]       // the page each commit wrote, in order
  var wal: List[Frame]      // the current generation's frames
  var gen: int              // the generation (the salts)
  var back: int             // frames copied into the database file
  var db: int -> int        // the database file: for each page, the commit that wrote what it holds

  // The node holds a read transaction, on mark 0.
  var held: bool

  // The journal: every captured file replayed, and where capture is.
  var journal: int -> int
  var capSeq: int           // the last captured commit
  var capGen: int           // the generation the last sync read
  var capFrames: int        // how many of its frames

  // 0 captured, 1 released, 3 in the gap of the LOCK_GAP variant.
  var phase: int

  // A snapshot: 0 none, 1 reading, 2 finished.
  var sPhase: int
  var sEnd: int
  var sGen: int
  var sMap: int -> int      // each page's newest frame the snapshot reads from the WAL (1-based), 0 for the file
  var sNext: int
  var sGot: int -> int

  pure def truthAt(h: List[int], s: int): int -> int =
    PAGES.mapBy(p => 1.to(s).filter(i => h[i - 1] == p).fold(0, (a, i) => if (i > a) i else a))

  // What a sync does, in terms of the journal's next values.
  def syncFrom: int = if (capGen == gen) capFrames else 0
  def syncFresh: List[Frame] = wal.slice(syncFrom, wal.length())
  def syncJournal: int -> int = syncFresh.foldl(journal, (j, f) => j.set(f.pg, f.sq))
  def syncSeq: int = if (syncFresh.length() > 0) syncFresh[syncFresh.length() - 1].sq else capSeq

  // The database file after a checkpoint that copies the whole WAL.
  def copiedAll: int -> int = wal.foldl(db, (d, f) => d.set(f.pg, f.sq))

  action keepWal = all { wal' = wal, gen' = gen, back' = back, db' = db }
  action keepJournal = all { journal' = journal, capSeq' = capSeq, capGen' = capGen, capFrames' = capFrames }
  action keepSnap = all { sPhase' = sPhase, sEnd' = sEnd, sGen' = sGen, sMap' = sMap, sNext' = sNext, sGot' = sGot }

  action init = all {
    hist' = List(),
    wal' = List(),
    gen' = 0,
    back' = 0,
    db' = PAGES.mapBy(p => 0),
    held' = true,
    journal' = PAGES.mapBy(p => 0),
    capSeq' = 0,
    capGen' = 0,
    capFrames' = 0,
    phase' = 0,
    sPhase' = 0,
    sEnd' = 0,
    sGen' = 0,
    sMap' = PAGES.mapBy(p => 0),
    sNext' = 0,
    sGot' = PAGES.mapBy(p => 0),
  }

  // The app commits one page. The writer restarts the WAL first if it can (every frame copied, and no reader on a
  // nonzero mark, of which there are none here).
  action commit(pg: int): bool = {
    val restart = wal.length() > 0 and back == wal.length()
    all {
      hist.length() < MAX_COMMITS,
      hist' = hist.append(pg),
      wal' = (if (restart) List() else wal).append({ pg: pg, sq: hist.length() + 1 }),
      gen' = if (restart) gen + 1 else gen,
      back' = if (restart) 0 else back,
      db' = db,
      held' = held, keepJournal, keepSnap,
      phase' = phase,
    }
  }

  // An app's checkpoint. It copies nothing while mark 0 is held and otherwise all of the WAL, perhaps not all the way.
  // TRUNCATE restarts when its copy was complete.
  action checkpoint(trunc: bool): bool = all {
    not(held),
    wal.length() > 0,
    nondet nb = oneOf(back.to(wal.length()))
    val done = trunc and nb == wal.length()
    all {
      db' = wal.slice(back, nb).foldl(db, (d, f) => d.set(f.pg, f.sq)),
      wal' = if (done) List() else wal,
      gen' = if (done) gen + 1 else gen,
      back' = if (done) 0 else nb,
      hist' = hist,
      held' = held, keepJournal, keepSnap,
      phase' = phase,
    }
  }

  // The node reads the frames it hasn't.
  action sync = all {
    phase == 0,
    journal' = syncJournal,
    capSeq' = syncSeq,
    capGen' = gen,
    capFrames' = wal.length(),
    hist' = hist, keepWal, keepSnap,
    held' = held,
    phase' = phase,
  }

  // The WAL is large (here any frames at all). Under the write lock, so atomic, the node syncs, ends its transaction,
  // runs a PASSIVE checkpoint that copies everything and begins a new transaction, which gets mark 0. It waits for a
  // snapshot to finish, as the snapshot needs the database file as it is. The WAL restarts at the app's next write.
  action lockedRestart = all {
    not(LOCK_GAP),
    phase == 0,
    sPhase == 0,
    wal.length() > 0,
    journal' = if (LOCK_NO_SYNC) journal else syncJournal,
    capSeq' = if (LOCK_NO_SYNC) capSeq else syncSeq,
    capGen' = gen,
    capFrames' = wal.length(),
    db' = copiedAll,
    back' = wal.length(),
    held' = true,
    wal' = wal, gen' = gen,
    hist' = hist, keepSnap,
    phase' = phase,
  }

  // LOCK_GAP: the node syncs and ends its transaction, and the checkpoint comes later, with the app free to commit in
  // between. What it commits is copied and then wiped by the restart, uncaptured.
  action gapStart = all {
    LOCK_GAP,
    phase == 0,
    sPhase == 0,
    wal.length() > 0,
    journal' = syncJournal,
    capSeq' = syncSeq,
    capGen' = gen,
    capFrames' = wal.length(),
    held' = false,
    phase' = 3,
    hist' = hist, keepWal, keepSnap,
  }

  action gapFinish = all {
    phase == 3,
    db' = copiedAll,
    back' = wal.length(),
    held' = true,
    phase' = 0,
    wal' = wal, gen' = gen,
    hist' = hist, keepJournal, keepSnap,
  }

  // A snapshot syncs and notes the captured end and the page map, under the capture lock. Its reads rely on the node's
  // transaction on mark 0, which keeps the database file as it is.
  action snapStart = all {
    phase == 0,
    sPhase == 0,
    journal' = syncJournal,
    capSeq' = syncSeq,
    capGen' = gen,
    capFrames' = wal.length(),
    sPhase' = 1,
    sEnd' = syncSeq,
    sGen' = gen,
    sMap' = PAGES.mapBy(p => 1.to(wal.length()).filter(i => wal[i - 1].pg == p and (MAP_ALL or i > back)).fold(0, (a, i) => if (i > a) i else a)),
    sNext' = 0,
    sGot' = PAGES.mapBy(p => -2),
    hist' = hist, keepWal,
    held' = held,
    phase' = phase,
  }

  // Reads the next page, from its frame if the map has one and from the database file if not.
  action snapRead = {
    val f = sMap.get(sNext)
    val v = if (f > 0) (if (sGen == gen and f <= wal.length() and wal[f - 1].pg == sNext) wal[f - 1].sq else -1) else db.get(sNext)
    all {
      sPhase == 1,
      sGot' = sGot.set(sNext, v),
      sNext' = sNext + 1,
      sPhase' = if (sNext + 1 == NPAGES) 2 else 1,
      sEnd' = sEnd, sGen' = sGen, sMap' = sMap,
      hist' = hist, keepWal, keepJournal,
      held' = held,
      phase' = phase,
    }
  }

  action snapClose = all {
    sPhase == 2,
    sPhase' = 0,
    sEnd' = sEnd, sGen' = sGen, sMap' = sMap, sNext' = sNext, sGot' = sGot,
    hist' = hist, keepWal, keepJournal,
    held' = held,
    phase' = phase,
  }

  // The node lets go: its transaction ends.
  action releaseStart = all {
    phase == 0,
    sPhase == 0,
    held' = false,
    phase' = 1,
    hist' = hist, keepWal, keepJournal, keepSnap,
  }

  // The release is refused. Under the write lock, so atomic, capture checks the generation and the length. Either it
  // syncs what is left in the WAL, or it writes every page as one file. Then it finishes as the locked restart does.
  action resume = {
    val same = (capGen == gen or NO_SALTS) and wal.length() >= capFrames
    all {
      phase == 1,
      journal' = if (same) syncJournal else truthAt(hist, hist.length()),
      capSeq' = if (same) syncSeq else hist.length(),
      capGen' = gen,
      capFrames' = wal.length(),
      db' = copiedAll,
      back' = wal.length(),
      held' = true,
      wal' = wal, gen' = gen,
      phase' = 0,
      hist' = hist, keepSnap,
    }
  }

  action step = any {
    nondet pg = oneOf(PAGES)
    commit(pg),
    nondet t = oneOf(Set(true, false))
    checkpoint(t),
    sync,
    lockedRestart,
    gapStart,
    gapFinish,
    snapStart,
    snapRead,
    snapClose,
    releaseStart,
    resume,
  }

  // Every commit not captured is still a frame in the WAL. (Not while the shard is released: the app may checkpoint
  // and restart then, and the resumed release makes up for it with the whole database.)
  val noLoss: bool = phase == 1 or wal.select(f => f.sq > capSeq).length() == hist.length() - capSeq

  // The captured files replayed are the database after the last captured commit.
  val replay: bool = journal == truthAt(hist, capSeq)

  // A finished snapshot is the database as of its captured end.
  val snapOk: bool = sPhase != 2 or sGot == truthAt(hist, sEnd)

  val safe: bool = noLoss and replay and snapOk

  // Reachability check, which should fail.
  val neverRestarts: bool = gen == 0
}

module captureProtocol {
  import capture(MAX_COMMITS = 5, LOCK_NO_SYNC = false, LOCK_GAP = false, NO_SALTS = false, MAP_ALL = false).*
}

module lockNoSync {
  import capture(MAX_COMMITS = 5, LOCK_NO_SYNC = true, LOCK_GAP = false, NO_SALTS = false, MAP_ALL = false).*
}

module lockGap {
  import capture(MAX_COMMITS = 5, LOCK_NO_SYNC = false, LOCK_GAP = true, NO_SALTS = false, MAP_ALL = false).*
}

module noSalts {
  import capture(MAX_COMMITS = 5, LOCK_NO_SYNC = false, LOCK_GAP = false, NO_SALTS = true, MAP_ALL = false).*
}

module mapAll {
  import capture(MAX_COMMITS = 5, LOCK_NO_SYNC = false, LOCK_GAP = false, NO_SALTS = false, MAP_ALL = true).*
}
