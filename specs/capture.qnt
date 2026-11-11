// How the node captures one shard's commits by reading SQLite's WAL itself, and why a checkpoint or a WAL restart
// can't drop a frame before it is captured.
//
// SQLite is the environment. The WAL has frames 1..mxFrame (every frame is a commit here), and `back` (nBackfill) of
// them are copied into the database file. Two of SQLite's rules are taken as given, not derived:
//   - The WAL restarts (a new generation, standing for new salts, with no frames and nothing copied) only when every
//     frame is copied into the database file. The app's writer restarts it at the start of a write whenever that
//     holds, and a TRUNCATE checkpoint does when its copy makes it hold.
//   - A checkpoint copies a frame only past every reader's mark. The node's read transaction is on mark 0, so while
//     it is held a checkpoint copies nothing (and a TRUNCATE one answers busy), and otherwise it copies the whole WAL.
// A reader at mark 0 doesn't stop a restart, so the first rule protects nothing that isn't captured yet: that the
// restart can't take an uncaptured frame is the node's side to make true, and the model's to check.
//
// The node's side:
//   - It holds a read transaction on mark 0 whenever the shard is captured. While it does, nothing is copied into the
//     database file and the WAL can't restart once a frame is waiting to be copied, so the WAL only grows.
//   - A sync reads the frames after the ones it has read in the same generation, or all of a new generation (the
//     salts changed), and adds them to the journal. It never waits for anything.
//   - When the WAL is large (more than RESTART_AT frames, with something to copy and no snapshot reading) a sync takes
//     the write lock, syncs, ends its transaction, runs a PASSIVE checkpoint that copies everything, begins a new
//     transaction (which gets mark 0) and lets go (`lockedRestart`). The app's writes wait for that, and its next
//     write restarts the WAL. A TRUNCATE checkpoint can't be used: it takes the write lock itself. So a sync of a
//     large WAL is a locked restart, and `sync` is the sync of one that is small or that a snapshot is reading.
//   - A snapshot syncs, notes the captured end and builds a page map of the frames in the WAL that the database file
//     doesn't hold. Then it reads one page at a time, while the app goes on, from the WAL or from the database file. The
//     locked restart waits for it.
//   - A release syncs, then ends the node's transaction. A refused release reopens, takes the write lock, and checks the generation.
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
// Left out: the app's own readers (they would only lower what a checkpoint copies and when the WAL restarts), a
// checkpoint that stops short (it only delays a restart), multi-page commits (a header is only ever written for a
// whole commit), page contents beyond which commit wrote them, the corrupt frame and the torn tail (the WAL reader's,
// and tested there), the locked restart's threshold (here any frames at all) and how the node wins the write lock
// against a busy writer, and liveness.
//
// Traces of this model are replayed against the capture code (docs/internals.md, The models): the reachability checks
// below end them, and internal/capture/testdata/traces/generate.sh makes them.
//
// Simulation is the check at depth: `quint verify` holds `safe` to 8 steps in about 8 minutes, and the ranges are
// bounded by MAX_COMMITS because Apalache needs constant ones. specs/check.sh runs this model's checks, and says which
// must fail (every variant, and `neverRestarts`, because a restart is reachable).

module capture {
  // At most this many commits.
  const MAX_COMMITS: int

  // A sync restarts the WAL when it has more than this many frames (capture.Config.RestartFrames).
  const RESTART_AT: int

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
  var gen: int              // the generation (the salts): 0 until the WAL is first written, then one more at each restart
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
    PAGES.mapBy(p => 1.to(MAX_COMMITS).filter(i => i <= s and h[i - 1] == p).fold(0, (a, i) => if (i > a) i else a))

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

  // The app commits one page. Its writer restarts the WAL first if every frame is copied. A WAL nobody has written has
  // no salts (the wal-index holds zeros), so the first commit gives it its own, which capture counts as another WAL.
  action commit(pg: int): bool = {
    val restart = wal.length() > 0 and back == wal.length()
    all {
      hist.length() < MAX_COMMITS,
      hist' = hist.append(pg),
      wal' = (if (restart) List() else wal).append({ pg: pg, sq: hist.length() + 1 }),
      gen' = if (restart or hist.length() == 0) gen + 1 else gen,
      back' = if (restart) 0 else back,
      db' = db,
      held' = held, keepJournal, keepSnap,
      phase' = phase,
    }
  }

  // An app's checkpoint. It copies nothing while the node holds mark 0 and the whole WAL otherwise. TRUNCATE restarts
  // the WAL when every frame is copied.
  action checkpoint(trunc: bool): bool = {
    val nb = if (held) back else wal.length()
    val done = trunc and nb == wal.length()
    all {
      wal.length() > 0,
      db' = wal.slice(back, nb).foldl(db, (d, f) => d.set(f.pg, f.sq)),
      wal' = if (done) List() else wal,
      gen' = if (done) gen + 1 else gen,
      back' = if (done) 0 else nb,
      hist' = hist,
      held' = held, keepJournal, keepSnap,
      phase' = phase,
    }
  }

  // The sync of a large WAL is the locked restart: it has frames to copy, more than the threshold, and no snapshot is
  // reading (which needs the database file as it is).
  def large: bool = sPhase == 0 and wal.length() > RESTART_AT and back < wal.length()

  // The node reads the frames it hasn't.
  action sync = all {
    phase == 0,
    not(large),
    journal' = syncJournal,
    capSeq' = syncSeq,
    capGen' = gen,
    capFrames' = wal.length(),
    hist' = hist, keepWal, keepSnap,
    held' = held,
    phase' = phase,
  }

  // The WAL is large. Under the write lock, so atomic, the node syncs, ends its transaction,
  // runs a PASSIVE checkpoint that copies everything and begins a new transaction, which gets mark 0. It waits for a
  // snapshot to finish, as the snapshot needs the database file as it is. The WAL restarts at the app's next write.
  action lockedRestart = all {
    not(LOCK_GAP),
    phase == 0,
    large,
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
    large,
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
    sMap' = PAGES.mapBy(p => 1.to(MAX_COMMITS).filter(i => i <= wal.length() and wal[i - 1].pg == p and (MAP_ALL or i > back)).fold(0, (a, i) => if (i > a) i else a)),
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

  // The node lets go: a final sync, then its transaction ends.
  action releaseStart = all {
    phase == 0,
    sPhase == 0,
    journal' = syncJournal,
    capSeq' = syncSeq,
    capGen' = gen,
    capFrames' = wal.length(),
    held' = false,
    phase' = 1,
    hist' = hist, keepWal, keepSnap,
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

  // Reachability checks, which should fail. Six of them end a path where the code is replayed along it (see
  // internal/capture/testdata/traces): a commit and a release, which is two steps; a WAL of three frames all copied
  // (a locked restart, or a resume after one); four commits captured with nothing open, which a random walk reaches
  // by many paths; a snapshot finished after a WAL restart; a snapshot half read after a commit (the path ends with
  // it open); and a release waiting after the app has committed (the path ends before the resume).
  val neverRestarts: bool = gen <= 1
  val neverReleasedAfterOneCommit: bool = not(phase == 1 and capSeq == 1 and back == 0)
  val neverThreeFramesCopied: bool = not(phase == 0 and back >= 3)
  val neverFourCapturedAtRest: bool = not(capSeq >= 4 and phase == 0 and sPhase == 0)
  val neverSnapshotAfterRestart: bool = not(sPhase == 2 and gen >= 1)
  val neverSnapshotHalfRead: bool = not(sPhase == 1 and sNext >= 1 and hist.length() >= 1)
  val neverReleaseWaitingAfterCommit: bool = not(phase == 1 and hist.length() > capSeq)
}

module captureProtocol {
  import capture(MAX_COMMITS = 5, RESTART_AT = 2, LOCK_NO_SYNC = false, LOCK_GAP = false, NO_SALTS = false, MAP_ALL = false).*
}

module lockNoSync {
  import capture(MAX_COMMITS = 5, RESTART_AT = 2, LOCK_NO_SYNC = true, LOCK_GAP = false, NO_SALTS = false, MAP_ALL = false).*
}

module lockGap {
  import capture(MAX_COMMITS = 5, RESTART_AT = 2, LOCK_NO_SYNC = false, LOCK_GAP = true, NO_SALTS = false, MAP_ALL = false).*
}

module noSalts {
  import capture(MAX_COMMITS = 5, RESTART_AT = 2, LOCK_NO_SYNC = false, LOCK_GAP = false, NO_SALTS = true, MAP_ALL = false).*
}

module mapAll {
  import capture(MAX_COMMITS = 5, RESTART_AT = 2, LOCK_NO_SYNC = false, LOCK_GAP = false, NO_SALTS = false, MAP_ALL = true).*
}
