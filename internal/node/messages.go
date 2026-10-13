package node

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/luci1900/shardlite/internal/capture"
)

// Messages (docs/guide.md, Messages): the outbox holds calls to other shards, which
// the node moves into the target's inbox once the commit that inserted them is
// durable. The inbox holds every call the shard's handlers receive, now or
// later, which the node delivers when they are due and the handler consumes.

// reservedSchema is run at every activation, through Litestream's connection,
// before the app gets the shard. For a shard that has the tables already it
// commits nothing.
const reservedSchema = `
CREATE TABLE IF NOT EXISTS _shardlite_outbox (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  target       TEXT NOT NULL,
  path         TEXT NOT NULL,
  content_type TEXT,
  body         BLOB,
  hops         INTEGER NOT NULL DEFAULT 0,
  created_at   INTEGER NOT NULL DEFAULT (unixepoch()),
  error        TEXT
);
CREATE TABLE IF NOT EXISTS _shardlite_inbox (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  key          TEXT UNIQUE,
  sender       TEXT,
  path         TEXT NOT NULL,
  content_type TEXT,
  body         BLOB,
  hops         INTEGER NOT NULL DEFAULT 0,
  due          INTEGER NOT NULL DEFAULT (unixepoch())
);
CREATE INDEX IF NOT EXISTS _shardlite_inbox_due ON _shardlite_inbox (due);
`

const (
	// pendingRowsLimit is how many rows of one shard the node holds in memory at
	// once (in flight, waiting to be retried or waiting to be deleted). More wait
	// in the table.
	pendingRowsLimit = 1000
	// pendingBytesLimit bounds the bodies of the rows one read loads.
	pendingBytesLimit = 32 << 20
	commitBatch       = 500
	commitLinger      = 5 * time.Millisecond
	maxBackoffShift   = 6
	holdWarnAfter     = time.Minute
)

// outRow is a row of _shardlite_outbox, as the node sends it.
type outRow struct {
	id          int64
	target      string
	path        string
	contentType string
	body        []byte
	hops        int
	createdAt   int64
}

// inRow is a row of _shardlite_inbox, as the node delivers it.
type inRow struct {
	id          int64
	key         string
	sender      string // "" for the shard's own rows
	path        string
	contentType string
	body        []byte
	hops        int
	due         int64
}

// rowKey names a row for the node's bookkeeping. An inbox row is its id and its
// due time: the app re-arming it gives it a new identity.
type rowKey struct {
	inbox bool
	id    int64
	due   int64
}

func (r *outRow) rowKey() rowKey { return rowKey{id: r.id} }
func (r *inRow) rowKey() rowKey  { return rowKey{inbox: true, id: r.id, due: r.due} }

// snapshot is what one read of a shard found.
type snapshot struct {
	out         []*outRow
	in          []*inRow
	earliest    int64 // the earliest due time of the inbox rows not given up on
	hasEarliest bool
	next        int64 // the earliest due time after the read (the timer's)
	hasNext     bool
}

// transport is how the messenger reaches other shards, set by the server.
type transport struct {
	// intake sends an outbox row to target's node, which inserts it into its
	// inbox and answers once that commit is durable. status is the answer (0 if
	// there was none), and err says why not.
	intake func(ctx context.Context, sender *ActiveShard, target string, row *outRow) (status int, err error)
	// inbox calls the handler of a shard for one of its own inbox rows.
	inbox   func(ctx context.Context, a *ActiveShard, row *inRow) (status int, err error)
	maxHops int
}

// messenger moves the rows of all of a manager's shards.
type messenger struct {
	m    *Manager
	send atomic.Pointer[transport] // set by the server; until then nothing is read or delivered
	node chan struct{}             // the node-wide limit on requests in flight

	mu   sync.Mutex
	recv map[string]*receiver
}

// receiver is the limit on requests in flight to one shard.
type receiver struct {
	sem chan struct{}
	n   int // senders holding or waiting for it, guarded by messenger.mu
}

func newMessenger(m *Manager) *messenger {
	return &messenger{m: m, node: make(chan struct{}, m.opts.MessageNodeLimit), recv: map[string]*receiver{}}
}

func (mg *messenger) setTransport(t *transport) { mg.send.Store(t) }

func (mg *messenger) enabled() bool { return mg.send.Load() != nil }

// spawn runs f in the manager's group, unless the manager is closing.
func (mg *messenger) spawn(f func()) {
	mg.mu.Lock()
	defer mg.mu.Unlock()
	if mg.m.ctx.Err() != nil {
		return
	}
	mg.m.g.Go(func() error { f(); return nil })
}

// acquire takes a place for one request to target: the receiver's limit, then the node's.
func (mg *messenger) acquire(ctx context.Context, target string) (release func(), err error) {
	mg.mu.Lock()
	r := mg.recv[target]
	if r == nil {
		r = &receiver{sem: make(chan struct{}, mg.m.opts.MessageReceiverLimit)}
		mg.recv[target] = r
	}
	r.n++
	mg.mu.Unlock()
	forget := func() {
		mg.mu.Lock()
		if r.n--; r.n == 0 {
			delete(mg.recv, target)
		}
		mg.mu.Unlock()
	}
	select {
	case r.sem <- struct{}{}:
	case <-ctx.Done():
		forget()
		return nil, context.Cause(ctx)
	}
	select {
	case mg.node <- struct{}{}:
	case <-ctx.Done():
		<-r.sem
		forget()
		return nil, context.Cause(ctx)
	}
	return func() { <-mg.node; <-r.sem; forget() }, nil
}

// backoff is the wait before attempt n+1 (n counts from 0): doubling from the
// base to the cap, with jitter.
func (mg *messenger) backoff(n int) time.Duration {
	d := min(mg.m.opts.MessageBackoff<<min(n, maxBackoffShift), mg.m.opts.MessageMaxBackoff)
	return d/2 + rand.N(d/2+1)
}

// shardMsgs is the delivery state of one active shard.
type shardMsgs struct {
	a      *ActiveShard
	ctx    context.Context // ends when the shard is let go of or the manager closes
	cancel context.CancelFunc

	mu         sync.Mutex
	known      map[rowKey]*rowState // rows read and not yet gone from a later read
	givenUp    map[rowKey]bool      // rows the node has given up on while the shard stays active
	epoch      uint64               // counts commits of deletes, and rows seen to be consumed
	more       bool                 // rows may be waiting that the last read did not take, or none was made yet
	inboxDue   bool                 // the inbox timer fired: rows are due
	queue      []outWrite           // outbox rows answered or given up on, waiting for their write
	checks     []*check             // inbox rows waiting to be looked up in the next snapshot
	committing bool

	timer       func() bool // stops the inbox timer
	read1       bool        // a read has been delivered (earliest is meaningful)
	earliest    int64
	hasEarliest bool
	hintDirty   bool // a hint write or delete failed

	hintMu     sync.Mutex      // serialises everything that writes or deletes this shard's hints
	hints      map[string]bool // wake hints that exist for this shard (the ones that woke it and the ones it wrote)
	recRunning bool
	recAgain   bool

	held   time.Time // when the janitor first found live rows holding the shard (only the janitor uses these)
	warned time.Time
	wait   time.Duration
}

// rowState is what the node remembers about a row it has read.
type rowState struct {
	doneEpoch uint64 // the epoch at which the row was deleted or consumed, 0 until then
}

// check is a question to the next snapshot: is this inbox row still in the
// table with the due time that was delivered?
type check struct {
	row rowKey
	res chan bool
}

func newShardMsgs(m *Manager, a *ActiveShard) *shardMsgs {
	s := &shardMsgs{a: a, known: map[rowKey]*rowState{}, givenUp: map[rowKey]bool{}, more: true}
	s.ctx, s.cancel = context.WithCancel(m.ctx)
	return s
}

func (s *shardMsgs) wantsRead() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.more || s.inboxDue
}

// given copies the set of rows given up on.
func (s *shardMsgs) given() map[rowKey]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := make(map[rowKey]bool, len(s.givenUp))
	for k := range s.givenUp {
		g[k] = true
	}
	return g
}

// ensureMessages creates the reserved tables and indexes through Litestream's
// connection. A shard that has them commits nothing; otherwise the commit is
// captured at once, so the shard's final sync has nothing new to find (a shard
// the app never wrote to is still deleted as never written).
func (m *Manager) ensureMessages(ctx context.Context, sh *capture.Shard) error {
	before, herr := sh.Header()
	if err := sh.Use(ctx, func(db *sql.DB) error {
		_, err := db.ExecContext(ctx, reservedSchema)
		return err
	}); err != nil {
		return fmt.Errorf("creating the outbox and inbox: %w", err)
	}
	if after, err := sh.Header(); herr != nil || err != nil || after != before {
		return sh.Capture(ctx)
	}
	return nil
}

// useDB runs fn on the shard's connection, which is Litestream's.
func (a *ActiveShard) useDB(ctx context.Context, fn func(*sql.DB) error) error {
	sh := a.cap.Load()
	if sh == nil {
		return capture.ErrClosed
	}
	return sh.Use(ctx, fn)
}

// querier is what a *sql.DB and a *sql.Tx share.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// firstOutbox says whether the outbox has a row the node has not given up on.
func firstOutbox(ctx context.Context, q querier, given map[rowKey]bool) (bool, error) {
	rows, err := q.QueryContext(ctx, `SELECT id FROM _shardlite_outbox ORDER BY id LIMIT ?`, len(given)+1)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return false, err
		}
		if !given[rowKey{id: id}] {
			return true, nil
		}
	}
	return false, rows.Err()
}

// firstInbox finds the earliest row of the inbox that matches where and that
// the node has not given up on (given is a copy of that set).
func firstInbox(ctx context.Context, q querier, given map[rowKey]bool, where string, args ...any) (due int64, ok bool, err error) {
	rows, err := q.QueryContext(ctx, `SELECT id, due FROM _shardlite_inbox `+where+` ORDER BY due, id LIMIT ?`, append(args, len(given)+1)...)
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, d int64
		if err := rows.Scan(&id, &d); err != nil {
			return 0, false, err
		}
		if !given[rowKey{inbox: true, id: id, due: d}] {
			return d, true, nil
		}
	}
	return 0, false, rows.Err()
}

// read returns the shard's live rows that the node has not read before, from one
// snapshot of the database: outbox rows, and inbox rows that are due. It also
// finds the shard's earliest inbox row. For a captured shard the caller reads
// before it starts a capture: everything the snapshot shows was committed
// before the capture began, so it is durable once that capture's flush has
// completed. A nil snapshot means the read failed.
func (s *shardMsgs) read(ctx context.Context) (*snapshot, error) {
	s.mu.Lock()
	e0 := s.epoch
	s.inboxDue = false
	// A row whose delete or consumption was seen before this read began
	// is not in its snapshot any more.
	for id, st := range s.known {
		if st.doneEpoch != 0 && st.doneEpoch <= e0 {
			delete(s.known, id)
		}
	}
	nKnown := len(s.known)
	checks := s.checks
	s.checks = nil
	s.more = false // a check or a row given up on during the read sets it again, and must not be lost at its end
	s.mu.Unlock()
	given := s.given()
	// Full: the rows stay in the table (and more is set), but the checks and the
	// earliest inbox row still need the snapshot.
	want := max(pendingRowsLimit-nKnown, 0)
	present := make([]bool, len(checks))

	snap := &snapshot{}
	more := false
	now := s.a.m.now().Unix()
	bytes := 0
	skip := func(k rowKey) bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		_, ok := s.known[k]
		return ok || s.givenUp[k]
	}
	err := s.a.useDB(ctx, func(db *sql.DB) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// The outbox.
		idRows, err := tx.QueryContext(ctx, `SELECT id FROM _shardlite_outbox ORDER BY id LIMIT ?`, nKnown+len(given)+want+1)
		if err != nil {
			return err
		}
		var ids []int64
		for idRows.Next() {
			var id int64
			if err := idRows.Scan(&id); err != nil {
				idRows.Close()
				return err
			}
			ids = append(ids, id)
		}
		if err := idRows.Err(); err != nil {
			return err
		}
		idRows.Close()
		var fresh []int64
		for _, id := range ids {
			if !skip(rowKey{id: id}) {
				fresh = append(fresh, id)
			}
		}
		if len(fresh) > want {
			fresh, more = fresh[:want], true
		}
		for _, id := range fresh {
			if bytes >= pendingBytesLimit {
				more = true
				break
			}
			row := &outRow{id: id}
			var ct sql.NullString
			err := tx.QueryRowContext(ctx, `SELECT target, path, content_type, body, hops, created_at FROM _shardlite_outbox WHERE id = ?`, id).
				Scan(&row.target, &row.path, &ct, &row.body, &row.hops, &row.createdAt)
			if err != nil {
				return err
			}
			row.contentType = ct.String
			bytes += len(row.body)
			snap.out = append(snap.out, row)
		}

		// The inbox: the questions, the earliest, and the rows that are due.
		for i, c := range checks {
			var one int
			err := tx.QueryRowContext(ctx, `SELECT 1 FROM _shardlite_inbox WHERE id = ? AND due = ?`, c.row.id, c.row.due).Scan(&one)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			present[i] = err == nil
		}
		earliest, ok, err := firstInbox(ctx, tx, given, ``)
		if err != nil {
			return err
		}
		snap.earliest, snap.hasEarliest = earliest, ok
		if ok && earliest <= now {
			room := max(want-len(snap.out), 0)
			dueRows, err := tx.QueryContext(ctx, `SELECT id, due FROM _shardlite_inbox WHERE due <= ? ORDER BY due, id LIMIT ?`, now, nKnown+len(given)+room+1)
			if err != nil {
				return err
			}
			var freshIn []rowKey
			for dueRows.Next() {
				var k rowKey
				k.inbox = true
				if err := dueRows.Scan(&k.id, &k.due); err != nil {
					dueRows.Close()
					return err
				}
				if !skip(k) {
					freshIn = append(freshIn, k)
				}
			}
			if err := dueRows.Err(); err != nil {
				return err
			}
			dueRows.Close()
			if len(freshIn) > room {
				freshIn, more = freshIn[:room], true
			}
			for _, k := range freshIn {
				if bytes >= pendingBytesLimit {
					more = true
					break
				}
				row := &inRow{id: k.id, due: k.due}
				var ct, key, sender sql.NullString
				err := tx.QueryRowContext(ctx, `SELECT key, sender, path, content_type, body, hops FROM _shardlite_inbox WHERE id = ? AND due = ?`, k.id, k.due).
					Scan(&key, &sender, &row.path, &ct, &row.body, &row.hops)
				if err != nil {
					return err
				}
				row.key, row.sender, row.contentType = key.String, sender.String, ct.String
				bytes += len(row.body)
				snap.in = append(snap.in, row)
			}
		}
		// The timer is for the earliest row that is not due yet.
		next, ok, err := firstInbox(ctx, tx, given, `WHERE due > ?`, now)
		if err != nil {
			return err
		}
		snap.next, snap.hasNext = next, ok
		return nil
	})
	if err != nil {
		s.mu.Lock()
		s.more = true
		s.checks = append(checks, s.checks...) // asked again by the next read
		s.mu.Unlock()
		return nil, err
	}
	for i, c := range checks {
		c.res <- present[i] // buffered
	}
	s.mu.Lock()
	for _, r := range snap.out {
		s.known[r.rowKey()] = &rowState{}
	}
	for _, r := range snap.in {
		s.known[r.rowKey()] = &rowState{}
	}
	s.more = s.more || more
	s.mu.Unlock()
	return snap, nil
}

// verify asks whether an inbox row that was delivered is still there with the
// same due time, as of the next snapshot (the next flush cycle's, taken before
// its capture). ok is false if the shard was let go of first.
func (s *shardMsgs) verify(row rowKey) (present, ok bool) {
	c := &check{row: row, res: make(chan bool, 1)}
	s.mu.Lock()
	s.checks = append(s.checks, c)
	s.more = true
	s.mu.Unlock()
	select {
	case v := <-c.res:
		return v, true
	case <-s.ctx.Done():
		return false, false
	}
}

// markGone notes that an inbox row has left the table for good (its handler
// consumed it), so a read that started before that does not deliver it again.
func (s *shardMsgs) markGone(row rowKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch++
	if st := s.known[row]; st != nil {
		st.doneEpoch = s.epoch
	}
}

// giveUp stops delivering an inbox row for as long as the shard stays active:
// it logs the error, and the row no longer holds the shard or counts towards
// its earliest time. The node never writes to the row.
func (mg *messenger) giveUp(s *shardMsgs, row *inRow, reason string) {
	mg.m.Stats.InboxGivenUp.Add(1)
	mg.m.log.Error("giving up on an inbox row: "+reason, "shard", s.a.Name, "id", row.id, "key", row.key, "sender", row.sender, "path", row.path, "due", row.due)
	s.mu.Lock()
	s.givenUp[row.rowKey()] = true
	delete(s.known, row.rowKey())
	s.more = true // the earliest time and the hints change
	s.mu.Unlock()
}

// afterRead is for a snapshot whose commits are durable now: it sets the timer for the
// earliest inbox row that is not due yet and notes the earliest one for the
// shard's wake hints.
func (s *shardMsgs) afterRead(snap *snapshot) {
	if snap == nil {
		return
	}
	s.armTimer(snap)
	s.mu.Lock()
	changed := !s.read1 || s.hasEarliest != snap.hasEarliest || s.earliest != snap.earliest || s.hintDirty
	s.read1, s.earliest, s.hasEarliest = true, snap.earliest, snap.hasEarliest
	s.mu.Unlock()
	if changed {
		s.scheduleReconcile()
	}
}

// armTimer replaces the shard's inbox timer: one per shard, for its earliest
// row that is not due yet. When it fires it only marks the shard, and the next
// flush cycle reads the due rows.
func (s *shardMsgs) armTimer(snap *snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.timer != nil {
		s.timer()
		s.timer = nil
	}
	if !snap.hasNext || s.ctx.Err() != nil {
		return
	}
	m := s.a.m
	d := max(time.Duration(snap.next-m.now().Unix())*time.Second, 0)
	s.timer = m.opts.Clock.AfterFunc(d, func() {
		s.mu.Lock()
		s.inboxDue = true
		s.mu.Unlock()
	})
}

func (s *shardMsgs) stopTimer() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.timer != nil {
		s.timer()
		s.timer = nil
	}
}

// start moves the outbox rows of a snapshot and delivers its inbox rows. Each
// row goes in a goroutine of its own, which ends when the row is acknowledged,
// consumed or given up on.
func (mg *messenger) start(s *shardMsgs, snap *snapshot) {
	if snap == nil {
		return
	}
	for _, row := range snap.out {
		mg.spawn(func() { mg.deliverOut(s, row) })
	}
	for _, row := range snap.in {
		mg.spawn(func() { mg.deliverInbox(s, row) })
	}
}

type outcome int

const (
	delivered outcome = iota
	retry
	gaveUp
)

// checkRow says why an outbox row cannot be sent, if it cannot: such a row is
// given up on and never sent.
func (mg *messenger) checkRow(row *outRow, maxHops int) error {
	m := mg.m
	if age := m.now().Sub(time.Unix(row.createdAt, 0)); age > m.opts.MessageMaxAge {
		return fmt.Errorf("older than %v", m.opts.MessageMaxAge)
	}
	if kind, _, err := ParseTarget(row.target); err != nil {
		return fmt.Errorf("invalid target %q", row.target)
	} else if _, err := m.opts.Kinds.Lookup(kind); err != nil {
		return fmt.Errorf("undeclared kind %q", kind)
	}
	if !strings.HasPrefix(row.path, "/") {
		return fmt.Errorf("path %q does not start with /", row.path)
	}
	if row.hops+1 > maxHops {
		return fmt.Errorf("hops past limit %d", maxHops)
	}
	return nil
}

// giveUpOut stops sending an outbox row for as long as the shard stays active:
// it logs the error, and the row no longer holds the shard or gets a wake hint.
// The node never writes to the row.
func (mg *messenger) giveUpOut(s *shardMsgs, row *outRow, reason error) {
	mg.m.Stats.MessagesGivenUp.Add(1)
	mg.m.log.Error("giving up on an outbox row: "+reason.Error(), "sender", s.a.Name, "target", row.target, "path", row.path, "id", row.id)
	s.mu.Lock()
	s.givenUp[row.rowKey()] = true
	delete(s.known, row.rowKey())
	s.more = true      // the hints change
	s.hintDirty = true // and a hint that only this row needed goes
	s.mu.Unlock()
	s.write(outWrite{id: row.id, reason: cutReason(reason.Error())})
}

// maxReasonBytes bounds what the node writes to an outbox row's error.
const maxReasonBytes = 200

// cutReason shortens a reason to about maxReasonBytes, at a character boundary.
func cutReason(s string) string {
	if len(s) <= maxReasonBytes {
		return s
	}
	n := maxReasonBytes
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// deliverOut moves an outbox row into its target's inbox until the intake is
// acknowledged, retrying with backoff.
func (mg *messenger) deliverOut(s *shardMsgs, row *outRow) {
	m := mg.m
	for attempt := 0; ; attempt++ {
		if s.ctx.Err() != nil {
			return
		}
		if tr := mg.send.Load(); tr != nil { // until the node serves, rows are only retried
			if err := mg.checkRow(row, tr.maxHops); err != nil {
				mg.giveUpOut(s, row, err)
				return
			}
		}
		out, err := mg.tryOut(s, row)
		switch out {
		case delivered:
			m.Stats.MessagesDelivered.Add(1)
			s.finish(row.id)
			return
		case gaveUp:
			mg.giveUpOut(s, row, err)
			return
		}
		m.Stats.MessagesRetried.Add(1)
		m.log.Warn("intake not delivered, will retry", "sender", s.a.Name, "target", row.target, "path", row.path, "id", row.id, "attempt", attempt+1, "err", err)
		wait, stop := m.opts.Clock.NewTimer(mg.backoff(attempt))
		select {
		case <-wait:
		case <-s.ctx.Done():
			stop()
			return
		}
	}
}

// tryOut makes one attempt at an outbox row.
func (mg *messenger) tryOut(s *shardMsgs, row *outRow) (outcome, error) {
	m := mg.m
	tr := mg.send.Load()
	if tr == nil {
		return retry, errors.New("the node is not serving yet")
	}
	release, err := mg.acquire(s.ctx, row.target)
	if err != nil {
		return retry, err
	}
	defer release()
	m.Stats.MessageAttempts.Add(1)
	status, err := tr.intake(s.ctx, s.a, row.target, row)
	o := classify(status, err)
	if status == http.StatusConflict {
		o = retry // the receiver's kind changed in between
	}
	if o != delivered && err == nil {
		err = fmt.Errorf("the receiver's node answered %d", status)
	}
	if o == gaveUp {
		err = receiverReason(status, row.target, err)
	}
	return o, err
}

// receiverReason says why a receiver refused an intake for good: its status, the
// target it was sent to, and the start of its answer.
func receiverReason(status int, target string, err error) error {
	body := err.Error()
	var ae *answerError
	if errors.As(err, &ae) {
		body = ae.body
	}
	if body == "" {
		return fmt.Errorf("%d from %s", status, target)
	}
	return fmt.Errorf("%d from %s: %s", status, target, cutReason(body))
}

// deliverInbox calls the handler for an inbox row until the row is consumed or
// given up on, retrying with backoff.
func (mg *messenger) deliverInbox(s *shardMsgs, row *inRow) {
	m := mg.m
	key := row.rowKey()
	for attempt := 0; ; attempt++ {
		if s.ctx.Err() != nil {
			return
		}
		if attempt > 0 {
			// The handler consumes a row (deletes it or sets a new time) in its own
			// transaction, and an earlier attempt that seemed to fail may have got
			// through. The next snapshot says: if the row is gone or changed, drop this.
			if present, ok := s.verify(key); !ok {
				return
			} else if !present {
				s.markGone(key)
				return
			}
		}
		out, err := mg.tryInbox(s, row)
		switch out {
		case delivered:
			m.Stats.InboxDelivered.Add(1)
			// The node never deletes an inbox row. The next snapshot shows whether the
			// handler consumed it.
			present, ok := s.verify(key)
			if !ok {
				return
			}
			if !present {
				s.markGone(key)
				return
			}
			mg.giveUp(s, row, "the handler answered 2xx but did not delete the row or change its due time")
			return
		case gaveUp:
			mg.giveUp(s, row, err.Error())
			return
		}
		if age := m.now().Sub(time.Unix(row.due, 0)); age > m.opts.MessageMaxAge {
			mg.giveUp(s, row, fmt.Sprintf("failing for more than %v past its due time: %v", m.opts.MessageMaxAge, err))
			return
		}
		m.Stats.MessagesRetried.Add(1)
		m.log.Warn("inbox row not delivered, will retry", "shard", s.a.Name, "id", row.id, "key", row.key, "path", row.path, "attempt", attempt+1, "err", err)
		wait, stop := m.opts.Clock.NewTimer(mg.backoff(attempt))
		select {
		case <-wait:
		case <-s.ctx.Done():
			stop()
			return
		}
	}
}

// tryInbox makes one attempt at an inbox row.
func (mg *messenger) tryInbox(s *shardMsgs, row *inRow) (outcome, error) {
	m := mg.m
	if !strings.HasPrefix(row.path, "/") {
		return gaveUp, fmt.Errorf("path %q does not start with /", row.path)
	}
	tr := mg.send.Load()
	if tr == nil {
		return retry, errors.New("the node is not serving yet")
	}
	// Under the same limits as everything else sent to the shard.
	release, err := mg.acquire(s.ctx, s.a.Name)
	if err != nil {
		return retry, err
	}
	defer release()
	m.Stats.MessageAttempts.Add(1)
	status, err := tr.inbox(s.ctx, s.a, row)
	o := classify(status, err)
	if o != delivered && err == nil {
		err = fmt.Errorf("the handler answered %d", status)
	}
	return o, err
}

// classify says what an answer means for the row: a 2xx is delivery; 429, 5xx
// (except 508, hops past the limit), timeouts and connection failures are
// retried; anything else is given up on.
func classify(status int, err error) outcome {
	switch {
	case status/100 == 2:
		return delivered
	case status == 0:
		return retry
	case status == http.StatusTooManyRequests:
		return retry
	case status == http.StatusLoopDetected:
		return gaveUp
	case status >= 500:
		return retry
	}
	return gaveUp
}

// outWrite is a change the node makes to an outbox row once it is done with it:
// the delete of an acknowledged row, or the error of one it gave up on.
type outWrite struct {
	id     int64
	reason string // "" for a delete
}

// finish queues an acknowledged outbox row for its delete.
func (s *shardMsgs) finish(id int64) {
	s.write(outWrite{id: id})
}

// write queues a change to an outbox row, and starts the commit loop if it isn't
// running.
func (s *shardMsgs) write(w outWrite) {
	mg := s.a.m.msgs
	s.mu.Lock()
	s.queue = append(s.queue, w)
	start := !s.committing
	s.committing = true
	s.mu.Unlock()
	if start {
		mg.spawn(func() { mg.commitLoop(s) })
	}
}

// commitLoop writes the node's changes to the sender's outbox: acknowledged
// rows are deleted, several to a commit. The writes go
// through Shard.Use, so they never run during the final sync of a deactivation.
func (mg *messenger) commitLoop(s *shardMsgs) {
	stop := func() {
		s.mu.Lock()
		s.committing = false
		s.mu.Unlock()
	}
	for {
		select {
		case <-time.After(commitLinger): // let more answers join the commit
		case <-s.ctx.Done():
			stop()
			return
		}
		s.mu.Lock()
		batch := s.queue
		s.queue = nil
		if len(batch) == 0 {
			s.committing = false
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
		err := s.a.useDB(s.ctx, func(db *sql.DB) error { return writeBatch(s.ctx, db, batch) })
		if err != nil {
			if s.ctx.Err() != nil || errors.Is(err, capture.ErrClosed) {
				stop() // the rows stay live and are sent again after the next activation
				return
			}
			mg.m.log.Warn("could not delete sent messages, will retry", "shard", s.a.Name, "err", err)
			s.mu.Lock()
			s.queue = append(batch, s.queue...)
			s.mu.Unlock()
			select {
			case <-time.After(200 * time.Millisecond):
			case <-s.ctx.Done():
				stop()
				return
			}
			continue
		}
		s.mu.Lock()
		s.epoch++
		for _, w := range batch {
			if st := s.known[rowKey{id: w.id}]; st != nil && w.reason == "" {
				st.doneEpoch = s.epoch
			}
		}
		s.mu.Unlock()
		s.scheduleReconcile()
	}
}

func writeBatch(ctx context.Context, db *sql.DB, batch []outWrite) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var deletes []int64
	for _, w := range batch {
		if w.reason != "" {
			if _, err := tx.ExecContext(ctx, `UPDATE _shardlite_outbox SET error = ? WHERE id = ?`, w.reason, w.id); err != nil {
				return err
			}
			continue
		}
		deletes = append(deletes, w.id)
	}
	for len(deletes) > 0 {
		chunk := deletes[:min(len(deletes), commitBatch)]
		deletes = deletes[len(chunk):]
		ids := make([]any, len(chunk))
		for i, id := range chunk {
			ids[i] = id
		}
		q := `DELETE FROM _shardlite_outbox WHERE id IN (?` + strings.Repeat(",?", len(ids)-1) + `)`
		if _, err := tx.ExecContext(ctx, q, ids...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// busy reports whether the shard has something to deliver now: an outbox row
// not given up on, or an inbox row that is due and not given up on. With later,
// an inbox row that is not due yet counts too (a shard that is deleted when it
// deactivates would lose it).
func (mg *messenger) busy(ctx context.Context, a *ActiveShard, later bool) (bool, error) {
	var live bool
	now := mg.m.now().Unix()
	given := a.msgs.given()
	err := a.useDB(ctx, func(db *sql.DB) error {
		var err error
		if live, err = firstOutbox(ctx, db, given); err != nil || live {
			return err
		}
		if later {
			_, live, err = firstInbox(ctx, db, given, ``)
		} else {
			_, live, err = firstInbox(ctx, db, given, `WHERE due <= ?`, now)
		}
		return err
	})
	return live, err
}

// holds reports whether the shard has something to deliver and so must not
// deactivate. It backs off the next look, and logs if a shard stays held for long.
func (mg *messenger) holds(ctx context.Context, a *ActiveShard) bool {
	if !mg.enabled() {
		return false
	}
	live, err := mg.busy(ctx, a, a.autoDeletes())
	s := a.msgs
	now := mg.m.now()
	if err != nil || !live {
		s.held, s.wait = time.Time{}, 0
		if err == nil {
			s.scheduleReconcile()
		}
		return false
	}
	if s.held.IsZero() {
		s.held, s.warned = now, now
	}
	s.wait = min(max(2*s.wait, mg.m.opts.Janitor), 10*time.Second)
	mg.m.mu.Lock()
	a.delay = now.Add(s.wait)
	mg.m.mu.Unlock()
	if now.Sub(s.held) >= holdWarnAfter && now.Sub(s.warned) >= holdWarnAfter {
		s.warned = now
		mg.m.log.Warn("a shard has had undelivered messages or inbox rows for a long time, keeping it active", "shard", a.Name, "for", now.Sub(s.held).Round(time.Second))
	}
	return true
}
