package node

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
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

// transport is how the messenger reaches other shards, given when Shards is
// built (by the server).
type transport struct {
	// intake sends an outbox row to target's node, which inserts it into its
	// inbox and answers once that commit is durable. status is the answer (0 if
	// there was none), and err says why not.
	intake func(ctx context.Context, sender *shard, target string, row *outRow) (status int, err error)
	// inbox calls the handler of a shard for one of its own inbox rows.
	inbox   func(ctx context.Context, a *shard, row *inRow) (status int, err error)
	maxHops int
}

// MessageOptions configure how messages are delivered (docs/guide.md, Messages).
type MessageOptions struct {
	Backoff       time.Duration // first wait before an outbox or inbox row is retried, 1 s if zero
	MaxBackoff    time.Duration // longest wait, 1 min if zero
	MaxAge        time.Duration // an outbox row older than this is given up on, and an inbox row failing this long past its due time is given up on, 24 h if zero
	ReceiverLimit int           // requests in flight to one receiver, 32 if zero
	NodeLimit     int           // requests in flight on the node, 256 if zero
}

// messenger moves the rows of all of the node's shards: the limits on requests
// in flight, the retries and the transport. The deliveries themselves belong to
// each shard's messages owner.
type messenger struct {
	opts  MessageOptions
	kinds *Kinds // the kinds a row may be sent to
	clock Clock
	log   *slog.Logger
	stats *Stats
	send  *transport
	node  chan struct{} // the node-wide limit on requests in flight

	intakes atomic.Uint64 // intakes inserted into an inbox, so that Drain notices a row moving between shards

	// mu guards recv and each receiver's n. It is a leaf: never held while
	// waiting on a semaphore.
	mu sync.Mutex
	// recv has a receiver for each shard that requests are being sent to. The
	// first sender adds it and the last to leave deletes it.
	recv map[string]*receiver
}

// receiver is the limit on requests in flight to one shard.
type receiver struct {
	sem chan struct{}
	n   int // senders holding or waiting for it, guarded by messenger.mu
}

func newMessenger(opts MessageOptions, kinds *Kinds, clock Clock, log *slog.Logger, stats *Stats, tr *transport) *messenger {
	return &messenger{opts: opts, kinds: kinds, clock: clock, log: log, stats: stats, send: tr, node: make(chan struct{}, opts.NodeLimit), recv: map[string]*receiver{}}
}

func (mg *messenger) now() time.Time { return mg.clock.Now() }

// acquire takes a place for one request to target: the receiver's limit, then the node's.
func (mg *messenger) acquire(ctx context.Context, target string) (release func(), err error) {
	mg.mu.Lock()
	r := mg.recv[target]
	if r == nil {
		r = &receiver{sem: make(chan struct{}, mg.opts.ReceiverLimit)}
		mg.recv[target] = r
	}
	r.n++
	mg.mu.Unlock()
	forget := func() {
		mg.mu.Lock()
		defer mg.mu.Unlock()
		if r.n--; r.n == 0 {
			delete(mg.recv, target)
		}
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
	d := min(mg.opts.Backoff<<min(n, maxBackoffShift), mg.opts.MaxBackoff)
	return d/2 + rand.N(d/2+1)
}

// ensureMessages creates the reserved tables and indexes through Litestream's
// connection. A shard that has them commits nothing; otherwise the commit is
// captured at once, so the shard's final sync has nothing new to find (a shard
// the app never wrote to is still deleted as never written).
func ensureMessages(ctx context.Context, sh *capture.Shard) error {
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
func (a *shard) useDB(ctx context.Context, fn func(*sql.DB) error) error {
	sh := a.cap.load()
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
// the node has not given up on (given).
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

type outcome int

const (
	delivered outcome = iota
	retry
	gaveUp
)

// checkRow says why an outbox row cannot be sent, if it cannot: such a row is
// given up on and never sent.
func (mg *messenger) checkRow(row *outRow, maxHops int) error {
	if age := mg.now().Sub(time.Unix(row.createdAt, 0)); age > mg.opts.MaxAge {
		return fmt.Errorf("older than %v", mg.opts.MaxAge)
	}
	if kind, _, err := ParseTarget(row.target); err != nil {
		return fmt.Errorf("invalid target %q", row.target)
	} else if _, err := mg.kinds.Lookup(kind); err != nil {
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
// The node writes the reason to the row's error and nothing else.
func (mg *messenger) giveUpOut(o *msgOwner, row *outRow, reason error) {
	mg.stats.MessagesGivenUp.Add(1)
	mg.log.Error("giving up on an outbox row: "+reason.Error(), "sender", o.a.Name, "target", row.target, "path", row.path, "id", row.id)
	o.send(o.dctx, func() {
		o.rows.giveUp(row.rowKey()) // the hints change
		o.k.hintDirty = true        // and a hint that only this row needed goes
		o.write(outWrite{id: row.id, reason: cutReason(reason.Error())})
	})
}

// giveUp stops delivering an inbox row for as long as the shard stays active:
// it logs the error, and the row no longer holds the shard or counts towards
// its earliest time. The node never writes to the row.
func (mg *messenger) giveUp(o *msgOwner, row *inRow, reason string) {
	mg.stats.InboxGivenUp.Add(1)
	mg.log.Error("giving up on an inbox row: "+reason, "shard", o.a.Name, "id", row.id, "key", row.key, "sender", row.sender, "path", row.path, "due", row.due)
	o.send(o.dctx, func() { o.rows.giveUp(row.rowKey()) })
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
func (mg *messenger) deliverOut(o *msgOwner, row *outRow) {
	ctx := o.dctx
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return
		}
		if err := mg.checkRow(row, mg.send.maxHops); err != nil {
			mg.giveUpOut(o, row, err)
			return
		}
		out, err := mg.tryOut(ctx, o, row)
		switch out {
		case delivered:
			mg.stats.MessagesDelivered.Add(1)
			o.send(ctx, func() { o.write(outWrite{id: row.id}) })
			return
		case gaveUp:
			mg.giveUpOut(o, row, err)
			return
		}
		mg.stats.MessagesRetried.Add(1)
		mg.log.Warn("intake not delivered, will retry", "sender", o.a.Name, "target", row.target, "path", row.path, "id", row.id, "attempt", attempt+1, "err", err)
		wait, stop := mg.clock.NewTimer(mg.backoff(attempt))
		select {
		case <-wait:
		case <-ctx.Done():
			stop()
			return
		}
	}
}

// tryOut makes one attempt at an outbox row.
func (mg *messenger) tryOut(ctx context.Context, o *msgOwner, row *outRow) (outcome, error) {
	release, err := mg.acquire(ctx, row.target)
	if err != nil {
		return retry, err
	}
	defer release()
	mg.stats.MessageAttempts.Add(1)
	status, err := mg.send.intake(ctx, o.a, row.target, row)
	out := classify(status, err)
	if status == http.StatusConflict {
		out = retry // the receiver's kind changed in between
	}
	if out != delivered && err == nil {
		err = fmt.Errorf("the receiver's node answered %d", status)
	}
	if out == gaveUp {
		err = receiverReason(status, row.target, err)
	}
	return out, err
}

// receiverReason says why a receiver refused an intake for good: its status, the
// target it was sent to, and the start of its answer.
func receiverReason(status int, target string, err error) error {
	body := err.Error()
	if ae, ok := errors.AsType[*answerError](err); ok {
		body = ae.body
	}
	if body == "" {
		return fmt.Errorf("%d from %s", status, target)
	}
	return fmt.Errorf("%d from %s: %s", status, target, cutReason(body))
}

// deliverInbox calls the handler for an inbox row until the row is consumed or
// given up on, retrying with backoff.
func (mg *messenger) deliverInbox(o *msgOwner, row *inRow) {
	ctx := o.dctx
	key := row.rowKey()
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return
		}
		if attempt > 0 {
			// The handler consumes a row (deletes it or sets a new time) in its own
			// transaction, and an earlier attempt that seemed to fail may have got
			// through. The next read says: if the row is gone or changed, drop this.
			if present, ok := o.verify(key); !ok || !present {
				return
			}
		}
		out, err := mg.tryInbox(ctx, o, row)
		switch out {
		case delivered:
			mg.stats.InboxDelivered.Add(1)
			// The node never deletes an inbox row. The next read shows whether the
			// handler consumed it.
			present, ok := o.verify(key)
			if !ok || !present {
				return
			}
			mg.giveUp(o, row, "the handler answered 2xx but did not delete the row or change its due time")
			return
		case gaveUp:
			mg.giveUp(o, row, err.Error())
			return
		}
		if age := mg.now().Sub(time.Unix(row.due, 0)); age > mg.opts.MaxAge {
			mg.giveUp(o, row, fmt.Sprintf("failing for more than %v past its due time: %v", mg.opts.MaxAge, err))
			return
		}
		mg.stats.MessagesRetried.Add(1)
		mg.log.Warn("inbox row not delivered, will retry", "shard", o.a.Name, "id", row.id, "key", row.key, "path", row.path, "attempt", attempt+1, "err", err)
		wait, stop := mg.clock.NewTimer(mg.backoff(attempt))
		select {
		case <-wait:
		case <-ctx.Done():
			stop()
			return
		}
	}
}

// tryInbox makes one attempt at an inbox row.
func (mg *messenger) tryInbox(ctx context.Context, o *msgOwner, row *inRow) (outcome, error) {
	if !strings.HasPrefix(row.path, "/") {
		return gaveUp, fmt.Errorf("path %q does not start with /", row.path)
	}
	// Under the same limits as everything else sent to the shard.
	release, err := mg.acquire(ctx, o.a.Name)
	if err != nil {
		return retry, err
	}
	defer release()
	mg.stats.MessageAttempts.Add(1)
	status, err := mg.send.inbox(ctx, o.a, row)
	out := classify(status, err)
	if out != delivered && err == nil {
		err = fmt.Errorf("the handler answered %d", status)
	}
	return out, err
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

// holds reports whether the shard has something to deliver and so must not
// deactivate. It backs off the next look (idleState.wait), and logs if a shard
// stays held for long. Only the shard's loop calls it.
func (a *shard) holds(ctx context.Context) bool {
	live, err := a.msgs.load().holds(ctx, a.autoDeletes())
	held, warn := a.idle.keptBy(err == nil && live, a.d.now(), a.d.opts.IdleRecheck)
	if warn > 0 {
		a.d.log.Warn("a shard has had undelivered messages or inbox rows for a long time, keeping it active", "shard", a.Name, "for", warn.Round(time.Second))
	}
	return held
}
