package node

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
)

// The intake (docs/internals.md, Message delivery): an outbox row goes to the node that has the
// target as the call it will become, marked as an intake. The receiver's node
// inserts it into the target's inbox in a commit of its own and answers 202
// once it is durable. The receiver's app is not involved.

// hdrInbox marks an intake between nodes (stage 2). Nothing an app or a client
// sends is believed: route strips it from every request, and the in-process
// intake is marked by its origin instead.
const hdrInbox = "Shardlite-Inbox"

// insertInbox puts a message into the shard's inbox as from sender, due now, in
// a transaction of its own. Intakes for one shard go one at a time because Use runs on the
// capture's loop.
func (a *shard) insertInbox(ctx context.Context, sender, path, contentType string, hops int, body []byte) error {
	due := a.d.now().Unix()
	err := a.useDB(ctx, func(db *sql.DB) error {
		_, err := db.ExecContext(ctx, `INSERT INTO _shardlite_inbox (sender, path, content_type, body, hops, due) VALUES (?, ?, ?, ?, ?, ?)`,
			sender, path, contentType, body, hops, due)
		return err
	})
	if err == nil {
		a.d.msgs.intakes.Add(1)
	}
	return err
}

// serveIntake is the receiver's node answering an intake for the shard ref
// holds: it inserts the row, waits for the commit to be durable and acknowledges with 202. info.hops is the
// row's hops plus one.
func (s *Server) serveIntake(w http.ResponseWriter, r *http.Request, info *reqInfo) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.fail(w, r, info.own, fmt.Errorf("intake: reading the body: %w", err))
		return
	}
	ct := cmp.Or(r.Header.Get("Content-Type"), defaultContentType)
	if err := info.ref.a.insertInbox(r.Context(), info.caller, r.URL.RequestURI(), ct, info.hops, body); err != nil {
		s.fail(w, r, info.own, fmt.Errorf("intake: inserting into the inbox: %w", err))
		return
	}
	if err := s.m.Gate(r.Context(), info.ref, true); err != nil {
		s.fail(w, r, info.own, fmt.Errorf("intake: %w", err))
		return
	}
	w.WriteHeader(http.StatusAccepted)
}
