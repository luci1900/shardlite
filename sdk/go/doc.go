// Package shardlite is the Go SDK for apps served by shardlite. It covers the
// contract between an app and its node that is the same in every app: declaring
// the kinds, reading what the node says about a request, opening the shard's
// database for the length of the request, calling other shards, setting
// reminders and marking a shard for deletion. It imports only the standard
// library, and the app brings its own SQLite driver.
//
// The guide in docs/guide.md is the contract and this package is its Go
// spelling. Package shardlitetest starts a real node to test an app against.
//
// # The app's Node
//
// An app builds one Node from a Config and keeps it for its lifetime. The
// Config holds the opener (the app's driver), the address of the node's control
// API, the app's version and its kinds. Declare the kinds as values and keep
// them together in one file, since their diff is the declaration's diff. New
// fails at startup on a kind name the node would refuse, a kind named twice, an
// unknown Sync, a negative IdleTimeout or no opener:
//
//	var (
//		Users  = shardlite.Kind{Name: "users", Public: true, IdleTimeout: 10 * time.Minute}
//		Guilds = shardlite.Kind{Name: "guild"}
//	)
//
//	func openSQLite(uri string) (*sql.DB, error) {
//		return sql.Open("sqlite", uri+"&_txlock=immediate")
//	}
//
//	node, err := shardlite.New(shardlite.Config{
//		Open:    openSQLite,
//		Version: "2026.10.0",
//		Kinds:   []shardlite.Kind{Users, Guilds},
//		OnOpen:  migrate, // optional, after the pragmas
//	})
//
// A library exports a function that takes the kind's name, so the app picks it
// as the guide says (matchmaker.Queue("matchmaker.queue")). Node.Ready serves
// the declaration built from the version and the kinds, and answers not ready
// while Config.Health fails. The SDK checks the grammar of shard names and
// never whether a kind is declared. The node answers a call for an undeclared
// kind with a 404, which keeps an app whose kinds change while it runs working.
//
// # A handler
//
// The app wraps its handler in Node.Middleware, which gives each request a
// database that opens on first use. A route that belongs to a shard works out
// which shard the request is for and passes its name to Expect. If that is the
// shard the node opened, Expect returns it. If not, it returns a *Refusal as
// the error, with a status and a message, which the app answers in its own
// format and protocol (the SDK writes no responses on the app's routes):
//
//	func (a *app) visit(w http.ResponseWriter, r *http.Request) {
//		opened, err := a.node.Expect(r, Users.Shard(r.PathValue("id")))
//		if err != nil {
//			var ref *shardlite.Refusal
//			errors.As(err, &ref)
//			http.Error(w, ref.Message, ref.Status)
//			return
//		}
//		db, err := opened.DB() // opened now, closed when the handler returns
//		...
//	}
//
//	mux.Handle("GET "+shardlite.ReadyPath, node.Ready())
//	mux.HandleFunc("POST /users/{id}/visit", a.visit)
//	http.ListenAndServe(addr, node.Middleware(mux))
//
// A database comes only from Expect or ExpectCall, so a handler cannot use a
// shard's file without checking what the request is for. ExpectCall is for a
// route that only takes calls and for the wake at RemindersPath: it takes the
// shard from what the node opened and refuses a client's request with a 403.
// Request is for a route that belongs to no shard (a login, a webhook): it can
// call and read the caller, and has no database.
//
// The Opener is the app's: it names the driver and sets its options (immediate
// transactions, as the guide recommends). Opened.DB gives it
// "file:{path}?mode=rw", so the open never creates a file. The database has one
// connection, which runs the pragmas (WAL mode and a busy timeout) and then
// Config.OnOpen, the place for a migration, before the handler sees it. The
// connection closes when the handler returns, so nothing is kept between
// requests and the app has no pool to size or to close in time for the kind's
// idle timeout. A stream or a long-lived response keeps its connection for as
// long as its handler runs. A transaction holds the connection, so run its
// statements on the transaction.
//
// Opening costs 45 to 100 µs a request on the drivers measured, against a few
// for a pooled connection, about what a local proxy hop costs. Never keep a
// connection, a prepared statement or a transaction in a field, a global or a
// goroutine that outlives the request: a connection kept open pins its shard on
// the node, and a commit through one that is open when the node stops is lost.
//
// The caller of a request is a Caller: *CallerClient for a client, *CallerApp
// for a call from the app and *CallerShard for a call from a shard. A route
// that grants access by caller switches over it, and the exhaustive linter
// (with typeswitch checking) reports a case left out.
//
// # A call
//
// A call to another shard goes through the node's control API and is made from
// the scope of the request being served, so the node knows the caller and how
// long the chain of calls is. Opened.Call and Request.Call send at once with
// the request's context, Shardlite-Context and traceparent, and the call ends
// with the request, so a client that gives up cancels it:
//
//	res, err := opened.Call(Guilds.Shard(id), http.MethodPost, "/join", body,
//		shardlite.IfExists()) // the id came from a client
//
// The options are IfActive and IfExists, which answer 404 rather than activate
// or create the target, WithSync, which overrides the target kind's durability
// tier for the call, and Header, which sets a header of the app's own protocol
// (Idempotency-Key, Content-Type) and refuses any Shardlite-* name. A target
// that is the zero ShardName or has an id outside the grammar is an error
// before anything is sent. ParseShard reads a name the app kept (the target of
// a durable send) and checks the grammar only.
//
// A call made while serving a request is made on behalf of that request's
// shard, whose commits it waits for before it leaves the node. Make it while
// serving, since one made after the handler returned is not gated. A call with
// no request behind it (at startup, from a worker or a timer) is
// Node.CallAsApp, which takes a context, sends none of the request's and is the
// one call that may upgrade to a WebSocket. Timeouts are the app's: give
// Config.HTTP an http.Client with one, or the request or context a deadline.
//
// A callee reads the caller with Caller and takes its own shard from
// ExpectCall or Expect.
//
// # An error of the node's
//
// A call's response is the callee's own answer unless the node refused the call
// or could not carry it, and then it carries Shardlite-Error. NodeError tells
// the two apart and reads the node's error as data, so the app switches on the
// reason and does not parse text. It returns false for the app's answer, which
// the app reads as it would any response:
//
//	res, err := opened.Call(Guilds.Shard(id), http.MethodPost, "/join", body,
//		shardlite.IfExists())
//	if err != nil {
//		... // the call did not reach the node
//	}
//	defer res.Body.Close()
//	if nerr, ok := shardlite.NodeError(res); ok {
//		if nerr.Reason == shardlite.ReasonUnknownShard {
//			... // no such guild
//		}
//		after, hinted := nerr.RetryAfter()
//		... // the node's own hint, when it gave one
//	}
//
// The Error has the Status, a Reason to switch on (ReasonOther for one a newer
// node sends, whose value RawReason keeps), the start of the body as Message
// and the node's Retry-After as RetryAfter. The SDK has no retry policy:
// whether, when and how often to retry is the app's, and the node's hint is the
// signal it has. Node.Sync, which waits until everything committed on the node
// is in blob storage (for side effects the node cannot see), returns the same
// *Error when the node refuses it, and errors.As finds it.
//
// A handler can make a response wait on the shard's commits whatever its
// kind's tier is: SetSync(w.Header(), SyncStorage) before the response is
// written. It acknowledges a write that must not be lost on a kind that does
// not wait for blob storage (SyncNone). SetSync sets nothing and returns an
// error for a tier that is neither SyncNone nor SyncStorage.
//
// gRPC calls are the app's own: the app sends the Shardlite-Context it was
// given as metadata under the key shardlite-context, and shardlite-target.
//
// # A reminder
//
// A reminder is a row in the shard's own _shardlite_reminders table.
// SetReminder and CancelReminder write it on a DBTX (the request's *sql.DB, or
// a transaction on it). Run them in the transaction that needs them, so the
// reminder and its data commit together. The node wakes the app at
// RemindersPath when the earliest is due, with the shard opened and named as
// the caller. The handler takes its shard from ExpectCall and uses DB:
//
//	mux.HandleFunc("POST "+shardlite.RemindersPath, a.wake)
//
//	err = shardlite.SetReminder(ctx, tx, "daily", time.Now().Add(24*time.Hour))
//
// The node ignores a reminder that is more than a day past due. SetReminder
// still writes it, so a mistake in the due time does not fail its transaction,
// and logs a warning with log/slog naming the key and the due time. A handler
// that repeats a reminder sets the next due time from now, not from the old due
// time, which can fall that far behind.
//
// A wake handler asks DueReminders which keys are due, does each one's work and
// its CancelReminder (or next SetReminder) in one transaction, and answers 2xx
// once everything due is done and 503 if a row could not be finished. The guide
// has the durable send, which makes a call from a wake so that it follows a
// commit.
//
// # Deleting the shard
//
// A shard that is finished (the match ended, the lobby emptied) deletes itself
// at its next deactivation: MarkDeleted writes the marker row in the same
// transaction as the work that decides it is finished. Until it deactivates the
// shard is served as usual and its due reminders are handled first. A handler
// that should refuse requests for a finished shard asks IsMarkedDeleted, and
// UnmarkDeleted takes the marker back so the shard is kept. All three take a
// DBTX, as the reminder functions do.
//
//	err = shardlite.MarkDeleted(r.Context(), tx)
//	gone, err := shardlite.IsMarkedDeleted(r.Context(), db)
//
// # A test
//
// Package shardlitetest runs the node itself for a test, on its in-memory store
// (SHARDLITE_BLOB=memory://: one node, with everything gone when it stops).
// Start takes the app's Config and a function that builds the app's handler
// from the app's Node, which it points at the node's control API. So a test
// sees what the app sees in production (the declaration, the hop limit, the
// gates, real wakes) and nothing stands in for it:
//
//	func TestVisit(t *testing.T) {
//		node := shardlitetest.Start(t, config(), newApp)
//		user := Users.Shard("visit-test-42") // an id of this test's own
//
//		// A client can reach a shard once it exists, which a call creates.
//		node.App.CallAsApp(t.Context(), user, http.MethodPost,
//			"/users/visit-test-42/visit", nil)
//
//		// A client's request: the public URL and Shardlite-Target.
//		req, _ := http.NewRequest(http.MethodPost,
//			node.URL+"/users/visit-test-42/visit", nil)
//		req.Header.Set(shardlite.HeaderTarget, user.String())
//		res, err := http.DefaultClient.Do(req)
//		// res.StatusCode, res.Body ...
//	}
//
// A call goes through node.App, the app's Node, and its callee's own route
// shows it arrived (its response, or what the route records). A wake is tested
// by setting a reminder that is due now and waiting, with a deadline of a few
// seconds, for the wake handler to run.
//
// The binary is the path in SHARDLITE_BIN, else shardlite on PATH. Build it
// with go build -o <dir>/shardlite ./cmd/shardlite in the shardlite repository.
// Without one Start fails the test and says so. A node takes a fraction of a
// second to start. Tests that share one node (started once in a parent test,
// say) give each test shard ids of its own, so state never leaks from one to
// the next: the node has no way to clear itself.
//
// The tests of this package, in guide_test.go, are this app and its tests in
// full.
package shardlite
