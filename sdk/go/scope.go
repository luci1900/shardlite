package shardlite

import (
	"database/sql"
	"fmt"
	"io"
	"net/http"
)

// Caller is who made a request. It is one of *CallerClient, *CallerApp and
// *CallerShard, so a type switch over it covers each case (the linter checks
// that it does):
//
//	switch c := opened.Caller().(type) {
//	case *shardlite.CallerClient:
//	case *shardlite.CallerApp:
//	case *shardlite.CallerShard:
//		... c.Name
//	}
type Caller interface{ caller() }

// CallerClient is a request from a client: the node sets no Shardlite-Caller on
// it. Authenticate it as any web app does.
type CallerClient struct{}

// CallerApp is a call made by the app with no shard behind it (see
// Node.CallAsApp).
type CallerApp struct{}

// CallerShard is a call made by a shard while it served a request, or the
// node's wake of a shard for its reminders, which names the shard itself.
type CallerShard struct{ Name ShardName }

func (*CallerClient) caller() {}
func (*CallerApp) caller()    {}
func (*CallerShard) caller()  {}

// callerApp is the value of Shardlite-Caller for a call with no shard behind
// it.
const callerApp = "app"

// callerOf reads the caller the node set on a request. A value the node would
// never write is taken as a client's, the least trusted.
func callerOf(h http.Header) Caller {
	switch v := h.Get(headerCaller); v {
	case "":
		return &CallerClient{}
	case callerApp:
		return &CallerApp{}
	default:
		if name, err := ParseShard(v); err == nil {
			return &CallerShard{Name: name}
		}
		return &CallerClient{}
	}
}

// Request is a request the app is serving, seen as the node sent it. It can
// call other shards and read its caller, and has no database: a route that
// belongs to a shard gets an Opened from Expect or ExpectCall.
type Request struct {
	n *Node
	r *http.Request
}

// Request returns the scope of r for a route that belongs to no shard (a login,
// a webhook). It reads r's headers only, so it works on a request that did not
// go through Middleware.
func (n *Node) Request(r *http.Request) *Request { return &Request{n: n, r: r} }

// Caller returns who made the request.
func (q *Request) Caller() Caller { return callerOf(q.r.Header) }

// Opened returns the shard the node opened for the request, if any. It says
// what the node did, not what the request is for, and gives a name and never a
// database: it is for logging and metrics.
func (q *Request) Opened() (ShardName, bool) {
	name, _, ok := openedOf(q.r.Header)
	return name, ok
}

// Call calls the shard target through the node, from the request being served,
// and returns the response, which the app closes. It carries the request's
// Shardlite-Context and traceparent, so the node knows the caller and the
// length of the chain and holds the call for the commits of the shard that made
// it. The call ends with the request's context, so a client that gives up
// cancels it. Timeouts are the app's: set a deadline on the request's context
// or a Timeout on Config.HTTP.
//
// The path starts with a slash. A zero target or an id outside the grammar is
// an error before anything is sent. The options are IfExists, IfActive,
// WithSync and Header.
func (q *Request) Call(target ShardName, method, path string, body io.Reader, opts ...CallOption) (*http.Response, error) {
	return q.n.send(q.r.Context(), q.r, target, method, path, body, opts)
}

// Opened is the shard the node opened for a request, which the app has checked
// the request is for. Only Expect and ExpectCall return one. It is good for the
// length of the handler: the database closes when the handler returns.
type Opened struct {
	q    *Request
	name ShardName
	file string
	rdb  *requestDB
}

// Name returns the shard the node opened.
func (o *Opened) Name() ShardName { return o.name }

// Caller returns who made the request.
func (o *Opened) Caller() Caller { return o.q.Caller() }

// Call calls the shard target from the request being served, as Request.Call
// does.
func (o *Opened) Call(target ShardName, method, path string, body io.Reader, opts ...CallOption) (*http.Response, error) {
	return o.q.Call(target, method, path, body, opts...)
}

// DB returns the shard's database, which Middleware closes when the handler
// returns. The first call in a request opens the file with mode=rw, limits it
// to one connection, runs the pragmas on it and then Config.OnOpen. Later calls
// return the same database, from a goroutine the handler started too, until the
// handler returns.
//
// The database has one connection, so a transaction holds it: a statement run
// on the database while a transaction is open waits for the transaction to end.
// Run a transaction's statements on the transaction.
//
// A file that is not there (the path is no longer the shard's) fails the open.
// An error from the open or from OnOpen is the same for every call in the
// request.
func (o *Opened) DB() (*sql.DB, error) {
	return o.rdb.get(o.q.r.Context(), o.name, o.file)
}

// openedOf reads the shard the node opened and its file from a request's
// headers.
func openedOf(h http.Header) (name ShardName, file string, ok bool) {
	name, err := ParseShard(h.Get(headerOpened))
	file = h.Get(headerFile)
	return name, file, err == nil && file != ""
}

// Refusal is why a request is not for the shard the node opened. Expect and
// ExpectCall return one, as an error, in place of an Opened, and write nothing:
// the app answers in its own protocol and format, with the status the refusal
// carries. An app with a central error handler returns the error and finds the
// refusal there with errors.As. A plain net/http handler answers it in place:
//
//	var ref *shardlite.Refusal
//	if errors.As(err, &ref) {
//		http.Error(w, ref.Message, ref.Status)
//	}
//
// A refusal is an expected answer, as os.ErrNotExist and sql.ErrNoRows are, so
// it is an error and not a failure of the SDK. Expect and ExpectCall return no
// other error.
type Refusal struct {
	Status  int
	Message string
}

// Error returns the message.
func (f *Refusal) Error() string { return f.Message }

// Expect is for a route clients can reach that belongs to a shard. The app
// works out which shard the request is for (from the path, the payload or the
// first gRPC message) and passes its name. If that is the shard the node
// opened, Expect returns it. Otherwise (another shard, or none) it returns a
// *Refusal as the error, with status 400 and a message saying so (the client fixes its
// Shardlite-Target). A wrong shard is a stale client or a bug, so not a 404,
// which a client reads as a shard to drop, nor a 421. Exactly one of the
// results is nil, the error is always a *Refusal, and nothing is written to the
// response.
//
//	opened, err := node.Expect(r, Users.Shard(r.PathValue("id")))
//	if err != nil {
//		return err // to the app's error handler, or answer it as Refusal shows
//	}
//	db, err := opened.DB()
//
// Expect is not authentication: an app that has users authenticates every
// request on a route clients can reach, and Shardlite-Caller is never a reason
// to skip that. A request that did not go through Middleware is the app's
// mistake: Expect returns a 500 refusal naming it.
func (n *Node) Expect(r *http.Request, name ShardName) (*Opened, error) {
	rdb, ref := n.requestDB(r)
	if ref != nil {
		return nil, ref
	}
	got, file, ok := openedOf(r.Header)
	switch {
	case !ok:
		return nil, &Refusal{http.StatusBadRequest, fmt.Sprintf("the request is for %s but names no shard to the node: send Shardlite-Target naming the shard", name)}
	case got != name:
		return nil, &Refusal{http.StatusBadRequest, fmt.Sprintf("the request is for %s but the node opened %s: send Shardlite-Target naming the shard", name, got)}
	}
	return &Opened{q: n.Request(r), name: got, file: file, rdb: rdb}, nil
}

// ExpectCall is for a route that only takes calls and for the wake at
// RemindersPath. The shard is the one the node opened (Shardlite-Opened) and
// the route needs a caller: a client's request is refused with 403, since the
// node sets Shardlite-Caller on calls only and strips it from clients. It
// returns a *Refusal in each refusal and when the request did not go through
// Middleware, as Expect does.
//
// The caller is *CallerShard for a call from a shard and for the wake (which
// names its own shard) and *CallerApp for a call from the app.
func (n *Node) ExpectCall(r *http.Request) (*Opened, error) {
	rdb, ref := n.requestDB(r)
	if ref != nil {
		return nil, ref
	}
	if _, client := callerOf(r.Header).(*CallerClient); client {
		return nil, &Refusal{http.StatusForbidden, "this route is for calls from the app and its shards, not for clients"}
	}
	name, file, ok := openedOf(r.Header)
	if !ok {
		return nil, &Refusal{http.StatusBadRequest, "this route is for a shard: send it through the node, which sets Shardlite-Opened"}
	}
	return &Opened{q: n.Request(r), name: name, file: file, rdb: rdb}, nil
}

// requestDB finds the request's database that Middleware set up. A request
// without one gets a 500 refusal, since the handler was not wrapped: the app's
// mistake, not the client's.
func (n *Node) requestDB(r *http.Request) (*requestDB, *Refusal) {
	rdb, _ := r.Context().Value(dbKey{}).(*requestDB)
	if rdb == nil || rdb.n != n {
		return nil, &Refusal{http.StatusInternalServerError, "shardlite: the handler is not wrapped in this Node's Middleware"}
	}
	return rdb, nil
}
