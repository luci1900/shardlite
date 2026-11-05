package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/cluster"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/pos"
	"github.com/luci1900/shardlite/internal/shard"
	"github.com/luci1900/shardlite/internal/shardfile"
)

// The headers the node and the app exchange (docs/guide.md, Reference).
const (
	hdrPrefix    = "Shardlite-"
	hdrTarget    = "Shardlite-Target"
	hdrOpened    = "Shardlite-Opened" // node to app: the shard the node opened for the request
	hdrFile      = "Shardlite-File"
	hdrCaller    = "Shardlite-Caller"
	hdrRelay     = "Shardlite-Relay" // app to node: the request is a client's, relayed
	hdrSync      = "Shardlite-Sync"
	hdrIfActive  = "Shardlite-If-Active"
	hdrIfExists  = "Shardlite-If-Exists"
	hdrHops      = "Shardlite-Hops"
	hdrOrigin    = "Shardlite-Origin" // on a forward: "client", "call", "relay" or "node"
	hdrOwner     = "Shardlite-Owner"  // on a 421: the owner the node found
	queryTarget  = "shardlite-target"
	apiPrefix    = "/_shardlite/"
	readyPath    = "/_shardlite/ready" // the node's, to the load balancer
	appReadyPath = "/_shardlite/ready" // the app's, which the node polls
	nodePath     = "/_shardlite/node"
	callerApp    = "app" // Shardlite-Caller of a call that has no shard behind it
)

// ServerOptions configure a Server.
type ServerOptions struct {
	App        *url.URL      // the app's base URL
	ReadyEvery time.Duration // how often the ready route is polled, 1 s if zero
	// MaxHops is the longest chain of hops allowed, DefaultMaxHops if zero.
	MaxHops int
	// ReminderTimeout is how long one wake of the app for a shard's reminders may take, 30 s if zero.
	ReminderTimeout time.Duration
	// Grace is how long stopping a server waits for requests in flight before it
	// ends them, 10 s if zero.
	Grace time.Duration
	// AnswerTimeout is how long a forward waits for the owner's node to
	// acknowledge it before the owner counts as unreachable, 1 s if zero.
	AnswerTimeout time.Duration
	// Clock, if set, is what GET /_shardlite/node reports of the node's clock.
	Clock  *coord.ClockWatch
	Logger *slog.Logger
}

// The timeouts of the node's HTTP transports and servers.
const (
	// dialTimeout bounds opening a connection to the app or a peer. A peer that is
	// down should fail a forward in seconds, not when TCP gives up.
	dialTimeout = 5 * time.Second
	// h2cPingAfter is how long a pooled h2c connection may be silent before
	// net/http pings it, and h2cPingWait how long it waits for the answer before it
	// closes the connection. Without them a peer that vanished without a FIN keeps
	// swallowing forwards on its pooled connection.
	h2cPingAfter = 15 * time.Second
	h2cPingWait  = 5 * time.Second
	// serverIdleTimeout closes a keep-alive connection that has sat idle (in
	// HTTP/2 it is how long the connection may have no open streams).
	serverIdleTimeout = 2 * time.Minute
)

// DefaultMaxHops is how many hops a chain of calls may have (Dapr limits calls the same way).
const DefaultMaxHops = 32

// Server is the node's HTTP side: a public handler for clients (behind the
// deployment's load balancer) and a control handler for the app on the same
// host. Both send requests for shards through the same path: activate the
// shard, proxy to the app with Shardlite-Opened and Shardlite-File, and hold
// the app's output at the shard's durability gate.
type Server struct {
	m      *Catalog
	kinds  *Kinds
	keeper *coord.Keeper
	opts   ServerOptions
	log    *slog.Logger
	proxy  *httputil.ReverseProxy
	// forwarder sends requests for shards other nodes own to their peer
	// listeners: over h2c, one multiplexed connection per pair of nodes, but an
	// upgrade over HTTP/1.1 on a connection of its own.
	forwarder *httputil.ReverseProxy
	peers     *http.Client // the node's own requests to other nodes (dispatches)
	ready     *ready       // a service of Run's: the poll of the app's ready route
}

// NewServer makes the node's servers. The app counts as unhealthy until its
// ready route has answered with a declaration (once Run serves s.ready). An
// app address of nil (a node that serves no kinds) runs no poll.
func NewServer(m *Catalog, kinds *Kinds, keeper *coord.Keeper, opts ServerOptions) *Server {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	orDefault(&opts.ReadyEvery, time.Second)
	orDefault(&opts.MaxHops, DefaultMaxHops)
	orDefault(&opts.ReminderTimeout, 30*time.Second)
	orDefault(&opts.Grace, 10*time.Second)
	orDefault(&opts.AnswerTimeout, time.Second)
	s := &Server{m: m, kinds: kinds, keeper: keeper, opts: opts, log: opts.Logger}

	h1 := http.DefaultTransport.(*http.Transport).Clone()
	h1.Proxy = nil               // the app and peers are never behind HTTP_PROXY (a Kubernetes Service IP would be)
	h1.DisableCompression = true // don't ask the app for gzip on a client's behalf only to decode it here
	h1.MaxIdleConnsPerHost = 256
	h1.Protocols = new(http.Protocols)
	h1.Protocols.SetHTTP1(true)
	h2c := newPeerTransport()
	// The ready poll shares h1 (the app's ready route speaks HTTP/1.1). A node
	// without an app address serves no kinds and polls nothing.
	if opts.App != nil {
		u := *opts.App
		u.Path = strings.TrimRight(u.Path, "/") + appReadyPath
		s.ready = &ready{kinds: kinds, url: u.String(), every: opts.ReadyEvery, client: &http.Client{Transport: h1, Timeout: 2 * time.Second}, log: opts.Logger}
	}

	s.proxy = &httputil.ReverseProxy{
		Rewrite:        s.rewrite,
		Transport:      splitTransport{h1: h1, h2c: h2c},
		ModifyResponse: s.modifyResponse,
		ErrorHandler:   s.proxyError,
		FlushInterval:  -1, // every write goes out at once: server-sent events and streaming RPCs
	}
	// Forwards share the transports with the app's requests: their pools are keyed
	// by address, and the request's context bounds their dials and waits.
	s.forwarder = &httputil.ReverseProxy{
		Rewrite:        s.rewrite,
		Transport:      splitTransport{h1: h1, h2c: h2c, peer: true},
		ModifyResponse: s.forwardedResponse,
		ErrorHandler:   s.forwardError,
		FlushInterval:  -1,
	}
	s.peers = &http.Client{Transport: splitTransport{h1: h1, h2c: h2c, peer: true}}
	return s
}

// splitTransport sends each request over h2c (prior knowledge) or HTTP/1.1. An
// upgrade always takes HTTP/1.1: net/http can't carry one over HTTP/2 (extended
// CONNECT is off by default, and its client can't send one). Of the rest, gRPC
// to the app takes h2c, which it needs, and everything to another node does, so
// that the two share one multiplexed connection.
type splitTransport struct {
	h1, h2c http.RoundTripper
	peer    bool // to another node's peer listener
}

func (t splitTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Header.Get("Upgrade") == "" && (t.peer || isGRPC(r)) {
		return t.h2c.RoundTrip(r)
	}
	return t.h1.RoundTrip(r)
}

func isGRPC(r *http.Request) bool {
	return strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/grpc")
}

// httpServer is one of the node's HTTP servers: the public server, or the
// control server on the control listener and the Unix socket. Every request's
// context derives from a base context the server owns, not from the context it
// serves on, and every handler is counted, so that stopping the server can end
// what is left of its requests and join them.
type httpServer struct {
	name    string
	log     *slog.Logger
	lns     []net.Listener
	handler http.Handler
	grace   time.Duration
}

func (h *httpServer) String() string { return h.name }

// publicServer serves clients on ln (HTTP/1.1 and h2c, which gRPC clients
// behind a load balancer use).
func (s *Server) publicServer(ln net.Listener) *httpServer {
	return &httpServer{name: "public server", log: s.log, lns: []net.Listener{ln}, handler: s.PublicHandler(), grace: s.opts.Grace}
}

// controlServer serves the control API on its listeners (localhost TCP and, if
// not nil, a Unix socket).
func (s *Server) controlServer(lns ...net.Listener) *httpServer {
	h := &httpServer{name: "control server", log: s.log, handler: s.ControlHandler(), grace: s.opts.Grace}
	for _, ln := range lns {
		if ln != nil {
			h.lns = append(h.lns, ln)
		}
	}
	return h
}

// peerServer serves other nodes on ln, which is only reachable from them.
func (s *Server) peerServer(ln net.Listener, name cluster.Node) *httpServer {
	return &httpServer{name: "peer server", log: s.log, lns: []net.Listener{ln}, handler: s.PeerHandler(name), grace: s.opts.Grace}
}

// Serve serves until ctx ends or a listener fails, then stops: it stops
// accepting and waits up to the grace period for requests in flight, cancels the
// base context (which ends upgraded connections: Shutdown and Close leave
// hijacked connections alone), closes what is left and returns once every
// handler has.
func (h *httpServer) Serve(ctx context.Context) error {
	defer serving(h.log, h.name)()
	base, cancelBase := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelBase()
	var handlers inFlight
	counted := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !handlers.enter() {
			// Read just as the server closed: its context has ended already.
			w.Header().Set("Connection", "close")
			w.Header().Set("Retry-After", "1")
			writeError(w, r, false, http.StatusServiceUnavailable, "the node is shutting down")
			return
		}
		defer handlers.leave()
		h.handler.ServeHTTP(w, r)
	})
	protos := new(http.Protocols)
	protos.SetHTTP1(true)
	protos.SetUnencryptedHTTP2(true)
	// One child per listener, and one that stops them all when the context ends
	// or a listener fails (WithContext cancels the others then).
	g, gctx := errgroup.WithContext(ctx)
	servers := make([]*http.Server, 0, len(h.lns))
	for _, ln := range h.lns {
		// No read or write timeouts: streams and upgraded connections live long.
		srv := &http.Server{
			Handler: counted, Protocols: protos, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: serverIdleTimeout,
			// Warnings: net/http logs here for clients' protocol errors as well as its
			// own failures (a handler that panicked, accepting that failed).
			ErrorLog:    slog.NewLogLogger(h.log.Handler(), slog.LevelWarn),
			BaseContext: func(net.Listener) context.Context { return base },
		}
		servers = append(servers, srv)
		g.Go(func() error {
			if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		})
	}
	g.Go(func() error {
		<-gctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), h.grace)
		defer cancel()
		var sg errgroup.Group
		for _, srv := range servers {
			sg.Go(func() error { srv.Shutdown(sctx); return nil })
		}
		sg.Wait()
		cancelBase()
		for _, srv := range servers {
			srv.Close()
		}
		handlers.close()
		return nil
	})
	return g.Wait() // a listener's failure: the server's group names it
}

// inFlight counts calls in progress. Once closed it lets no more in, so closing
// it joins every call it let in.
type inFlight struct {
	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

// enter counts a call in, false (with nothing counted) once closed.
func (f *inFlight) enter() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return false
	}
	f.wg.Add(1)
	return true
}

func (f *inFlight) leave() { f.wg.Done() }

// close lets no more calls in and waits for those in progress. It may be called again.
func (f *inFlight) close() {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	f.wg.Wait()
}

// PublicHandler serves clients: shard requests are routed by Shardlite-Target
// or ?shardlite-target=, everything else is proxied as it is, and the
// node's own paths are refused (except the readiness check a load balancer needs).
func (s *Server) PublicHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isAPIPath(r.URL.Path) {
			if r.URL.Path == readyPath && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
				s.readyRoute(w, r)
				return
			}
			writeError(w, r, false, http.StatusNotFound, "not found")
			return
		}
		s.route(w, r, origin{kind: fromClient})
	})
}

// ControlHandler serves the app on this host: the node's API under
// /_shardlite/, and calls to shards, which are requests with a
// Shardlite-Target that go through the same path as clients' (and may carry
// Shardlite-Caller, Shardlite-If-Active and Shardlite-Sync). A request marked
// Shardlite-Relay: true is a client's request the app relays, served with the
// client's limits. Any other value is refused, since reading it as a call would
// widen what the client can reach.
func (s *Server) ControlHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isAPIPath(r.URL.Path) {
			s.api(w, r)
			return
		}
		o := origin{kind: fromCall}
		if v := r.Header.Get(hdrRelay); v != "" {
			if !strings.EqualFold(v, "true") {
				writeError(w, r, false, http.StatusBadRequest, "bad Shardlite-Relay")
				return
			}
			o.kind = fromRelay
		}
		s.route(w, r, o)
	})
}

// PeerHandler serves other nodes: it says who this node is, and serves the
// requests they forward, each with the origin it had on the forwarding node.
// Node-only headers are honoured here and nowhere else.
func (s *Server) PeerHandler(name cluster.Node) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == nodePath && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
			s.nodeRoute(w, name)
			return
		}
		if r.URL.Path == dispatchPath && r.Method == http.MethodPost {
			s.serveDispatch(w, r)
			return
		}
		o := origin{forwarded: true}
		switch r.Header.Get(hdrOrigin) {
		case originClient:
			o.kind = fromClient
		case originCall:
			o.kind = fromCall
		case originRelay:
			o.kind = fromRelay
		case originNode:
			o.kind = fromNode
		default:
			writeError(w, r, true, http.StatusNotFound, "not found")
			return
		}
		// Only the node's own requests use paths under /_shardlite/ (a system app's
		// that only the node may send), and only another node forwards them here.
		if isAPIPath(r.URL.Path) && o.kind != fromNode {
			writeError(w, r, true, http.StatusNotFound, "not found")
			return
		}
		// The forward has reached this node: the forwarding node can tell a slow app
		// from a node that doesn't answer.
		w.WriteHeader(http.StatusProcessing)
		s.route(w, r, o)
	})
}

// The values of Shardlite-Origin.
const (
	originClient = "client"
	originCall   = "call"
	originRelay  = "relay"
	originNode   = "node"
)

// NodeInfoReply is the answer of GET /_shardlite/node.
type NodeInfoReply struct {
	Name  cluster.Node `json:"name"`
	Epoch pos.Epoch    `json:"epoch"`
	// Clock is how the node's clock stands against the store's, nil when the
	// node does not check it.
	Clock *ClockInfo `json:"clock,omitempty"`
}

// ClockInfo is the clock watch's report (coord.ClockReport).
type ClockInfo struct {
	// OffsetMillis is the store's time less the node's, good to a second.
	OffsetMillis int64 `json:"offset_ms"`
	Samples      int   `json:"samples"`
	// Warning is set when the offset is over a second, either way.
	Warning bool `json:"warning,omitempty"`
	// Blind is set when the store's responses carry no usable time.
	Blind bool `json:"blind,omitempty"`
}

// nodeRoute answers GET /_shardlite/node, on the peer listener for other
// nodes and on the control API for whoever runs the node: who this node is and
// how its clock stands against the store's.
func (s *Server) nodeRoute(w http.ResponseWriter, name cluster.Node) {
	w.Header().Set("Content-Type", "application/json")
	reply := NodeInfoReply{Name: name, Epoch: s.keeper.Lease().Epoch}
	if c := s.opts.Clock; c != nil {
		r := c.Report()
		reply.Clock = &ClockInfo{OffsetMillis: r.Offset.Milliseconds(), Samples: r.Samples, Warning: r.Warning, Blind: r.Blind}
	}
	json.NewEncoder(w).Encode(reply)
}

// isAPIPath reports whether the path is under /_shardlite/, however it is
// spelled: cleaned of dot segments and repeated slashes and compared without
// regard to case, since the app's router might not be as strict.
func isAPIPath(p string) bool {
	parts := make([]string, 0, 4)
	for _, seg := range strings.Split(p, "/") {
		switch seg {
		case "", ".":
		case "..":
			if len(parts) > 0 {
				parts = parts[:len(parts)-1]
			}
		default:
			parts = append(parts, seg)
		}
	}
	return len(parts) > 0 && strings.EqualFold(parts[0], "_shardlite")
}

func (s *Server) api(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == readyPath && (r.Method == http.MethodGet || r.Method == http.MethodHead):
		s.readyRoute(w, r)
	case r.URL.Path == apiPrefix+"sync" && r.Method == http.MethodPost:
		// Everything committed before the call is in blob storage when this returns.
		if err := s.m.Barrier(r.Context()); err != nil {
			s.fail(w, r, false, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case r.URL.Path == nodePath && (r.Method == http.MethodGet || r.Method == http.MethodHead):
		s.nodeRoute(w, s.m.Name())
	default:
		writeError(w, r, false, http.StatusNotFound, "no such endpoint")
	}
}

// reqInfo is what the proxy needs to know about a request, kept in its context.
type reqInfo struct {
	name       shard.Name // zero for a shardless request
	path       string
	caller     string     // "" on a client request and on a relay
	callerName shard.Name // the calling shard, zero when the caller is the app
	hold       *Hold
	public     bool // a client's request to the public handler
	relay      bool // a client's request the app relayed (the client's limits, hops counted)
	own        bool // the node's own request (a delivery), answered in process
	node       bool // the node's request for a system shard (fromNode)
	// forwarded is a request another node forwarded here. owner is the node that
	// owns the target when that isn't this one, and peer the address it is
	// forwarded to.
	forwarded bool
	owner     cluster.Node
	peer      string
	// moved is set when the owner answered a forward with 421 (the shard isn't
	// its own), and movedTo is the owner it named: empty if it named none.
	moved   bool
	movedTo cluster.Node
	// answer is what the forward in progress has heard from the owner's node
	// (answerAwaited, answerHeard or answerLate), and unreachable is set when it
	// failed before the owner's node answered.
	answer      atomic.Int32
	unreachable bool

	hops int // sent to the app as Shardlite-Hops
	// gates counts the request's gates in progress (see route), and held is
	// what the request holds until it ends.
	gates inFlight
	held  []*Hold
}

type ctxKey struct{}

// route works out the shard, activates it and proxies. o says who asked: the
// handlers set it from the request, and the node's own requests (send.go) say
// what they are.
func (s *Server) route(w http.ResponseWriter, r *http.Request, o origin) {
	// The header, then the query parameter. Either way the parameter is stripped,
	// so the app sees its own path and query.
	target := r.Header.Get(hdrTarget)
	if q, rest, ok := takeQueryParam(r.URL.RawQuery, queryTarget); ok {
		r.URL.RawQuery = rest
		if target == "" {
			target = q
		}
	}
	var name shard.Name
	if target != "" {
		n, err := ParseTarget(target)
		if err != nil {
			s.answer(w, r, o.internal(), err)
			return
		}
		name = n
	}
	info := &reqInfo{name: name, public: o.kind == fromClient, relay: o.kind == fromRelay, own: o.internal(), node: o.kind == fromNode, forwarded: o.forwarded}
	// The proxy can leave goroutines behind that are still reading through a gate
	// when the handler returns: the second copier of an upgraded connection, or
	// the transport writing a request body. Their gates end with the request's
	// context, and the request's shards are released only once none is left.
	ctx, cancel := context.WithCancel(r.Context())
	r = r.WithContext(ctx)
	defer func() {
		cancel()
		info.gates.close()
		for _, hold := range info.held {
			hold.Release()
		}
	}()
	if err := s.parseCall(r, o, info); err != nil {
		s.answer(w, r, o.internal(), err)
		return
	}
	if !info.name.IsZero() {
		if err := s.checkTarget(r, o, info); err != nil {
			s.answer(w, r, o.internal(), err)
			return
		}
		if err := s.holdShards(r, o, info); err != nil {
			s.answer(w, r, o.internal(), err)
			return
		}
		if info.owner != "" {
			s.forward(w, r, info)
			return
		}
		if info.hold.a.sys != nil {
			s.serveSystem(w, r, info)
			return
		}
	}
	s.proxy.ServeHTTP(noInformational{w}, r.WithContext(context.WithValue(r.Context(), ctxKey{}, info)))
}

// statusError is an answer the node gives with a status of its own.
type statusError struct {
	status int
	msg    string
}

func (e *statusError) Error() string { return e.msg }

// answer writes the answer for an error from one of route's steps.
func (s *Server) answer(w http.ResponseWriter, r *http.Request, own bool, err error) {
	if se, ok := errors.AsType[*statusError](err); ok {
		writeError(w, r, own, se.status, se.msg)
		return
	}
	if ue, ok := errors.AsType[*unavailable](err); ok {
		w.Header().Set("Retry-After", strconv.Itoa(int((ue.retry+time.Second-1)/time.Second)))
		writeError(w, r, own, http.StatusServiceUnavailable, ue.msg)
		return
	}
	s.fail(w, r, own, err)
}

// parseCall reads what a call or relay says about itself into info: its caller
// (a relay has none) and its hops.
func (s *Server) parseCall(r *http.Request, o origin, info *reqInfo) error {
	if o.control() {
		if info.name.IsZero() {
			return &statusError{http.StatusBadRequest, "a call or relay needs a Shardlite-Target"}
		}
		info.caller = r.Header.Get(hdrCaller)
		switch {
		case o.kind == fromRelay:
			// The client's request has no caller, and the handler treats it as one, so a
			// relay naming one is the app mixing the two up.
			if info.caller != "" {
				return &statusError{http.StatusBadRequest, "a relay carries no Shardlite-Caller"}
			}
		case info.caller == "" || info.caller == callerApp:
			info.caller = callerApp
		default:
			n, err := ParseTarget(info.caller)
			if err != nil {
				return &statusError{http.StatusBadRequest, "bad Shardlite-Caller"}
			}
			info.callerName = n
			// What a calling shard sends is gated on its commits, and an upgraded
			// connection would need every write gated for as long as it lasts. Streams
			// carry the same traffic, gated as any call's body is.
			if r.Header.Get("Upgrade") != "" {
				return &statusError{http.StatusBadRequest, "a call from a shard can't be upgraded: use a streaming request (server-sent events or a gRPC stream)"}
			}
		}
	}
	// Hops: the call is one more than the one the app says it is serving
	// (an app that doesn't pass the header on starts again at 0). It is refused
	// before anything is gated or activated. A forward carries the hops its node
	// worked out: forwarding is not a hop.
	if v := r.Header.Get(hdrHops); v != "" && (o.control() || o.forwarded) {
		d, err := strconv.Atoi(v)
		if err != nil || d < 0 {
			return &statusError{http.StatusBadRequest, "bad Shardlite-Hops"}
		}
		info.hops = d
		if !o.forwarded {
			info.hops = d + 1
			if info.hops > s.opts.MaxHops {
				s.log.Warn("call refused: calls are chained too far", "caller", info.caller, "target", info.name, "hops", info.hops, "max", s.opts.MaxHops)
				return &statusError{http.StatusLoopDetected, fmt.Sprintf("calls are chained more than %d hops", s.opts.MaxHops)}
			}
		}
	}
	return nil
}

// checkTarget refuses a target a client may not name (a relay of a client's
// request included), and a call that wants only shards that are already active
// when the target isn't active here. If another node owns it, it may be active
// there, and the call goes to it.
func (s *Server) checkTarget(r *http.Request, o origin, info *reqInfo) error {
	if o.client() {
		if err := s.kinds.CheckPublic(info.name); err != nil {
			if errors.Is(err, ErrNotPublic) {
				s.log.Info(`a client named a kind that is not public (set "public": true in the declaration to allow it)`, "target", info.name)
				err = fmt.Errorf("%w: %q", ErrUnknownKind, info.name.Kind())
			}
			return err
		}
	}
	if o.call() && strings.EqualFold(r.Header.Get(hdrIfActive), "true") && !s.m.IsActive(info.name) {
		// Not activated for a call that only wants shards that are already running.
		if owner, ok := s.cachedOwner(o, info.name); ok {
			info.owner = owner
			return nil
		}
		m, _, err := s.m.n.Files.GetManifest(r.Context(), info.name)
		switch {
		case err != nil && !errors.Is(err, blob.ErrNotFound):
			return err
		case err == nil && m.Owner != "" && m.Owner != s.m.n.Name:
			info.owner = m.Owner
			return nil
		}
		return &statusError{http.StatusNotFound, "shard is not active"}
	}
	return nil
}

// holdShards gates the caller and acquires the target, unless another node owns
// it (info.owner). Everything it takes is added to info.held, which route
// releases when the request ends.
func (s *Server) holdShards(r *http.Request, o origin, info *reqInfo) error {
	// A call is bytes leaving the calling shard: everything it sends, for as long
	// as the call lasts, waits for what that shard committed (docs/internals.md,
	// Durability gates). The caller stays active until the call ends. The node's
	// own requests aren't gated: their row is durable already. Nor is a forwarded
	// call: the caller's node gates it. A relay has no caller to gate.
	if o.call() && info.caller != callerApp && !o.internal() && !o.forwarded {
		cg, err := s.gateCaller(r, info)
		if err != nil {
			return err
		}
		if cg != nil {
			info.held = append(info.held, cg.hold)
			if r.Body != nil && r.Body != http.NoBody {
				r.Body = gatedBody{r.Body, cg}
			}
		}
	}
	if info.owner != "" {
		return nil
	}
	if owner, ok := s.cachedOwner(o, info.name); ok {
		info.owner = owner
		return nil
	}
	// Clients never create a shard (nor does a relay of their request), and a call
	// can ask not to (Shardlite-If-Exists). Shardlite-If-Active is the same ask,
	// made before the table is looked at.
	existing := o.client() || (o.call() && strings.EqualFold(r.Header.Get(hdrIfExists), "true"))
	var hold *Hold
	var err error
	if existing {
		hold, err = s.m.acquireExisting(r.Context(), info.name)
	} else {
		hold, err = s.m.Acquire(r.Context(), info.name)
	}
	if errors.Is(err, blob.ErrNotFound) {
		// The same answer as a kind the client may not name, so it says nothing of
		// kinds or ids. A flood of misses must not be a flood of log lines.
		s.log.Debug("no shard to reach: it does not exist, and this request may not create it", "target", info.name)
		return fmt.Errorf("%w: %q", ErrUnknownKind, info.name.Kind())
	}
	if oe, ok := errors.AsType[*shardfile.OwnedElsewhereError](err); ok && o.kind != fromWake {
		info.owner = oe.Owner
		return nil
	}
	if errors.Is(err, shardfile.ErrOwnerNotAdopted) {
		// The owner's lease has expired, or it restarted, so there is nothing to
		// wait for but the adoption.
		return &unavailable{err.Error(), time.Second}
	}
	if err != nil {
		return err
	}
	// Held for the whole request: a stream or an upgraded connection keeps the shard active.
	info.held = append(info.held, hold)
	info.hold, info.path = hold, hold.Path()
	return nil
}

// cachedOwner is the node the owner cache names for target, if it names one
// that forwards should go to. A forwarded request is for the node that
// received it to decide, so it never looks, and a delivery (which is for the
// node that owns the shard and is never forwarded) doesn't either.
func (s *Server) cachedOwner(o origin, n shard.Name) (cluster.Node, bool) {
	if o.forwarded || o.kind == fromWake {
		return "", false
	}
	return s.m.owners.Get(n)
}

// gateCaller holds a call until the calling shard's earlier commits are
// durable, in the store the caller's tier names (or the one the call says with
// Shardlite-Sync), and returns the gate for the rest of the call. The gate
// keeps the caller active: the hold is released when the call ends. It is nil,
// with nothing held, when the call isn't gated: the caller isn't active on this
// node (it committed nothing here to wait for).
func (s *Server) gateCaller(r *http.Request, info *reqInfo) (*gate, error) {
	hold := s.m.tryHold(info.callerName)
	if hold == nil {
		return nil, nil
	}
	sync, err := syncSetting(hold, r.Header.Get(hdrSync))
	if err != nil {
		hold.Release()
		return nil, err
	}
	g := &gate{m: s.m, hold: hold, sync: sync, ctx: r.Context(), calls: &info.gates}
	if err := g.wait(1, nil); err != nil { // the request line and headers
		hold.Release()
		return nil, err
	}
	return g, nil
}

// syncSetting applies a Shardlite-Sync override ("none" or "storage") to the
// kind's tier. An unknown name is refused with a 400: copying the kind's tier
// could quietly give less than what the app said, and the guide's status table
// already fail a malformed Shardlite-* header.
func syncSetting(hold *Hold, override string) (kind.Sync, error) {
	if override != "" {
		s, err := kind.ParseSync(override)
		if err != nil {
			return 0, &statusError{http.StatusBadRequest, fmt.Sprintf("bad Shardlite-Sync: %v", err)}
		}
		return s, nil
	}
	return hold.Sync(), nil
}

// takeQueryParam removes the first parameter called name from a raw query.
func takeQueryParam(raw, name string) (value, rest string, ok bool) {
	if raw == "" {
		return "", raw, false
	}
	var kept []string
	for _, kv := range strings.Split(raw, "&") {
		k, v, _ := strings.Cut(kv, "=")
		if uk, err := url.QueryUnescape(k); err == nil && uk == name {
			if !ok {
				if uv, err := url.QueryUnescape(v); err == nil {
					value, ok = uv, true
				}
			}
			continue
		}
		kept = append(kept, kv)
	}
	if !ok {
		return "", raw, false
	}
	return value, strings.Join(kept, "&"), true
}

// rewrite makes the request to the app, or the forward to the owner's node
// (info.peer), which carries what the owner needs to serve it as this node would
// have.
func (s *Server) rewrite(pr *httputil.ProxyRequest) {
	info, _ := pr.In.Context().Value(ctxKey{}).(*reqInfo)
	if info != nil && info.peer != "" {
		pr.SetURL(&url.URL{Scheme: "http", Host: info.peer})
	} else {
		pr.SetURL(s.opts.App)
	}
	pr.Out.Host = pr.In.Host
	// Nothing the sender wrote in a Shardlite-* header gets through. The node
	// sets the ones the app (or the owner's node) may trust.
	stripShardliteHeaders(pr.Out.Header)
	if info == nil {
		return
	}
	pr.Out.Header.Set(hdrHops, strconv.Itoa(info.hops))
	if info.peer != "" {
		origin := originCall
		switch {
		case info.public:
			origin = originClient
		case info.relay:
			origin = originRelay
		case info.node:
			origin = originNode
			fallthrough
		default:
			for _, h := range []string{hdrIfActive, hdrIfExists, hdrSync} {
				if v := pr.In.Header.Get(h); v != "" {
					pr.Out.Header.Set(h, v)
				}
			}
		}
		pr.Out.Header.Set(hdrOrigin, origin)
	}
	if !info.name.IsZero() {
		if info.peer == "" {
			// To the app it is what the node opened, not what the request is for.
			pr.Out.Header.Set(hdrOpened, info.name.String())
			pr.Out.Header.Set(hdrFile, info.path)
		} else {
			// To the owner's node it is the chosen target.
			pr.Out.Header.Set(hdrTarget, info.name.String())
		}
	}
	if info.caller != "" {
		pr.Out.Header.Set(hdrCaller, info.caller)
	}
	switch {
	case info.forwarded || info.relay:
		// The forwarding node added the client: the app sees what it would have seen
		// there. A relay carries them as the app sent them, which are what the client's
		// request carried when the app received it. The control API is local and the
		// app is trusted, so the node adds nothing of its own (the app's address would
		// only lengthen the chain).
		passOn(pr, "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto")
	case info.public:
		// The load balancer is the trust boundary: its chain is extended with its
		// address (httputil's documented way under Rewrite), and the protocol and
		// host it saw are kept over SetXForwarded's view from inside the network.
		pr.Out.Header["X-Forwarded-For"] = pr.In.Header["X-Forwarded-For"]
		pr.SetXForwarded()
		passOn(pr, "X-Forwarded-Host", "X-Forwarded-Proto")
	}
}

// passOn copies the named headers of the inbound request to the outbound one,
// leaving any that the inbound request lacks as they are.
func passOn(pr *httputil.ProxyRequest, names ...string) {
	for _, h := range names {
		if v := pr.In.Header.Values(h); len(v) > 0 {
			pr.Out.Header[h] = v
		}
	}
}

// modifyResponse is where the app's output meets the gate.
func (s *Server) modifyResponse(resp *http.Response) error {
	info, _ := resp.Request.Context().Value(ctxKey{}).(*reqInfo)
	override := resp.Header.Get(hdrSync)
	stripShardliteHeaders(resp.Header)
	if info == nil || info.hold == nil {
		return nil // shardless: a plain reverse proxy
	}
	sync, err := syncSetting(info.hold, override)
	if err != nil {
		return err
	}
	ctx := resp.Request.Context()
	// The status line and headers are the first chunk.
	if err := s.m.Gate(ctx, info.hold, sync); err != nil {
		return err
	}
	g := &gate{m: s.m, hold: info.hold, sync: sync, ctx: ctx, resp: resp, calls: &info.gates}
	if rwc, ok := resp.Body.(io.ReadWriteCloser); ok && resp.StatusCode == http.StatusSwitchingProtocols {
		resp.Body = gatedConn{rwc, g, &info.gates}
	} else {
		resp.Body = gatedBody{resp.Body, g}
	}
	return nil
}

// noInformational drops the app's 1xx responses (103 Early Hints). ReverseProxy
// writes them to the client as they arrive, before ModifyResponse runs, so they
// would get past the gate and keep their Shardlite-* headers. A 101 is the final
// response of an upgrade, which ReverseProxy writes itself, so it never comes
// through here. Unwrap lets ResponseController reach the real writer to flush and
// hijack.
type noInformational struct{ http.ResponseWriter }

func (w noInformational) WriteHeader(code int) {
	if code >= 100 && code <= 199 {
		return
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w noInformational) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// stripShardliteHeaders removes every Shardlite-* header. A key is matched
// without regard to case and with "_" read as "-", because Go keeps a header sent
// as Shardlite_Caller under a key that isn't canonical, while CGI-style
// frameworks (WSGI, Rack, PHP) give it the same variable as Shardlite-Caller.
func stripShardliteHeaders(h http.Header) {
	for k := range h {
		if len(k) >= len(hdrPrefix) && strings.EqualFold(strings.ReplaceAll(k[:len(hdrPrefix)], "_", "-"), hdrPrefix) {
			delete(h, k)
		}
	}
}

func (s *Server) proxyError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, context.Canceled) && r.Context().Err() != nil {
		return // the client went away
	}
	info, _ := r.Context().Value(ctxKey{}).(*reqInfo)
	s.fail(w, r, info != nil && info.own, err)
}

// fail answers with the status an error maps to.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, own bool, err error) {
	status := http.StatusBadGateway
	if se, ok := errors.AsType[*statusError](err); ok {
		status = se.status
	} else if _, ok := errors.AsType[*unavailable](err); ok {
		// A tier the node can't meet: the retry hint is the default one.
		status = http.StatusServiceUnavailable
	} else {
		switch {
		case errors.Is(err, ErrUnknownKind):
			status = http.StatusNotFound
		case errors.Is(err, ErrBadTarget):
			status = http.StatusBadRequest
		case errors.Is(err, ErrAppHolds), errors.Is(err, errMoved),
			errors.Is(err, coord.ErrLeaseLost), errors.Is(err, coord.ErrLeaseExpired),
			errors.Is(err, shardfile.ErrBadFile):
			// A shard whose files are not the ones it holds cannot be served, and
			// neither can a shard the node may not serve yet.
			status = http.StatusServiceUnavailable
		case errors.Is(err, context.DeadlineExceeded):
			status = http.StatusGatewayTimeout
		}
	}
	if status >= 500 {
		s.log.Warn("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		if status == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", "1")
		}
	}
	writeError(w, r, own, status, err.Error())
}

// grpcCodes maps the statuses the node produces onto gRPC status codes.
var grpcCodes = map[int]int{
	http.StatusBadRequest:         3,  // INVALID_ARGUMENT
	http.StatusNotFound:           5,  // NOT_FOUND
	http.StatusConflict:           9,  // FAILED_PRECONDITION
	http.StatusLoopDetected:       8,  // RESOURCE_EXHAUSTED
	http.StatusTooManyRequests:    8,  // RESOURCE_EXHAUSTED
	http.StatusBadGateway:         14, // UNAVAILABLE
	http.StatusServiceUnavailable: 14, // UNAVAILABLE
	http.StatusGatewayTimeout:     4,  // DEADLINE_EXCEEDED
}

// writeError answers with an error the node produced itself. For a request
// with content-type application/grpc it is a gRPC status (a "trailers-only"
// response: HTTP 200 with grpc-status and grpc-message), which is what a
// gRPC client understands. own says the request is the node's own (answered in process), which is not given a gRPC status.
func writeError(w http.ResponseWriter, r *http.Request, own bool, status int, msg string) {
	// The node's own requests read the status themselves, whatever it carries.
	if isGRPC(r) && !own {
		code, ok := grpcCodes[status]
		if !ok {
			code = 13 // INTERNAL
		}
		h := w.Header()
		h.Set("Content-Type", "application/grpc")
		h.Set("Grpc-Status", strconv.Itoa(code))
		h.Set("Grpc-Message", grpcMessage(msg))
		w.WriteHeader(http.StatusOK)
		return
	}
	if strings.Contains(r.Header.Get("Accept"), "application/json") || strings.HasPrefix(r.URL.Path, apiPrefix) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(map[string]string{"error": msg})
		return
	}
	http.Error(w, msg, status)
}

// grpcMessage percent-encodes a grpc-message as the gRPC spec says.
func grpcMessage(s string) string {
	var b strings.Builder
	for _, c := range []byte(s) {
		if c >= 0x20 && c <= 0x7e && c != '%' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// newPeerTransport is the transport of the node's requests to other nodes'
// peer listeners: h2c, one multiplexed connection per pair of nodes, a dial
// timeout so a peer that is down fails in seconds, and pings so a peer that
// vanished without a FIN stops swallowing requests on its pooled connection.
func newPeerTransport() *http.Transport {
	t := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: dialTimeout}).DialContext,
		DisableCompression:  true,
		MaxIdleConnsPerHost: 256, IdleConnTimeout: 90 * time.Second,
		HTTP2: &http.HTTP2Config{SendPingTimeout: h2cPingAfter, PingTimeout: h2cPingWait},
	}
	t.Protocols = new(http.Protocols)
	t.Protocols.SetUnencryptedHTTP2(true)
	return t
}
