package node

import (
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/luci1900/shardlite/internal/lease"
)

// The headers the node and the app exchange (docs/guide.md, Reference).
const (
	hdrPrefix    = "Shardlite-"
	hdrTarget    = "Shardlite-Target"
	hdrOpened    = "Shardlite-Opened" // node to app: the shard the node opened for the request
	hdrFile      = "Shardlite-File"
	hdrCaller    = "Shardlite-Caller"
	hdrSync      = "Shardlite-Sync"
	hdrIfActive  = "Shardlite-If-Active"
	hdrIfExists  = "Shardlite-If-Exists"
	hdrOrigin    = "Shardlite-Origin" // on a forward: "client", "call" or "node"
	hdrOwner     = "Shardlite-Owner"  // on a 421: the owner the node found
	hdrError     = "Shardlite-Error"  // on an error the node made: its reason
	queryTarget  = "shardlite-target"
	apiPrefix    = "/_shardlite/"
	readyPath    = "/_shardlite/ready" // the node's, to the load balancer
	appReadyPath = "/_shardlite/ready" // the app's, which the node polls
	callerApp    = "app"               // Shardlite-Caller of a call that has no shard behind it
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
	// Clock is the declaration poll's period and its clock for telling that the app
	// is lost, the real one if nil.
	Clock Clock
	// AppLost is called once, from the declaration poll's goroutine, when the app
	// is lost: it has answered before and then gone appLostAfter without any HTTP answer.
	AppLost func()
	Logger  *slog.Logger
}

// The timeouts of the node's HTTP transports and servers.
const (
	// defaultGrace is how long stopping a server waits for requests in flight before it ends them.
	defaultGrace = 10 * time.Second
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
	m      *Shardset
	kinds  *Kinds
	keeper *lease.Keeper
	opts   ServerOptions
	log    *slog.Logger
	proxy  *httputil.ReverseProxy
	// forwarder sends requests for shards other nodes own to their peer
	// listeners: over h2c, one multiplexed connection per pair of nodes, but an
	// upgrade over HTTP/1.1 on a connection of its own.
	forwarder    *httputil.ReverseProxy
	declarations *declarations // a service of Run's: the poll of the app's ready route
}

// NewServer makes the node's servers. The app counts as unhealthy until its
// ready route has answered with a declaration (once Run serves s.declarations). An
// app address of nil (a node that serves no kinds) runs no poll.
func NewServer(m *Shardset, kinds *Kinds, keeper *lease.Keeper, opts ServerOptions) *Server {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	orDefault(&opts.ReadyEvery, time.Second)
	orDefault(&opts.MaxHops, DefaultMaxHops)
	orDefault(&opts.ReminderTimeout, 30*time.Second)
	orDefault(&opts.Grace, defaultGrace)
	orDefault(&opts.AnswerTimeout, time.Second)
	s := &Server{m: m, kinds: kinds, keeper: keeper, opts: opts, log: opts.Logger}

	h1 := http.DefaultTransport.(*http.Transport).Clone()
	h1.Proxy = nil               // the app and peers are never behind HTTP_PROXY (a Kubernetes Service IP would be)
	h1.DisableCompression = true // don't ask the app for gzip on a client's behalf only to decode it here
	h1.MaxIdleConnsPerHost = 256
	h1.Protocols = new(http.Protocols)
	h1.Protocols.SetHTTP1(true)
	h2c := newPeerTransport()
	// The declaration poll shares h1 (the app's ready route speaks HTTP/1.1). A node
	// without an app address serves no kinds and polls nothing.
	if opts.App != nil {
		u := *opts.App
		u.Path = strings.TrimRight(u.Path, "/") + appReadyPath
		s.declarations = &declarations{kinds: kinds, url: u.String(), every: opts.ReadyEvery, client: &http.Client{Transport: h1, Timeout: 2 * time.Second}, log: opts.Logger, clock: opts.Clock, lost: opts.AppLost}
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
			writeError(w, r, false, http.StatusNotFound, reasonNoEndpoint, "not found")
			return
		}
		s.route(w, r, origin{kind: fromClient})
	})
}

// ControlHandler serves the app on this host: the node's API under
// /_shardlite/, and calls to shards, which are requests with a
// Shardlite-Target that go through the same path as clients' (and may carry
// Shardlite-Context, Shardlite-If-Active and Shardlite-Sync).
func (s *Server) ControlHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isAPIPath(r.URL.Path) {
			s.api(w, r)
			return
		}
		s.route(w, r, origin{kind: fromCall})
	})
}

// PeerHandler serves other nodes: the requests they forward, each with the
// origin it had on the forwarding node.
// Node-only headers are honoured here and nowhere else.
func (s *Server) PeerHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Every answer says avoid while the node drains, the 102 included, and a
		// request names its sender and may say the same.
		from, avoid := s.m.placer.Received(remoteHost(r), r.Header)
		s.m.placer.StampPlacement(w.Header())
		o := origin{forwarded: true, from: from, avoid: avoid}
		switch r.Header.Get(hdrOrigin) {
		case originClient:
			o.kind = fromClient
		case originCall:
			o.kind = fromCall
		case originNode:
			o.kind = fromNode
		default:
			writeError(w, r, true, http.StatusNotFound, reasonNoEndpoint, "not found")
			return
		}
		// Only the node's own requests use paths under /_shardlite/ (the dispatcher's
		// activation), and only another node forwards them here.
		if isAPIPath(r.URL.Path) && o.kind != fromNode {
			writeError(w, r, true, http.StatusNotFound, reasonNoEndpoint, "not found")
			return
		}
		// The forward has reached this node: the forwarding node can tell a slow app
		// from a node that doesn't answer.
		w.WriteHeader(http.StatusProcessing)
		s.route(w, r, o)
	})
}

// remoteHost is the address a request connected from, without the port, for the log.
func remoteHost(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// The values of Shardlite-Origin.
const (
	originClient = "client"
	originCall   = "call"
	originNode   = "node"
)

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
	default:
		writeError(w, r, false, http.StatusNotFound, reasonNoEndpoint, "no such endpoint")
	}
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
