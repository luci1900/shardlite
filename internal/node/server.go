package node

import (
	"cmp"
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

	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/supervisor"
)

// The headers the node and the app exchange (docs/guide.md, Reference).
const (
	hdrPrefix   = "Shardlite-"
	hdrTarget   = "Shardlite-Target"
	hdrPath     = "Shardlite-Path"
	hdrCaller   = "Shardlite-Caller"
	hdrSync     = "Shardlite-Sync"
	hdrIfActive = "Shardlite-If-Active"
	hdrHops     = "Shardlite-Hops"
	hdrDeadline = "Shardlite-Deadline"
	hdrInboxID  = "Shardlite-Inbox-Id"
	queryTarget = "shardlite-target"
	apiPrefix   = "/_shardlite/"
	readyPath   = "/_shardlite/ready"
	callerApp   = "app" // Shardlite-Caller of a call that has no shard behind it
)

// ServerOptions configure a Server.
type ServerOptions struct {
	App         *url.URL      // the app's base URL
	HealthPath  string        // the app's health endpoint, /health if empty
	HealthEvery time.Duration // 1 s if zero
	// MaxHops is the longest chain of hops allowed, DefaultMaxHops if zero.
	MaxHops int
	// MessageTimeout is how long one intake or one call to a handler for an inbox row may take, 30 s if zero.
	MessageTimeout time.Duration
	// Grace is how long stopping a server waits for requests in flight before it
	// ends them, 10 s if zero.
	Grace  time.Duration
	Logger *slog.Logger
}

// DefaultMaxHops is how many hops a chain of calls and messages may have (Dapr limits calls the same way).
const DefaultMaxHops = 32

// Server is the node's HTTP side: a public handler for clients (behind the
// deployment's load balancer) and a control handler for the app on the same
// host. Both send requests for shards through the same path: activate the
// shard, proxy to the app with Shardlite-Target and Shardlite-Path, and hold
// the app's output at the shard's durability gate.
type Server struct {
	m      *Shards
	kinds  *Kinds
	keeper *coord.Keeper
	opts   ServerOptions
	log    *slog.Logger
	proxy  *httputil.ReverseProxy
	health *health // a service of Run's
}

// NewServer makes the node's servers. The app counts as unhealthy until its
// health endpoint has answered (once Run serves s.health).
func NewServer(m *Shards, kinds *Kinds, keeper *coord.Keeper, opts ServerOptions) *Server {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	opts.HealthPath = cmp.Or(opts.HealthPath, "/health")
	orDefault(&opts.HealthEvery, time.Second)
	orDefault(&opts.MaxHops, DefaultMaxHops)
	orDefault(&opts.MessageTimeout, 30*time.Second)
	orDefault(&opts.Grace, 10*time.Second)
	u := *opts.App
	u.Path = strings.TrimRight(u.Path, "/") + opts.HealthPath
	s := &Server{m: m, kinds: kinds, keeper: keeper, opts: opts, log: opts.Logger}
	s.health = &health{url: u.String(), every: opts.HealthEvery, client: &http.Client{Timeout: 2 * time.Second}, log: opts.Logger}

	h1 := http.DefaultTransport.(*http.Transport).Clone()
	h1.MaxIdleConnsPerHost = 256
	h1.Protocols = new(http.Protocols)
	h1.Protocols.SetHTTP1(true)
	h2c := &http.Transport{MaxIdleConnsPerHost: 256, IdleConnTimeout: 90 * time.Second}
	h2c.Protocols = new(http.Protocols)
	h2c.Protocols.SetUnencryptedHTTP2(true)

	s.proxy = &httputil.ReverseProxy{
		Rewrite:        s.rewrite,
		Transport:      appTransport{h1: h1, h2c: h2c},
		ModifyResponse: s.modifyResponse,
		ErrorHandler:   s.proxyError,
		FlushInterval:  -1, // every write goes out at once: server-sent events and streaming RPCs
	}
	return s
}

// appTransport sends gRPC to the app over h2c (prior knowledge) and
// everything else over HTTP/1.1, which is what upgrades need.
type appTransport struct{ h1, h2c http.RoundTripper }

func (t appTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if isGRPC(r) && r.Header.Get("Upgrade") == "" {
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
	failed  chan<- error // told why the server failed (a listener did), if not nil
	lns     []net.Listener
	handler http.Handler
	grace   time.Duration
}

func (h *httpServer) String() string { return h.name }

// publicServer serves clients on ln (HTTP/1.1 and h2c, which gRPC clients
// behind a load balancer use).
func (s *Server) publicServer(ln net.Listener, failed chan<- error) *httpServer {
	return &httpServer{name: "public server", log: s.log, failed: failed, lns: []net.Listener{ln}, handler: s.PublicHandler(), grace: s.opts.Grace}
}

// controlServer serves the control API on its listeners (localhost TCP and, if
// not nil, a Unix socket).
func (s *Server) controlServer(failed chan<- error, lns ...net.Listener) *httpServer {
	h := &httpServer{name: "control server", log: s.log, failed: failed, handler: s.ControlHandler(), grace: s.opts.Grace}
	for _, ln := range lns {
		if ln != nil {
			h.lns = append(h.lns, ln)
		}
	}
	return h
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
			Handler: counted, Protocols: protos, ReadHeaderTimeout: 10 * time.Second,
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
	if err := g.Wait(); err != nil {
		report(h.failed, fmt.Errorf("%s: %w", h.name, err))
	}
	return supervisor.NoRestart(nil)
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
				s.ready(w, r)
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
// Shardlite-Caller, Shardlite-If-Active and Shardlite-Sync).
func (s *Server) ControlHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isAPIPath(r.URL.Path) {
			s.api(w, r)
			return
		}
		s.route(w, r, origin{kind: fromCall})
	})
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
		s.ready(w, r)
	case r.URL.Path == apiPrefix+"sync" && r.Method == http.MethodPost:
		// Everything committed before the call is in blob storage when this returns.
		if err := s.m.Barrier(r.Context()); err != nil {
			s.fail(w, r, false, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case strings.HasPrefix(r.URL.Path, apiPrefix+"shards/") && r.Method == http.MethodDelete:
		target := strings.TrimPrefix(r.URL.Path, apiPrefix+"shards/")
		if err := s.m.Delete(r.Context(), target); err != nil {
			s.fail(w, r, false, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, r, false, http.StatusNotFound, "no such endpoint")
	}
}

// reqInfo is what the proxy needs to know about a request, kept in its context.
type reqInfo struct {
	target string // "" for a shardless request
	path   string
	caller string // "" on a client request
	ref    *Ref
	public bool
	own    bool // the node's own request (an intake or a delivery), answered in process

	hops     int       // sent to the app as Shardlite-Hops
	deadline time.Time // zero if the request has none
	inboxID  int64     // the inbox row a handler is called for, 0 if none
	// callerGate gates what the calling shard sends for the whole call (the
	// request body and, after an upgrade, what it writes), nil if the call isn't gated.
	callerGate *gate
	// gates counts the request's gates in progress (see route), and held is
	// what the request holds until it ends.
	gates inFlight
	held  []*Ref
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
	info := &reqInfo{target: target, public: !o.control(), own: o.internal()}
	// The proxy can leave goroutines behind that are still reading through a gate
	// when the handler returns: the second copier of an upgraded connection, or
	// the transport writing a request body. Their gates end with the request's
	// context, and the request's shards are released only once none is left.
	ctx, cancel := context.WithCancel(r.Context())
	r = r.WithContext(ctx)
	defer func() {
		cancel()
		info.gates.close()
		for _, ref := range info.held {
			ref.Release()
		}
	}()
	// The one place that decides a request is an intake. Between nodes it will be
	// the header, which only a node may set: whatever an app or a client sent is
	// removed here, so nothing else can ask for it.
	r.Header.Del(hdrInbox)
	if err := s.parseCall(r, o, info); err != nil {
		s.answer(w, r, o.internal(), err)
		return
	}
	if !info.deadline.IsZero() {
		ctx, cancel := context.WithDeadline(r.Context(), info.deadline)
		defer cancel()
		r = r.WithContext(ctx)
	}
	if target != "" {
		if err := s.checkTarget(r, o, info); err != nil {
			s.answer(w, r, o.internal(), err)
			return
		}
		if err := s.holdShards(r, o, info); err != nil {
			s.answer(w, r, o.internal(), err)
			return
		}
	}
	if o.kind == fromIntake {
		s.serveIntake(w, r, info) // answered by the node, not the app
		return
	}
	s.proxy.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, info)))
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
	s.fail(w, r, own, err)
}

// parseCall reads what a call says about itself into info: its caller, its hops
// and its deadline.
func (s *Server) parseCall(r *http.Request, o origin, info *reqInfo) error {
	if o.control() {
		if info.target == "" {
			return &statusError{http.StatusBadRequest, "a call needs a Shardlite-Target"}
		}
		info.caller = r.Header.Get(hdrCaller)
		if info.caller == "" {
			info.caller = callerApp
		} else if _, _, err := ParseTarget(info.caller); err != nil && info.caller != callerApp {
			return &statusError{http.StatusBadRequest, "bad Shardlite-Caller"}
		}
		// Hops: the call is one more than the one the app says it is serving
		// (an app that doesn't pass the header on starts again at 0). It is refused
		// before anything is gated or activated.
		if v := r.Header.Get(hdrHops); v != "" {
			d, err := strconv.Atoi(v)
			if err != nil || d < 0 {
				return &statusError{http.StatusBadRequest, "bad Shardlite-Hops"}
			}
			info.hops = d + 1
			if info.hops > s.opts.MaxHops {
				s.log.Warn("call refused: calls are chained too far", "caller", info.caller, "target", info.target, "hops", info.hops, "max", s.opts.MaxHops)
				return &statusError{http.StatusLoopDetected, fmt.Sprintf("calls are chained more than %d hops", s.opts.MaxHops)}
			}
		}
	}

	// Deadline: from a call's header, or from grpc-timeout on a gRPC request
	// (the smaller if both). Clients can't send Shardlite-Deadline.
	var left time.Duration
	hasDeadline := false
	if o.control() {
		if v := r.Header.Get(hdrDeadline); v != "" {
			ms, err := strconv.ParseInt(v, 10, 64)
			if err != nil || ms < 0 {
				return &statusError{http.StatusBadRequest, "bad Shardlite-Deadline"}
			}
			left, hasDeadline = time.Duration(ms)*time.Millisecond, true
		}
	}
	if isGRPC(r) {
		if v := r.Header.Get("Grpc-Timeout"); v != "" {
			d, err := parseGRPCTimeout(v)
			if err != nil {
				return &statusError{http.StatusBadRequest, "bad grpc-timeout"}
			}
			if !hasDeadline || d < left {
				left, hasDeadline = d, true
			}
		}
	}
	if hasDeadline {
		info.deadline = time.Now().Add(left)
		if left <= 0 {
			return context.DeadlineExceeded
		}
	}
	if o.kind == fromDelivery {
		// The handler sees the row's own hops, not one more.
		info.hops, info.inboxID = o.hops, o.inboxID
	}
	return nil
}

// checkTarget refuses a target a client may not name, and a call that wants only
// shards that are already active.
func (s *Server) checkTarget(r *http.Request, o origin, info *reqInfo) error {
	if !o.control() {
		if err := s.kinds.CheckPublic(info.target); err != nil {
			if errors.Is(err, ErrNotPublic) {
				s.log.Info(`a client named a kind that is not public (set "public": true in the kinds file to allow it)`, "target", info.target)
				err = fmt.Errorf("%w: %q", ErrUnknownKind, strings.SplitN(info.target, "/", 2)[0])
			}
			return err
		}
	}
	if o.control() && strings.EqualFold(r.Header.Get(hdrIfActive), "true") && !s.m.IsActive(info.target) {
		// Not activated for a call that only wants shards that are already running.
		if _, _, err := ParseTarget(info.target); err != nil {
			return err
		}
		return &statusError{http.StatusNotFound, "shard is not active"}
	}
	return nil
}

// holdShards gates the caller and acquires the target. Everything it takes is
// added to info.held, which route releases when the request ends.
func (s *Server) holdShards(r *http.Request, o origin, info *reqInfo) error {
	// A call is bytes leaving the calling shard: everything it sends, for as long
	// as the call lasts, waits for what that shard committed (docs/internals.md,
	// Durability gates). The caller stays active until the call ends. The node's
	// own requests aren't gated: their row is durable already.
	if o.control() && info.caller != callerApp && !o.internal() {
		cg, err := s.gateCaller(r, info)
		if err != nil {
			return err
		}
		if cg != nil {
			info.held = append(info.held, cg.ref)
			info.callerGate = cg
			if r.Body != nil && r.Body != http.NoBody {
				r.Body = gatedBody{r.Body, cg}
			}
		}
	}
	ref, err := s.m.Acquire(r.Context(), info.target)
	if err != nil {
		return err
	}
	// Held for the whole request: a stream or an upgraded connection keeps the shard active.
	info.held = append(info.held, ref)
	info.ref, info.path = ref, ref.Path()
	return nil
}

// gateCaller holds a call until the calling shard's earlier commits are
// durable, if its kind has sync on (or the call says Shardlite-Sync: on), and
// returns the gate for the rest of the call. The gate holds the caller's ref:
// the caller releases it when the call ends. It is nil, with nothing held, when
// the call isn't gated: the caller isn't active on this node (it committed
// nothing here to wait for) or its setting is off.
func (s *Server) gateCaller(r *http.Request, info *reqInfo) (*gate, error) {
	ref := s.m.hold(info.caller)
	if ref == nil {
		return nil, nil
	}
	sync := syncSetting(ref, r.Header.Get(hdrSync))
	if !sync {
		ref.Release()
		return nil, nil
	}
	g := &gate{m: s.m, ref: ref, sync: true, ctx: r.Context(), calls: &info.gates}
	if err := g.wait(1, nil); err != nil { // the request line and headers
		ref.Release()
		return nil, err
	}
	return g, nil
}

// syncSetting applies a Shardlite-Sync override ("on" or "off") to the kind's setting.
func syncSetting(ref *Ref, override string) bool {
	switch strings.ToLower(override) {
	case "on":
		return true
	case "off":
		return false
	}
	return ref.Sync()
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

func (s *Server) rewrite(pr *httputil.ProxyRequest) {
	info, _ := pr.In.Context().Value(ctxKey{}).(*reqInfo)
	pr.SetURL(s.opts.App)
	pr.Out.Host = pr.In.Host
	// Nothing the sender wrote in a Shardlite-* header gets through. The node
	// sets the ones the app may trust.
	stripShardliteHeaders(pr.Out.Header)
	if info == nil {
		return
	}
	pr.Out.Header.Set(hdrHops, strconv.Itoa(info.hops))
	if info.inboxID != 0 {
		pr.Out.Header.Set(hdrInboxID, strconv.FormatInt(info.inboxID, 10))
	}
	if !info.deadline.IsZero() {
		// The time still left, so the app (and its own calls) stop when the node would.
		left := max(time.Until(info.deadline), time.Millisecond)
		ms := strconv.FormatInt((left + time.Millisecond - 1).Milliseconds(), 10) // rounded up
		pr.Out.Header.Set(hdrDeadline, ms)
		if pr.Out.Header.Get("Grpc-Timeout") != "" {
			pr.Out.Header.Set("Grpc-Timeout", ms+"m")
		}
	}
	if info.target != "" {
		pr.Out.Header.Set(hdrTarget, info.target)
		pr.Out.Header.Set(hdrPath, info.path)
	}
	if info.caller != "" {
		pr.Out.Header.Set(hdrCaller, info.caller)
	}
	if info.public {
		pr.SetXForwarded()
	}
}

// modifyResponse is where the app's output meets the gate.
func (s *Server) modifyResponse(resp *http.Response) error {
	info, _ := resp.Request.Context().Value(ctxKey{}).(*reqInfo)
	override := resp.Header.Get(hdrSync)
	stripShardliteHeaders(resp.Header)
	if info != nil && !info.deadline.IsZero() && isGRPC(resp.Request) {
		// The app is told the time left, so it usually answers at about the moment
		// the node's own deadline passes. If that cuts the body short after the headers
		// went out, the client gets a gRPC status, not a reset stream.
		if resp.Trailer == nil {
			resp.Trailer = http.Header{}
		}
		defer func() { resp.Body = deadlineBody{resp.Body, resp, resp.Request.Context()} }()
	}
	if info == nil || info.ref == nil {
		return nil // shardless: a plain reverse proxy
	}
	sync := syncSetting(info.ref, override)
	ctx := resp.Request.Context()
	// The status line and headers are the first chunk.
	if err := s.m.Gate(ctx, info.ref, sync); err != nil {
		return err
	}
	g := &gate{m: s.m, ref: info.ref, sync: sync, ctx: ctx, resp: resp, calls: &info.gates}
	if rwc, ok := resp.Body.(io.ReadWriteCloser); ok && resp.StatusCode == http.StatusSwitchingProtocols {
		resp.Body = gatedConn{rwc, g, info.callerGate}
	} else {
		resp.Body = gatedBody{resp.Body, g}
	}
	return nil
}

// deadlineBody ends a gRPC response whose read failed because the node's deadline
// passed with DEADLINE_EXCEEDED in its trailers.
type deadlineBody struct {
	io.ReadCloser
	resp *http.Response
	ctx  context.Context
}

func (b deadlineBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && err != io.EOF && errors.Is(b.ctx.Err(), context.DeadlineExceeded) {
		b.resp.Trailer.Set("Grpc-Status", strconv.Itoa(grpcCodes[http.StatusGatewayTimeout]))
		b.resp.Trailer.Set("Grpc-Message", grpcMessage(context.DeadlineExceeded.Error()))
		return n, io.EOF
	}
	return n, err
}

func stripShardliteHeaders(h http.Header) {
	for k := range h {
		if strings.HasPrefix(http.CanonicalHeaderKey(k), hdrPrefix) {
			delete(h, k)
		}
	}
}

// gate holds back each chunk of a shard's output until the shard's earlier
// commits are durable. A response is gated on the callee that produced it, and
// what a calling shard sends (a request body, an upgraded connection's writes)
// on the caller.
type gate struct {
	m    *Shards
	ref  *Ref
	sync bool
	ctx  context.Context
	resp *http.Response // the response being gated, nil for a caller's gate
	// calls are the request's gates in progress: once the request has ended, a
	// gate fails without looking at the shard, whose reference is released.
	calls *inFlight
}

// errRequestEnded is what a gate returns once its request has ended.
var errRequestEnded = errors.New("the request has ended")

// wait gates a chunk that was just read (n bytes, or the end of the body, whose
// trailers are output too).
func (g *gate) wait(n int, err error) error {
	if n > 0 || errors.Is(err, io.EOF) {
		if !g.calls.enter() {
			return errRequestEnded
		}
		defer g.calls.leave()
		return g.m.Gate(g.ctx, g.ref, g.sync)
	}
	return nil
}

func (g *gate) read(n int, err error) (int, error) {
	if gerr := g.wait(n, err); gerr != nil {
		return 0, gerr
	}
	if errors.Is(err, io.EOF) && g.resp != nil {
		stripShardliteHeaders(g.resp.Trailer)
	}
	return n, err
}

// gatedBody is a body whose chunks are gated: a response's on the callee that
// produced it, and a call's request body on the caller. Each chunk, and the end
// of the body with its trailers, gets through only once what the shard
// committed before it is durable.
type gatedBody struct {
	io.ReadCloser
	g *gate
}

func (b gatedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	return b.g.read(n, err)
}

// gatedConn is an upgraded connection (a WebSocket): what the app sends is
// gated on its shard. What the client sends is not, but what a calling shard
// sends is, on the caller (cg): the proxy copies it to this connection with Write.
type gatedConn struct {
	io.ReadWriteCloser
	g  *gate
	cg *gate // nil unless the upgrade is a gated call
}

func (c gatedConn) Read(p []byte) (int, error) {
	n, err := c.ReadWriteCloser.Read(p)
	return c.g.read(n, err)
}

func (c gatedConn) Write(p []byte) (int, error) {
	if c.cg != nil {
		if err := c.cg.wait(len(p), nil); err != nil {
			return 0, err
		}
	}
	return c.ReadWriteCloser.Write(p)
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
	switch {
	case errors.Is(err, ErrUnknownKind), errors.Is(err, ErrBadTarget):
		status = http.StatusBadRequest
	case errors.Is(err, ErrBusy):
		status = http.StatusConflict
	case errors.Is(err, ErrAppHolds),
		errors.Is(err, coord.ErrLeaseLost), errors.Is(err, coord.ErrLeaseExpired):
		status = http.StatusServiceUnavailable
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
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

// Ready reports whether the node should get traffic: the app is healthy, the
// kinds file is valid and the node's lease is good.
func (s *Server) Ready() (bool, string) {
	switch {
	case !s.health.Healthy():
		return false, "the app is not healthy"
	case !s.kinds.Valid():
		return false, "the kinds file is not valid"
	case s.keeper.Check() != nil:
		return false, "the node's lease is not valid"
	}
	return true, "ok"
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ok, why := s.Ready()
	status := http.StatusOK
	if !ok {
		status = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		io.WriteString(w, why+"\n")
	}
}

// health polls the app's health endpoint.
type health struct {
	url    string
	every  time.Duration
	client *http.Client
	log    *slog.Logger
	ok     atomic.Bool
}

func (*health) String() string { return "health poll" }

// Healthy reports whether the last poll got a 2xx answer.
func (h *health) Healthy() bool { return h.ok.Load() }

func (h *health) Serve(ctx context.Context) error {
	defer serving(h.log, h.String())()
	check := func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.url, nil)
		if err != nil {
			h.ok.Store(false)
			return
		}
		resp, err := h.client.Do(req)
		if err != nil {
			h.ok.Store(false)
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		h.ok.Store(resp.StatusCode/100 == 2)
	}
	check()
	t := time.NewTicker(h.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return supervisor.NoRestart(nil)
		case <-t.C:
			check()
		}
	}
}

// parseGRPCTimeout parses a grpc-timeout value: up to 8 digits and a unit
// (H, M, S, m, u or n).
func parseGRPCTimeout(v string) (time.Duration, error) {
	if len(v) < 2 || len(v) > 9 {
		return 0, fmt.Errorf("bad grpc-timeout %q", v)
	}
	n, err := strconv.ParseUint(v[:len(v)-1], 10, 32)
	if err != nil {
		return 0, fmt.Errorf("bad grpc-timeout %q", v)
	}
	unit := map[byte]time.Duration{'H': time.Hour, 'M': time.Minute, 'S': time.Second, 'm': time.Millisecond, 'u': time.Microsecond, 'n': time.Nanosecond}[v[len(v)-1]]
	if unit == 0 {
		return 0, fmt.Errorf("bad grpc-timeout %q", v)
	}
	return time.Duration(n) * unit, nil
}
