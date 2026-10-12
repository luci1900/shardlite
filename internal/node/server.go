package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/coord"
)

// The headers the node and the app exchange (design.md, Interface inventory).
const (
	hdrPrefix   = "Shardlite-"
	hdrTarget   = "Shardlite-Target"
	hdrPath     = "Shardlite-Path"
	hdrCaller   = "Shardlite-Caller"
	hdrSync     = "Shardlite-Sync"
	hdrIfActive = "Shardlite-If-Active"
	hdrDepth    = "Shardlite-Depth"
	hdrDeadline = "Shardlite-Deadline"
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
	// MaxCallDepth is the deepest chain of calls allowed, DefaultMaxCallDepth if zero.
	MaxCallDepth int
	Logger       *slog.Logger
}

// DefaultMaxCallDepth is how deep calls between shards may nest (Dapr's limit).
const DefaultMaxCallDepth = 32

// Server is the node's HTTP side: a public handler for clients (behind the
// deployment's load balancer) and a control handler for the app on the same
// host. Both send requests for shards through the same path: activate the
// shard, proxy to the app with Shardlite-Target and Shardlite-Path, and hold
// the app's output at the shard's durability gate.
type Server struct {
	m     *Manager
	opts  ServerOptions
	log   *slog.Logger
	proxy *httputil.ReverseProxy

	healthy atomic.Bool
	cancel  context.CancelFunc
	g       errgroup.Group // owns the health poll; Close joins it
}

// NewServer starts polling the app's health endpoint.
func NewServer(m *Manager, opts ServerOptions) *Server {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.HealthPath == "" {
		opts.HealthPath = "/health"
	}
	if opts.HealthEvery <= 0 {
		opts.HealthEvery = time.Second
	}
	if opts.MaxCallDepth <= 0 {
		opts.MaxCallDepth = DefaultMaxCallDepth
	}
	s := &Server{m: m, opts: opts, log: opts.Logger}

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
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.g.Go(func() error { s.watchHealth(ctx); return nil })
	return s
}

// Close stops the health poll.
func (s *Server) Close() {
	s.cancel()
	s.g.Wait()
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

// Serve serves the handlers until ctx ends: the public listener (HTTP/1.1
// and h2c, which gRPC clients behind a load balancer use), and the control API
// on localhost TCP and, if given, a Unix socket. Any of the listeners may be nil.
func (s *Server) Serve(ctx context.Context, public, control, socket net.Listener) error {
	protos := new(http.Protocols)
	protos.SetHTTP1(true)
	protos.SetUnencryptedHTTP2(true)
	mk := func(h http.Handler) *http.Server {
		// No read or write timeouts: streams and upgraded connections live long.
		return &http.Server{Handler: h, Protocols: protos, ReadHeaderTimeout: 10 * time.Second}
	}
	// One child per listener, and one that shuts them all down when the context
	// ends or a listener fails (WithContext cancels the others then).
	g, gctx := errgroup.WithContext(ctx)
	var servers []*http.Server
	for _, l := range []struct {
		ln net.Listener
		h  http.Handler
	}{{public, s.PublicHandler()}, {control, s.ControlHandler()}, {socket, s.ControlHandler()}} {
		if l.ln == nil {
			continue
		}
		srv := mk(l.h)
		servers = append(servers, srv)
		g.Go(func() error {
			if err := srv.Serve(l.ln); !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		})
	}
	g.Go(func() error {
		<-gctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, srv := range servers {
			srv.Shutdown(sctx)
		}
		return nil
	})
	return g.Wait()
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
			writeError(w, r, http.StatusNotFound, "not found")
			return
		}
		s.route(w, r, false)
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
		s.route(w, r, true)
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
		if err := s.m.BarrierFor(r.Context(), r.Header.Get(hdrCaller)); err != nil {
			s.fail(w, r, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case strings.HasPrefix(r.URL.Path, apiPrefix+"shards/") && r.Method == http.MethodDelete:
		target := strings.TrimPrefix(r.URL.Path, apiPrefix+"shards/")
		if err := s.m.Delete(r.Context(), target); err != nil {
			s.fail(w, r, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, r, http.StatusNotFound, "no such endpoint")
	}
}

// reqInfo is what the proxy needs to know about a request, kept in its context.
type reqInfo struct {
	target string // "" for a shardless request
	path   string
	caller string // "" on a client request
	ref    *Ref
	public bool
	log    *slog.Logger

	depth    int       // sent to the app as Shardlite-Depth
	deadline time.Time // zero if the request has none
	// callerGate gates what the calling shard sends for the whole call (the
	// request body and, after an upgrade, what it writes), nil if the call isn't gated.
	callerGate *gate
}

type ctxKey struct{}

// route works out the shard, activates it and proxies.
func (s *Server) route(w http.ResponseWriter, r *http.Request, control bool) {
	// 1. The header, then the query parameter. Either way the parameter is
	// stripped, so the app sees its own path and query.
	target := r.Header.Get(hdrTarget)
	if q, rest, ok := takeQueryParam(r.URL.RawQuery, queryTarget); ok {
		r.URL.RawQuery = rest
		if target == "" {
			target = q
		}
	}
	info := &reqInfo{target: target, public: !control, log: s.log}
	if control {
		if target == "" {
			writeError(w, r, http.StatusBadRequest, "a call needs a Shardlite-Target")
			return
		}
		info.caller = r.Header.Get(hdrCaller)
		if info.caller == "" {
			info.caller = callerApp
		} else if _, _, err := ParseTarget(info.caller); err != nil && info.caller != callerApp {
			writeError(w, r, http.StatusBadRequest, "bad Shardlite-Caller")
			return
		}
		// Depth: the call is one deeper than the one the app says it is serving
		// (an app that doesn't pass the header on starts again at 0). It is refused
		// before anything is gated or activated.
		if v := r.Header.Get(hdrDepth); v != "" {
			d, err := strconv.Atoi(v)
			if err != nil || d < 0 {
				writeError(w, r, http.StatusBadRequest, "bad Shardlite-Depth")
				return
			}
			info.depth = d + 1
			if info.depth > s.opts.MaxCallDepth {
				s.log.Warn("call refused: calls are nested too deep", "caller", info.caller, "target", target, "depth", info.depth, "max", s.opts.MaxCallDepth)
				writeError(w, r, http.StatusLoopDetected, fmt.Sprintf("calls are nested more than %d deep", s.opts.MaxCallDepth))
				return
			}
		}
	}

	// Deadline: from a call's header, or from grpc-timeout on a gRPC request
	// (the smaller if both). Clients can't send Shardlite-Deadline.
	var left time.Duration
	hasDeadline := false
	if control {
		if v := r.Header.Get(hdrDeadline); v != "" {
			ms, err := strconv.ParseInt(v, 10, 64)
			if err != nil || ms < 0 {
				writeError(w, r, http.StatusBadRequest, "bad Shardlite-Deadline")
				return
			}
			left, hasDeadline = time.Duration(ms)*time.Millisecond, true
		}
	}
	if isGRPC(r) {
		if v := r.Header.Get("Grpc-Timeout"); v != "" {
			d, err := parseGRPCTimeout(v)
			if err != nil {
				writeError(w, r, http.StatusBadRequest, "bad grpc-timeout")
				return
			}
			if !hasDeadline || d < left {
				left, hasDeadline = d, true
			}
		}
	}
	if hasDeadline {
		info.deadline = time.Now().Add(left)
		ctx, cancel := context.WithDeadline(r.Context(), info.deadline)
		defer cancel()
		r = r.WithContext(ctx)
		if left <= 0 {
			s.fail(w, r, context.DeadlineExceeded)
			return
		}
	}

	if target != "" {
		if !control {
			if err := s.m.opts.Kinds.CheckPublic(target); err != nil {
				if errors.Is(err, ErrNotPublic) {
					s.log.Info(`a client named a kind that is not public (set "public": true in the kinds file to allow it)`, "target", target)
					err = fmt.Errorf("%w: %q", ErrUnknownKind, strings.SplitN(target, "/", 2)[0])
				}
				s.fail(w, r, err)
				return
			}
		}
		if control && strings.EqualFold(r.Header.Get(hdrIfActive), "true") && !s.m.IsActive(target) {
			// Not activated for a call that only wants shards that are already running.
			if _, _, err := ParseTarget(target); err != nil {
				s.fail(w, r, err)
				return
			}
			writeError(w, r, http.StatusNotFound, "shard is not active")
			return
		}
		// A call is bytes leaving the calling shard: everything it sends, for as long
		// as the call lasts, waits for what that shard committed (design.md,
		// Durability gates). The caller stays active until the call ends.
		if control && info.caller != callerApp {
			cg, err := s.gateCaller(r, info.caller)
			if err != nil {
				s.fail(w, r, err)
				return
			}
			if cg != nil {
				defer cg.ref.Release()
				info.callerGate = cg
				if r.Body != nil && r.Body != http.NoBody {
					r.Body = gatedRequestBody{r.Body, cg}
				}
			}
		}
		ref, err := s.m.Acquire(r.Context(), target)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		defer ref.Release() // held for the whole request: a stream or an upgraded connection keeps the shard active
		info.ref, info.path = ref, ref.Path()
	}
	s.proxy.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, info)))
}

// gateCaller holds a call until the calling shard's earlier commits are
// durable, if its kind has sync on (or the call says Shardlite-Sync: on), and
// returns the gate for the rest of the call. The gate holds the caller's ref:
// the caller releases it when the call ends. It is nil, with nothing held, when
// the call isn't gated: the caller isn't active on this node (it committed
// nothing here to wait for) or its setting is off.
func (s *Server) gateCaller(r *http.Request, caller string) (*gate, error) {
	ref := s.m.hold(caller)
	if ref == nil {
		return nil, nil
	}
	sync := syncSetting(ref, r.Header.Get(hdrSync))
	if !sync {
		ref.Release()
		return nil, nil
	}
	g := &gate{m: s.m, ref: ref, sync: true, ctx: r.Context()}
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
	for k := range pr.Out.Header {
		if strings.HasPrefix(http.CanonicalHeaderKey(k), hdrPrefix) {
			delete(pr.Out.Header, k)
		}
	}
	if info == nil {
		return
	}
	pr.Out.Header.Set(hdrDepth, strconv.Itoa(info.depth))
	if !info.deadline.IsZero() {
		// The time still left, so the app (and its own calls) stop when the node would.
		left := max(time.Until(info.deadline), time.Millisecond)
		pr.Out.Header.Set(hdrDeadline, strconv.FormatInt((left+time.Millisecond-1).Milliseconds(), 10))
		if pr.Out.Header.Get("Grpc-Timeout") != "" {
			pr.Out.Header.Set("Grpc-Timeout", strconv.FormatInt((left+time.Millisecond-1).Milliseconds(), 10)+"m")
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
	if info == nil || info.ref == nil {
		return nil // shardless: a plain reverse proxy
	}
	sync := syncSetting(info.ref, override)
	ctx := resp.Request.Context()
	// The status line and headers are the first chunk.
	if err := s.m.Gate(ctx, info.ref, sync); err != nil {
		return err
	}
	g := &gate{m: s.m, ref: info.ref, sync: sync, ctx: ctx, resp: resp}
	if rwc, ok := resp.Body.(io.ReadWriteCloser); ok && resp.StatusCode == http.StatusSwitchingProtocols {
		resp.Body = gatedConn{rwc, g, info.callerGate}
	} else {
		resp.Body = gatedBody{resp.Body, g}
	}
	return nil
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
	m    *Manager
	ref  *Ref
	sync bool
	ctx  context.Context
	resp *http.Response // the response being gated, nil for a caller's gate
}

// wait gates a chunk that was just read (n bytes, or the end of the body, whose
// trailers are output too).
func (g *gate) wait(n int, err error) error {
	if n > 0 || errors.Is(err, io.EOF) {
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

type gatedBody struct {
	io.ReadCloser
	g *gate
}

func (b gatedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	return b.g.read(n, err)
}

// gatedRequestBody is the body of a call, gated on the caller: each chunk the
// caller sends, and the end of the body with its trailers, reaches the callee
// only once what the caller committed before it is durable.
type gatedRequestBody struct {
	io.ReadCloser
	g *gate
}

func (b gatedRequestBody) Read(p []byte) (int, error) {
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
	s.fail(w, r, err)
}

// fail answers with the status an error maps to.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusBadGateway
	switch {
	case errors.Is(err, ErrUnknownKind), errors.Is(err, ErrBadTarget):
		status = http.StatusBadRequest
	case errors.Is(err, ErrBusy), errors.Is(err, ErrKindChanged):
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
	writeError(w, r, status, err.Error())
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
// gRPC client understands.
func writeError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	if isGRPC(r) {
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
	case !s.healthy.Load():
		return false, "the app is not healthy"
	case !s.m.opts.Kinds.Valid():
		return false, "the kinds file is not valid"
	case s.m.n.Keeper.Check() != nil:
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

// watchHealth polls the app's health endpoint.
func (s *Server) watchHealth(ctx context.Context) {
	client := &http.Client{Timeout: 2 * time.Second}
	u := *s.opts.App
	u.Path = strings.TrimRight(u.Path, "/") + s.opts.HealthPath
	check := func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			s.healthy.Store(false)
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			s.healthy.Store(false)
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		s.healthy.Store(resp.StatusCode/100 == 2)
	}
	check()
	t := time.NewTicker(s.opts.HealthEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
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
