package node

import (
	"context"
	"errors"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

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
	if info.peer != "" {
		s.m.placer.StampNode(pr.Out.Header)
		s.m.placer.StampPlacement(pr.Out.Header)
		origin := originCall
		switch {
		case info.public:
			origin = originClient
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
		// A call's context goes on unchanged, for the owner's node to read as this one
		// did. A client's has none: the public listener never reads one.
		if v := pr.In.Header.Values(hdrContext); len(v) > 0 && !info.public {
			pr.Out.Header[hdrContext] = v
		}
	}
	if !info.name.IsZero() {
		if info.peer == "" {
			// To the app it is what the node opened, not what the request is for.
			pr.Out.Header.Set(hdrOpened, info.name.String())
			pr.Out.Header.Set(hdrFile, info.path)
			ctx := callContext{shard: info.name, hops: info.hops}
			if v, err := ctx.format(); err != nil {
				s.log.Error("could not write the context of a request", "shard", info.name, "err", err)
			} else {
				pr.Out.Header.Set(hdrContext, v)
			}
		} else {
			// To the owner's node it is the chosen target.
			pr.Out.Header.Set(hdrTarget, info.name.String())
		}
	}
	if info.caller != "" && info.peer == "" {
		pr.Out.Header.Set(hdrCaller, info.caller)
	}
	switch {
	case info.forwarded:
		// The forwarding node added the client: the app sees what it would have seen
		// there. The control API is local and the app is trusted, so the node adds
		// nothing of its own (the app's address would only lengthen the chain).
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

// proxyError ends a request to the app that failed in the proxy.
func (s *Server) proxyError(w http.ResponseWriter, r *http.Request, err error) {
	s.upstreamError(w, r, err, reasonAppFailed, reasonAppUnreachable)
}

// upstreamError answers a proxy failure with the reason the upstream (the app or
// the owner's node) gets: failed once the request may have reached it, unreached
// if the dial to it failed.
func (s *Server) upstreamError(w http.ResponseWriter, r *http.Request, err error, failed, unreached reason) {
	if errors.Is(err, context.Canceled) && r.Context().Err() != nil {
		return // the client went away
	}
	why := failed
	if neverReached(err) {
		why = unreached
	}
	info, _ := r.Context().Value(ctxKey{}).(*reqInfo)
	s.fail(w, r, info != nil && info.own, &reasonError{why, err})
}
