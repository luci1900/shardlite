package node

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/model/cluster"
	"github.com/luci1900/shardlite/internal/model/shard"
	"github.com/luci1900/shardlite/internal/node/dispatch"
	"github.com/luci1900/shardlite/internal/storage/blob"
	"github.com/luci1900/shardlite/internal/storage/shardfile"
)

// reqInfo is what the proxy needs to know about a request, kept in its context.
type reqInfo struct {
	name       shard.Name // zero for a shardless request
	path       string
	caller     string     // "" on a client request
	callerName shard.Name // the calling shard, zero when the caller is the app
	hold       *Hold
	origin     origin // who asked, and whether another node forwarded it here
	// owner is the node that owns the target when that isn't this one, and peer
	// the address the request is forwarded to.
	owner cluster.Node
	peer  string
	// moved is set when the owner answered a forward with 421 (the shard isn't
	// its own), and movedTo is the owner it named: empty if it named none.
	moved   bool
	movedTo cluster.Node
	// answer is what the forward in progress has heard from the owner's node
	// (answerAwaited, answerHeard or answerLate), and unreachable is set when it
	// failed before the owner's node answered.
	answer      atomic.Int32
	unreachable bool

	hops int64 // sent to the app in Shardlite-Context
	// gates counts the request's gates in progress (see route), and held is
	// what the request holds until it ends.
	gates inFlight
	held  []*Hold
}

type ctxKey struct{}

// route works out the shard, activates it and proxies. o says who asked: the
// handlers set it from the request, and the node's own requests (origin.go) say
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
			s.fail(w, r, err)
			return
		}
		name = n
	}
	info := &reqInfo{name: name, origin: o}
	activate := o.kind == fromNode && r.URL.Path == dispatch.Activate
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
		s.fail(w, r, err)
		return
	}
	if !info.name.IsZero() {
		if err := s.checkTarget(r, o, info); err != nil {
			s.fail(w, r, err)
			return
		}
		if err := s.holdShards(r, o, info); err != nil {
			s.fail(w, r, err)
			return
		}
		if info.owner != "" {
			s.forward(w, r, info)
			return
		}
		if activate {
			// The node's own dispatch.Activate request. Only a node sends one (the
			// app can't reach a /_shardlite/ path). It routes as any call does and
			// answers 200 if it activated the shard and 204 if it was active already.
			if info.hold.started {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
			return
		}
	}
	s.proxy.ServeHTTP(noInformational{w}, r.WithContext(context.WithValue(r.Context(), ctxKey{}, info)))
}

// parseCall reads what a call says about itself into info: its caller and its
// hops. A call's context (Shardlite-Context, which the node gave the request the
// call was made from) names the caller and the hops so far, and the call is one
// more. A call with none is from the app, at hop 1. A forward carries the context
// of the call it forwards, so the owner's node reads it as the control API does,
// and the node's own wake of the app, which is not a call over the control API,
// names its caller on the request. The hops are refused before anything is gated
// or activated.
func (s *Server) parseCall(r *http.Request, o origin, info *reqInfo) error {
	if !o.call() {
		return nil
	}
	if info.name.IsZero() {
		return &statusError{http.StatusBadRequest, reasonBadRequest, "a call needs a Shardlite-Target"}
	}
	if o.kind == fromWake {
		n, err := ParseTarget(r.Header.Get(hdrCaller))
		if err != nil {
			return &statusError{http.StatusBadRequest, reasonBadRequest, "bad Shardlite-Caller"}
		}
		info.caller, info.callerName = n.String(), n
	} else {
		c, ok, err := parseContext(r.Header)
		switch {
		case err != nil:
			return badContext(err)
		case !ok:
			info.caller, info.hops = callerApp, 1
		default:
			if c.hops >= int64(s.opts.MaxHops) {
				s.log.Warn("call refused: calls are chained too far", "caller", c.shard.String(), "target", info.name, "hops", c.hops+1, "max", s.opts.MaxHops)
				return &statusError{http.StatusLoopDetected, reasonHops, fmt.Sprintf("calls are chained more than %d hops", s.opts.MaxHops)}
			}
			info.caller, info.callerName, info.hops = c.shard.String(), c.shard, c.hops+1
		}
	}
	// What a calling shard sends is gated on its commits, and an upgraded
	// connection would need every write gated for as long as it lasts. Streams
	// carry the same traffic, gated as any call's body is.
	if !info.callerName.IsZero() && r.Header.Get("Upgrade") != "" {
		return &statusError{http.StatusBadRequest, reasonBadRequest, "a call from a shard can't be upgraded: use a streaming request (server-sent events or a gRPC stream)"}
	}
	return nil
}

// checkTarget refuses a target a client may not name, and a call that wants
// only shards that are already active when the target isn't active here. If
// another node owns it, it may be active there, and the call goes to it.
func (s *Server) checkTarget(r *http.Request, o origin, info *reqInfo) error {
	if o.client() {
		// A kind that isn't public gets the same error as an undeclared one, so
		// clients can't tell it exists. Only the log says which it was.
		if c, err := s.kinds.Lookup(info.name.Kind()); err == nil && !c.Public {
			s.log.Info(`a client named a kind that is not public (set "public": true in the declaration to allow it)`, "target", info.name)
			return fmt.Errorf("%w: %q", ErrUnknownKind, info.name.Kind())
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
		return &statusError{http.StatusNotFound, reasonNotActive, "shard is not active"}
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
	// call: the caller's node gates it.
	if o.call() && info.caller != callerApp && o.kind != fromWake && !o.forwarded {
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
	// Clients never create a shard, and a call can ask not to
	// (Shardlite-If-Exists). Shardlite-If-Active is the same ask, made before the
	// table is looked at.
	existing := o.client() || (o.call() && strings.EqualFold(r.Header.Get(hdrIfExists), "true"))
	// A call from a shard may claim its target on another node, picked at random: the
	// caller's node is wherever the caller happens to live, so it is no reason to
	// put the callee there. So may the node's own request, which the dispatcher
	// sends for a due shard that nobody owns: the shard belongs on any node,
	// not on whichever node ranks first for its lane. A client's request has its session
	// on this node, as does a call with no shard behind it (a login), and the shard
	// belongs here. A wake is for the node it is made on, and a forward has been
	// placed already.
	anywhere := (o.kind == fromNode || (o.kind == fromCall && !info.callerName.IsZero())) && !o.forwarded
	// A client's request forwarded here for a shard that nobody owns went by the forwarding
	// node's owner cache, which is stale: whoever owned the shard let it go (at its idle timeout
	// or at a stop) and the cache still names it. The client's session is on the forwarding
	// node, so the shard is claimed there: the only forwards this node claims for are the ones
	// a draining node placed (they carry Shardlite-Placement: avoid).
	var sendBack cluster.Node
	if o.forwarded && o.client() && o.from != "" && !o.avoid {
		sendBack = o.from
	}
	hold, err := s.m.take(r.Context(), info.name, acquireOpts{existing: existing, anywhere: anywhere, sendBack: sendBack})
	if errors.Is(err, errNoShard) {
		// The same answer as a kind the client may not name, so it says nothing of
		// kinds or ids. A flood of misses must not be a flood of log lines. Any
		// other object found missing is a fault, answered and logged as one.
		s.log.Debug("no shard to reach: it does not exist, and this request may not create it", "target", info.name)
		return fmt.Errorf("%w: %q", ErrUnknownKind, info.name.Kind())
	}
	if node, ok := forwardTo(err); ok && o.kind != fromWake {
		info.owner = node
		return nil
	}
	if errors.Is(err, shardfile.ErrOwnerNotAdopted) {
		// The owner's lease has expired, or it restarted, so there is nothing to
		// wait for but the adoption.
		return &unavailable{reasonOwnerUnreachable, err.Error(), time.Second}
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
