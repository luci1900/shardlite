package node

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof" // served only on SHARDLITE_LISTEN_PPROF's own listener
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/dispatch"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/peers"
	"github.com/luci1900/shardlite/internal/service"
)

// RunOption changes how Run is assembled. Tests use them to swap parts the
// environment cannot name.
type RunOption func(*runOptions)

type runOptions struct {
	backend  blob.Backend
	grace    time.Duration // the servers' grace period, ServerOptions' default if zero
	public   net.Listener  // the public listener, in place of env.Public
	shardset func(*ShardsetOptions, *Shardset)
	faults   Faults
	appClock Clock // the declaration poll's clock, the real one if nil
}

// ErrAppLost is what Run returns after it stopped because the app was lost: it
// had answered the declaration poll, and then went appLostAfter without any
// HTTP answer. The stop is a graceful one (the same as for a context that ends),
// and the command exits non-zero, so that a supervisor restarts the node to wait
// for its app.
var ErrAppLost = errors.New("the app was lost: it stopped answering")

// WithBackend makes Run use b as the blob store in place of the S3 bucket of
// env.Blob (which is then not read), so tests can run the node on a backend of
// their own (SHARDLITE_BLOB=memory:// runs it on a plain blob.Memory).
// The prefix of env.Prefix still applies.
func WithBackend(b blob.Backend) RunOption {
	return func(o *runOptions) { o.backend = b }
}

// withGrace sets how long stopping the servers waits for requests in flight.
func withGrace(d time.Duration) RunOption {
	return func(o *runOptions) { o.grace = d }
}

// withPublicListener makes Run serve the public API on ln instead of listening
// on env.Public.
func withPublicListener(ln net.Listener) RunOption {
	return func(o *runOptions) { o.public = ln }
}

// withAppClock makes the declaration poll tick and tell time by c, so a test
// moves the 30 seconds an app has to be gone for instead of waiting them.
func withAppClock(c Clock) RunOption {
	return func(o *runOptions) { o.appClock = c }
}

// withShardset calls fn with the shardset's options before it is built (and nil),
// then again with the shardset, for tests that tune it or look inside.
func withShardset(fn func(*ShardsetOptions, *Shardset)) RunOption {
	return func(o *runOptions) { o.shardset = fn }
}

// Run is the node: it starts everything from env, serves until ctx ends or a
// part of the node fails, then lets go of its shards and gives up its lease
// (docs/internals.md, Concurrency: Shutdown). A stop that ctx asked for drains
// first (Shardset.Drain) and keeps the peer and control listeners up until the
// shards are let go of and the lease is released, so that a forward or call for a
// shard on its way out is answered by another node. A lost lease or a fenced writer stops
// everything at once and keeps the lease, and a failed node doesn't drain.
// Run returns nil after a shutdown that ctx asked for, and the failure otherwise.
// A node whose app is lost (ErrAppLost) stops as if ctx had ended, through the
// same path, and returns ErrAppLost. It returns only after every
// goroutine it started has ended. ready (if not nil) is called with the public,
// control and peer addresses once the node is serving.
func Run(ctx context.Context, env Env, log *slog.Logger, ready func(public, control, peer string), opts ...RunOption) error {
	var ro runOptions
	for _, o := range opts {
		o(&ro)
	}
	// A lost app ends this context, which is how a stop is asked for.
	ctx, stopAsked := context.WithCancelCause(ctx)
	defer stopAsked(nil)
	appURL, err := url.Parse(env.App)
	if err != nil || appURL.Host == "" {
		return fmt.Errorf("SHARDLITE_APP is %q, want the app's base URL such as http://127.0.0.1:9000", env.App)
	}
	kinds := NewKinds(env.IdleTimeout)

	var bucket string
	if ro.backend == nil && !env.inMemory() {
		var err error
		if bucket, err = env.blobBucket(); err != nil {
			return err
		}
	}
	// The data directory first: the check looks at the filesystem it is on.
	if err := os.MkdirAll(env.Data, 0o755); err != nil {
		return err
	}
	checks := defaultStartupChecks(log)
	if err := checks.refuse(env.Data); err != nil {
		return err
	}
	raw := ro.backend
	if raw == nil && env.inMemory() {
		log.Warn("storage is in memory: it is lost when the node stops", "blob", env.Blob)
		raw = blob.NewMemory()
	}
	if raw == nil {
		var err error
		if raw, err = blob.NewS3(ctx, blob.S3Options{Bucket: bucket, Endpoint: env.S3Endpoint, Logger: log}); err != nil {
			return err
		}
	}
	backend := blob.WithPrefix(raw, env.Prefix)
	if ro.faults.Backend != nil {
		backend = ro.faults.Backend(backend)
	}
	// The store must do what fencing relies on, before the node takes a lease or
	// adopts anything.
	if err := blob.CheckStore(ctx, backend, env.Name); err != nil {
		return err
	}
	checks.warn()
	// The peer listener is bound before the node starts, since the lease
	// publishes where other nodes reach it.
	peer, err := net.Listen("tcp", env.Peer)
	if err != nil {
		return err
	}
	address, err := peerAddress(env.Advertise, peer, upAddrs)
	if err != nil {
		peer.Close()
		return err
	}
	// The shardset empties it of what a crash left (cleanDataDir), once adoption is done.
	tempDir := filepath.Join(env.Data, tempDirName)

	expireAge := JournalExpiry(env.DeltaAge)
	n, err := Start(ctx, Config{
		Name: env.Name, Address: address, Backend: backend, Data: env.Data,
		Lease:   lease.RegistryOptions{TTL: env.LeaseTTL},
		Keeper:  lease.KeeperOptions{Interval: env.LeaseTTL / 3, Margin: env.LeaseTTL / 5, NoExpiry: ro.faults.NoLeaseExpiry},
		Logger:  log,
		TempDir: tempDir,
		Faults:  ro.faults,
	})
	if err != nil {
		peer.Close()
		return err
	}
	if n.Adopted.Deltas > 0 || len(n.Adopted.Lost) > 0 {
		log.Info("adopted the previous process's journal", "deltas", n.Adopted.Deltas, "lost", n.Adopted.Lost, "fence", n.Adopted.Fence)
	}
	// The stages are groups of services that Run starts and stops in order (see
	// below). The journal's stage starts now, so the lease is renewed while the
	// rest is built: cleaning the data directory can take a while on a large one.
	journal := service.New(context.Background(), log)
	journal.Add(n.Writer)
	journal.Add(n.Keeper)
	journal.Add(n.Confirmer)
	stopJournal := func() { stopStage(log, "the journal", journal) } // a failure before the other stages run
	// Every listener is bound before the shards are built, since binding is quick
	// and fails fast while cleaning the data directory can take a while. A listener only
	// queues connections until its server runs. Nothing after NewShardset can fail
	// before the services start, so a failure closes the listeners and the journal.
	lns := []net.Listener{peer}
	fail := func(err error) error {
		for _, ln := range lns {
			ln.Close()
		}
		stopJournal()
		return err
	}
	public := ro.public
	if public == nil {
		if public, err = net.Listen("tcp", env.Public); err != nil {
			return fail(err)
		}
	}
	lns = append(lns, public)
	control, err := net.Listen("tcp", env.Control)
	if err != nil {
		return fail(err)
	}
	lns = append(lns, control)
	// Diagnosis only: if it can't be bound, the node goes on without it.
	var pprof net.Listener
	if env.Pprof != "" {
		if pprof, err = net.Listen("tcp", env.Pprof); err != nil {
			log.Error("pprof listener failed, going on without it", "err", err)
			pprof = nil
		} else {
			lns = append(lns, pprof)
		}
	}

	var srv *Server
	so := ShardsetOptions{
		Kinds: kinds, Data: env.Data, Logger: log,
		Flush:     FlushOptions{Interval: env.Flush, Min: env.FlushMin},
		Lifecycle: LifecycleOptions{DeltaAge: env.DeltaAge, DeltaSize: env.DeltaSize, At: ro.faults.At, MaxFiles: ro.faults.MaxFiles},
		Leases:    peers.ScanOptions{TTL: env.LeaseTTL},
		Dispatch:  &dispatch.Options{},
	}
	if ro.shardset != nil {
		ro.shardset(&so, nil)
	}
	m, err := NewShardset(n, so, func(m *Shardset) routing {
		srv = NewServer(m, kinds, n.Keeper, ServerOptions{
			App: appURL, MaxHops: env.MaxHops, Grace: ro.grace, Logger: log,
			Clock: ro.appClock, AppLost: func() { stopAsked(ErrAppLost) },
		})
		return routing{wake: srv.wakeApp, call: srv.call}
	})
	if err != nil {
		return fail(err)
	}
	if ro.shardset != nil {
		ro.shardset(nil, m)
	}

	// Run's services (docs/internals.md, Concurrency). From here on everything the node runs
	// is in one of four stages, which Run stops one after another before it
	// returns. The journal's stage is already running, and everything else is
	// built and every listener bound before the other stages start.
	// The order of a shutdown is the order of the stages, so each outlives
	// what depends on it:
	//
	//	front   the public server and the services that depend on nothing
	//	callers the control server (the app's handlers can still make calls
	//	        until the shards are let go of) and the peer server (other
	//	        nodes' requests)
	//	shardset every request has been joined once both servers have ended
	//	journal the writer and the keeper, last
	//
	// A stop that was asked for moves callers to the very end: the shards are let
	// go of and the lease released with the control and peer servers still
	// serving, so that a forward or call for a shard on its way out is placed on
	// another node rather than met by a closed port. A request that holds a
	// shard keeps it from being let go of, so the letting go waits for it, and
	// stops these servers after their grace period if it still has not ended.
	//
	// A service that fails (its stage's Done), a lost lease or a fenced writer
	// ends the wait below, and Run then runs the stops with callers before shardset,
	// so that every request has been joined before the shards are let go of.
	// Each stage has a limit on how long its stop waits; the shards have their
	// own.
	shardsetClosed := make(chan error, 1)
	front := service.New(context.Background(), log)
	front.Add(srv.serverOn("public server", public, srv.PublicHandler()))
	front.Add(srv.declarations)
	front.Add(&expiry{journal: n.Journal, age: expireAge, log: log})
	if pprof != nil {
		front.Add(&httpServer{name: "pprof", log: log, ln: pprof, handler: http.DefaultServeMux, grace: ro.grace})
	}
	callers := service.New(context.Background(), log)
	callers.Add(srv.serverOn("control server", control, srv.ControlHandler()))
	callers.Add(srv.serverOn("peer server", peer, srv.PeerHandler()))
	var callersErr error
	stopCallers := sync.OnceFunc(func() { callersErr = stopStage(log, "the control and peer servers", callers) })
	shardset := service.New(context.Background(), log)
	// A request in flight on the control or peer server keeps its shard from being let go of, and one that
	// never ends (a stream, a long poll) would keep it for the whole of the letting go. So a stop that
	// was asked for gives such a request the servers' grace, and then stops those servers, which ends it.
	grace := cmp.Or(ro.grace, defaultGrace)
	m.heldAtStop = func(held time.Duration) {
		if held >= grace {
			stopCallers()
		}
	}
	shardset.Go("shardset", func(ctx context.Context) error {
		defer serving(log, "shardset")()
		shardsetClosed <- m.Serve(ctx) // it returns once ctx has ended and it has stopped
		return nil
	})

	log.Info("shardlite is up", "node", env.Name, "epoch", n.Keeper.Lease().Epoch, "public", public.Addr().String(), "control", control.Addr().String(), "peer", peer.Addr().String(), "address", address, "app", env.App)
	if ready != nil {
		ready(public.Addr().String(), control.Addr().String(), peer.Addr().String())
	}
	failed := true
	select {
	case <-ctx.Done():
		failed = false
	case <-front.Done():
	case <-callers.Done():
	case <-journal.Done():
	case <-n.Writer.Fenced():
	case <-n.Keeper.Lost():
	}
	cause := n.Err()
	if failed {
		log.Error("a part of the node failed, stopping the node", "err", cause)
	}

	// A stop that was asked for keeps the control and peer servers up until the lease is released.
	graceful := !failed && cause == nil
	if graceful {
		m.Drain()
	}

	log.Info("shutting down")
	if err := stopStage(log, "the public server and the services that depend on nothing", front); cause == nil {
		cause = err
	}
	if !graceful {
		stopCallers()
	}
	stopStage(log, "the shardset", shardset)
	stopStage(log, "the journal", journal)
	// The journal's state is read again, so a lost lease or a fenced writer that
	// came while the stages stopped is not missed.
	fatal := n.Err()
	if cause == nil {
		cause = fatal
	}

	var closeErr error
	select {
	case closeErr = <-shardsetClosed:
	default:
		closeErr = errors.New("the shardset did not stop")
	}
	if closeErr != nil {
		log.Error("some shards were not deactivated, keeping the lease", "err", closeErr)
	}
	// The lease is only given up once every shard is let go of: the node never
	// lets go of a shard the app still has open.
	if closeErr == nil && fatal == nil && n.Keeper.Check() == nil {
		rctx, cancel := context.WithTimeout(context.Background(), letGoDeadline)
		defer cancel()
		if err := n.Keeper.Release(rctx); err != nil {
			log.Error("releasing the lease", "err", err)
		}
	}
	stopCallers() // after the release, for a stop that was asked for
	if cause == nil {
		cause = callersErr
	}
	if cause == nil && errors.Is(context.Cause(ctx), ErrAppLost) {
		cause = ErrAppLost
	}
	return cause
}

const (
	// letGoDeadline is how long the shardset has to let go of every shard, from the
	// moment its own stop starts: a release that hasn't finished by then is
	// given up on and the lease is kept. Releasing the lease has as long.
	letGoDeadline = 30 * time.Second
	// stopTimeout is how long a stop waits for a group of services (a stage of
	// Run's, or a part of the shardset). It is longer than the deadline, so the
	// deadline is what ends a release and nothing is left running.
	stopTimeout = letGoDeadline + 5*time.Second
)

// stopStage stops one of Run's stages, waiting for it up to stopTimeout (the
// stage names a service that doesn't stop by then), and returns what its
// services failed with.
func stopStage(log *slog.Logger, what string, g *service.Group) error {
	ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()
	err := g.Stop(ctx)
	if err != nil {
		log.Error(what+" failed or did not stop in time", "err", err)
	}
	return err
}

// serving logs that a service of Run's has started, and returns the func that
// logs that it has stopped. The tests read the two lines to check the order of
// a shutdown.
func serving(log *slog.Logger, name string) (stopped func()) {
	log.Debug("service started", "service", name)
	return func() { log.Debug("service stopped", "service", name) }
}

// privateIPv4 are the ranges peerAddress accepts. They are Consul's rule for its
// own address (go-sockaddr's private IPv4: RFC 6890's special-purpose ranges less
// the ones that aren't forwardable, so loopback and link-local are out).
var privateIPv4 = []netip.Prefix{
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("192.88.99.0/24"),
}

// peerAddress is how other nodes reach the peer listener: the setting, or the
// listener's own address when that names a host. On a wildcard address it is the
// one private IPv4 address among the node's interfaces (as Consul finds its own),
// joined with the listener's port. With none or several it can't tell which
// other nodes can reach, so it refuses. There is no IPv6 rule: IPv6 deployments
// set the address.
func peerAddress(set string, peer net.Listener, interfaceAddrs func() ([]netip.Addr, error)) (string, error) {
	if set != "" {
		return set, nil
	}
	addr := peer.Addr().String()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	bound, err := netip.ParseAddr(host)
	if err == nil && !bound.IsUnspecified() {
		return addr, nil
	}
	all, err := interfaceAddrs()
	if err != nil {
		return "", fmt.Errorf("SHARDLITE_LISTEN_PEER listens on %s, which other nodes can't reach, and the node's addresses can't be listed (%w): set SHARDLITE_ADVERTISE_PEER to the host:port they should use", addr, err)
	}
	var found []string
	for _, ip := range all {
		ip = ip.Unmap()
		if ip.Is4() && slices.ContainsFunc(privateIPv4, func(p netip.Prefix) bool { return p.Contains(ip) }) {
			found = append(found, ip.String())
		}
	}
	switch len(found) {
	case 1:
		return net.JoinHostPort(found[0], port), nil
	case 0:
		return "", fmt.Errorf("SHARDLITE_LISTEN_PEER listens on %s, which other nodes can't reach, and it has no private IPv4 address: set SHARDLITE_ADVERTISE_PEER to the host:port they should use", addr)
	default:
		return "", fmt.Errorf("SHARDLITE_LISTEN_PEER listens on %s, which other nodes can't reach, and it has several private IPv4 addresses (%s): set SHARDLITE_ADVERTISE_PEER to the host:port they should use", addr, strings.Join(found, ", "))
	}
}

// upAddrs lists the addresses of the interfaces that are up.
func upAddrs() ([]netip.Addr, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var out []netip.Addr
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return nil, err
		}
		for _, a := range addrs {
			var ip net.IP
			if n, ok := a.(*net.IPNet); ok {
				ip = n.IP
			}
			if ip, ok := netip.AddrFromSlice(ip); ok {
				out = append(out, ip)
			}
		}
	}
	return out, nil
}
