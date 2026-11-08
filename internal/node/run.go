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
	"sync"
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/lease"
	"github.com/luci1900/shardlite/internal/reconcile"
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
}

// WithBackend makes Run use b as the blob store in place of the S3 bucket of
// env.Blob (which is then not read), so tests can run the node on blob.Memory.
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
// Run returns nil after a shutdown that ctx asked for, and the failure otherwise. It returns only after every
// goroutine it started has ended. ready (if not nil) is called with the public,
// control and peer addresses once the node is serving.
func Run(ctx context.Context, env Env, log *slog.Logger, ready func(public, control, peer string), opts ...RunOption) error {
	var ro runOptions
	for _, o := range opts {
		o(&ro)
	}
	appURL, err := url.Parse(env.App)
	if err != nil || appURL.Host == "" {
		return fmt.Errorf("SHARDLITE_APP is %q, want the app's base URL such as http://127.0.0.1:9000", env.App)
	}
	kinds := NewKinds(env.IdleTimeout)

	var bucket string
	if ro.backend == nil {
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
	clock := lease.NewClockWatch(log)
	raw := ro.backend
	if raw == nil {
		var err error
		if raw, err = blob.NewS3(ctx, blob.S3Options{Bucket: bucket, Endpoint: env.S3Endpoint, ClockSink: clock.Sink}); err != nil {
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
	address, err := peerAddress(env.Advertise, peer)
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
		Keeper:  lease.KeeperOptions{Interval: env.LeaseTTL / 3, Margin: env.LeaseTTL / 5},
		Logger:  log,
		Clock:   clock,
		TempDir: tempDir,
		Faults:  ro.faults,
		// Orphans are left alone for as long as the journal keeps its segments: past that a
		// delta whose manifest update still fails has lost its tail anyway.
		OrphanAge: expireAge,
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
	var socket net.Listener
	if env.Socket != "" {
		os.Remove(env.Socket)
		if socket, err = net.Listen("unix", env.Socket); err != nil {
			return fail(err)
		}
		lns = append(lns, socket)
	}
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
		Leases:    LeaseScanOptions{TTL: env.LeaseTTL},
		Dispatch:  &reconcile.Options{},
		Collect:   &reconcile.Options{},
	}
	if ro.shardset != nil {
		ro.shardset(&so, nil)
	}
	m, err := NewShardset(n, so, func(m *Shardset) routing {
		srv = NewServer(m, kinds, n.Keeper, ServerOptions{App: appURL, MaxHops: env.MaxHops, Grace: ro.grace, Logger: log})
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
	front.Add(srv.publicServer(public))
	if srv.declarations != nil {
		front.Add(srv.declarations)
	}
	front.Add(&expiry{store: n.Store, node: n.Name, age: expireAge, log: log})
	if pprof != nil {
		front.Add(&httpServer{name: "pprof", log: log, lns: []net.Listener{pprof}, handler: http.DefaultServeMux, grace: ro.grace})
	}
	callers := service.New(context.Background(), log)
	callers.Add(srv.controlServer(control, socket))
	callers.Add(srv.peerServer(peer))
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

// peerAddress is how other nodes reach the peer listener: the setting, or the
// listener's own address when that names a host.
func peerAddress(set string, peer net.Listener) (string, error) {
	if set != "" {
		return set, nil
	}
	addr := peer.Addr().String()
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	if ip, err := netip.ParseAddr(host); err != nil || ip.IsUnspecified() {
		return "", fmt.Errorf("SHARDLITE_LISTEN_PEER listens on %s, which other nodes can't reach: set SHARDLITE_ADVERTISE_PEER to the host:port they should use", addr)
	}
	return addr, nil
}
