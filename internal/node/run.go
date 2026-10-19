package node

import (
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
	"time"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/service"
	"github.com/luci1900/shardlite/internal/system/dispatch"
	"github.com/luci1900/shardlite/internal/system/sweep"
)

// RunOption changes how Run is assembled. Tests use them to swap parts the
// environment cannot name.
type RunOption func(*runOptions)

type runOptions struct {
	backend blob.Backend
	grace   time.Duration // the servers' grace period, ServerOptions' default if zero
	public  net.Listener  // the public listener, in place of env.Public
	shards  func(*CatalogOptions, *Catalog)
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

// withShards calls fn with the shards' options before they are built (and nil),
// then again with the shards, for tests that tune them or look inside.
func withShards(fn func(*CatalogOptions, *Catalog)) RunOption {
	return func(o *runOptions) { o.shards = fn }
}

// Run is the node: it starts everything from env, serves until ctx ends or a
// part of the node fails, then lets go of its shards and gives up its lease
// (docs/concurrency.md, Shutdown). A lost lease or a fenced writer stops
// everything at once and keeps the lease. Run returns nil after a shutdown
// that ctx asked for, and the failure otherwise. It returns only after every
// goroutine it started has ended. ready (if not nil) is called with the public,
// control and peer addresses once the node is serving.
func Run(ctx context.Context, env Env, log *slog.Logger, ready func(public, control, peer string), opts ...RunOption) error {
	var ro runOptions
	for _, o := range opts {
		o(&ro)
	}
	app, err := url.Parse(env.App)
	if err != nil || env.App == "" || app.Host == "" {
		return fmt.Errorf("SHARDLITE_APP is %q, want the app's base URL such as http://127.0.0.1:9000", env.App)
	}
	kinds, err := LoadKinds(env.KindsFile, env.IdleTimeout)
	if err != nil {
		return err
	}

	var bucket string
	if ro.backend == nil {
		var err error
		if bucket, err = env.blobBucket(); err != nil {
			return err
		}
	}
	// Directories first: the checks look at the filesystems they are on.
	for _, d := range []string{env.Data, env.Scratch} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	checks := defaultStartupChecks(log)
	if err := checks.refuse(env.Data); err != nil {
		return err
	}
	raw := ro.backend
	if raw == nil {
		var err error
		if raw, err = blob.NewS3(ctx, blob.S3Options{Bucket: bucket, Endpoint: env.S3Endpoint}); err != nil {
			return err
		}
	}
	backend := blob.WithPrefix(raw, env.Prefix)
	// The store must do what fencing relies on, before the node takes a lease or
	// adopts anything.
	if err := blob.CheckStore(ctx, backend, env.Name); err != nil {
		return err
	}
	checks.warn(env.Scratch)
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
	// The catalog empties it of what a crash left (clearCache), once adoption is done.
	tempDir := filepath.Join(env.Data, tempDirName)

	expireAge := JournalExpiry(env.DeltaAge)
	n, err := Start(ctx, Config{
		Name: env.Name, Address: address, Backend: backend,
		Lease:   coord.RegistryOptions{TTL: env.LeaseTTL},
		Keeper:  coord.KeeperOptions{Interval: env.LeaseTTL / 3, Margin: env.LeaseTTL / 5},
		Logger:  log,
		TempDir: tempDir,
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
	// rest is built: opening the cache can take a while on a large data directory.
	journal := service.New(context.Background(), log)
	journal.Add(n.Writer)
	journal.Add(n.Keeper)
	stopJournal := func() { stopStage(log, "the journal", journal) } // a failure before the other stages run
	// Every listener is bound before the shards are built, since binding is quick
	// and fails fast while opening the cache can take a while. A listener only
	// queues connections until its server runs. Nothing after NewShards can fail
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
	so := CatalogOptions{
		Kinds: kinds, Data: env.Data, Scratch: env.Scratch, Logger: log,
		Cache:     CacheOptions{Limit: env.CacheLimit},
		Flush:     FlushOptions{Interval: env.Flush, Min: env.FlushMin},
		Lifecycle: LifecycleOptions{DeltaAge: env.DeltaAge, DeltaSize: env.DeltaSize},
		Leases:    LeaseScanOptions{TTL: env.LeaseTTL},
		Dispatch:  &dispatch.Options{},
		// The system kinds (docs/internals.md, System kinds), with what each is given
		// of the node's.
		System: []kind.Kind{
			sweep.New(n.Store, n.Keeper.Check, time.Now, log, sweep.Options{Age: expireAge}),
		},
	}
	if ro.shards != nil {
		ro.shards(&so, nil)
	}
	m, err := NewCatalog(n, so, func(m *Catalog) routing {
		srv = NewServer(m, kinds, n.Keeper, ServerOptions{App: app, HealthPath: env.HealthPath, MaxHops: env.MaxHops, Grace: ro.grace, Logger: log})
		return routing{wake: srv.wakeApp, dispatch: srv.dispatch, call: srv.call}
	})
	if err != nil {
		return fail(err)
	}
	if ro.shards != nil {
		ro.shards(nil, m)
	}

	// Run's services (docs/concurrency.md). From here on everything the node runs
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
	//	shards  every request has been joined once both servers have ended
	//	journal the writer and the keeper, last
	//
	// A service that fails (its stage's Done), a lost lease or a fenced writer
	// ends the wait below, and Run then runs the same stops as for a shutdown.
	// Each stage has a limit on how long its stop waits; the shards have their
	// own.
	shardsClosed := make(chan error, 1)
	front := service.New(context.Background(), log)
	front.Add(srv.publicServer(public))
	front.Add(srv.health)
	front.Add(&expiry{store: n.Store, node: n.Name, age: expireAge, log: log})
	front.Add(&kindsReload{kinds, log})
	if pprof != nil {
		front.Add(&httpServer{name: "pprof", log: log, lns: []net.Listener{pprof}, handler: http.DefaultServeMux, grace: ro.grace})
	}
	callers := service.New(context.Background(), log)
	callers.Add(srv.controlServer(control, socket))
	callers.Add(srv.peerServer(peer, env.Name))
	shards := service.New(context.Background(), log)
	shards.Go("catalog", func(ctx context.Context) error {
		defer serving(log, "catalog")()
		shardsClosed <- m.Serve(ctx) // it returns once ctx has ended and it has stopped
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

	log.Info("shutting down")
	if err := stopStage(log, "the public server and the services that depend on nothing", front); cause == nil {
		cause = err
	}
	if err := stopStage(log, "the control and peer servers", callers); cause == nil {
		cause = err
	}
	stopStage(log, "the shards", shards)
	stopStage(log, "the journal", journal)
	// The journal's state is read again, so a lost lease or a fenced writer that
	// came while the stages stopped is not missed.
	fatal := n.Err()
	if cause == nil {
		cause = fatal
	}

	var closeErr error
	select {
	case closeErr = <-shardsClosed:
	default:
		closeErr = errors.New("the shards did not stop")
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
	return cause
}

const (
	// letGoDeadline is how long the catalog has to let go of every shard, from the
	// moment its own stop starts: a release that hasn't finished by then is
	// given up on and the lease is kept. Releasing the lease has as long.
	letGoDeadline = 30 * time.Second
	// stopTimeout is how long a stop waits for a group of services (a stage of
	// Run's, or a part of the catalog's). It is longer than the deadline, so the
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
