package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof" // served only on SHARDLITE_PPROF's own listener
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/thejerf/suture/v4"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
	"github.com/luci1900/shardlite/internal/supervisor"
)

// RunOption changes how Run is assembled. Tests use them to swap parts the
// environment cannot name.
type RunOption func(*runOptions)

type runOptions struct {
	backend blob.Backend
	grace   time.Duration // the servers' grace period, ServerOptions' default if zero
	public  net.Listener  // the public listener, in place of env.Listen
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
// on env.Listen.
func withPublicListener(ln net.Listener) RunOption {
	return func(o *runOptions) { o.public = ln }
}

// Run is the node: it starts everything from env, serves until ctx ends or a
// part of the node fails, then lets go of its shards and gives up its lease
// (docs/concurrency.md, Shutdown). A lost lease or a fenced writer stops
// everything at once and keeps the lease. Run returns nil after a shutdown
// that ctx asked for, and the failure otherwise. It returns only after every
// goroutine it started has ended. ready (if not nil) is called with the public
// and control addresses once the node is serving.
func Run(ctx context.Context, env Env, log *slog.Logger, ready func(public, control string), opts ...RunOption) error {
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
	// Shards empties it of what a crash left (clearCache), once adoption is done.
	tempDir := filepath.Join(env.Data, tempDirName)

	expiry := JournalExpiry(env.DeltaAge)
	n, err := Start(ctx, Config{
		Name: env.Name, Address: env.Address, Backend: backend,
		Lease:   coord.RegistryOptions{TTL: env.LeaseTTL},
		Keeper:  coord.KeeperOptions{Interval: env.LeaseTTL / 3, Margin: env.LeaseTTL / 5},
		Logger:  log,
		TempDir: tempDir,
	})
	if err != nil {
		return err
	}
	if n.Adopted.Deltas > 0 || len(n.Adopted.Lost) > 0 {
		log.Info("adopted the previous process's journal", "deltas", n.Adopted.Deltas, "lost", n.Adopted.Lost, "fence", n.Adopted.Fence)
	}
	// The stages are children of one supervisor, served until Run returns (see
	// below). The journal's stage starts now, so the lease is renewed while the
	// rest is built: opening the cache can take a while on a large data directory.
	failed := make(chan error, 1) // the first failure of a service
	spec := supervisor.Spec(log, stopTimeout)
	journal := suture.New("journal", spec)
	journal.Add(n.Writer)
	journal.Add(n.Keeper)
	node := suture.New("node", spec)
	journalToken := node.Add(journal)
	nodeCtx, stopNode := context.WithCancel(context.Background())
	served := node.ServeBackground(nodeCtx)
	stopJournal := func() { // a failure before the other stages run
		stopNode()
		<-served
	}
	// Every listener is bound before the shards are built, since binding is quick
	// and fails fast while opening the cache can take a while. A listener only
	// queues connections until its server runs. Nothing after NewShards can fail
	// before the services start, so a failure closes the listeners and the journal.
	var lns []net.Listener
	fail := func(err error) error {
		for _, ln := range lns {
			ln.Close()
		}
		stopJournal()
		return err
	}
	public := ro.public
	if public == nil {
		if public, err = net.Listen("tcp", env.Listen); err != nil {
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
	m, err := NewShards(n, ShardsOptions{
		Kinds: kinds, Data: env.Data, Scratch: env.Scratch, Logger: log,
		Cache:     CacheOptions{Limit: env.CacheLimit},
		Flush:     FlushOptions{Interval: env.Flush, Min: env.FlushMin},
		Lifecycle: LifecycleOptions{DeltaAge: env.DeltaAge, DeltaSize: env.DeltaSize},
	}, func(m *Shards) *transport {
		srv = NewServer(m, kinds, n.Keeper, ServerOptions{App: app, HealthPath: env.HealthPath, MaxHops: env.MaxHops, Grace: ro.grace, Logger: log})
		return srv.transport()
	})
	if err != nil {
		return fail(err)
	}

	// Run's services (docs/concurrency.md). From here on everything the node runs
	// is in one of four stages, which Run stops one after another before it
	// returns. The journal's stage is already running, and everything else is
	// built and every listener bound before the other stages start. A stage with
	// one service has no supervisor of its own: the service sits under node.
	// The order of a shutdown is the order of the stages, so each outlives
	// what depends on it:
	//
	//	front   the public server and the services that depend on nothing
	//	drain   not a service: Run delivers the live messages
	//	control the control server (the app's handlers can still make calls
	//	        while the drain runs and until the shards are let go of)
	//	shards  every request has been joined once both servers have ended
	//	journal the writer and the keeper, last
	//
	// A service that fails, a lost lease or a fenced writer tells Run, which then
	// runs the same stops as for a shutdown. Each stage has a limit on how long
	// its stop waits; the drain and the shards have their own.
	shardsClosed := make(chan error, 1)
	front := suture.New("front", spec)
	front.Add(srv.publicServer(public, failed))
	front.Add(srv.health)
	front.Add(&sweeper{n.Store, n.Name, expiry, log})
	front.Add(&kindsReload{kinds, log})
	if pprof != nil {
		front.Add(&httpServer{name: "pprof", log: log, lns: []net.Listener{pprof}, handler: http.DefaultServeMux, grace: ro.grace})
	}
	// Run removes the stages one at a time to stop them in order, and
	// RemoveAndWait returns once the stage has ended. It waits longer than the
	// stage does, so the stage's own timeout reports first.
	frontToken := node.Add(front)
	controlToken := node.Add(srv.controlServer(failed, control, socket))
	shardsToken := node.Add(shardsService{m, shardsClosed, log})
	wait := 2 * stopTimeout

	log.Info("shardlite is up", "node", env.Name, "epoch", n.Keeper.Lease().Epoch, "public", public.Addr().String(), "control", control.Addr().String(), "app", env.App)
	if ready != nil {
		ready(public.Addr().String(), control.Addr().String())
	}
	var cause error
	select {
	case <-ctx.Done():
	case cause = <-failed:
	case <-n.Writer.Fenced():
		cause = n.Err()
	case <-n.Keeper.Lost():
		cause = n.Err()
	}
	if cause != nil {
		log.Error("a part of the node failed, stopping the node", "err", cause)
	}

	log.Info("shutting down")
	if err := node.RemoveAndWait(frontToken, wait); err != nil {
		log.Error("the public server and the services that depend on nothing did not stop in time", "err", err)
	}
	// Messages that are still live get a few seconds to go out before the shards
	// are let go of (the rest get a wake hint as each shard is).
	log.Debug("service started", "service", "drain")
	m.Drain(context.Background()) // bounded by ShardsOptions.Drain
	log.Debug("service stopped", "service", "drain")
	if err := node.RemoveAndWait(controlToken, wait); err != nil {
		log.Error("the control server did not stop in time", "err", err)
	}
	if err := node.RemoveAndWait(shardsToken, wait); err != nil {
		log.Error("the shards did not stop in time", "err", err)
	}
	if err := node.RemoveAndWait(journalToken, wait); err != nil {
		log.Error("the journal did not stop in time", "err", err)
	}
	stopNode()
	<-served
	// failed holds the first failure only. The journal's state is read directly,
	// so a fatal error that came later, or while the stages stopped, is not
	// missed.
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
	// letGoDeadline is how long Shards has to let go of every shard, from the
	// moment its own stop starts: a release that hasn't finished by then is
	// given up on and the lease is kept. Releasing the lease has as long.
	letGoDeadline = 30 * time.Second
	// stopTimeout is how long the supervisors inside Shards wait for their
	// services to stop. It is longer than the deadline, so the deadline is what
	// ends a release and nothing is left running.
	stopTimeout = letGoDeadline + 5*time.Second
)

// The services Run serves, each a struct with a Serve and a String, as suture
// wants them. A service says for itself whether it can be restarted: none can,
// so each returns supervisor.NoRestart when it ends. One that fails reports the
// error to Run (see report) and ends the same way: Run stops the node in order
// then, and returning the error to suture would stop everything at once.

// report tells Run that a service failed. Run needs the first failure only, and
// a service never waits to report one.
func report(failed chan<- error, err error) {
	select {
	case failed <- err:
	default:
	}
}

// serving logs that a service of Run's has started, and returns the func that
// logs that it has stopped. The tests read the two lines to check the order of
// a shutdown.
func serving(log *slog.Logger, name string) (stopped func()) {
	log.Debug("service started", "service", name)
	return func() { log.Debug("service stopped", "service", name) }
}

// shardsService runs the shards, and sends why some could not be let go of
// (nil if all were) to closed, which decides the lease's fate.
type shardsService struct {
	m      *Shards
	closed chan<- error
	log    *slog.Logger
}

func (shardsService) String() string { return "shards" }
func (s shardsService) Serve(ctx context.Context) error {
	defer serving(s.log, s.String())()
	s.closed <- s.m.Serve(ctx) // it returns once ctx has ended and it has stopped
	return supervisor.NoRestart(nil)
}
