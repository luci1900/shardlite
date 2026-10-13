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

	"golang.org/x/sync/errgroup"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
)

// Run is the node: it starts everything from env, serves until ctx ends or the
// node's lease is lost, then lets go of its shards and gives up its lease. It
// returns only after every goroutine it started has ended. ready (if not nil)
// is called with the public and control addresses once the node is serving.
func Run(ctx context.Context, env Env, log *slog.Logger, ready func(public, control string)) error {
	app, err := url.Parse(env.App)
	if err != nil || env.App == "" || app.Host == "" {
		return fmt.Errorf("SHARDLITE_APP is %q, want the app's base URL such as http://127.0.0.1:9000", env.App)
	}
	kinds, err := LoadKinds(env.KindsFile, env.IdleTimeout)
	if err != nil {
		return err
	}
	for _, name := range env.Ignored {
		log.Warn(name+" is set but no longer a setting, so it is ignored (the tail alert and journal expiry follow SHARDLITE_DELTA_AGE)", "variable", name)
	}

	bucket, err := env.blobBucket()
	if err != nil {
		return err
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
	raw, err := blob.NewS3(ctx, blob.S3Options{Bucket: bucket, Endpoint: env.S3Endpoint})
	if err != nil {
		return err
	}
	backend := blob.WithPrefix(raw, env.Prefix)
	// The store must do what fencing relies on, before the node takes a lease or
	// adopts anything.
	if err := blob.CheckStore(ctx, backend, env.Name); err != nil {
		return err
	}
	checks.warn(env.Scratch)
	// Whatever a crash left of deltas, snapshots and downloads.
	tempDir := filepath.Join(env.Data, tempDirName)
	if err := os.RemoveAll(tempDir); err != nil {
		return err
	}
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		return err
	}

	n, err := Start(ctx, Config{
		Name: env.Name, Address: env.Address, Backend: backend,
		Flush: env.Flush, ManualFlush: true,
		Lease:   coord.RegistryOptions{TTL: env.LeaseTTL},
		Keeper:  coord.KeeperOptions{Interval: env.LeaseTTL / 3, Margin: env.LeaseTTL / 5},
		Logger:  log,
		TempDir: tempDir,
	})
	if err != nil {
		return err
	}
	// Expiry starts only now: a node that was down longer than the expiry age
	// has adopted its old segments before any of them can be deleted.
	if err := n.StartJournalSweeper(JournalExpiry(env.DeltaAge)); err != nil {
		n.Close(context.Background(), false)
		return err
	}
	if n.Adopted.Deltas > 0 || len(n.Adopted.Lost) > 0 {
		log.Info("adopted the previous process's journal", "deltas", n.Adopted.Deltas, "lost", n.Adopted.Lost, "fence", n.Adopted.Fence)
	}
	m, err := NewManager(n, ManagerOptions{
		Kinds: kinds, Data: env.Data, Scratch: env.Scratch, CacheLimit: env.CacheLimit, Flush: env.Flush, FlushMin: env.FlushMin, DeltaAge: env.DeltaAge, DeltaSize: env.DeltaSize, Logger: log,
	})
	if err != nil {
		return err
	}
	srv := NewServer(m, ServerOptions{App: app, HealthPath: env.HealthPath, MaxHops: env.MaxHops, Logger: log})
	abort := func(err error) error { // a failure before serving: stop what is running
		srv.Close()
		m.Close(context.Background())
		n.Close(context.Background(), false)
		return err
	}

	public, err := net.Listen("tcp", env.Listen)
	if err != nil {
		return abort(err)
	}
	control, err := net.Listen("tcp", env.Control)
	if err != nil {
		return abort(err)
	}
	var socket net.Listener
	if env.Socket != "" {
		os.Remove(env.Socket)
		if socket, err = net.Listen("unix", env.Socket); err != nil {
			return abort(err)
		}
	}
	log.Info("shardlite is up", "node", env.Name, "epoch", n.Keeper.Lease().Epoch, "public", public.Addr().String(), "control", control.Addr().String(), "app", env.App)
	if ready != nil {
		ready(public.Addr().String(), control.Addr().String())
	}

	// Everything that runs for the life of the node is a child of g, and g is
	// joined before the node shuts down. A child that fails ends the others.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		kinds.RunReloadOnSIGHUP(gctx, log)
		return nil
	})
	if env.Pprof != "" {
		ps := &http.Server{Addr: env.Pprof}
		g.Go(func() error {
			if err := ps.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				log.Error("pprof listener ended", "err", err) // diagnosis only: the node goes on
			}
			return nil
		})
		g.Go(func() error {
			<-gctx.Done()
			return ps.Close()
		})
	}
	// The lease being lost ends the node: it must not go on acting on shards it may no longer own.
	g.Go(func() error {
		select {
		case <-n.Keeper.Lost():
			err := n.Keeper.Check()
			log.Error("the node's lease is gone, stopping", "err", err)
			return err
		case <-gctx.Done():
			return nil
		}
	})
	g.Go(func() error { return srv.Serve(gctx, public, nil, nil) })
	// The control API outlives the public listener: while the node drains, the
	// app's handlers still make calls through it.
	ctlCtx, stopControl := context.WithCancel(context.Background())
	defer stopControl()
	var cg errgroup.Group
	ctlDone := make(chan struct{})
	var ctlErr error
	cg.Go(func() error {
		defer close(ctlDone)
		ctlErr = srv.Serve(ctlCtx, nil, control, socket)
		return nil
	})
	g.Go(func() error { // a control listener that fails ends the node
		select {
		case <-ctlDone:
			return ctlErr
		case <-gctx.Done():
			return nil
		}
	})
	serveErr := g.Wait()

	// Shutdown: stop taking requests (done), let go of the shards, give up the lease.
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Messages that are still live get a few seconds to go out before the shards
	// are let go of (the rest get a wake hint as each shard is).
	m.Drain(sctx)
	stopControl()
	cg.Wait()
	if ctlErr != nil && serveErr == nil {
		serveErr = ctlErr
	}
	srv.Close()
	closeErr := m.Close(sctx)
	if closeErr != nil {
		log.Error("some shards were not deactivated, keeping the lease", "err", closeErr)
	}
	// The lease is only given up once every shard is let go of: the node never
	// lets go of a shard the app still has open.
	released := closeErr == nil && n.Keeper.Check() == nil
	if err := n.Close(sctx, released); err != nil {
		log.Error("closing the node", "err", err)
	}
	return serveErr
}
