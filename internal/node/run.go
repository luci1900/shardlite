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

	if err := env.checkExpiry(); err != nil {
		return err
	}
	bucket, dir, err := env.blobTarget()
	if err != nil {
		return err
	}
	// Directories first: the checks look at the filesystems they are on.
	for _, d := range []string{env.Data, env.Meta, dir} {
		if d == "" {
			continue // no directory with the S3 backend
		}
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	checks := defaultStartupChecks(log)
	if err := checks.refuse(env.Data, dir); err != nil {
		return err
	}
	var raw blob.Backend
	if bucket != "" {
		raw, err = blob.NewS3(ctx, blob.S3Options{Bucket: bucket, Endpoint: env.S3Endpoint})
	} else {
		raw, err = blob.NewLocal(dir)
	}
	if err != nil {
		return err
	}
	backend := blob.WithPrefix(raw, env.Prefix)
	// The store must do what fencing relies on, before the node takes a lease or
	// adopts anything.
	if err := blob.CheckStore(ctx, backend, env.Name); err != nil {
		return err
	}
	checks.warn(env.Meta)

	n, err := Start(ctx, Config{
		Name: env.Name, Address: env.Address, Backend: backend,
		Flush: env.Flush, ManualFlush: true,
		Lease:  coord.RegistryOptions{TTL: env.LeaseTTL},
		Keeper: coord.KeeperOptions{Interval: env.LeaseTTL / 3, Margin: env.LeaseTTL / 5},
		Logger: log,
	})
	if err != nil {
		return err
	}
	// Expiry starts only now: a node that was down longer than the expiry age
	// has adopted its old segments before any of them can be deleted.
	if err := n.StartJournalSweeper(env.JournalExpiry); err != nil {
		n.Close(context.Background(), false)
		return err
	}
	if n.Adopted.Deltas > 0 || len(n.Adopted.Lost) > 0 {
		log.Info("adopted the previous process's journal", "deltas", n.Adopted.Deltas, "lost", n.Adopted.Lost, "fence", n.Adopted.Fence)
	}
	m, err := NewManager(n, ManagerOptions{
		Kinds: kinds, Data: env.Data, Meta: env.Meta, CacheLimit: env.CacheLimit, Flush: env.Flush, DeltaAge: env.DeltaAge, TailAlert: env.TailAlert, Logger: log,
	})
	if err != nil {
		return err
	}
	srv := NewServer(m, ServerOptions{App: app, HealthPath: env.HealthPath, MaxCallDepth: env.MaxCallDepth, Logger: log})
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
	g.Go(func() error { return srv.Serve(gctx, public, control, socket) })
	serveErr := g.Wait()

	// Shutdown: stop taking requests (done), let go of the shards, give up the lease.
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
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
