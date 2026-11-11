package node

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// silentGrace is how long a stop lets a connection that has sent nothing stay
// open. Shutdown only counts such a connection as idle after 5 seconds
// (golang/go#22682), and a client that has been silent for a second at a stop
// has no request the app has seen. Clients open these without using them (a
// spare dial that lost the race for an idle connection, a health probe).
const silentGrace = time.Second

// httpServer is one of the node's HTTP servers: the public, control or peer
// server, or the pprof one. Every request's context derives from a base context
// the server owns, not from the context it serves on, and every handler is
// counted, so that stopping the server can end what is left of its requests and
// join them.
type httpServer struct {
	name    string
	log     *slog.Logger
	ln      net.Listener
	handler http.Handler
	grace   time.Duration
}

func (h *httpServer) String() string { return h.name }

// serverOn is a server for handler on ln (HTTP/1.1 and h2c, which gRPC clients
// behind a load balancer use).
func (s *Server) serverOn(name string, ln net.Listener, handler http.Handler) *httpServer {
	return &httpServer{name: name, log: s.log, ln: ln, handler: handler, grace: s.opts.Grace}
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
			writeError(w, r, http.StatusServiceUnavailable, reasonDraining, "the node is shutting down")
			return
		}
		defer handlers.leave()
		h.handler.ServeHTTP(w, r)
	})
	protos := new(http.Protocols)
	protos.SetHTTP1(true)
	protos.SetUnencryptedHTTP2(true)
	// One child serves, and one stops the server when the context ends or the
	// listener fails (WithContext cancels the other then).
	g, gctx := errgroup.WithContext(ctx)
	silent := &silentConns{conns: map[net.Conn]struct{}{}}
	// No read or write timeouts: streams and upgraded connections live long.
	srv := &http.Server{
		Handler: counted, Protocols: protos, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: serverIdleTimeout,
		// Warnings: net/http logs here for clients' protocol errors as well as its
		// own failures (a handler that panicked, accepting that failed).
		ErrorLog:    slog.NewLogLogger(h.log.Handler(), slog.LevelWarn),
		BaseContext: func(net.Listener) context.Context { return base },
		ConnState:   silent.track,
	}
	g.Go(func() error {
		if err := srv.Serve(h.ln); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), h.grace)
		defer cancel()
		// The closer ends when Shutdown does, so a quiet stop is not held up.
		cctx, stopped := context.WithCancel(sctx)
		var sg errgroup.Group
		sg.Go(func() error { srv.Shutdown(sctx); stopped(); return nil })
		sg.Go(func() error {
			select {
			case <-time.After(silentGrace):
				if n := silent.closeAll(); n > 0 {
					h.log.Debug("closed connections that had sent nothing", "server", h.name, "n", n)
				}
			case <-cctx.Done():
			}
			return nil
		})
		sg.Wait()
		cancelBase()
		srv.Close()
		handlers.close()
		return nil
	})
	return g.Wait() // a listener's failure: the server's group names it
}

// silentConns are a server's connections that have not yet sent a request.
type silentConns struct {
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

// track is the server's ConnState hook.
func (s *silentConns) track(c net.Conn, st http.ConnState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st == http.StateNew {
		s.conns[c] = struct{}{}
	} else {
		delete(s.conns, c)
	}
}

// closeAll closes the connections that are still silent and counts them.
func (s *silentConns) closeAll() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		c.Close()
	}
	return len(s.conns)
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
