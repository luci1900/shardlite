package e2e

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

// storageProxy forwards a node's connections to the store and can cut the node
// off from it. While cut, it holds the bytes in both directions and the
// connections that open, as TCP holds them across a short partition, and
// delivers them once healed. So a request whose client gave up during the cut
// can still reach the store after it, as a late write does across a real
// partition. It is modelled on the pause of etcd's test proxy
// (go.etcd.io/etcd/pkg/v3/proxy).
type storageProxy struct {
	ln   net.Listener
	to   string
	ctx  context.Context // ends at close, which ends every connection
	stop context.CancelFunc

	mu   sync.Mutex
	open chan struct{} // closed while healed, open while cut

	held atomic.Int64 // chunks and connections that waited on a cut
	g    errgroup.Group
}

// newStorageProxy listens on a free local port and forwards to the address to
// (host:port). It is healed to begin with, and closed when the test ends.
func newStorageProxy(t testing.TB, to string) *storageProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	p := &storageProxy{ln: ln, to: to, ctx: ctx, stop: stop, open: make(chan struct{})}
	close(p.open)
	p.g.Go(p.accept)
	t.Cleanup(p.close)
	return p
}

// addr is where the node connects, host:port.
func (p *storageProxy) addr() string { return p.ln.Addr().String() }

// cut holds everything from now until heal.
func (p *storageProxy) cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.open:
		p.open = make(chan struct{})
	default: // cut already
	}
}

// heal delivers what the cut held and lets everything through again.
func (p *storageProxy) heal() {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.open: // healed already
	default:
		close(p.open)
	}
}

// pass waits while the proxy is cut, and reports false if it closed first.
func (p *storageProxy) pass() bool {
	p.mu.Lock()
	open := p.open
	p.mu.Unlock()
	select {
	case <-open:
		return true
	default:
	}
	p.held.Add(1)
	select {
	case <-open:
		return true
	case <-p.ctx.Done():
		return false
	}
}

func (p *storageProxy) accept() error {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return nil // closed
		}
		p.g.Go(func() error {
			p.serve(c)
			return nil
		})
	}
}

// serve forwards one connection, dialling the store once the proxy lets it.
// Each connection is closed when the proxy closes.
func (p *storageProxy) serve(c net.Conn) {
	defer c.Close()
	defer context.AfterFunc(p.ctx, func() { c.Close() })()
	if !p.pass() {
		return
	}
	u, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(p.ctx, "tcp", p.to)
	if err != nil {
		return // the node sees its connection close
	}
	defer u.Close()
	defer context.AfterFunc(p.ctx, func() { u.Close() })()
	var g errgroup.Group
	g.Go(func() error { p.pipe(u, c); return nil })
	g.Go(func() error { p.pipe(c, u); return nil })
	g.Wait()
}

// pipe copies src to dst, holding each chunk while the proxy is cut. When
// either side ends, it closes both, so the other direction ends too.
func (p *storageProxy) pipe(dst, src net.Conn) {
	defer dst.Close()
	defer src.Close()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if !p.pass() {
				return
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// close stops listening, ends every connection and waits for its goroutines.
func (p *storageProxy) close() {
	p.stop()
	p.ln.Close()
	p.g.Wait()
}

// The proxy forwards while healed, holds bytes and new connections while cut
// and delivers them once healed.
func TestStorageProxyHoldsWhileCut(t *testing.T) {
	t.Parallel()
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var got atomic.Int64 // bytes the store received
	var eg errgroup.Group
	eg.Go(func() error {
		for {
			c, err := echo.Accept()
			if err != nil {
				return nil
			}
			eg.Go(func() error {
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					got.Add(int64(n))
					if n > 0 {
						c.Write(buf[:n])
					}
					if err != nil {
						return nil
					}
				}
			})
		}
	})
	t.Cleanup(func() { echo.Close(); eg.Wait() })

	p := newStorageProxy(t, echo.Addr().String())
	c, err := net.Dial("tcp", p.addr())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	roundTrip := func(c net.Conn, s string) {
		t.Helper()
		if _, err := c.Write([]byte(s)); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, len(s))
		if _, err := io.ReadFull(c, buf); err != nil || string(buf) != s {
			t.Fatalf("read %q, %v, want %q", buf, err, s)
		}
	}
	roundTrip(c, "a")

	// Cut: a write on an open connection and a new connection are both held,
	// and the store sees neither.
	p.cut()
	before := got.Load()
	if _, err := c.Write([]byte("b")); err != nil {
		t.Fatal(err)
	}
	late, err := net.Dial("tcp", p.addr())
	if err != nil {
		t.Fatal(err)
	}
	defer late.Close()
	for p.held.Load() < 2 {
		time.Sleep(time.Millisecond)
	}
	if n := got.Load(); n != before {
		t.Fatalf("the store got %d bytes during the cut", n-before)
	}

	// Heal: the held write arrives and is answered, and the held connection works.
	p.heal()
	buf := make([]byte, 1)
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "b" {
		t.Fatalf("after the heal, read %q, %v, want b", buf, err)
	}
	roundTrip(late, "c")
}
