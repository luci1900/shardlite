package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// WebSocket through the node with the reference client and server.
func TestWebSocketThroughTheNode(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	v := newVerifier(t, s)
	ctx, cancel := timeout(90 * time.Second)
	defer cancel()
	dial := func(t *testing.T, shard string, opts ...func(*websocket.DialOptions)) *websocket.Conn {
		t.Helper()
		c, err := dialWS(ctx, s.pub, shard, opts...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.CloseNow() })
		return c
	}

	s.create(t, "e2e.wallet/s1", "e2e.wallet/s2", "e2e.wallet/s3", "e2e.wallet/s4", "e2e.wallet/s5", "e2e.arena/s7")

	t.Run("text and binary both ways", func(t *testing.T) {
		c := dial(t, "e2e.wallet/s1")
		if echo, err := wsEcho(ctx, c, "hello"); err != nil || echo != "ok:hello" {
			t.Fatalf("%q %v", echo, err)
		}
		if !v.has("e2e.wallet/s1", "hello") {
			t.Fatal("the echo came before the commit was durable")
		}
		payload := []byte{0, 1, 2, 0xff, 0xfe, 'a'}
		if err := c.Write(ctx, websocket.MessageBinary, payload); err != nil {
			t.Fatal(err)
		}
		typ, got, err := c.Read(ctx)
		if err != nil || typ != websocket.MessageBinary || !bytes.Equal(got, payload) {
			t.Fatalf("binary: %v %v %v", typ, got, err)
		}
		// A text message the app originates.
		if err := c.Write(ctx, websocket.MessageText, []byte("big:10")); err != nil {
			t.Fatal(err)
		}
		if typ, got, err := c.Read(ctx); err != nil || typ != websocket.MessageText || string(got) != strings.Repeat("x", 10) {
			t.Fatalf("text from the app: %v %q %v", typ, got, err)
		}
	})

	t.Run("messages of several megabytes", func(t *testing.T) {
		c := dial(t, "e2e.wallet/s2")
		big := bytes.Repeat([]byte("0123456789abcdef"), 4<<20/16) // 4 MB
		for i := range 3 {
			big[i*1000] = byte(i + 1)
		}
		if err := c.Write(ctx, websocket.MessageBinary, big); err != nil {
			t.Fatal(err)
		}
		if _, got, err := c.Read(ctx); err != nil || !bytes.Equal(got, big) {
			t.Fatalf("4 MB binary echo: %d bytes, %v", len(got), err)
		}
		const n = 5 << 20
		if err := c.Write(ctx, websocket.MessageText, []byte(fmt.Sprintf("big:%d", n))); err != nil {
			t.Fatal(err)
		}
		if _, got, err := c.Read(ctx); err != nil || len(got) != n || got[0] != 'x' || got[n-1] != 'x' {
			t.Fatalf("5 MB text from the app: %d bytes, %v", len(got), err)
		}
		// Still usable afterwards.
		if echo, err := wsEcho(ctx, c, "after"); err != nil || echo != "ok:after" {
			t.Fatalf("%q %v", echo, err)
		}
	})

	t.Run("ping and pong", func(t *testing.T) {
		c := dial(t, "e2e.wallet/s3")
		// The client pings the app: Ping needs a reader running, and the app answers from its own read loop.
		msgs := make(chan string, 8)
		go func() {
			for {
				_, b, err := c.Read(ctx)
				if err != nil {
					return
				}
				msgs <- string(b)
			}
		}()
		pctx, pcancel := timeout(5 * time.Second)
		defer pcancel()
		if err := c.Ping(pctx); err != nil {
			t.Fatalf("client ping: %v", err)
		}
		// The app pings the client, which answers from its read loop.
		if err := c.Write(ctx, websocket.MessageText, []byte("ping")); err != nil {
			t.Fatal(err)
		}
		select {
		case m := <-msgs:
			if m != "pinged" {
				t.Fatalf("app ping: %q", m)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the app's ping was never answered")
		}
	})

	t.Run("close codes and reasons both ways", func(t *testing.T) {
		// Connections of earlier subtests may still be ending (the app sees those
		// last, with no close frame), so the records of this shard are the ones looked at.
		before := len(s.appDebug().WSClose)
		closes := func() (codes []int, reasons []string) {
			for _, rec := range s.appDebug().WSClose[before:] {
				if rec.Shard == "e2e.wallet/s4" {
					codes, reasons = append(codes, rec.Code), append(reasons, rec.Reason)
				}
			}
			return
		}
		c := dial(t, "e2e.wallet/s4")
		if err := c.Close(websocket.StatusCode(4001), "bye from the client"); err != nil {
			t.Logf("close: %v", err) // the handshake may report the peer's reply
		}
		eventually(t, "the app to see the close", func() bool { codes, _ := closes(); return len(codes) > 0 })
		if codes, reasons := closes(); codes[0] != 4001 || reasons[0] != "bye from the client" {
			t.Fatalf("the app saw %v %q", codes, reasons)
		}

		c = dial(t, "e2e.wallet/s4")
		if err := c.Write(ctx, websocket.MessageText, []byte("close:4002:bye from the app")); err != nil {
			t.Fatal(err)
		}
		_, _, err := c.Read(ctx)
		var ce websocket.CloseError
		if !errors.As(err, &ce) || ce.Code != 4002 || ce.Reason != "bye from the app" {
			t.Fatalf("the client saw %v", err)
		}

		c = dial(t, "e2e.wallet/s4")
		c.Close(websocket.StatusNormalClosure, "")
		eventually(t, "the app to see a normal close", func() bool {
			codes, _ := closes()
			return len(codes) > 0 && codes[len(codes)-1] == 1000
		})
	})

	t.Run("a client that disappears", func(t *testing.T) {
		before := s.appDebug().WSEnded
		var mu sync.Mutex
		var raw net.Conn
		hc := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			mu.Lock()
			raw = c
			mu.Unlock()
			return c, err
		}}}
		c := dial(t, "e2e.wallet/s5", func(o *websocket.DialOptions) { o.HTTPClient = hc })
		if echo, err := wsEcho(ctx, c, "here"); err != nil || echo != "ok:here" {
			t.Fatalf("%q %v", echo, err)
		}
		mu.Lock()
		raw.Close() // gone, with no closing handshake
		mu.Unlock()
		eventually(t, "the app to see the connection end", func() bool { return s.appDebug().WSEnded > before })
		cl := s.appDebug().WSClose
		if rec := cl[len(cl)-1]; rec.Shard != "e2e.wallet/s5" || rec.Code != -1 {
			t.Fatalf("the app saw %+v, want an end without a close frame", rec)
		}
	})

	t.Run("an open connection keeps its shard active", func(t *testing.T) {
		// arena has an idle timeout of 300 ms, and the node's janitor looks every
		// second. The shard is marked for deletion, so its manifest exists only while
		// it is active, and blob storage tells whether it is without probing the
		// node (which would reset the shard's idle timer).
		active := func(shard string) bool {
			_, _, err := v.files.GetManifest(ctx, shardName(shard))
			if err != nil && !isNotFound(err) {
				t.Fatal(err)
			}
			return err == nil
		}
		c := dial(t, "e2e.arena/s7")
		if echo, err := wsEcho(ctx, c, "kept"); err != nil || echo != "ok:kept" {
			t.Fatalf("%q %v", echo, err)
		}
		if reply, err := wsEcho(ctx, c, "mark"); err != nil || reply != "marked" {
			t.Fatalf("mark: %q %v", reply, err)
		}
		if !active("e2e.arena/s7") {
			t.Fatal("no manifest for an active shard")
		}
		// 1.5 s is five idle timeouts, with a janitor pass in it after the shard
		// had been idle for one, with the connection idle.
		time.Sleep(1500 * time.Millisecond)
		if !active("e2e.arena/s7") {
			t.Fatal("the shard was deactivated under an open WebSocket")
		}
		if n, err := wsEcho(ctx, c, "count"); err != nil || n != "1" {
			t.Fatalf("the shard lost its data: %q %v", n, err)
		}
		c.Close(websocket.StatusNormalClosure, "")
		eventually(t, "the shard to deactivate after the WebSocket closed (its manifest goes)", func() bool { return !active("e2e.arena/s7") })
	})
}
