package e2e

import (
	"fmt"
	"github.com/luci1900/shardlite/internal/apptest"
	"io"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// gRPC through the node with the reference client and server: every kind of
// call, the app's statuses, headers and trailers, the errors the node makes,
// deadlines, cancellation and a call from the app to another shard.
func TestGRPCThroughTheNode(t *testing.T) {
	t.Parallel()
	s := newStack(t)
	v := newVerifier(t, s)
	pub, ctl := dialGRPC(t, s.pub), dialGRPC(t, s.ctl)
	ctx, cancel := timeout(60 * time.Second)
	defer cancel()

	s.create(t, "wallet/g1", "wallet/g2", "wallet/g3", "wallet/g4", "wallet/g5", "wallet/g7", "wallet/g8", "wallet/m1", "wallet/m2", "wallet/r1")

	t.Run("unary with headers and trailers", func(t *testing.T) {
		var hdr, tr metadata.MD
		out, err := unary(onShard(ctx, "wallet/g1"), pub, "Add", "u1", grpc.Header(&hdr), grpc.Trailer(&tr))
		if err != nil || out != "ok:u1" {
			t.Fatalf("%q %v", out, err)
		}
		if h := hdr.Get("x-app-header"); len(h) != 1 || h[0] != "h-u1" {
			t.Fatalf("header: %v", hdr)
		}
		if h := tr.Get("x-app-trailer"); len(h) != 1 || h[0] != "t-u1" {
			t.Fatalf("trailer: %v", tr)
		}
		if !v.has("wallet/g1", "u1") {
			t.Fatal("the reply came before the commit was durable")
		}
	})

	t.Run("server streaming", func(t *testing.T) {
		st, err := open(onShard(ctx, "wallet/g2"), pub, "Watch", true, false)
		if err != nil || send(st, "w 4") != nil || st.CloseSend() != nil {
			t.Fatal(err)
		}
		for i := 1; i <= 4; i++ {
			m, err := recv(st)
			if err != nil || m != fmt.Sprintf("ok:w-%d", i) {
				t.Fatalf("message %d: %q %v", i, m, err)
			}
			if !v.has("wallet/g2", fmt.Sprintf("w-%d", i)) {
				t.Fatalf("message %d reached the client before its commit was durable", i)
			}
		}
		if _, err := recv(st); err != io.EOF {
			t.Fatalf("end of stream: %v", err)
		}
		if tr := st.Trailer().Get("x-app-trailer"); len(tr) != 1 || tr[0] != "watched" {
			t.Fatalf("trailer: %v", st.Trailer())
		}
	})

	t.Run("client streaming", func(t *testing.T) {
		st, err := open(onShard(ctx, "wallet/g3"), pub, "Collect", false, true)
		if err != nil {
			t.Fatal(err)
		}
		for i := range 5 {
			if err := send(st, fmt.Sprintf("c-%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := st.CloseSend(); err != nil {
			t.Fatal(err)
		}
		if n, err := recv(st); err != nil || n != "5" {
			t.Fatalf("count: %q %v", n, err)
		}
		if tr := st.Trailer().Get("x-app-trailer"); len(tr) != 1 || tr[0] != "collected" {
			t.Fatalf("trailer: %v", st.Trailer())
		}
		if !v.has("wallet/g3", "c-0", "c-1", "c-2", "c-3", "c-4") {
			t.Fatal("the reply came before the commits were durable")
		}
	})

	t.Run("bidirectional streaming", func(t *testing.T) {
		st, err := open(onShard(ctx, "wallet/g4"), pub, "Chat", true, true)
		if err != nil {
			t.Fatal(err)
		}
		for i := range 6 {
			tok := fmt.Sprintf("b-%d", i)
			if err := send(st, tok); err != nil {
				t.Fatal(err)
			}
			if m, err := recv(st); err != nil || m != "ok:"+tok {
				t.Fatalf("reply %d: %q %v", i, m, err)
			}
			if !v.has("wallet/g4", tok) {
				t.Fatalf("reply %d reached the client before its commit was durable", i)
			}
		}
		st.CloseSend()
		if _, err := recv(st); err != io.EOF {
			t.Fatalf("end of stream: %v", err)
		}
	})

	t.Run("the app's status and trailers", func(t *testing.T) {
		var tr metadata.MD
		_, err := unary(onShard(ctx, "wallet/g5"), pub, "Fail", "7 not for you, 100% sure", grpc.Trailer(&tr))
		if status.Code(err) != codes.PermissionDenied || status.Convert(err).Message() != "not for you, 100% sure" {
			t.Fatalf("%v", err)
		}
		if h := tr.Get("x-app-trailer"); len(h) != 1 || h[0] != "failed" {
			t.Fatalf("trailer: %v", tr)
		}
	})

	t.Run("the node's own errors", func(t *testing.T) {
		for _, c := range []struct {
			name, shard string
			code        codes.Code
		}{{"undeclared kind", "typo/1", codes.NotFound}, {"private kind", "audit/1", codes.NotFound}, {"missing shard", "wallet/none", codes.NotFound}, {"bad target", "nokind", codes.InvalidArgument}} {
			_, err := unary(onShard(ctx, c.shard), pub, "Add", "x")
			if status.Code(err) != c.code {
				t.Errorf("%s: %v, want %v", c.name, err, c.code)
			}
		}
		// Shardlite-If-Active on a shard that isn't active: NotFound, and it stays inactive.
		_, err := unary(onShard(ctx, "wallet/g6", "shardlite-if-active", "true"), ctl, "Who", "")
		if status.Code(err) != codes.NotFound {
			t.Errorf("inactive shard: %v, want NotFound", err)
		}
		if _, err := unary(onShard(ctx, "wallet/g6"), ctl, "Add", "g6"); err != nil { // a call creates it
			t.Fatal(err)
		}
		if _, err := unary(onShard(ctx, "wallet/g6", "shardlite-if-active", "true"), ctl, "Who", ""); err != nil {
			t.Errorf("active shard: %v", err)
		}
		// The hop limit (32 by default): a call made while serving hop 32 would be hop 33.
		_, err = unary(onShard(ctx, "wallet/g6", "shardlite-context", `shard="wallet/g7", hops=32`), ctl, "Who", "")
		if status.Code(err) != codes.ResourceExhausted {
			t.Errorf("hop limit: %v, want ResourceExhausted", err)
		}
		if out, err := unary(onShard(ctx, "wallet/g6", "shardlite-context", `shard="wallet/g7", hops=31`), ctl, "Who", ""); err != nil || !strings.HasSuffix(out, `context=shard="wallet/g6", hops=32`) {
			t.Errorf("last hop allowed: %q %v", out, err)
		}
		// A node error in a stream is the stream's status too.
		st, err := open(onShard(ctx, "typo/1"), pub, "Chat", true, true)
		if err == nil {
			_, err = recv(st)
		}
		if status.Code(err) != codes.NotFound {
			t.Errorf("stream to an undeclared kind: %v", err)
		}
	})

	t.Run("deadlines reach the app", func(t *testing.T) {
		before := s.appDebug()
		c, cancel := timeout(2 * time.Second)
		defer cancel()
		hold := make(chan error, 1)
		go func() {
			_, err := unary(onShard(c, "wallet/g7"), pub, "Hold", "", grpc.WaitForReady(false))
			hold <- err
		}()
		apptest.Eventually(t, apptest.Timeout, "the call to reach the app", func() bool { return s.appDebug().HoldStarted > before.HoldStarted })
		// The client's grpc-timeout passes through as it sent it (the client enforces
		// its own deadline).
		rec := s.appDebug().LastHold
		if rec.GRPCTimeout == "" {
			t.Errorf("the app saw no grpc-timeout")
		}
		if rec.Opened != "wallet/g7" || rec.DeadlineMS <= 0 || rec.DeadlineMS > 2000 {
			t.Errorf("handler context: %+v", rec)
		}
		if err := <-hold; status.Code(err) != codes.DeadlineExceeded {
			t.Errorf("the call ended with %v, want DeadlineExceeded", err)
		}
		apptest.Eventually(t, apptest.Timeout, "the app's handler to see the deadline", func() bool { return s.appDebug().HoldCancel > before.HoldCancel })
	})

	t.Run("cancelling a call cancels the handler", func(t *testing.T) {
		before := s.appDebug()
		c, cancel := timeout(30 * time.Second)
		done := make(chan error, 1)
		go func() {
			_, err := unary(onShard(c, "wallet/g8"), pub, "Hold", "")
			done <- err
		}()
		apptest.Eventually(t, apptest.Timeout, "the call to reach the app", func() bool { return s.appDebug().HoldStarted > before.HoldStarted })
		cancel()
		if err := <-done; status.Code(err) != codes.Canceled {
			t.Errorf("the client saw %v, want Canceled", err)
		}
		apptest.Eventually(t, apptest.Timeout, "the app's handler to be cancelled", func() bool { return s.appDebug().HoldCancel > before.HoldCancel })
	})

	t.Run("a hint naming another shard, or none, is refused", func(t *testing.T) {
		// x-shard says which shard a call is for and shardlite-target is the hint
		// the node opens a shard by. When they differ the app refuses the call.
		const m1, m2 = "wallet/m1", "wallet/m2"
		before := s.appDebug().Served
		_, err := unary(withMD(ctx, "shardlite-target", m2, "x-shard", m1), pub, "Add", "mm1")
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("a wrong hint: %v, want InvalidArgument", err)
		}
		// No hint at all: the node opens nothing.
		_, err = unary(withMD(ctx, "x-shard", m1), pub, "Add", "mm2")
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("no hint: %v, want InvalidArgument", err)
		}
		// A stream is refused as its call is.
		st, err := open(withMD(ctx, "shardlite-target", m2, "x-shard", m1), pub, "Watch", true, false)
		if err == nil {
			err = send(st, "mw 3")
		}
		if err == nil {
			err = st.CloseSend()
		}
		if err == nil {
			_, err = recv(st)
		}
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("a stream with a wrong hint: %v, want InvalidArgument", err)
		}
		// A call that doesn't say which shard it is for is refused.
		if _, err := unary(withMD(ctx, "shardlite-target", m1), pub, "Add", "x"); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("no x-shard: %v, want InvalidArgument", err)
		}
		// The app wrote nothing and served nothing on either shard.
		if v.has(m1, "mm1") || v.has(m1, "mm2") || v.has(m1, "mw-1") || v.has(m2, "mm1") {
			t.Fatal("a refused call wrote to a shard")
		}
		if after := s.appDebug().Served; after[m1] != before[m1] || after[m2] != before[m2] {
			t.Fatalf("the app served %v after %v on shards it refused", after, before)
		}
		// The hint sent as metadata alone is enough for a call that is for the
		// shard the node opened.
		out, err := unary(withMD(ctx, "shardlite-target", m1), pub, "Who", "")
		if err != nil || !strings.HasPrefix(out, "opened="+m1+" caller= ") {
			t.Fatalf("a hint as metadata: %q %v", out, err)
		}
	})

	t.Run("a call from the app to another shard", func(t *testing.T) {
		out, err := unary(onShard(ctx, "wallet/r1"), pub, "Transfer", "wallet/r2 rt1")
		if err != nil || out != "ok:credit:rt1" {
			t.Fatalf("%q %v", out, err)
		}
		// Sync is on for the kind: both ends are durable when the reply arrives.
		if !v.has("wallet/r1", "debit:rt1") || !v.has("wallet/r2", "credit:rt1") {
			t.Fatal("the transfer returned before both commits were durable")
		}
		// The callee sees who called it and one more hop (this is what the app sent).
		out, err = unary(onShard(ctx, "wallet/r2", "shardlite-context", `shard="wallet/r1", hops=3`), ctl, "Who", "")
		if err != nil || out != `opened=wallet/r2 caller=wallet/r1 context=shard="wallet/r2", hops=4` {
			t.Fatalf("who: %q %v", out, err)
		}
		// The app passes the context of the call it is serving on to the call it makes,
		// as shardlite-context metadata. Serving a call at hop 31, its call is at hop 32,
		// the last one allowed. (A call that dropped the context would be a call from the
		// app, at hop 1, whatever the hops it was serving.)
		out, err = unary(onShard(ctx, "wallet/r1", "shardlite-context", `shard="wallet/r3", hops=30`), ctl, "Transfer", "wallet/r2 rt3")
		if err != nil || out != "ok:credit:rt3" {
			t.Fatalf("transfer one hop under the limit: %q %v", out, err)
		}
		// The callee's status passes back: the hop limit, hit by the app's own call.
		_, err = unary(onShard(ctx, "wallet/r1", "shardlite-context", `shard="wallet/r3", hops=31`), ctl, "Transfer", "wallet/r2 rt2")
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("transfer at the hop limit: %v, want ResourceExhausted", err)
		}
	})
}
