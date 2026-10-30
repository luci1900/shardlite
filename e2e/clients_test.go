package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/luci1900/shardlite/e2e/e2eapp"
)

// The clients are the reference libraries: google.golang.org/grpc for gRPC
// (messages are protobuf's StringValue, see e2eapp/rpc.go) and
// github.com/coder/websocket for WebSocket.

// dialGRPC connects a gRPC client to addr (the node's public listener or control API).
func dialGRPC(t testing.TB, addr string) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return cc
}

// withMD adds metadata (key, value, key, value...) to a call, such as shardlite-target.
func withMD(ctx context.Context, kv ...string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, kv...)
}

// onShard adds the metadata of a call to shard as a client SDK for the app makes
// it: x-shard says which shard the call is for, and shardlite-target names the
// same shard to save a hop. More metadata follows as in withMD.
func onShard(ctx context.Context, shard string, kv ...string) context.Context {
	return withMD(ctx, append([]string{"shardlite-target", shard, "x-shard", shard}, kv...)...)
}

// unary makes a unary call to e2e.Shards/method.
func unary(ctx context.Context, cc *grpc.ClientConn, method, in string, opts ...grpc.CallOption) (string, error) {
	out := new(wrapperspb.StringValue)
	if err := cc.Invoke(ctx, "/e2e.Shards/"+method, wrapperspb.String(in), out, opts...); err != nil {
		return "", err
	}
	return out.Value, nil
}

// open starts a streaming call to e2e.Shards/method.
func open(ctx context.Context, cc *grpc.ClientConn, method string, server, client bool) (grpc.ClientStream, error) {
	return cc.NewStream(ctx, &grpc.StreamDesc{StreamName: method, ServerStreams: server, ClientStreams: client}, "/e2e.Shards/"+method)
}

func recv(st grpc.ClientStream) (string, error) {
	m := new(wrapperspb.StringValue)
	if err := st.RecvMsg(m); err != nil {
		return "", err
	}
	return m.Value, nil
}

func send(st grpc.ClientStream, s string) error { return st.SendMsg(wrapperspb.String(s)) }

// dialWS opens a WebSocket to the app's ws route for shard through the node at
// addr, with the shard in the path and as the hint.
func dialWS(ctx context.Context, addr, shard string, opts ...func(*websocket.DialOptions)) (*websocket.Conn, error) {
	o := &websocket.DialOptions{HTTPHeader: http.Header{"Shardlite-Target": {shard}}}
	for _, f := range opts {
		f(o)
	}
	c, _, err := websocket.Dial(ctx, "ws://"+addr+at(shard, "/ws"), o)
	if c != nil {
		c.SetReadLimit(64 << 20)
	}
	return c, err
}

// wsEcho sends a text message and returns the reply.
func wsEcho(ctx context.Context, c *websocket.Conn, text string) (string, error) {
	if err := c.Write(ctx, websocket.MessageText, []byte(text)); err != nil {
		return "", err
	}
	_, b, err := c.Read(ctx)
	return string(b), err
}

// relayCount is how many requests the app has relayed so far.
func (s *stack) relayCount() int { return len(s.appDebug().Relays) }

// checkRelays checks the requests the app relayed after the first n, in order.
// The app records a relay before it answers, so no waiting is needed once the
// client has its answer.
func (s *stack) checkRelays(t testing.TB, n int, want ...e2eapp.RelayRec) {
	t.Helper()
	if got := s.appDebug().Relays[n:]; !slices.Equal(got, want) {
		t.Errorf("the app relayed %+v, want %+v", got, want)
	}
}

func timeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// appDebug is the test app's counters, read from its own address.
func (s *stack) appDebug() *e2eapp.Debug {
	s.t.Helper()
	resp, err := http.Get("http://" + s.appAddr + "/debug")
	if err != nil {
		s.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	d := new(e2eapp.Debug)
	if err := json.Unmarshal(b, d); err != nil {
		s.t.Fatalf("debug: %v: %s", err, b)
	}
	return d
}
