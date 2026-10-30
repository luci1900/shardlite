package e2eapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// The gRPC service e2e.Shards. It is registered by hand (a grpc.ServiceDesc)
// and its messages are protobuf's well-known StringValue, so nothing needs
// protoc to build. The methods are:
//
//	Add      unary: the message is the token; replies "ok:"+token with a header and a trailer
//	Watch    server streaming: "prefix n"; n replies, each after a commit
//	Collect  client streaming: one token per message; replies with the count
//	Chat     bidirectional: replies "ok:"+token to each token after its commit
//	Fail     unary: "code message"; ends with that status and a trailer
//	Hold     unary: waits until the call is cancelled (or "ms" passes) and records what it saw
//	Relay    unary: "to token"; debits this shard, then calls Add on another shard through the control API
//	Who      unary: reports the opened shard, caller and hops the app was called with
//
// gRPC has no path to put an id in, so a call says which shard it is for in
// x-shard metadata, which is this app's own key and sits beside the
// shardlite-target hint. (A real gRPC app would more likely put the id in its
// messages. Reading it would mean looking at the first message before relaying
// a stream, which a server SDK would do and this sketch leaves out.) The shard
// is worked out and relayed by the same wrapper as the HTTP routes, in front of
// the gRPC server.

type rawHeadersKey struct{}

// rawHeaders are the HTTP headers of a gRPC request, as the app received them
// (the transport consumes grpc-timeout, so metadata doesn't have it).
func rawHeaders(ctx context.Context) http.Header {
	h, _ := ctx.Value(rawHeadersKey{}).(http.Header)
	return h
}

// HoldRecord is what the last Hold call saw.
type HoldRecord struct {
	GRPCTimeout string `json:"grpc_timeout"` // the raw grpc-timeout header
	DeadlineMS  int64  `json:"deadline_ms"`  // time left on the handler's context when it started, -1 if none
	Opened      string `json:"opened"`       // the shard the node opened
}

// Debug is the app's counters, served at GET /debug on the app's own address.
type Debug struct {
	mu          sync.Mutex
	HoldStarted int            `json:"hold_started"`
	HoldCancel  int            `json:"hold_cancelled"`
	LastHold    HoldRecord     `json:"last_hold"`
	WSEnded     int            `json:"ws_ended"`
	WSClose     []WSCloseRec   `json:"ws_closes"` // how each connection ended, in order
	OpenDBs     int            `json:"open_dbs"`  // shard databases the app has open right now
	Relays      []RelayRec     `json:"relays"`    // the requests the app relayed to another shard, in order
	Served      map[string]int `json:"served"`    // how many requests the app has served on each shard, by the shard the node opened
}

// RelayRec is a request the app relayed: the node opened Opened for it (empty if
// it opened nothing), and it was for For.
type RelayRec struct {
	Opened string `json:"opened"`
	For    string `json:"for"`
}

// WSCloseRec is how a WebSocket connection of the app ended.
type WSCloseRec struct {
	Shard  string `json:"shard"`
	Code   int    `json:"code"` // -1 if the connection ended without a close frame
	Reason string `json:"reason"`
}

type shardKey struct{}

// grpcShard is the shard a gRPC call is for. Fail, Hold and Who touch no shard
// data and answer for whichever shard the node opened.
func grpcShard(r *http.Request) string {
	switch r.URL.Path {
	case "/e2e.Shards/Fail", "/e2e.Shards/Hold", "/e2e.Shards/Who":
		return r.Header.Get("Shardlite-Opened")
	}
	return r.Header.Get("X-Shard")
}

// serveGRPC runs the gRPC server on a call for the shard the node opened. The
// handlers find the shard in the context.
func (a *App) serveGRPC(w http.ResponseWriter, r *http.Request, s *shardReq) {
	ctx := context.WithValue(r.Context(), rawHeadersKey{}, r.Header.Clone())
	ctx = context.WithValue(ctx, shardKey{}, s)
	a.grpc.ServeHTTP(w, r.WithContext(ctx))
}

func isGRPC(r *http.Request) bool {
	return r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc")
}

// refuse answers a request the app won't serve: a gRPC status for a gRPC call,
// an HTTP error otherwise.
func refuse(w http.ResponseWriter, r *http.Request, httpStatus int, msg string) {
	if !isGRPC(r) {
		http.Error(w, msg, httpStatus)
		return
	}
	code := codes.Internal
	if httpStatus == http.StatusBadRequest {
		code = codes.InvalidArgument
	}
	w.Header().Set("Content-Type", "application/grpc")
	w.Header().Set("Grpc-Status", strconv.Itoa(int(code)))
	w.Header().Set("Grpc-Message", url.PathEscape(msg))
	w.WriteHeader(http.StatusOK)
}

func shardOf(ctx context.Context) *shardReq { return ctx.Value(shardKey{}).(*shardReq) }

func str(v any) string { return v.(*wrapperspb.StringValue).GetValue() }

func (a *App) newGRPC() *grpc.Server {
	srv := grpc.NewServer()
	unary := func(name string, h func(ctx context.Context, s *shardReq, in string) (string, error)) grpc.MethodDesc {
		return grpc.MethodDesc{MethodName: name, Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			in := new(wrapperspb.StringValue)
			if err := dec(in); err != nil {
				return nil, err
			}
			out, err := h(ctx, shardOf(ctx), in.Value)
			if err != nil {
				return nil, err
			}
			return wrapperspb.String(out), nil
		}}
	}
	stream := func(name string, server, client bool, h func(st grpc.ServerStream, s *shardReq) error) grpc.StreamDesc {
		return grpc.StreamDesc{StreamName: name, ServerStreams: server, ClientStreams: client, Handler: func(_ any, st grpc.ServerStream) error {
			return h(st, shardOf(st.Context()))
		}}
	}
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "e2e.Shards",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			unary("Add", a.rpcAdd),
			unary("Fail", a.rpcFail),
			unary("Hold", a.rpcHold),
			unary("Relay", a.rpcRelay),
			unary("Who", a.rpcWho),
		},
		Streams: []grpc.StreamDesc{
			stream("Watch", true, false, a.rpcWatch),
			stream("Collect", false, true, a.rpcCollect),
			stream("Chat", true, true, a.rpcChat),
		},
	}, struct{}{})
	return srv
}

func (a *App) rpcAdd(ctx context.Context, s *shardReq, tok string) (string, error) {
	if _, _, err := s.insert(ctx, tok); err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}
	grpc.SetHeader(ctx, metadata.Pairs("x-app-header", "h-"+tok))
	grpc.SetTrailer(ctx, metadata.Pairs("x-app-trailer", "t-"+tok))
	return "ok:" + tok, nil
}

// rpcFail ends with the status it is asked for: "3 some message".
func (a *App) rpcFail(ctx context.Context, _ *shardReq, in string) (string, error) {
	c, msg, _ := strings.Cut(in, " ")
	n, _ := strconv.Atoi(c)
	grpc.SetTrailer(ctx, metadata.Pairs("x-app-trailer", "failed"))
	return "", status.Error(codes.Code(n), msg)
}

// rpcHold waits for the call to be cancelled, or for the given milliseconds.
func (a *App) rpcHold(ctx context.Context, _ *shardReq, in string) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	rec := HoldRecord{DeadlineMS: -1}
	if v := md.Get("shardlite-opened"); len(v) > 0 {
		rec.Opened = v[0]
	}
	if h := rawHeaders(ctx); h != nil {
		rec.GRPCTimeout = h.Get("Grpc-Timeout")
	}
	if d, ok := ctx.Deadline(); ok {
		rec.DeadlineMS = time.Until(d).Milliseconds()
	}
	a.debug.mu.Lock()
	a.debug.HoldStarted++
	a.debug.LastHold = rec
	a.debug.mu.Unlock()
	var wait <-chan time.Time
	if ms, err := strconv.Atoi(in); err == nil {
		wait = time.After(time.Duration(ms) * time.Millisecond)
	}
	select {
	case <-ctx.Done():
		a.debug.mu.Lock()
		a.debug.HoldCancel++
		a.debug.mu.Unlock()
		return "", status.FromContextError(ctx.Err()).Err()
	case <-wait:
		return "released", nil
	}
}

// rpcRelay debits this shard, then credits another one through the control
// API with a gRPC client: "to token".
func (a *App) rpcRelay(ctx context.Context, s *shardReq, in string) (string, error) {
	to, tok, _ := strings.Cut(in, " ")
	if to == "" || tok == "" {
		return "", status.Error(codes.InvalidArgument, "want \"to token\"")
	}
	if _, _, err := s.insert(ctx, "debit:"+tok); err != nil {
		return "", status.Error(codes.Internal, err.Error())
	}
	md, _ := metadata.FromIncomingContext(ctx)
	out := metadata.Pairs("shardlite-target", to, "x-shard", to, "shardlite-caller", s.name)
	if v := md.Get("shardlite-hops"); len(v) > 0 {
		out.Set("shardlite-hops", v[0])
	}
	cc, err := a.controlConn()
	if err != nil {
		return "", status.Error(codes.Unavailable, err.Error())
	}
	reply := new(wrapperspb.StringValue)
	err = cc.Invoke(metadata.NewOutgoingContext(ctx, out), "/e2e.Shards/Add", wrapperspb.String("credit:"+tok), reply)
	if err != nil {
		return "", err // the callee's status passes on unchanged
	}
	return reply.Value, nil
}

func (a *App) rpcWho(ctx context.Context, _ *shardReq, _ string) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	get := func(k string) string {
		if v := md.Get(k); len(v) > 0 {
			return v[0]
		}
		return ""
	}
	return fmt.Sprintf("opened=%s caller=%s hops=%s", get("shardlite-opened"), get("shardlite-caller"), get("shardlite-hops")), nil
}

func (a *App) controlConn() (*grpc.ClientConn, error) {
	a.ccMu.Lock()
	defer a.ccMu.Unlock()
	if a.cc == nil {
		cc, err := grpc.NewClient(a.opts.Control, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		a.cc = cc
	}
	return a.cc, nil
}

func (a *App) rpcWatch(st grpc.ServerStream, s *shardReq) error {
	in := new(wrapperspb.StringValue)
	if err := st.RecvMsg(in); err != nil {
		return status.Error(codes.InvalidArgument, "no request message")
	}
	prefix, nstr, _ := strings.Cut(in.Value, " ")
	n, _ := strconv.Atoi(nstr)
	for i := 1; i <= n; i++ {
		tok := fmt.Sprintf("%s-%d", prefix, i)
		if _, _, err := s.insert(st.Context(), tok); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		if err := st.SendMsg(wrapperspb.String("ok:" + tok)); err != nil {
			return err
		}
	}
	st.SetTrailer(metadata.Pairs("x-app-trailer", "watched"))
	return nil
}

func (a *App) rpcCollect(st grpc.ServerStream, s *shardReq) error {
	n := 0
	for {
		in := new(wrapperspb.StringValue)
		if err := st.RecvMsg(in); err != nil {
			if errors.Is(err, io.EOF) {
				st.SetTrailer(metadata.Pairs("x-app-trailer", "collected"))
				return st.SendMsg(wrapperspb.String(strconv.Itoa(n)))
			}
			return err
		}
		if _, _, err := s.insert(st.Context(), in.Value); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		n++
	}
}

func (a *App) rpcChat(st grpc.ServerStream, s *shardReq) error {
	for {
		in := new(wrapperspb.StringValue)
		if err := st.RecvMsg(in); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if _, _, err := s.insert(st.Context(), in.Value); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		if err := st.SendMsg(wrapperspb.String("ok:" + in.Value)); err != nil {
			return err
		}
	}
}
