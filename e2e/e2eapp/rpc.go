package e2eapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
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
//	Who      unary: reports the target, caller and hops the app was called with

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
	Target      string `json:"target"`
}

// Debug is the app's counters, served at GET /debug on the app's own address.
type Debug struct {
	mu          sync.Mutex
	HoldStarted int          `json:"hold_started"`
	HoldCancel  int          `json:"hold_cancelled"`
	LastHold    HoldRecord   `json:"last_hold"`
	WSEnded     int          `json:"ws_ended"`
	WSClose     []WSCloseRec `json:"ws_closes"` // how each connection ended, in order
	OpenDBs     int          `json:"open_dbs"`  // shard databases the app has open right now
}

// WSCloseRec is how a WebSocket connection of the app ended.
type WSCloseRec struct {
	Shard  string `json:"shard"`
	Code   int    `json:"code"` // -1 if the connection ended without a close frame
	Reason string `json:"reason"`
}

func (a *App) rpcShard(ctx context.Context) (*shardReq, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	one := func(k string) string {
		if v := md.Get(k); len(v) > 0 {
			return v[0]
		}
		return ""
	}
	name, path := one("shardlite-target"), one("shardlite-path")
	if name == "" || path == "" {
		return nil, status.Error(codes.InvalidArgument, "this method is for a shard: name one with shardlite-target")
	}
	db, err := a.pool.get(path)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &shardReq{name: name, path: path, db: db}, nil
}

func str(v any) string { return v.(*wrapperspb.StringValue).GetValue() }

func (a *App) newGRPC() *grpc.Server {
	srv := grpc.NewServer()
	unary := func(name string, h func(ctx context.Context, s *shardReq, in string) (string, error), needShard bool) grpc.MethodDesc {
		return grpc.MethodDesc{MethodName: name, Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			in := new(wrapperspb.StringValue)
			if err := dec(in); err != nil {
				return nil, err
			}
			var s *shardReq
			if needShard {
				var err error
				if s, err = a.rpcShard(ctx); err != nil {
					return nil, err
				}
				defer s.db.release()
			}
			out, err := h(ctx, s, in.Value)
			if err != nil {
				return nil, err
			}
			return wrapperspb.String(out), nil
		}}
	}
	stream := func(name string, server, client bool, h func(st grpc.ServerStream, s *shardReq) error) grpc.StreamDesc {
		return grpc.StreamDesc{StreamName: name, ServerStreams: server, ClientStreams: client, Handler: func(_ any, st grpc.ServerStream) error {
			s, err := a.rpcShard(st.Context())
			if err != nil {
				return err
			}
			defer s.db.release()
			return h(st, s)
		}}
	}
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "e2e.Shards",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			unary("Add", a.rpcAdd, true),
			unary("Fail", a.rpcFail, false),
			unary("Hold", a.rpcHold, false),
			unary("Relay", a.rpcRelay, true),
			unary("Who", a.rpcWho, false),
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
	if v := md.Get("shardlite-target"); len(v) > 0 {
		rec.Target = v[0]
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
	out := metadata.Pairs("shardlite-target", to, "shardlite-caller", s.name)
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
	return fmt.Sprintf("target=%s caller=%s hops=%s", get("shardlite-target"), get("shardlite-caller"), get("shardlite-hops")), nil
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
