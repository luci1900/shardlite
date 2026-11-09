package shardlite

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// CallOption sets one option of a call: IfActive, IfExists, WithSync or Header.
type CallOption struct{ apply func(*callParams) }

type callParams struct {
	header http.Header
	err    error
}

// IfActive makes the call answer 404 rather than activate the target
// (Shardlite-If-Active), for presence checks and fan-out to members who may be
// offline.
func IfActive() CallOption {
	return CallOption{func(p *callParams) { p.header.Set(headerIfActive, "true") }}
}

// IfExists makes the call answer 404 rather than create the target
// (Shardlite-If-Exists). Use it for any call whose id came from a client.
func IfExists() CallOption {
	return CallOption{func(p *callParams) { p.header.Set(headerIfExists, "true") }}
}

// WithSync overrides the target kind's durability tier for this call
// (Shardlite-Sync). The zero Sync leaves the kind's own.
func WithSync(s Sync) CallOption {
	return CallOption{func(p *callParams) {
		switch s {
		case "":
		case SyncNone, SyncStorage:
			p.header.Set(headerSync, string(s))
		default:
			p.err = fmt.Errorf("shardlite: %q is not a Sync tier", s)
		}
	}}
}

// SetSync sets the durability tier of a response (Shardlite-Sync) on its
// headers, before the handler writes it: SetSync(w.Header(), SyncStorage). On a
// SyncNone kind it makes this acknowledgement wait for the shard's commits to
// reach blob storage.
//
// It sets nothing and returns an error for a Sync that is neither SyncNone nor
// SyncStorage (the zero value included), so check it.
func SetSync(h http.Header, s Sync) error {
	switch s {
	case SyncNone, SyncStorage:
		h.Set(headerSync, string(s))
		return nil
	}
	return fmt.Errorf("shardlite: %q is not a Sync tier: use SyncNone or SyncStorage", s)
}

// Header sets a header of the app's own protocol on the call (Idempotency-Key,
// Content-Type). The call fails before it is sent if the name is a Shardlite-*
// header, which the SDK sets.
func Header(name, value string) CallOption {
	return CallOption{func(p *callParams) {
		if strings.HasPrefix(strings.ToLower(strings.ReplaceAll(name, "_", "-")), "shardlite-") {
			p.err = fmt.Errorf("shardlite: Header cannot set %s: the options of a call and its request's scope set the Shardlite-* headers", name)
			return
		}
		p.header.Set(name, value)
	}}
}

// CallAsApp calls target from the app with no request behind it (at startup,
// from a worker or a timer) and sends at once. It carries no request's context,
// so the node takes the call as the app's, restarts the hops and does not hold
// it for any shard's commits. A call while serving a request goes through its
// scope (Opened.Call, Request.Call). It ends with ctx.
//
// It is the one call that may upgrade to a WebSocket. The target, path, body
// and options are as for Request.Call.
func (n *Node) CallAsApp(ctx context.Context, target ShardName, method, path string, body io.Reader, opts ...CallOption) (*http.Response, error) {
	return n.send(ctx, nil, target, method, path, body, opts)
}

// send makes a call and sends it. from is the request being served, nil for a
// call from the app.
func (n *Node) send(ctx context.Context, from *http.Request, target ShardName, method, path string, body io.Reader, opts []CallOption) (*http.Response, error) {
	if target == (ShardName{}) {
		return nil, fmt.Errorf("shardlite: a call needs a target")
	}
	if !target.valid() {
		return nil, fmt.Errorf("shardlite: %s is not a valid shard name to call", target)
	}
	if !strings.HasPrefix(path, "/") {
		return nil, fmt.Errorf("shardlite: the path of a call starts with a slash: %q", path)
	}
	p := callParams{header: http.Header{}}
	for _, o := range opts {
		o.apply(&p)
	}
	if p.err != nil {
		return nil, p.err
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://"+n.addr+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set(HeaderTarget, target.String())
	if from != nil {
		if v := from.Header.Values(HeaderContext); len(v) > 0 {
			req.Header[HeaderContext] = append([]string(nil), v...)
		}
		if v := from.Header.Get("traceparent"); v != "" {
			req.Header.Set("traceparent", v)
		}
	}
	for k, v := range p.header {
		req.Header[k] = v
	}
	return n.http.Do(req)
}

// Sync waits until everything committed on the node is in blob storage and the
// node has confirmed it still owns its shards. It is for side effects the node
// can't see (publishing to a queue, an external call) and for a write that must
// not be lost on a kind that does not wait for storage. If the node lost its
// lease or could not say, the error is an *Error (find it with errors.As) with
// the node's reason and retry hint. Any other failure is the call itself,
// wrapped.
func (n *Node) Sync(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+n.addr+syncPath, nil)
	if err != nil {
		return err
	}
	res, err := n.http.Do(req)
	if err != nil {
		return fmt.Errorf("shardlite: sync: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode/100 == 2 {
		io.Copy(io.Discard, res.Body)
		return nil
	}
	if nerr, ok := NodeError(res); ok {
		return nerr
	}
	msg, _ := io.ReadAll(io.LimitReader(res.Body, 512))
	return fmt.Errorf("shardlite: sync: %d %s: %s", res.StatusCode, http.StatusText(res.StatusCode), strings.TrimSpace(string(msg)))
}
