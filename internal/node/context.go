package node

import (
	"fmt"
	"net/http"

	"github.com/dunglas/httpsfv"

	"github.com/luci1900/shardlite/internal/shard"
)

// hdrContext is what the node hands an app with every request it opens a shard for, and the app passes back unchanged on the calls it makes while serving it. It is a Structured Fields Dictionary (RFC 9651): the shard the request was opened for (shard, a string) and the request's hops (hops, an integer). The node keeps nothing, so the header is the whole of it. Keys the node does not know are ignored.
const hdrContext = "Shardlite-Context"

// The keys of hdrContext.
const (
	ctxShard = "shard"
	ctxHops  = "hops"
)

// callContext is a parsed hdrContext: the calling shard and the hops of the request the call was made from.
type callContext struct {
	shard shard.Name
	hops  int64
}

// format is the value of hdrContext for c. The node writes one for a request it opened c.shard for, which is c.hops into a chain of calls.
func (c callContext) format() (string, error) {
	d := httpsfv.NewDictionary()
	d.Add(ctxShard, httpsfv.NewItem(c.shard.String()))
	d.Add(ctxHops, httpsfv.NewItem(c.hops))
	return httpsfv.Marshal(d)
}

// parseContext reads the context a call carries. ok is false for a request without the header, which is no error. A header that does not parse, lacks shard or hops, or names a shard that is not one is an error. Both the control API and the peer listener read a call's context with it.
func parseContext(h http.Header) (c callContext, ok bool, err error) {
	values := h.Values(hdrContext)
	if len(values) == 0 {
		return callContext{}, false, nil
	}
	d, err := httpsfv.UnmarshalDictionary(values)
	if err != nil {
		return callContext{}, false, fmt.Errorf("does not parse: %w", err)
	}
	name, err := contextString(d, ctxShard)
	if err != nil {
		return callContext{}, false, err
	}
	if c.shard, err = ParseTarget(name); err != nil {
		return callContext{}, false, fmt.Errorf("%s: %w", ctxShard, err)
	}
	hops, err := contextInt(d, ctxHops)
	if err != nil {
		return callContext{}, false, err
	}
	if hops < 0 {
		return callContext{}, false, fmt.Errorf("%s is negative", ctxHops)
	}
	c.hops = hops
	return c, true, nil
}

func contextString(d *httpsfv.Dictionary, key string) (string, error) {
	m, ok := d.Get(key)
	if !ok {
		return "", fmt.Errorf("%s is missing", key)
	}
	item, ok := m.(httpsfv.Item)
	if !ok {
		return "", fmt.Errorf("%s is not a string", key)
	}
	s, ok := item.Value.(string)
	if !ok {
		return "", fmt.Errorf("%s is not a string", key)
	}
	return s, nil
}

func contextInt(d *httpsfv.Dictionary, key string) (int64, error) {
	m, ok := d.Get(key)
	if !ok {
		return 0, fmt.Errorf("%s is missing", key)
	}
	item, ok := m.(httpsfv.Item)
	if !ok {
		return 0, fmt.Errorf("%s is not an integer", key)
	}
	n, ok := item.Value.(int64)
	if !ok {
		return 0, fmt.Errorf("%s is not an integer", key)
	}
	return n, nil
}

// badContext is the 400 for a context that was refused.
func badContext(err error) error {
	return &statusError{http.StatusBadRequest, reasonBadRequest, "bad " + hdrContext + ": " + err.Error()}
}
