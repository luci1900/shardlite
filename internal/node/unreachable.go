package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/luci1900/shardlite/internal/blob"
	"github.com/luci1900/shardlite/internal/coord"
)

// errNoAnswer is the cause a forward is cancelled with when the owner's node
// hasn't acknowledged it within the answer timeout.
var errNoAnswer = errors.New("the owner's node did not answer")

// What a forward has heard from the owner's node (reqInfo.answer). A node
// acknowledges every forward with 102 Processing as soon as it arrives, so the
// forwarding node can tell a node that doesn't answer from a slow app.
const (
	answerAwaited int32 = iota // nothing yet
	answerHeard                // the owner's 102, or the response's headers
	answerLate                 // nothing within the answer timeout: the forward was cancelled
)

// leaseCheckTimeout bounds one read of an unreachable owner's lease, which the
// requests waiting for it share.
const leaseCheckTimeout = 5 * time.Second

// ownerChecks reads the leases of owners that forwards can't reach
// (docs/internals.md, Routing and forwarding). Requests that find a node
// unreachable at once share one read, and a read is used for the requests that
// come after it for a period, so a burst against a dead node costs one lease
// read a period.
type ownerChecks struct {
	get   func(ctx context.Context, node string) (*coord.NodeLease, error)
	now   func() time.Time
	every time.Duration
	log   *slog.Logger

	flight singleflight.Group
	// mu guards found: the nodes found unreachable, until a forward to one is
	// answered. It is a leaf.
	mu    sync.Mutex
	found map[string]*ownerCheck
}

// ownerCheck is one read of an unreachable owner's lease.
type ownerCheck struct {
	lease *coord.NodeLease // nil if the read failed or there is none
	err   error
	at    time.Time // when the read returned
	dead  bool      // the lease is invalid or missing
}

// check returns a read of the node's lease no older than the period.
func (c *ownerChecks) check(ctx context.Context, node string) *ownerCheck {
	if oc := c.fresh(node); oc != nil {
		return oc
	}
	v, _, _ := c.flight.Do(node, func() (any, error) {
		if oc := c.fresh(node); oc != nil {
			return oc, nil // a read that ended just before this one began
		}
		// Shared: one caller giving up must not fail the others.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaseCheckTimeout)
		defer cancel()
		l, err := c.get(rctx, node)
		oc := &ownerCheck{lease: l, err: err, at: c.now()}
		oc.dead = errors.Is(err, blob.ErrNotFound) || (err == nil && !l.ValidAt(oc.at))
		c.mu.Lock()
		if c.found == nil {
			c.found = map[string]*ownerCheck{}
		}
		prev := c.found[node]
		c.found[node] = oc
		c.mu.Unlock()
		switch {
		case prev == nil:
			c.log.Warn("a node does not answer forwards", "node", node, "lease", oc)
		case oc.dead && !prev.dead:
			c.log.Warn("a node that does not answer forwards is dead", "node", node, "lease", oc)
		}
		return oc, nil
	})
	return v.(*ownerCheck)
}

// fresh is the node's last read if it is younger than the period.
func (c *ownerChecks) fresh(node string) *ownerCheck {
	c.mu.Lock()
	defer c.mu.Unlock()
	if oc := c.found[node]; oc != nil && c.now().Sub(oc.at) < c.every {
		return oc
	}
	return nil
}

// answered is for a forward the node answered: it is reachable again.
func (c *ownerChecks) answered(node string) {
	c.mu.Lock()
	delete(c.found, node)
	c.mu.Unlock()
}

// answer is what a request for one of the owner's shards gets at now: how long
// to wait before trying again, and whether the owner's journal has been adopted
// (or it released its lease), so that its shards can be claimed.
func (oc *ownerCheck) answer(now time.Time) (retry time.Duration, adopted bool) {
	l := oc.lease
	switch {
	case errors.Is(oc.err, blob.ErrNotFound):
		return time.Second, true // a node with no lease has no journal to adopt
	case oc.err != nil, l.ValidAt(now):
		return time.Second, false
	case l.Adopted >= l.Epoch:
		return time.Second, true
	}
	// Dead and not adopted yet: adopters wait for the lease to expire.
	if left := l.ExpiresAt().Sub(now); left > 0 {
		return left + time.Second, false
	}
	return time.Second, false
}

func (oc *ownerCheck) String() string {
	switch {
	case errors.Is(oc.err, blob.ErrNotFound):
		return "none"
	case oc.err != nil:
		return "unread: " + oc.err.Error()
	case oc.dead:
		return "invalid"
	}
	return "valid"
}

// unreachable answers a request whose forward got no answer from the owner's
// node: 503, with a Retry-After from the owner's lease. If the owner is adopted
// its cache entry goes and Retry-After is 0: the request never reached the
// owner's app, and the next attempt claims the shard through Acquire. The lease
// read also refreshes the owner's address, in case it restarted somewhere else.
func (s *Server) unreachable(w http.ResponseWriter, r *http.Request, info *reqInfo) {
	oc := s.m.checks.check(r.Context(), info.owner)
	if oc.err == nil || errors.Is(oc.err, blob.ErrNotFound) {
		s.m.addrs.set(info.owner, oc.lease)
	}
	retry, adopted := oc.answer(s.m.now())
	msg := fmt.Sprintf("%s, the owner of %s, does not answer", info.owner, info.target)
	switch {
	case adopted:
		s.m.owners.Remove(info.target)
		retry, msg = 0, msg+", and it is adopted: try again"
	case oc.err != nil:
		msg += fmt.Sprintf(", and its lease could not be read: %v", oc.err)
	case oc.dead:
		msg += ", and it is dead and not adopted yet"
	}
	s.answer(w, r, info.own, &unavailable{msg, retry})
}

// unavailable is a 503 the node answers with, and the Retry-After it gives.
type unavailable struct {
	msg   string
	retry time.Duration
}

func (e *unavailable) Error() string { return e.msg }
