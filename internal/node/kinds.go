package node

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/model/kind"
	"github.com/luci1900/shardlite/internal/model/shard"
)

// DefaultIdleTimeout is how long a shard stays active without requests when
// neither a declaration nor the environment says otherwise.
const DefaultIdleTimeout = 10 * time.Minute

var (
	// ErrUnknownKind: the kind isn't declared by an app.
	ErrUnknownKind = errors.New("shard kind is not declared")
	// ErrBadTarget: not a kind/id.
	ErrBadTarget = errors.New("bad shard name")
)

// ParseTarget parses a Shardlite-Target value: the kind and the id, joined by
// a slash (users/42). The grammar is internal/model/shard's; ErrBadTarget is the
// node's answer for anything it refuses.
func ParseTarget(target string) (shard.Name, error) {
	n, err := shard.ParseName(target)
	if err != nil {
		return shard.Name{}, fmt.Errorf("%w: %w", ErrBadTarget, err)
	}
	return n, nil
}

// Kinds is the node's table of declared kinds, the app's, parsed from its ready
// route. The kinds are keyed by their names (users). A poll that fails keeps the kinds of the
// last declaration that was good; the node reports ready only while the polls
// are succeeding (see the declaration poll in declarations.go).
type Kinds struct {
	defaultIdle time.Duration
	app         atomic.Pointer[kind.App] // the app's last good declaration
}

// NewKinds makes the table, with no app kinds yet: the first poll adds them.
func NewKinds(defaultIdle time.Duration) *Kinds {
	if defaultIdle <= 0 {
		defaultIdle = DefaultIdleTimeout
	}
	return &Kinds{defaultIdle: defaultIdle}
}

// Update makes a the app's declaration current, for new activations and new
// responses. It is the declaration poll's success path.
func (k *Kinds) Update(a kind.App) { k.app.Store(&a) }

// Lookup returns a kind as it was declared in the app's ready route:
// ErrUnknownKind if it wasn't.
func (k *Kinds) Lookup(name shard.Kind) (kind.Kind, error) {
	if a := k.app.Load(); a != nil {
		if c, ok := a.Kinds[name]; ok {
			return c, nil
		}
	}
	return kind.Kind{}, fmt.Errorf("%w: %q", ErrUnknownKind, name)
}
