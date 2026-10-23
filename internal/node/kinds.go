package node

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/luci1900/shardlite/internal/kind"
	"github.com/luci1900/shardlite/internal/shard"
)

// DefaultIdleTimeout is how long a shard stays active without requests when
// neither a declaration nor the environment says otherwise.
const DefaultIdleTimeout = 10 * time.Minute

var (
	// ErrUnknownKind: the kind isn't declared by an app.
	ErrUnknownKind = errors.New("shard kind is not declared")
	// ErrNotPublic: a client named a kind that isn't public. It is answered as
	// ErrUnknownKind is, and only the node's journal says which it was.
	ErrNotPublic = errors.New("shard kind is not public")
	// ErrBadTarget: not a kind/id.
	ErrBadTarget = errors.New("bad shard name")
)

// ParseTarget parses a Shardlite-Target value: the kind qualified by the app
// that serves it, and the id, joined by a slash (game.users/42). The grammar is
// internal/shard's; ErrBadTarget is the node's answer for anything it refuses.
func ParseTarget(target string) (shard.Name, error) {
	n, err := shard.ParseName(target)
	if err != nil {
		return shard.Name{}, fmt.Errorf("%w: %w", ErrBadTarget, err)
	}
	return n, nil
}

// Kinds is the node's table of declared kinds: the node's own app's, built as
// a declaration in code, and the app's, parsed from its ready route. The kinds
// are keyed by their qualified names (game.users, _system.dispatch), so the
// table says which app serves a kind. A poll that fails keeps the kinds of the
// last declaration that was good; the node reports ready only while the polls
// are succeeding (see the ready poll in ready.go).
type Kinds struct {
	defaultIdle time.Duration
	app         atomic.Pointer[kind.App] // the app's last good declaration
	sys         kind.App                 // the node's own app, set as the catalog is built (setSystem), before anything reads it
}

// NewKinds makes the table, with no app kinds yet: the first poll adds them.
func NewKinds(defaultIdle time.Duration) *Kinds {
	if defaultIdle <= 0 {
		defaultIdle = DefaultIdleTimeout
	}
	return &Kinds{defaultIdle: defaultIdle}
}

// Parse parses a declaration with the node's default idle timeout.
func (k *Kinds) Parse(data []byte) (kind.App, error) {
	return kind.ParseApp(data, k.defaultIdle)
}

// Update makes a the app's declaration current, for new activations and new
// responses. It is the ready poll's success path.
func (k *Kinds) Update(a kind.App) { k.app.Store(&a) }

// setSystem adds the node's own app, which nothing changes afterwards. Its
// kinds already carry their defaults (newSystemApp).
func (k *Kinds) setSystem(a kind.App) {
	k.sys = a
}

// Lookup returns a kind as it was declared, in an app's ready route or by its
// system kind's package: ErrUnknownKind if it is neither.
func (k *Kinds) Lookup(name shard.Kind) (kind.Kind, error) {
	if c, ok := k.sys.Kinds[name]; ok {
		return c, nil
	}
	if a := k.app.Load(); a != nil {
		if c, ok := a.Kinds[name]; ok {
			return c, nil
		}
	}
	return kind.Kind{}, fmt.Errorf("%w: %q", ErrUnknownKind, name)
}

// CheckPublic is for a target a client named: a kind that isn't public gets
// the same error as an undeclared one, so clients can't tell it exists. Other
// errors (an undeclared kind) are left to Lookup.
func (k *Kinds) CheckPublic(n shard.Name) error {
	if c, err := k.Lookup(n.Kind()); err == nil && !c.Public {
		return fmt.Errorf("%w: %q", ErrNotPublic, n.Kind())
	}
	return nil
}
