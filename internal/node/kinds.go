package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/luci1900/shardlite/internal/kind"
)

// DefaultIdleTimeout is how long a shard stays active without requests when
// neither the kinds file nor the environment says otherwise.
const DefaultIdleTimeout = 10 * time.Minute

var (
	// ErrUnknownKind: the kind isn't declared in the kinds file.
	ErrUnknownKind = errors.New("shard kind is not declared")
	// ErrNotPublic: a client named a kind that isn't public. It is answered as
	// ErrUnknownKind is, and only the node's journal says which it was.
	ErrNotPublic = errors.New("shard kind is not public")
	// ErrBadTarget: not a kind/id.
	ErrBadTarget = errors.New("bad shard name")
)

var (
	kindName   = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)   // a kind of the kinds file
	targetKind = regexp.MustCompile(`^_?[a-z][a-z0-9_-]{0,31}$`) // and a system kind's, which starts with _
)

const maxIDLen = 128

var idName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// ParseTarget splits a Shardlite-Target value into its kind and id, at the
// first "/". Ids are restricted to ASCII letters, digits, "-" and "_", up to
// maxIDLen bytes (enough for any UUID, ULID or number), so they go into object
// keys, file paths and headers as they are, with nothing to escape.
func ParseTarget(target string) (kind, id string, err error) {
	kind, id, ok := strings.Cut(target, "/")
	if !ok {
		return "", "", fmt.Errorf("%w: %q is not kind/id", ErrBadTarget, target)
	}
	if !targetKind.MatchString(kind) {
		return "", "", fmt.Errorf("%w: %q has no valid kind", ErrBadTarget, target)
	}
	if !idName.MatchString(id) {
		return "", "", fmt.Errorf("%w: %q needs an id of 1 to %d letters, digits, - or _", ErrBadTarget, target, maxIDLen)
	}
	return kind, id, nil
}

type kindJSON struct {
	Sync        string `json:"sync"` // "on" (the default) or "off": a string, like Shardlite-Sync, so more modes can follow
	IdleTimeout string `json:"idle_timeout"`
	Public      bool   `json:"public"`
}

type kindsJSON struct {
	Kinds map[string]kindJSON `json:"kinds"`
}

// ParseKinds parses a kinds file (docs/guide.md, Kinds file). defaultIdle is
// used for kinds without an idle_timeout of their own: a kind's own setting wins.
func ParseKinds(data []byte, defaultIdle time.Duration) (map[string]kind.Kind, error) {
	var f kindsJSON
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("kinds file: %w", err)
	}
	if f.Kinds == nil {
		return nil, errors.New(`kinds file: no "kinds" object`)
	}
	out := make(map[string]kind.Kind, len(f.Kinds))
	for name, k := range f.Kinds {
		if !kindName.MatchString(name) {
			return nil, fmt.Errorf("kinds file: %q is not a valid kind name (lowercase letters, digits, - and _, starting with a letter)", name)
		}
		idle := defaultIdle
		if k.IdleTimeout != "" {
			d, err := time.ParseDuration(k.IdleTimeout)
			if err != nil || d <= 0 {
				return nil, fmt.Errorf("kinds file: %s: bad idle_timeout %q", name, k.IdleTimeout)
			}
			idle = d
		}
		var sync kind.Sync
		switch k.Sync {
		case "", "on":
		case "off":
			sync = kind.SyncOff
		default:
			return nil, fmt.Errorf(`kinds file: %s: sync is "on" or "off", not %q`, name, k.Sync)
		}
		out[name] = kind.Kind{Name: name, Sync: sync, IdleTimeout: idle, Public: k.Public}
	}
	return out, nil
}

// Kinds is the node's view of the kinds file, and of the system kinds. A
// reload that fails keeps the kinds that were loaded and marks the node not
// ready (Valid is false) until a reload succeeds.
type Kinds struct {
	path        string
	defaultIdle time.Duration
	cur         atomic.Pointer[map[string]kind.Kind]
	valid       atomic.Bool
	sys         systemKinds // set as the catalog is built (setSystem), before anything reads it
}

// LoadKinds reads the kinds file. The first load must succeed.
func LoadKinds(path string, defaultIdle time.Duration) (*Kinds, error) {
	if defaultIdle <= 0 {
		defaultIdle = DefaultIdleTimeout
	}
	k := &Kinds{path: path, defaultIdle: defaultIdle}
	if err := k.Reload(); err != nil {
		return nil, err
	}
	return k, nil
}

// Reload reads the file again. New activations and new responses see the
// result.
func (k *Kinds) Reload() error {
	data, err := os.ReadFile(k.path)
	var m map[string]kind.Kind
	if err == nil {
		m, err = ParseKinds(data, k.defaultIdle)
	}
	if err != nil {
		k.valid.Store(false)
		return err
	}
	k.cur.Store(&m)
	k.valid.Store(true)
	return nil
}

// Valid reports whether the last load succeeded. The node reports ready only
// while it is true.
func (k *Kinds) Valid() bool { return k.valid.Load() }

// setSystem adds the system kinds, which a reload never changes, with the
// node's idle timeout for those that leave theirs at zero.
func (k *Kinds) setSystem(sys systemKinds) {
	for _, s := range sys {
		if s.IdleTimeout == 0 {
			s.IdleTimeout = k.defaultIdle
		}
	}
	k.sys = sys
}

// Lookup returns a kind as it was declared, in the kinds file or by its system
// kind's package: ErrUnknownKind if it is neither.
func (k *Kinds) Lookup(name string) (kind.Kind, error) {
	if s := k.sys[name]; s != nil {
		return *s, nil
	}
	m := *k.cur.Load()
	c, ok := m[name]
	if !ok {
		return kind.Kind{}, fmt.Errorf("%w: %q", ErrUnknownKind, name)
	}
	return c, nil
}

// CheckPublic is for a target a client named: a kind that isn't public gets
// the same error as an undeclared one, so clients can't tell it exists. Other
// errors (a bad target, an undeclared kind) are left to Lookup.
func (k *Kinds) CheckPublic(target string) error {
	kind, _, err := ParseTarget(target)
	if err != nil {
		return err
	}
	if c, err := k.Lookup(kind); err == nil && !c.Public {
		return fmt.Errorf("%w: %q", ErrNotPublic, kind)
	}
	return nil
}

// kindsReload reloads the kinds file whenever the process gets SIGHUP.
type kindsReload struct {
	kinds *Kinds
	log   *slog.Logger
}

func (*kindsReload) String() string { return "kinds reload" }

func (k *kindsReload) Serve(ctx context.Context) error {
	defer serving(k.log, k.String())()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	defer signal.Stop(ch)
	for {
		select {
		case <-ch:
			if err := k.kinds.Reload(); err != nil {
				k.log.Error("kinds file not reloaded, keeping the kinds already loaded and reporting not ready", "path", k.kinds.path, "err", err)
			} else {
				k.log.Info("kinds file reloaded", "path", k.kinds.path)
			}
		case <-ctx.Done():
			return nil
		}
	}
}
