// Package shard is the names of a shard (docs/guide.md, Shard names): the app
// that serves its kind, the kind's qualified name, the shard's id and the
// generation of its local file, each a type of its own, as package pos does for
// the protocol's numbers. A shard's name is parsed once at the edge and then
// carried as a Name, so a kind can't be used as an id and the parts can't
// disagree with the name they came from.
package shard

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	// SystemApp is the app the node's own kinds belong to. A name that starts
	// with an underscore is the node's: no app may take one.
	SystemApp App = "_system"
	// MaxAppLen and MaxKindLen are the lengths of an app's name and of a
	// kind's own name, so a qualified kind's name is at most 64 characters.
	MaxAppLen  = 31
	MaxKindLen = 32
	// MaxIDLen is the length of a shard's id.
	MaxIDLen = 128
)

var (
	appName  = regexp.MustCompile(`^[a-z_][a-z0-9_]*(\.[a-z0-9_]+)*$`)
	kindName = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	idName   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
)

// App is an app's name: the namespace its kinds are qualified by.
type App string

// Kind is a kind's qualified name: the app that serves it and the kind's own
// name, joined by a dot (game.users).
type Kind string

// ID is a shard's id under its kind (42). An id is 1 to MaxIDLen ASCII
// letters, digits, "-" or "_", so it goes into object keys, file paths and
// headers as it is, with nothing to escape.
type ID string

// Gen is the generation of a shard: a random name that makes the keys and the
// local file of a shard deleted and created again distinct.
type Gen string

// Name is a shard's parsed name: its kind and its id.
type Name struct {
	kind Kind
	id   ID
}

// New is a name from parts that are already valid: a declared kind's name and
// either an id parsed from a target or a system kind's fixed id. It can't take
// the id as the kind. ParseName is the way in from a string.
func New(kind Kind, id ID) Name { return Name{kind: kind, id: id} }

// ParseName parses a shard's name, kind/id, checking both parts.
func ParseName(s string) (Name, error) {
	kind, id, ok := strings.Cut(s, "/")
	if !ok {
		return Name{}, fmt.Errorf("%q is not kind/id", s)
	}
	if !ValidKindName(kind) {
		return Name{}, fmt.Errorf("%q has no valid kind (an app's name and a kind's name, joined by a dot)", s)
	}
	if !ValidID(id) {
		return Name{}, fmt.Errorf("%q needs an id of 1 to %d letters, digits, - or _", s, MaxIDLen)
	}
	return New(Kind(kind), ID(id)), nil
}

// MustParse is ParseName for a literal that is known to be valid: a test's
// shard name, or a name built by the node itself.
func MustParse(s string) Name {
	n, err := ParseName(s)
	if err != nil {
		panic(err)
	}
	return n
}

// Kind is the kind that serves the shard.
func (n Name) Kind() Kind { return n.kind }

// ID is the shard's id under its kind.
func (n Name) ID() ID { return n.id }

// String is the shard's name, kind/id.
func (n Name) String() string {
	if n.IsZero() {
		return ""
	}
	return string(n.kind) + "/" + string(n.id)
}

// IsZero reports whether the name is empty: a request with no target.
func (n Name) IsZero() bool { return n.kind == "" && n.id == "" }

// System reports whether the shard belongs to one of the node's own kinds.
func (n Name) System() bool { return strings.HasPrefix(string(n.kind), "_") }

// MarshalText is the name as it goes into JSON and the logs.
func (n Name) MarshalText() ([]byte, error) {
	if n.IsZero() {
		return nil, errors.New("shard: a zero name cannot be encoded")
	}
	return []byte(n.String()), nil
}

// UnmarshalText parses a name from JSON or another text field.
func (n *Name) UnmarshalText(b []byte) error {
	parsed, err := ParseName(string(b))
	if err != nil {
		return err
	}
	*n = parsed
	return nil
}

// Value is the name as the cache index stores it.
func (n Name) Value() (driver.Value, error) {
	if n.IsZero() {
		return nil, errors.New("shard: a zero name cannot be stored")
	}
	return n.String(), nil
}

// Scan parses a name from the cache index.
func (n *Name) Scan(src any) error {
	switch v := src.(type) {
	case string:
		return n.UnmarshalText([]byte(v))
	case []byte:
		return n.UnmarshalText(v)
	case nil:
		return errors.New("shard: a name is not null")
	default:
		return fmt.Errorf("shard: cannot scan a name from %T", src)
	}
}

// ValidApp reports whether s is an app's name: lowercase letters, digits,
// underscores and dots, up to MaxAppLen characters, starting with a letter or
// an underscore.
func ValidApp(s string) bool { return len(s) <= MaxAppLen && appName.MatchString(s) }

// ValidKind reports whether s is a kind's own name: lowercase letters, digits,
// dashes and underscores, up to MaxKindLen characters, starting with a letter.
func ValidKind(s string) bool { return kindName.MatchString(s) }

// ValidKindName reports whether s is a kind's qualified name: an app's name
// and the kind's own name, joined by a dot.
func ValidKindName(s string) bool {
	app, kind, ok := Split(Kind(s))
	return ok && ValidApp(string(app)) && ValidKind(kind)
}

// ValidID reports whether s is a shard id.
func ValidID(s string) bool { return idName.MatchString(s) }

// Reserved reports whether an app's name is the node's: a name that starts
// with an underscore belongs to the node, and no app may take one.
func Reserved(app string) bool { return strings.HasPrefix(app, "_") }

// Qualify is a kind's qualified name, from the app that serves it and the
// kind's own name. Both parts are checked by the caller.
func Qualify(app App, kind string) Kind { return Kind(string(app) + "." + kind) }

// Split splits a qualified kind's name into the app that serves it and the
// kind's own name, at the last dot. ok is false if the name has no dot.
func Split(qualified Kind) (app App, kind string, ok bool) {
	i := strings.LastIndex(string(qualified), ".")
	if i < 0 {
		return "", "", false
	}
	return App(qualified[:i]), string(qualified)[i+1:], true
}
