// Package shard is the names of a shard (docs/guide.md, Shard names): the
// shard's kind, its id and the generation of its local file, each a type of its
// own, as package pos does for the protocol's numbers. A shard's name is kind/id
// and a kind's name is the whole name of its kind (users, or matchmaker.queue
// for a library's), with no app in front of it. A shard's name is parsed once at
// the edge and then carried as a Name, so a kind can't be used as an id and the
// parts can't disagree with the name they came from.
package shard

import (
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"strconv"
	"strings"
)

const (
	// MaxKindLen is the length of a kind's name, dots included.
	MaxKindLen = 64
	// MaxIDLen is the length of a shard's id.
	MaxIDLen = 128
)

var (
	// A kind is one or more segments joined by dots. The node never splits
	// at a dot, so matchmaker.queue is one kind. One leading underscore marks
	// a kind as the node's (Kind.Reserved).
	kindName = regexp.MustCompile(`^_?[a-z][a-z0-9_-]*(\.[a-z][a-z0-9_-]*)*$`)
	idName   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
)

// Kind is a kind's name (users, or matchmaker.queue): the whole of it, with no
// app in front. A dot is an ordinary character in it, which the node never
// splits at.
type Kind string

// Reserved reports whether the kind is the node's: a kind whose name starts
// with an underscore is, and no app may declare one.
func (k Kind) Reserved() bool { return strings.HasPrefix(string(k), "_") }

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
// an id parsed from a target. It can't take the id as the kind. ParseName is the way in from a string.
func New(kind Kind, id ID) Name { return Name{kind: kind, id: id} }

// ParseName parses a shard's name, kind/id, checking both parts.
func ParseName(s string) (Name, error) {
	kind, id, ok := strings.Cut(s, "/")
	if !ok {
		return Name{}, fmt.Errorf("%q is not kind/id", s)
	}
	if !ValidKind(kind) {
		return Name{}, fmt.Errorf("%q has no valid kind (lowercase letters, digits, - and _, starting with a letter, in segments joined by dots, up to %d characters)", s, MaxKindLen)
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

// Lane is one of the Lanes slices that shard names fall into by a hash of the name.
// Keys in blob storage that are per shard lead with it, so each lane lists on its
// own and a lane's work can be given to one node.
type Lane uint8

// Lanes is how many lanes there are.
const Lanes = 16

// Lane is the shard's lane: a hash of its name modulo Lanes, which never changes.
func (n Name) Lane() Lane {
	h := fnv.New32a()
	h.Write([]byte(n.String()))
	return Lane(h.Sum32() % Lanes)
}

// String is the lane's one hex digit, as it appears in keys.
func (l Lane) String() string { return strconv.FormatUint(uint64(l), 16) }

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

// ValidKind reports whether s is a kind's name: segments joined by single dots,
// each a lowercase letter followed by lowercase letters, digits, dashes and
// underscores, with one optional leading underscore for the node's own, up to
// MaxKindLen characters in all.
func ValidKind(s string) bool { return len(s) <= MaxKindLen && kindName.MatchString(s) }

// ValidID reports whether s is a shard id.
func ValidID(s string) bool { return idName.MatchString(s) }
