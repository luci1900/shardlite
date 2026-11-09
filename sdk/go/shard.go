package shardlite

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	maxKindLen = 64
	maxIDLen   = 128
)

var (
	// A kind is one or more segments joined by dots, each a lowercase letter
	// then lowercase letters, digits, - or _. A leading underscore marks the
	// node's own kinds, which an app never names.
	kindName = regexp.MustCompile(`^[a-z][a-z0-9_-]*(\.[a-z][a-z0-9_-]*)*$`)
	idName   = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
)

func validKind(kind string) bool { return len(kind) <= maxKindLen && kindName.MatchString(kind) }

// ShardName is the name of a shard, kind/id. Kind.Shard builds one from a
// declared kind and ParseShard from a stored string. The zero value names
// nothing and is refused wherever a name is used.
type ShardName struct {
	kind, id string
}

// String is the name as the node writes it, kind/id, and "" for the zero value.
func (s ShardName) String() string {
	if s == (ShardName{}) {
		return ""
	}
	return s.kind + "/" + s.id
}

// valid reports whether the name fits the grammar.
func (s ShardName) valid() bool { return validKind(s.kind) && idName.MatchString(s.id) }

// ParseShard reads a shard name, kind/id, from a string the app kept (the
// target of a durable send, say). It checks the grammar of the guide's Shard
// names and nothing else: whether the kind is declared is the node's to answer,
// with a 404.
func ParseShard(s string) (ShardName, error) {
	kind, id, ok := strings.Cut(s, "/")
	switch {
	case !ok:
		return ShardName{}, fmt.Errorf("shardlite: %q is not kind/id", s)
	case !validKind(kind):
		return ShardName{}, fmt.Errorf("shardlite: %q has no valid kind (lowercase letters, digits, - and _, starting with a letter, in segments joined by dots, up to %d characters)", s, maxKindLen)
	case !idName.MatchString(id):
		return ShardName{}, fmt.Errorf("shardlite: %q needs an id of 1 to %d letters, digits, - or _", s, maxIDLen)
	}
	return ShardName{kind: kind, id: id}, nil
}
