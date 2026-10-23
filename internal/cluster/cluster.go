// Package cluster is the name of a node: its syntax and, as Node, the type
// every layer carries it as. A node's name is checked once at the edge
// (ParseEnv) and then carried as a Node, so it can't be passed where an
// address, a journal key or a shard's name belongs.
package cluster

import "regexp"

// Node is a node's name. It is what a lease, a blob write id and a manifest's
// owner all carry. The empty string means "no node" (a released manifest, no
// owner) and is the zero value.
type Node string

// nodeName is what a node's name may be. Host names fit.
var nodeName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// ValidNode reports whether s is a node's name: 1 to 128 ASCII letters, digits,
// '.', '-' and '_'. The name goes into object keys as it is (docs/guide.md, the
// reference), so nothing else may be in it.
func ValidNode(s string) bool { return nodeName.MatchString(s) }
