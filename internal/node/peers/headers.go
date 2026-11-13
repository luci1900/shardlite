package peers

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/dunglas/httpsfv"

	"github.com/luci1900/shardlite/internal/model/cluster"
)

// The two headers nodes send each other (docs/internals.md, Routing and forwarding). Both are trusted only between nodes: they are read on the peer listener's requests and on answers from peers, and removed from what a client or the app would see.
const (
	// HeaderNode is the sender's name, on the forwarder's requests only (the receiver of a forward knows only an address with an ephemeral port). It is a plain value, checked with cluster.ValidNode.
	HeaderNode = "Shardlite-Node"
	// HeaderPlacement is an instruction about the sender, a Structured Fields Item (RFC 9651). The one value is the token PlacementAvoid. Parameters a node does not know are ignored, so the instruction can grow one without a new header.
	HeaderPlacement = "Shardlite-Placement"
)

// PlacementAvoid tells a peer to place nothing on the sender. A draining node sets it on every answer from its peer listener and on its forwards.
const PlacementAvoid = "avoid"

// errBadPeerHeader is why a header from a peer was ignored.
var errBadPeerHeader = errors.New("bad peer header")

// parseNode reads the sender's name from a request's headers. A request without the header is not an error and gives the empty node.
func parseNode(h http.Header) (cluster.Node, error) {
	values := h.Values(HeaderNode)
	switch {
	case len(values) == 0:
		return "", nil
	case len(values) > 1:
		return "", fmt.Errorf("%w: %s is set %d times", errBadPeerHeader, HeaderNode, len(values))
	case !cluster.ValidNode(values[0]):
		return "", fmt.Errorf("%w: %q is not a node's name", errBadPeerHeader, values[0])
	}
	return cluster.Node(values[0]), nil
}

// parsePlacement reads the placement a peer asked for, which is only whether it asked to be avoided. A message without the header is not an error and does not ask. A value that does not parse, or is not the token PlacementAvoid, is an error.
func parsePlacement(h http.Header) (avoid bool, err error) {
	values := h.Values(HeaderPlacement)
	if len(values) == 0 {
		return false, nil
	}
	item, err := httpsfv.UnmarshalItem(values)
	if err != nil {
		return false, fmt.Errorf("%w: %v", errBadPeerHeader, err)
	}
	if tok, ok := item.Value.(httpsfv.Token); !ok || tok != PlacementAvoid {
		return false, fmt.Errorf("%w: %s is not the token %s", errBadPeerHeader, HeaderPlacement, PlacementAvoid)
	}
	return true, nil
}
