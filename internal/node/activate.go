package node

import (
	"net/http"
)

// serveActivate answers a request of the node's own for dispatch.Activate once
// route holds the shard here, and says nothing else: no listener lets the app
// send a path under /_shardlite/, and a peer takes one only from a forward of a
// node's request. The request routes as any call does, so it follows the owner
// cache and the manifest to the owner, is placed at random when a shard nobody
// live owns is called for, and creates nothing when the call says
// Shardlite-If-Exists. The answer is 200 if this request activated the shard and 204 if it was active already.
func serveActivate(w http.ResponseWriter, activated bool) {
	if activated {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
