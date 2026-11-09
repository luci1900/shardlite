package node

import (
	"io"
	"net/http"
)

// Ready reports whether the node should get traffic: the app has answered its
// ready route with a declaration (a node with no app address skips this), and
// the node's lease is good, and the node is not draining.
func (s *Server) Ready() (bool, string) {
	if s.m.draining.Load() {
		return false, "draining"
	}
	if s.declarations != nil && !s.declarations.ok.Load() {
		if why := s.declarations.why.Load(); why != nil {
			return false, *why
		}
		return false, "waiting for the app's first declaration"
	}
	if err := s.keeper.Check(); err != nil {
		return false, "the node's lease is not valid"
	}
	return true, "ok"
}

func (s *Server) readyRoute(w http.ResponseWriter, r *http.Request) {
	ok, why := s.Ready()
	status := http.StatusOK
	if !ok {
		status = http.StatusServiceUnavailable
		w.Header().Set(hdrError, string(reasonNotReady))
	}
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		io.WriteString(w, why+"\n")
	}
}
