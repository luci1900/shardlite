package node

import (
	"io"
	"net/http"
)

// Ready reports whether the node should get traffic: the app has answered its
// ready route with a declaration, and
// the node's lease is good, and the node is not draining.
func (s *Server) Ready() (bool, string) {
	if s.m.draining.Load() {
		return false, "draining"
	}
	switch why := s.declarations.state.Load(); {
	case why == nil:
		return false, "waiting for the app's first declaration"
	case *why != "":
		return false, *why
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
