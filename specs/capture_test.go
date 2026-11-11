//go:build model

package specs_test

import "testing"

// The checks of capture.qnt (see check_test.go for what a row says).
func TestCapture(t *testing.T) {
	check(t, []row{
		r(pass, "typecheck", "capture.qnt"),
		withSim(pass, "run", "capture.qnt", "--main=captureProtocol", "--invariant=safe"),
		withSim(violated, "run", "capture.qnt", "--main=lockNoSync", "--invariant=safe"),
		withSim(violated, "run", "capture.qnt", "--main=lockGap", "--invariant=safe"),
		withSim(violated, "run", "capture.qnt", "--main=noSalts", "--invariant=safe"),
		withSim(violated, "run", "capture.qnt", "--main=mapAll", "--invariant=safe"),
		// A restart is reachable, which is what makes the model worth running.
		withSim(violated, "run", "capture.qnt", "--main=captureProtocol", "--invariant=neverRestarts"),
	})
}
