//go:build model

package specs_test

import "testing"

// The checks of dispatch.qnt (see check_test.go for what a row says).
func TestDispatch(t *testing.T) {
	check(t, []row{
		r(pass, "typecheck", "dispatch.qnt"),
		withSim(pass, "run", "dispatch.qnt", "--main=dispatchProtocol", "--invariant=safe"),
		withSim(violated, "run", "dispatch.qnt", "--main=pruneWithoutBase", "--invariant=covered"),
		withSim(violated, "run", "dispatch.qnt", "--main=deleteOnAnswer", "--invariant=covered"),
		withSim(violated, "run", "dispatch.qnt", "--main=manifestFirst", "--invariant=covered"),
		withSim(violated, "run", "dispatch.qnt", "--main=noAdoptionEntry", "--invariant=covered"),
		r(pass, "test", "dispatch.qnt", "--main=dispatchProtocol"),
		r(expect, "test", "dispatch.qnt", "--main=sharedEntry", "--match=sharedEntryExample"),
	})
}
