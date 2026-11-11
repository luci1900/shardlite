//go:build model

package specs_test

import "testing"

// The checks of cluster.qnt (see check_test.go for what a row says).
func TestCluster(t *testing.T) {
	check(t, []row{
		r(pass, "typecheck", "cluster.qnt"),
		withSim(pass, "run", "cluster.qnt", "--main=protocol", "--invariant=safe"),
		withSim(pass, "run", "cluster.qnt", "--main=protocol", "--invariant=liveNeverFenced"),
		withSim(pass, "run", "cluster.qnt", "--main=protocol", "--invariant=claimRulesAgree"),
		r(pass, "test", "cluster.qnt", "--main=uncheckedConfirm", "--match=staleConfirm"),
		r(violated, "run", "cluster.qnt", "--main=claimAfterLease", "--invariant=ackedKept", "--max-steps=40"),
		r(violated, "run", "cluster.qnt", "--main=resumeAfterUnknown", "--invariant=ackedKept", "--max-steps=40"),
		r(violated, "run", "cluster.qnt", "--main=restartOverrides", "--invariant=liveNeverFenced", "--max-steps=40"),
		// staleConfirmTest can't take its confirm step under protocol, which is the point of it.
		r(pass, "test", "cluster.qnt", "--main=protocol", `--match=^(?!staleConfirmTest$).*Test$`),
		r(stuck("QNT508"), "test", "cluster.qnt", "--main=protocol", "--match=staleConfirmTest"),
		r(stuck("QNT508"), "test", "cluster.qnt", "--main=claimNeedsCleared", "--match=namedJournalClaim"),
		r(stuck("QNT513"), "test", "cluster.qnt", "--main=claimNeedsAdopted", "--match=earlyClaim"),
	})
}
