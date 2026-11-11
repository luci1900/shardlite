#!/bin/sh
# Regenerates the traces of specs/capture.qnt that TestReplayModelTraces plays
# against the capture code. It needs Quint (the tests themselves do not). Run it
# from anywhere, and review the diff: a trace changes when the model does. It
# writes to the directory given as its first argument, by default its own.
#
# Each trace is the first one the simulator finds, for a seed, that violates a
# reachability check of the model (a val named never..., which should fail), so
# it ends where the check says, with --mbt so that every state carries the action
# taken. The model's RESTART_AT is restartAt in replay_test.go. The traces in
# variants are the first counterexamples to safe of the model's broken variants,
# which TestReplayCounterexamples plays.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
out=$(mkdir -p "${1:-$here}" && cd "${1:-$here}" && pwd)
cd "$here"
spec=../../../../specs/capture.qnt

# The version of Quint the model suite pins: another may make other traces.
want=$(sed -n 's/^quint_version=//p' ../../../tools/testwatch/model.sh)
if [ "$(quint --version)" != "$want" ]; then
	echo "generate.sh: quint is $(quint --version), the traces are made with $want" >&2
	exit 1
fi

# trace name check steps seed [instance]
trace() {
	quint run "$spec" --main="${5:-captureProtocol}" --invariant="$2" --max-steps="$3" --seed="$4" --max-samples=100000 --mbt --out-itf="$out/$1.itf.json" >/dev/null || [ -f "$out/$1.itf.json" ]
}

mkdir -p "$out/variants"
rm -f "$out"/*.itf.json "$out"/variants/*.itf.json

trace commit-release neverReleasedAfterOneCommit 2 1
trace locked-restart neverThreeFramesCopied 12 3
trace locked-restart-around-a-snapshot neverThreeFramesCopied 14 17
trace locked-restart-after-a-snapshot neverThreeFramesCopied 14 4242
trace resumed-release neverThreeFramesCopied 14 101
trace snapshot-after-restart-1 neverSnapshotAfterRestart 16 101
trace snapshot-after-restart-2 neverSnapshotAfterRestart 16 17
trace snapshot-after-restart-3 neverSnapshotAfterRestart 16 4242
trace snapshot-after-restart-4 neverSnapshotAfterRestart 16 99991
# The app truncates the WAL while a release waits, and a snapshot follows the resume.
trace snapshot-after-truncate neverSnapshotAfterRestart 18 237570
trace at-rest-1 neverFourCapturedAtRest 16 101
trace at-rest-2 neverFourCapturedAtRest 16 17
trace at-rest-3 neverFourCapturedAtRest 16 3
trace at-rest-4 neverFourCapturedAtRest 16 4242
trace at-rest-5 neverFourCapturedAtRest 16 99991
# A trace may end inside a snapshot or a release: the replay lets the snapshot go and drops the release.
trace snapshot-half-read neverSnapshotHalfRead 12 1
trace release-waiting neverReleaseWaitingAfterCommit 12 1

trace variants/lockNoSync safe 40 1 lockNoSync
trace variants/noSalts safe 40 1 noSalts
trace variants/mapAll safe 40 1 mapAll
