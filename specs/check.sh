#!/bin/sh
# Runs the checks of the models in this directory: each model's typecheck, its
# simulation runs and its scripted tests. It needs Quint and runs from anywhere.
# It takes about 3 minutes, 2 of them in dispatch.qnt's run of its invariant.
#
# Each command says what it must do. Most pass. The broken variants of a model
# must fail, and in a named way: a run must violate the invariant it names, and
# a test must fail on its own `expect` or on a step that can't be taken. Any
# other failure (a parse or type error, a typo in a name, another test failing)
# counts as a mismatch, as does a command that was meant to fail and passes.
# Every command runs even after a mismatch, and the exit status is non-zero if
# any mismatched. The tail of Quint's output comes with a mismatch (a random
# run's seed is in it, to repeat the run).
#
# `quint verify` is left out: it often doesn't terminate, so simulation is the
# check (capture.qnt's header says how far verification gets there).
set -eu

if ! command -v quint >/dev/null 2>&1; then
	echo "check.sh: quint is not on the path (npm install -g @informalsystems/quint)" >&2
	exit 1
fi

here=$(cd "$(dirname "$0")" && pwd)
cd "$here/.."
tmp=$(mktemp -d)
trap 'rm -rf "${tmp:?}"' EXIT
out=$tmp/out
mismatches=0

# The text of the first line of the output that starts with the given prefix.
first() {
	grep -m1 "^$1" "$out" | cut -c1-160 || true
}

# What the command did, in the words the expectations use: pass, an invariant
# violated, a test failing on its expect, a test stuck on a step, or an error.
# $1 is the exit status, the rest are Quint's arguments.
outcome() {
	rc=$1
	shift
	sub=$1
	inv=
	test=
	for a in "$@"; do
		case $a in
		--invariant=*) inv=${a#--invariant=} ;;
		--match=*) test=${a#--match=} ;;
		esac
	done
	case $sub in
	typecheck)
		[ "$rc" -eq 0 ] && echo pass && return
		;;
	run)
		if [ "$rc" -eq 0 ] && grep -q '^\[ok\] No violation found' "$out"; then
			echo pass
			return
		fi
		if [ "$rc" -ne 0 ] && grep -q '^error: Invariant violated' "$out"; then
			echo "invariant $inv violated"
			return
		fi
		;;
	test)
		failed=$(grep -c 'failed after' "$out" || true)
		if [ "$rc" -eq 0 ] && [ "$failed" -eq 0 ] && grep -Eq '^ +[0-9]+ passing' "$out"; then
			echo pass
			return
		fi
		if [ "$rc" -ne 0 ] && [ "$failed" -eq 1 ]; then
			name=$(grep 'failed after' "$out" | sed 's/^ *[0-9]*) *//; s/ failed after.*//')
			err=$(sed -n 's/^ *Error \[\(QNT[0-9]*\)\]: \(.*\)/\1 \2/p' "$out" | head -1)
			case $name:$err in
			*"$test"*:QNT*" Expect condition does not hold true"*)
				echo "test ${test:-$name} fails on its expect"
				return
				;;
			*"$test"*:QNT*" Cannot continue"*)
				echo "test ${test:-$name} is stuck (${err%% *})"
				return
				;;
			esac
			echo "test $name fails: $err"
			return
		fi
		if [ "$rc" -ne 0 ] && [ "$failed" -gt 1 ]; then
			echo "$failed tests fail"
			return
		fi
		;;
	esac
	e=$(first "error")
	echo "${e:-exit $rc without the expected output}"
}

# check EXPECTATION quint-arguments...
# The expectation is pass, violated (a run, of its --invariant), expect (a test,
# of its --match, fails on its expect) or stuck:QNTnnn (a test fails on a step
# that can't be taken, with that error code).
check() {
	want=$1
	shift
	sub=$1
	inv=
	test=
	for a in "$@"; do
		case $a in
		--invariant=*) inv=${a#--invariant=} ;;
		--match=*) test=${a#--match=} ;;
		esac
	done
	case $want in
	pass) expected=pass ;;
	violated) expected="invariant $inv violated" ;;
	expect) expected="test $test fails on its expect" ;;
	stuck:*) expected="test $test is stuck (${want#stuck:})" ;;
	*)
		echo "check.sh: unknown expectation $want" >&2
		exit 2
		;;
	esac
	start=$(date +%s)
	rc=0
	quint "$@" >"$out" 2>&1 || rc=$?
	secs=$(($(date +%s) - start))
	got=$(outcome "$rc" "$@")
	if [ "$got" = "$expected" ]; then
		printf 'ok        %4ss  expected %s, got %s: quint %s\n' "$secs" "$expected" "$got" "$*"
	else
		printf 'MISMATCH  %4ss  expected %s, got %s: quint %s\n' "$secs" "$expected" "$got" "$*"
		tail -n 15 "$out" | cut -c1-200 | sed 's/^/    | /'
		mismatches=$((mismatches + 1))
	fi
}

sim="--max-steps=40 --max-samples=50000"

# capture.qnt
check pass typecheck specs/capture.qnt
check pass run specs/capture.qnt --main=captureProtocol --invariant=safe $sim
check violated run specs/capture.qnt --main=lockNoSync --invariant=safe $sim
check violated run specs/capture.qnt --main=lockGap --invariant=safe $sim
check violated run specs/capture.qnt --main=noSalts --invariant=safe $sim
check violated run specs/capture.qnt --main=mapAll --invariant=safe $sim
# A restart is reachable, which is what makes the model worth running.
check violated run specs/capture.qnt --main=captureProtocol --invariant=neverRestarts $sim

# cluster.qnt
check pass typecheck specs/cluster.qnt
check pass run specs/cluster.qnt --main=protocol --invariant=safe $sim
check pass run specs/cluster.qnt --main=protocol --invariant=liveNeverFenced $sim
check pass run specs/cluster.qnt --main=protocol --invariant=claimRulesAgree $sim
check pass test specs/cluster.qnt --main=uncheckedConfirm --match=staleConfirm
check violated run specs/cluster.qnt --main=claimAfterLease --invariant=ackedKept --max-steps=40
check violated run specs/cluster.qnt --main=resumeAfterUnknown --invariant=ackedKept --max-steps=40
check violated run specs/cluster.qnt --main=restartOverrides --invariant=liveNeverFenced --max-steps=40
# staleConfirmTest can't take its confirm step under protocol, which is the point of it.
check pass test specs/cluster.qnt --main=protocol '--match=^(?!staleConfirmTest$).*Test$'
check stuck:QNT508 test specs/cluster.qnt --main=protocol --match=staleConfirmTest
check stuck:QNT508 test specs/cluster.qnt --main=claimNeedsCleared --match=namedJournalClaim
check stuck:QNT513 test specs/cluster.qnt --main=claimNeedsAdopted --match=earlyClaim

# dispatch.qnt
check pass typecheck specs/dispatch.qnt
check pass run specs/dispatch.qnt --main=dispatchProtocol --invariant=safe $sim
check violated run specs/dispatch.qnt --main=pruneWithoutBase --invariant=covered $sim
check violated run specs/dispatch.qnt --main=deleteOnAnswer --invariant=covered $sim
check violated run specs/dispatch.qnt --main=manifestFirst --invariant=covered $sim
check violated run specs/dispatch.qnt --main=noAdoptionEntry --invariant=covered $sim
check pass test specs/dispatch.qnt --main=dispatchProtocol
check expect test specs/dispatch.qnt --main=sharedEntry --match=sharedEntryExample

# shard.qnt
check pass typecheck specs/shard.qnt
check pass run specs/shard.qnt --invariant=safe --max-steps=40 --max-samples=20000

if [ "$mismatches" -ne 0 ]; then
	echo "check.sh: $mismatches command(s) did not do what they must" >&2
	exit 1
fi
echo "check.sh: every command did what it must"
