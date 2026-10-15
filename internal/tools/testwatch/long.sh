#!/bin/sh
# The long suite: what fast.sh leaves out, run through testwatch so a stuck test
# binary is dumped and killed after 30 seconds without CPU. It needs RustFS (it
# checks that RustFS answers and says how to start it, but starts nothing).
#
#	internal/tools/testwatch/long.sh                  # the runs below
#	internal/tools/testwatch/long.sh -race            # -race in the runs that have none too
#	internal/tools/testwatch/long.sh ./internal/node  # a package pattern for the main module's runs (e2e is skipped)
#
# The runs, each with its own report (failures, a line per package, the twenty
# slowest tests):
#
#	1. the main module with -tags fault, without -race
#	2. the main module with -tags fault and -race
#	3. the e2e module with -tags fault and -race
#	4. on Linux only: TestMeasurePerShardCost and the real-filesystem check on
#	   /dev/shm (the /proc/self/io check in internal/capture is part of runs 1 and 2)
#
# If TESTWATCH_OUT names a directory, each run's go test -json output and report
# are copied there (CI uploads them).
#
# Arguments starting with - go to go test in every run. Other arguments are
# package patterns for runs 1, 2 and 4 (the default is ./internal/...).
set -u
cd "$(dirname "$0")/../../.." || exit 1
root=$(pwd)

endpoint=${SHARDLITE_TEST_S3_ENDPOINT:-http://127.0.0.1:19000}
if ! curl -s -o /dev/null --max-time 5 "$endpoint"; then
	cat >&2 <<MSG
RustFS does not answer at $endpoint.
Start it with:
	docker compose -f docker/compose.s3.yaml up -d
(or set SHARDLITE_TEST_S3_ENDPOINT to another S3 endpoint) and run this again.
Stop it afterwards with: docker compose -f docker/compose.s3.yaml down
MSG
	exit 2
fi
export SHARDLITE_TEST_S3_ENDPOINT="$endpoint"
export SHARDLITE_TEST_S3_BUCKET="${SHARDLITE_TEST_S3_BUCKET:-shardlite-test}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-shardlite}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-shardlite-secret}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
unset SHARDLITE_MEASURE SHARDLITE_MEASURE_MB SHARDLITE_TEST_FS_PATH SHARDLITE_TEST_FS_WANT

flags=
pkgs=
for a in "$@"; do
	case $a in
	-*) flags="$flags $a" ;;
	*) pkgs="$pkgs $a" ;;
	esac
done
all=
[ -z "$pkgs" ] && all=1 && pkgs=./internal/...

bin=$(mktemp -d)
trap 'rm -rf "$bin"' EXIT
go build -o "$bin/testwatch" ./internal/tools/testwatch || exit 1

summary=
status=0
# run NAME DIR ENV... -- go test arguments. Prints the run's report and adds a line to the summary.
run() {
	name=$1 dir=$2
	shift 2
	envs=
	while [ "$1" != -- ]; do
		envs="$envs $1"
		shift
	done
	shift
	echo
	echo "=== $name"
	start=$(date +%s)
	rc=0
	# shellcheck disable=SC2086
	(cd "$root/$dir" && env $envs go test -json -exec "$bin/testwatch" -timeout 30m "$@" >"$bin/out.json") || rc=$?
	"$bin/testwatch" report -n 20 <"$bin/out.json" >"$bin/report.txt" 2>&1 || rc=$?
	cat "$bin/report.txt"
	secs=$(($(date +%s) - start))
	result=ok
	if [ $rc -ne 0 ]; then
		result=FAILED
		status=1
	fi
	summary="$summary
$result	${secs}s	$name"
	if [ -n "${TESTWATCH_OUT:-}" ]; then
		mkdir -p "$TESTWATCH_OUT"
		tag=$(echo "$name" | tr -c 'A-Za-z0-9\n' _)
		cp "$bin/out.json" "$TESTWATCH_OUT/$tag.json"
		cp "$bin/report.txt" "$TESTWATCH_OUT/$tag.report.txt"
	fi
}

# shellcheck disable=SC2086
run "main module, -tags fault" . -- -tags fault $flags $pkgs
# shellcheck disable=SC2086
run "main module, -tags fault, -race" . -- -tags fault -race $flags $pkgs
if [ -n "$all" ]; then
	# shellcheck disable=SC2086
	run "e2e module, -tags fault, -race" e2e -- -tags fault -race $flags ./...
else
	summary="$summary
skipped	-	e2e module (a package pattern was given)"
fi
if [ "$(uname -s)" = Linux ]; then
	# shellcheck disable=SC2086
	run "Linux-only checks" . SHARDLITE_MEASURE=1 SHARDLITE_TEST_FS_PATH=/dev/shm SHARDLITE_TEST_FS_WANT=tmpfs -- -tags fault -v -run 'TestMeasurePerShardCost|TestRealFilesystem' $flags ./internal/node
else
	summary="$summary
skipped	-	Linux-only checks (not on Linux: run them in the container, see the README)"
fi

echo
echo "=== long suite"
printf '%s\n' "$summary" | sed 1d | awk -F'\t' '{printf "%-8s %-7s %s\n", $1, $2, $3}'
exit $status
