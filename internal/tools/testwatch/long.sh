#!/bin/sh
# The long suite: what the fast suite leaves out, under the same watcher and
# against RustFS, with gotestsum's report: one run per call of run below. How
# to call it is in the testwatch README.
set -eu
cd "$(dirname "$0")/../../.."
root=$(pwd)
unset SHARDLITE_MEASURE SHARDLITE_MEASURE_MB SHARDLITE_TEST_FS_PATH SHARDLITE_TEST_FS_WANT

endpoint=${SHARDLITE_TEST_S3_ENDPOINT:-http://127.0.0.1:19000}
if ! curl -s -o /dev/null --max-time 5 "$endpoint"; then
	echo "RustFS does not answer at $endpoint. Start it with" >&2
	echo "	docker compose -f docker/compose.s3.yaml up -d" >&2
	echo "(or point SHARDLITE_TEST_S3_ENDPOINT at another live S3 endpoint), and run this again." >&2
	echo "Stop it afterwards with: docker compose -f docker/compose.s3.yaml down" >&2
	exit 2
fi
export SHARDLITE_TEST_S3_ENDPOINT="$endpoint" SHARDLITE_TEST_S3_BUCKET="${SHARDLITE_TEST_S3_BUCKET:-shardlite-test}" AWS_ACCESS_KEY_ID=shardlite AWS_SECRET_ACCESS_KEY=shardlite-secret AWS_REGION=us-east-1

watch=$(mktemp); trap 'rm -f "$watch" "$watch.json" "$watch.rep"' EXIT
go build -o "$watch" ./internal/tools/testwatch
status=0

# run TAG NAME DIR ENVS GO TEST ARGUMENTS runs the arguments through the
# watcher in that directory, prints gotestsum's report and copies the run's
# output and report under TAG into TESTWATCH_OUT when that names a directory.
run() {
	tag=$1 name=$2 dir=$3 envs=$4
	shift 4
	echo "=== $name"
	rc=0
	(cd "$dir" && env $envs go tool -modfile="$root/tools.mod" gotestsum --jsonfile "$watch.json" -- -exec "$watch" -timeout 30m "$@") >"$watch.rep" 2>&1 || rc=$?
	go tool -modfile="$root/tools.mod" gotestsum tool slowest --num 20 --jsonfile "$watch.json" >>"$watch.rep" 2>&1 || true
	cat "$watch.rep"
	[ $rc -eq 0 ] || status=1
	if [ -n "${TESTWATCH_OUT:-}" ]; then
		mkdir -p "$TESTWATCH_OUT"
		cp "$watch.json" "$TESTWATCH_OUT/$tag.json"
		cp "$watch.rep" "$TESTWATCH_OUT/$tag.report.txt"
	fi
}

flags=
pkgs=
for a in "$@"; do
	case $a in -*) flags="$flags $a" ;; *) pkgs="$pkgs $a" ;; esac
done
[ -n "$pkgs" ] || pkgs=./internal/...

run main "main module, -tags fault" . "" -tags fault $flags $pkgs
run main-race "main module, -tags fault, -race" . "" -tags fault -race $flags $pkgs
if [ "$pkgs" = ./internal/... ]; then
	run e2e "e2e module, -tags fault, -race" e2e "" -tags fault -race $flags ./...
fi
if [ "$(uname -s)" = Linux ]; then
	run linux "Linux-only checks" . "SHARDLITE_MEASURE=1 SHARDLITE_TEST_FS_PATH=/dev/shm SHARDLITE_TEST_FS_WANT=tmpfs" -tags fault -v -run 'TestMeasurePerShardCost|TestRealFilesystem' $flags ./internal/node
fi
exit $status
