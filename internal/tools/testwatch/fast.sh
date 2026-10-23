#!/bin/sh
# The fast suite: every test in the main module on the in-memory blob store,
# under testwatch, with gotestsum's report. Details and the arguments are in
# the testwatch README.
set -eu
cd "$(dirname "$0")/../../.."
unset SHARDLITE_MEASURE SHARDLITE_MEASURE_MB SHARDLITE_TEST_S3_ENDPOINT
watch=$(mktemp); trap 'rm -f "$watch" "$watch.json" "$watch.rep"' EXIT
go build -o "$watch" ./internal/tools/testwatch
status=0
go tool -modfile=tools.mod gotestsum --jsonfile "$watch.json" -- -exec "$watch" -timeout 10m "$@" ./internal/... >"$watch.rep" 2>&1 || status=$?
go tool -modfile=tools.mod gotestsum tool slowest --num 20 --jsonfile "$watch.json" >>"$watch.rep" 2>&1 || true
cat "$watch.rep"
if [ -n "${TESTWATCH_OUT:-}" ]; then
	mkdir -p "$TESTWATCH_OUT"
	cp "$watch.json" "$TESTWATCH_OUT/out.json"
	cp "$watch.rep" "$TESTWATCH_OUT/report.txt"
fi
exit $status
