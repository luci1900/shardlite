#!/bin/sh
# The fast suite: every package's tests in the main module on the in-memory
# blob store only (no S3 emulator, containers, fault tag or measurements), run
# through testwatch so a stuck test binary is dumped and killed after 30 seconds
# without CPU. The failures, a line per package and the twenty slowest tests are printed at the end.
#
#	internal/tools/testwatch/fast.sh          # without -race
#	internal/tools/testwatch/fast.sh -race    # with -race
#
# Extra arguments go to go test. SHARDLITE_MEASURE must be unset. If TESTWATCH_OUT
# names a directory, the go test -json output and the report are copied there
# (CI uploads them).
set -e
cd "$(dirname "$0")/../../.."
unset SHARDLITE_MEASURE SHARDLITE_MEASURE_MB SHARDLITE_TEST_S3_ENDPOINT
bin=$(mktemp -d)
trap 'rm -rf "$bin"' EXIT
go build -o "$bin/testwatch" ./internal/tools/testwatch
status=0
go test -json -exec "$bin/testwatch" -timeout 15m "$@" ./internal/... >"$bin/out.json" || status=$?
"$bin/testwatch" report -n 20 <"$bin/out.json" >"$bin/report.txt" 2>&1 || status=$?
cat "$bin/report.txt"
if [ -n "${TESTWATCH_OUT:-}" ]; then
	mkdir -p "$TESTWATCH_OUT"
	tag=fast$(printf '%s' "$*" | tr -c 'A-Za-z0-9' _)
	cp "$bin/out.json" "$TESTWATCH_OUT/$tag.json"
	cp "$bin/report.txt" "$TESTWATCH_OUT/$tag.report.txt"
fi
exit $status
