#!/bin/sh
# The fast suite: every test in the main module, in the Go SDK's module
# (sdk/go) and in the TypeScript SDK (sdk/ts) on the in-memory blob store,
# under testwatch, with gotestsum's report. The SDKs' tests start a real node,
# so the suite builds the binary once and gives them its path in SHARDLITE_BIN.
# Details and the arguments are in the testwatch README.
set -eu
cd "$(dirname "$0")/../../.."
root=$(pwd)
unset SHARDLITE_MEASURE SHARDLITE_MEASURE_MB SHARDLITE_TEST_S3_ENDPOINT
watch=$(mktemp); bindir=$(mktemp -d)
trap 'rm -f "$watch" "$watch.json" "$watch.rep" "$watch.migrate.json" "$watch.migrate.rep" "$watch.sdk.json" "$watch.sdk.rep" "$watch.ts.rep"; rm -rf "$bindir"' EXIT
go build -o "$watch" ./internal/test/testwatch
status=0

# The arguments are go test flags and package patterns for the main module. The
# SDK's module runs with the flags, and only when no package was named.
flags=
pkgs=
for a in "$@"; do
	case $a in -*) flags="$flags $a" ;; *) pkgs="$pkgs $a" ;; esac
done
[ -n "$pkgs" ] || pkgs=./internal/...

go tool -modfile=tools.mod gotestsum --jsonfile "$watch.json" -- -exec "$watch" -timeout 10m $flags $pkgs >"$watch.rep" 2>&1 || status=$?
go tool -modfile=tools.mod gotestsum tool slowest --num 20 --jsonfile "$watch.json" >>"$watch.rep" 2>&1 || true
cat "$watch.rep"

sdk=false
if [ "$pkgs" = ./internal/... ]; then
	sdk=true
	go build -o "$bindir/shardlite" ./cmd/shardlite
	export SHARDLITE_BIN="$bindir/shardlite"
	echo "=== migrate module"
	(cd migrate && go tool -modfile="$root/tools.mod" gotestsum --jsonfile "$watch.migrate.json" -- -exec "$watch" -timeout 10m $flags ./...) >"$watch.migrate.rep" 2>&1 || status=$?
	go tool -modfile=tools.mod gotestsum tool slowest --num 20 --jsonfile "$watch.migrate.json" >>"$watch.migrate.rep" 2>&1 || true
	cat "$watch.migrate.rep"
	echo "=== sdk/go module"
	(cd sdk/go && go tool -modfile="$root/tools.mod" gotestsum --jsonfile "$watch.sdk.json" -- -exec "$watch" -timeout 10m $flags ./...) >"$watch.sdk.rep" 2>&1 || status=$?
	go tool -modfile=tools.mod gotestsum tool slowest --num 20 --jsonfile "$watch.sdk.json" >>"$watch.sdk.rep" 2>&1 || true
	cat "$watch.sdk.rep"
	echo "=== sdk/ts package"
	(cd sdk/ts && npm test --silent) >"$watch.ts.rep" 2>&1 || status=$?
	cat "$watch.ts.rep"
fi

if [ -n "${TESTWATCH_OUT:-}" ]; then
	mkdir -p "$TESTWATCH_OUT"
	cp "$watch.json" "$TESTWATCH_OUT/out.json"
	cp "$watch.rep" "$TESTWATCH_OUT/report.txt"
	if $sdk; then
		cp "$watch.migrate.json" "$TESTWATCH_OUT/migrate.json"
		cp "$watch.migrate.rep" "$TESTWATCH_OUT/migrate.report.txt"
		cp "$watch.sdk.json" "$TESTWATCH_OUT/sdk.json"
		cp "$watch.sdk.rep" "$TESTWATCH_OUT/sdk.report.txt"
		cp "$watch.ts.rep" "$TESTWATCH_OUT/ts.report.txt"
	fi
fi
exit $status
