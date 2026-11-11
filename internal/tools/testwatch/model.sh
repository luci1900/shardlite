#!/bin/sh
# The model suite: the capture tests that need Quint (-tags model) under the
# same watcher, with gotestsum's report, then the models' own checks
# (specs/check.sh). The runs, the reports and the arguments are in the
# testwatch README.
set -eu
cd "$(dirname "$0")/../../.."
root=$(pwd)
unset SHARDLITE_MEASURE SHARDLITE_MEASURE_MB SHARDLITE_TEST_S3_ENDPOINT

# The Quint the committed traces were made with. Another can make other traces
# from the same seed, which the check of the committed traces would report as a
# change to the model. generate.sh and CI read the version from this line.
quint_version=0.33.0

if ! command -v quint >/dev/null 2>&1; then
	echo "quint is not on the path. Install it with" >&2
	echo "	npm install -g @informalsystems/quint@$quint_version" >&2
	exit 2
fi
if [ "$(quint --version)" != "$quint_version" ]; then
	echo "quint is $(quint --version), the suite needs $quint_version. Install it with" >&2
	echo "	npm install -g @informalsystems/quint@$quint_version" >&2
	exit 2
fi

watch=$(mktemp); trap 'rm -f "$watch" "$watch.json" "$watch.rep" "$watch.specs.rep"' EXIT
go build -o "$watch" ./internal/tools/testwatch
status=0

# The arguments are go test flags for the capture tests.
echo "=== capture, -tags model"
go tool -modfile=tools.mod gotestsum --jsonfile "$watch.json" -- -exec "$watch" -timeout 30m -tags model "$@" ./internal/capture >"$watch.rep" 2>&1 || status=1
go tool -modfile=tools.mod gotestsum tool slowest --num 20 --jsonfile "$watch.json" >>"$watch.rep" 2>&1 || true
cat "$watch.rep"

echo "=== specs/check.sh"
sh specs/check.sh >"$watch.specs.rep" 2>&1 || status=1
cat "$watch.specs.rep"

if [ -n "${TESTWATCH_OUT:-}" ]; then
	mkdir -p "$TESTWATCH_OUT"
	cp "$watch.json" "$TESTWATCH_OUT/model.json"
	cp "$watch.rep" "$TESTWATCH_OUT/model.report.txt"
	cp "$watch.specs.rep" "$TESTWATCH_OUT/specs.report.txt"
fi
exit $status
