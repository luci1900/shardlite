# The linter for the code's closed sets. See docs/internals.md.
#
# The test suites are more plumbing than a recipe line wants, so their logic
# stays in scripts beside testwatch (internal/test/testwatch/fast.sh and
# long.sh) and these targets are only the way to call them from anywhere, as
# CI does. The suites take their extra go test arguments in ARGS: one flag
# bare, more than one word quoted on the assignment alone, as in
# make test-long ARGS='-race ./internal/node'. Quoting the whole line instead
# defines a variable named "test-long ARGS".

.PHONY: lint test test-long test-model test-all

lint:
	go tool -modfile=tools.mod exhaustive -check switch,typeswitch,mapliteral ./...
	go tool -C e2e -modfile=../tools.mod exhaustive -check switch,typeswitch,mapliteral ./...
	go tool -C migrate -modfile=../tools.mod exhaustive -check switch,typeswitch,mapliteral ./...
	go tool -C sdk/go -modfile=../../tools.mod exhaustive -check switch,typeswitch,mapliteral ./...
	# The TypeScript SDK (see its engines field and .npmrc for what npm runs on). Each line below runs in its own shell, so the cd does not reach make or the next line.
	if [ ! -d sdk/ts/node_modules/typescript ]; then (cd sdk/ts && npm ci --no-fund --no-audit); fi
	(cd sdk/ts && npm run lint --silent)

test:
	./internal/test/testwatch/fast.sh $(ARGS)

test-long:
	./internal/test/testwatch/long.sh $(ARGS)

# The spec checks wait on quint, which testwatch can't see. ARGS goes to the capture tests only.
test-model:
	status=0; \
	./internal/test/testwatch/fast.sh -tags=model ./internal/capture $(ARGS) || status=1; \
	go tool -modfile=tools.mod gotestsum -- -tags=model -timeout 15m ./specs || status=1; \
	exit $$status

# Every suite in turn, each run even if one before it failed.
test-all:
	status=0; \
	$(MAKE) test || status=1; \
	$(MAKE) test-long || status=1; \
	$(MAKE) test-model || status=1; \
	exit $$status
