# The linter for the code's closed sets. See docs/internals.md.
#
# The test suites are more plumbing than a recipe line wants, so their logic
# stays in scripts beside testwatch (internal/tools/testwatch/fast.sh and
# long.sh) and these targets are only the way to call them from anywhere, as
# CI does. The suites take their extra go test arguments in ARGS: one flag
# bare, more than one word quoted on the assignment alone, as in
# make test-long ARGS='-race ./internal/node'. Quoting the whole line instead
# defines a variable named "test-long ARGS".

.PHONY: lint test test-long

lint:
	go tool -modfile=tools.mod exhaustive -check switch,typeswitch,mapliteral ./...
	go tool -C e2e -modfile=../tools.mod exhaustive -check switch,typeswitch,mapliteral ./...

test:
	./internal/tools/testwatch/fast.sh $(ARGS)

test-long:
	./internal/tools/testwatch/long.sh $(ARGS)
