# testwatch

`testwatch` runs each test binary for `go test -exec` and stops one that is stuck, with every goroutine's stack (how, and its settings, are in `main.go`). Every suite below runs under it except the spec checks, which wait on `quint` and get a deadline per command from `internal/test/quinttest` instead. While iterating on one package, `TESTWATCH_IDLE=10s make test ARGS=./internal/node` fails a stuck test in ten seconds.

The suites take extra `go test` arguments in `ARGS`, quoted on the assignment when there is more than one word: `make test ARGS='-race ./internal/node'`. Setting `TESTWATCH_OUT` to a directory makes them copy their `go test -json` output and reports there, which CI uploads on failure.

## The fast suite

`make test` (`fast.sh`) needs nothing: the main module on the in-memory store, then the migrate module and both SDKs, which run only when `ARGS` names no package.

## The long suite

`make test-long` (`long.sh`) needs RustFS, which it doesn't start:

```
docker compose -f docker/compose.s3.yaml up -d
make test-long
docker compose -f docker/compose.s3.yaml down
```

A package in `ARGS` limits the main module's runs and skips e2e. The Linux-only checks print nothing on macOS. To run them from a Mac, run `make test-long` in the image from `docker/Dockerfile` with RustFS reachable from the container (for instance `--add-host=host.docker.internal:host-gateway -e SHARDLITE_TEST_S3_ENDPOINT=http://host.docker.internal:19000`) and a named volume on `/tmp` to keep the tests off the overlay filesystem. The measurements that report numbers (`SHARDLITE_MEASURE`, `SHARDLITE_MEASURE_MB`) stay manual.

## The model suite

`make test-model` needs [Quint](https://quint-lang.org), at the version in `internal/test/quinttest/quint-version` (a missing or different one fails the suite and names the install command). It runs the capture tests tagged `model` (the committed traces are still what the model generates, and random traces replay against the code) and the spec checks (each model's in the test file of the same name in `specs/`). `ARGS` goes to the capture tests only.

- `SHARDLITE_SEED` repeats a run of random traces (the test logs the seed) and `SHARDLITE_TRACES` sets how many (100 by default). A failed trace is kept, and the test logs where.
- After changing `specs/capture.qnt`, regenerate the committed traces and review the diff: `go test -tags model ./internal/capture -run TestModelTracesCurrent -update`.

## Every suite

`make test-all` runs all three in turn, each even if one before it failed. It needs RustFS and Quint, and only flags make sense in its `ARGS`.
