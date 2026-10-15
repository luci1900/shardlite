# testwatch

`testwatch` is a program that `go test -exec` runs each test binary through. It catches a stuck test instead of waiting for `-timeout`. It starts the binary in its own process group and samples its CPU time (with `ps -o cputime=`) every 3 seconds. If the CPU time hasn't moved for 30 seconds it sends `SIGQUIT`, so the Go runtime prints every goroutine's stack, waits 5 seconds, then kills the process group (which takes the `testapp` children with it). Output and exit status pass through. `TESTWATCH_IDLE` and `TESTWATCH_SAMPLE` change the limits. `-timeout` stays as a backstop.

A test that sits idle for 30 seconds is too slow for the fast suite anyway.

## The fast suite

Every test in the main module's `internal/...` on the in-memory blob store only (no S3 emulator, containers, `fault` build tag or `SHARDLITE_MEASURE`):

```
internal/tools/testwatch/fast.sh          # without -race
internal/tools/testwatch/fast.sh -race    # with -race
```

The script builds `testwatch`, runs `go test -json -exec <testwatch> -timeout 15m ./internal/...`, then prints the output of failed tests, a line per package and the twenty slowest tests (`testwatch report`). Extra arguments go to `go test`.

The goroutine dump of a binary that was stopped (by testwatch or `-timeout`) is printed under `--- STOPPED: <package>`, and the tests that never finished under `--- NOT FINISHED`. `go test -json` gives each line of output to the test that printed a line of its own last, which with parallel tests is rarely the stuck one, so the report takes every line of the package's output from the start of the dump on as the dump's.

## The long suite

What the fast suite leaves out, run against RustFS (an S3-compatible store, from `docker/compose.s3.yaml`) under the same watcher:

1. the main module with `-tags fault`, without `-race`
2. the main module with `-tags fault` and `-race`
3. the `e2e` module with `-tags fault` and `-race`
4. on Linux only: `TestMeasurePerShardCost` (descriptors and mappings per shard, from `/proc`) and `TestRealFilesystem` on `/dev/shm` (expecting `tmpfs`). The `/proc/self/io` check in `internal/capture` runs in the fast suite and in runs 1 and 2, and only checks on Linux.

The measurements that report numbers (`SHARDLITE_MEASURE` without `-run TestMeasurePerShardCost`, `SHARDLITE_MEASURE_MB`) stay manual.

```
docker compose -f docker/compose.s3.yaml up -d
internal/tools/testwatch/long.sh                   # all the runs
internal/tools/testwatch/long.sh -race             # -race is passed to every run
internal/tools/testwatch/long.sh ./internal/node   # a package pattern for runs 1, 2 and 4 (e2e is skipped)
docker compose -f docker/compose.s3.yaml down
```

`long.sh` checks that RustFS answers at `SHARDLITE_TEST_S3_ENDPOINT` (default `http://127.0.0.1:19000`) and, if not, says how to start it rather than starting containers itself. It sets the test environment, prints one report per run (failures, a line per package, the twenty slowest tests) and a last line per run with its time. On macOS it says the Linux-only checks were skipped.

To run the suite on Linux from a Mac, build the image from `docker/Dockerfile` and run `long.sh` in it with RustFS reachable from the container (for instance `--add-host=host.docker.internal:host-gateway -e SHARDLITE_TEST_S3_ENDPOINT=http://host.docker.internal:19000`, with the compose file's port published on the host). Mount a named volume for `/tmp` if the tests are to avoid the container's overlay filesystem.

Setting `TESTWATCH_OUT` to a directory makes `fast.sh` and `long.sh` copy each run's `go test -json` output and report there (CI uploads them when a job fails, and the stack dumps of stopped binaries are in them).

## In CI

`.github/workflows/test.yml` runs on Linux (`ubuntu-latest`). It runs only when started by hand (Actions, test, Run workflow). The `fast` job always runs: build (also with `CGO_ENABLED=0`), `go vet` (main module, `-tags fault` and `e2e`), `gofmt -l`, then `fast.sh` and `fast.sh -race`. The `long` job runs only if the `long` input is ticked: it starts RustFS from the compose file, waits until it answers and runs `long.sh`.

## By hand

For one package:

```
go build -o /tmp/testwatch ./internal/tools/testwatch
go test -exec /tmp/testwatch ./internal/node
go test -json ./internal/... | /tmp/testwatch report -n 20
```
