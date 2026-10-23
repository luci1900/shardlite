# testwatch

`testwatch` is a program that `go test -exec` runs each test binary through. It catches a stuck test instead of waiting for `-timeout`. It starts the binary in its own process group and samples its CPU time (with `ps -o cputime=`) every 3 seconds. If the CPU time hasn't moved for 30 seconds it sends `SIGQUIT`, so the Go runtime prints every goroutine's stack, waits 5 seconds, then kills the process group (which takes the `testapp` children with it). Output and exit status pass through. `TESTWATCH_IDLE` and `TESTWATCH_SAMPLE` change the limits. `-timeout` stays as a backstop.

A test that sits idle for 30 seconds is too slow for the fast suite anyway. An agent iterating on one package can shorten that: `TESTWATCH_IDLE=10s make test ARGS=./internal/node` fails a stuck test in ten seconds.

## The fast suite

Every test in the main module's `internal/...` on the in-memory blob store only (no S3 emulator, containers, `fault` build tag or `SHARDLITE_MEASURE`):

```
make test                              # without -race
make test ARGS=-race                   # with -race
make test ARGS='-race ./internal/node' # with -race, one package
```

The suite (`internal/tools/testwatch/fast.sh`) builds `testwatch` and runs [gotestsum](https://github.com/gotestyourself/gotestsum) over `go test -exec <testwatch> -timeout 15m ./internal/...`, which prints the output of failed tests, a line per package and the twenty slowest tests (`gotestsum tool slowest`). gotestsum is pinned in `tools.mod`, beside the linter, and runs with `go tool -modfile=tools.mod gotestsum`. The extra `go test` arguments go in `ARGS`, with quotes on the assignment itself when there is more than one word.

The goroutine dump of a binary that was stopped (by testwatch or `-timeout`) comes out with its package's failure, the way `go test` reports it.

## The long suite

What the fast suite leaves out, run against RustFS (an S3-compatible store, from `docker/compose.s3.yaml`) under the same watcher:

1. the main module with `-tags fault`, without `-race`
2. the main module with `-tags fault` and `-race`
3. the `e2e` module with `-tags fault` and `-race`
4. on Linux only: `TestMeasurePerShardCost` (descriptors and mappings per shard, from `/proc`) and `TestRealFilesystem` on `/dev/shm` (expecting `tmpfs`), which live in `./internal/node`. The `/proc/self/io` check in `internal/capture` runs in the fast suite and in runs 1 and 2, and only checks on Linux.

The measurements that report numbers (`SHARDLITE_MEASURE` without `-run TestMeasurePerShardCost`, `SHARDLITE_MEASURE_MB`) stay manual.

```
docker compose -f docker/compose.s3.yaml up -d
make test-long                         # all the runs
make test-long ARGS=-race              # -race is passed to every run
make test-long ARGS=./internal/node    # a package pattern for runs 1 and 2 (e2e is skipped)
docker compose -f docker/compose.s3.yaml down
```

`make test-long` calls `internal/tools/testwatch/long.sh`, which checks that RustFS answers at `SHARDLITE_TEST_S3_ENDPOINT` (default `http://127.0.0.1:19000`) and, if not, says how to start it rather than starting containers itself. It sets the test environment and prints one report per run under a banner naming the run (failures, a line per package, the twenty slowest tests). The suite fails when any run does, so the exit code and the reports are the summary. Run 4 runs on Linux only, so on macOS nothing prints for it.

To run the suite on Linux from a Mac, build the image from `docker/Dockerfile` and run `make test-long` in it with RustFS reachable from the container (for instance `--add-host=host.docker.internal:host-gateway -e SHARDLITE_TEST_S3_ENDPOINT=http://host.docker.internal:19000`, with the compose file's port published on the host). Mount a named volume for `/tmp` if the tests are to avoid the container's overlay filesystem.

Setting `TESTWATCH_OUT` to a directory makes the suites copy their `go test -json` output and report there: `out.json` and `report.txt` (fast suite), and one pair per long-suite run named `main`, `main-race`, `e2e` and `linux`. CI points each job at a directory and uploads it on failure, with the stack dumps of stopped binaries inside the outputs.

## In CI

`.github/workflows/test.yml` runs on Linux (`ubuntu-latest`). It runs only when started by hand (Actions, test, Run workflow). The `fast` job always runs: build (also with `CGO_ENABLED=0`), `go vet` (main module, `-tags fault` and `e2e`), `gofmt -l`, then `make test` and `make test ARGS=-race`. The `long` job runs only if the `long` input is ticked: it starts RustFS from the compose file, waits until it answers and runs `make test-long`.

## By hand

For one package:

```
go build -o /tmp/testwatch ./internal/tools/testwatch
go test -exec /tmp/testwatch ./internal/node
go tool -modfile=tools.mod gotestsum -- ./internal/...
```
