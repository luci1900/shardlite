# Agents

Shardlite serves SQLite databases as actors, with blob storage as the system of record. It's written in Go and uses Litestream for WAL capture.

## Read first

- [docs/overview.md](docs/overview.md) for the concepts and goals
- [docs/internals.md](docs/internals.md) for how the node works
- [docs/system-kinds.md](docs/system-kinds.md) for the node's own shards and cluster-wide jobs like `_dispatch`
- [docs/concurrency.md](docs/concurrency.md) for goroutine ownership and shutdown rules (read it before touching any goroutine)
- [docs/guide.md](docs/guide.md) for app-facing behaviour, headers, endpoints and settings
- [docs/roadmap.md](docs/roadmap.md) for stages

The docs are the source of truth for the design. If code and docs disagree, say so rather than quietly picking one. Update the docs in the same change when behaviour changes.

`plans/` holds the plan for the current stage (`stage-{major}.{minor}.md`). Plans for finished stages are deleted, so look at their commits for history. `specs/` holds Quint models.

## Workflow

- Never stage, commit, amend or push. Stop and wait for review after each part of a plan and at the end of each stage.
- Only spawn sub-agents when they'd run on a smaller model than yours, so they actually save tokens. Otherwise do the work yourself.
- Prefer removing dead, redundant and wrapper code over keeping it.

## Tests

- After a change, run the fast suite: `go test ./...` from the repo root (in-memory blob backend).
- Ask before running the slow suites: S3 (`SHARDLITE_TEST_S3_BUCKET` and `SHARDLITE_TEST_S3_ENDPOINT`), fault tests (`-tags fault`) and end-to-end tests (the separate module in `e2e/`).
- Tests wait on counters or events, never a fixed sleep followed by an assert.

## Code

- Use `errgroup` for fork-join goroutines and library concurrency over hand-rolled machinery.
- Name services by role, never with a `Service` or `Svc` suffix.
- Config booleans default to false, and false is the safe value.

## Writing

For docs and comments: plain language, no hard wraps, no Oxford comma, few em dashes or semicolons, parentheses for asides.
