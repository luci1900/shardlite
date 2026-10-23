# Agents

Shardlite serves SQLite databases as actors, with blob storage as the system of record. It's written in Go and reads SQLite's WAL itself, storing LTX files.

## Read first

- [docs/overview.md](docs/overview.md) for the concepts and goals
- [docs/internals.md](docs/internals.md) for how the node works, including its goroutine ownership and shutdown rules (read the Concurrency section before touching any goroutine)
- [docs/guide.md](docs/guide.md) for app-facing behaviour, headers, endpoints and settings
- [docs/roadmap.md](docs/roadmap.md) for stages

The docs are the source of truth for the design. If code and docs disagree, say so rather than quietly picking one. Update the docs in the same change when behaviour changes.

## Plans

`plans/` holds the plan for the current stage (`stage-{major}.{minor}.md`). A plan is a local working file, not in git: `plans/` is ignored, and a plan and its report are deleted once the stage is done. So nothing outside `plans/` refers to a plan or a report, and what a plan decides has to land in the docs and the code, which are the source of truth. A code comment that needs a measurement or a decision points at the code that carries it.

A stage plan opens by saying what changes, what it builds on and why it sits where it does, then has:

- **The problem**: what is wrong today, with file references.
- **The design**: the shape of the change in prose, then **Decisions** as bullets, each with its rationale and the alternative it was chosen over.
- **Docs**: which docs change and how.
- **Open questions**: what is unresolved, when there is anything.
- **Testing**: the tests to write and where each lives.
- **Parts**: ordered parts, each one diff to read, with "Stop after each part" and a section per part in `stage-{major}.{minor}-report.md`.

Always use simulation for non-trivial Quint models (`quint run`): verification (`quint verify`) often doesn't terminate.

## Workflow

- Never stage, commit, amend or push. Stop and wait for review after each part of a plan and at the end of each stage.
- Only spawn sub-agents when they'd run on a smaller model than yours, so they actually save tokens. Otherwise do the work yourself.
- Prefer removing dead, redundant and wrapper code over keeping it.

## Tests

- After a change, run the fast suite: `make test` from the repo root (in-memory blob backend, under testwatch).
- Ask before running the long suite: `make test-long` (RustFS, the fault tests and the e2e module). It starts nothing itself, so bring RustFS up with `docker compose -f docker/compose.s3.yaml up -d` first.
- Tests wait on counters or events, never a fixed sleep followed by an assert.

## Code

- Every mechanism must be able to report its own failure, and every mechanism we write ourselves must be one the fault rig can make fail (docs/internals.md, Failing loudly).
- Positions are named types (`pos.Position`, `pos.Epoch`, `pos.Seg` in `internal/pos`), so a shard's position, a lease's epoch and a journal's segment number can't be swapped.
- A closed set of states is a named type with a constant per state, or a variant per state where the states carry different data, the interface of a variant set sealed with an unexported method so only its own package adds a variant. A switch over one covers every case, which the `exhaustive` linter checks (`make lint`).
- Use `errgroup` for fork-join goroutines and library concurrency over hand-rolled machinery.
- Name services by role, never with a `Service` or `Svc` suffix.
- Config booleans default to false, and false is the safe value.

## Writing

For docs and comments: plain language, no hard wraps, no Oxford comma, few em dashes or semicolons, parentheses for asides.

Never name a stage in docs or comments. Stages live in `plans/`, which is deleted once the stage is done, so a reference to one dangles. Name the thing itself (the mirrors, the local tier, the lease scan) or point at the code or doc that carries the fact.
