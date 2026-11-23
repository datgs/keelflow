# Execution Plan: Make `run_worker` accept a Kernel

Date: 2026-11-23

## Status

Completed 2026-11-23.

## Outcome

Allow applications to run the standalone worker entry point with an explicitly
selected Kernel adapter, while preserving the current Chancy behavior when no
kernel is supplied:

```python
run_worker(registry=registry)                    # existing Chancy default
run_worker(registry=registry, kernel=native)     # explicit adapter
```

This completes backend selection at the worker bootstrap boundary. It does not
make `worker.dispatch`, the engine storage layer, or workflow operations
portable beyond PostgreSQL.

## Context — verified

- `run_worker` currently creates `chancy.Chancy`, migrates it, declares a
  default `chancy.Queue`, creates/starts a worker, and awaits shutdown
  (`src/keelflow/engine/worker.py:336-383`).
- The `Kernel` protocol already provides `migrate`, `declare_queue`, and
  `create_worker` (`src/keelflow/engine/kernel.py:55-73`). All three current
  implementations satisfy the worker lifecycle contract: Chancy, native
  Postgres, and SQLite.
- Chancy startup uses a context-managed client and a Chancy-specific
  `queues: list[chancy.Queue]` argument. Native and SQLite kernels have
  implicit queues and their own worker options; they do not consume Chancy
  Queue objects.
- Existing example entry points call `run_worker` without `kernel` and pass a
  custom Chancy prefix. Their behavior and signatures must continue working.
- `event_bus` starts a separate PostgreSQL event dispatcher, so it remains
  independent of the selected kernel in this change.

## Scope

In scope:

- Add an optional keyword-only `kernel` parameter to `run_worker`.
- Preserve `kernel=None` as the current Chancy bootstrap, including prefix,
  Chancy queues, migration, default queue configuration, context-managed client
  lifetime, and worker shutdown behavior.
- For an explicit Kernel, configure dispatch with it, call its migration and
  worker lifecycle methods, and leave lifecycle ownership clear.
- Add focused tests for default Chancy startup and injected native/SQLite
  Kernel startup using fakes or temporary SQLite state where appropriate.
- Update the guide with the standalone invocation pattern and the limitation
  that the engine dispatcher is still Postgres-backed.

Out of scope:

- Changing `worker.dispatch`, `JobService`, the engine schema, or the storage
  dialect.
- Changing example workers or the Chancy default.
- A generic CLI backend selector, signals redesign, or worker configuration
  overhaul.

## Approach and contract

Recommended API: `run_worker(registry, dsn=None, *, kernel=None, queues=None,
prefix=..., event_bus=None)`.

- With `kernel is None`, run the existing Chancy-managed path unchanged.
- With an explicit `Kernel`, it is caller-created and caller-owned: do not
  close it. `run_worker` calls `configure(registry, dsn, kernel)`, awaits
  `kernel.migrate()`, starts `kernel.create_worker()` and awaits
  `wait_for_shutdown()`. Always stop the worker in `finally` if the lifecycle
  exits or the event dispatcher fails.
- `queues` remains a Chancy-only option in this bounded change. If supplied
  alongside a non-Chancy Kernel, raise a clear `ValueError` before starting;
  do not silently ignore Chancy Queue objects. Backend-neutral concurrency
  configuration is a separate design task.
- Resolve DSN from explicit `dsn`, then `KEELFLOW_DSN`; an explicit kernel
  without a DSN may use `kernel.dsn` for dispatch configuration. Preserve the
  current error when neither source provides a DSN. This avoids requiring
  callers to repeat the DSN already owned by a kernel.
- For explicit ChancyKernel, caller remains responsible for entering/exiting
  its Chancy client context. Native/SQLite adapters manage their own worker
  connections through `create_worker`.
- `event_bus`, when provided, continues to run alongside worker shutdown using
  the existing Postgres event dispatcher and resolved DSN.

## Tasks

### T1 — Implement optional Kernel bootstrap

- **Goal:** Make `run_worker` choose the legacy Chancy bootstrap or a supplied
  Kernel without changing existing callers.
- **Required outcomes:** Implement the API and lifecycle contract above; use
  `try/finally` to stop the started worker; preserve Chancy client context
  ownership and existing queue setup; reject Chancy-only `queues` when a
  non-Chancy kernel is passed; derive DSN from explicit/env/kernel sources as
  specified.
- **Constraints:** No changes to the engine dispatcher or database support
  claims. Keep the Chancy default path behavior-compatible.
- **Acceptance evidence:** Unit tests prove default Chancy client is entered,
  migrated, queue declared, worker started/stopped; explicit Kernel is
  migrated and its worker started/stopped without closing the caller-owned
  Kernel; invalid `queues` combination fails before starting.
- **Verification:** Focused `run_worker` tests, `uv run ruff check src tests`,
  `uv run mypy src/keelflow`.
- **Ownership:** `src/keelflow/engine/worker.py`, focused worker bootstrap tests.
- **Dependencies:** None.

### T2 — Document and verify backend invocation

- **Goal:** Make the new entry point usable without implying database-portable
  dispatch.
- **Required outcomes:** Document default and explicit-kernel examples, queue
  configuration limitation, resource ownership, and Postgres-only dispatcher
  dependency. Run default suite and kernel contract suite.
- **Constraints:** Do not change README backend positioning or declare full
  engine support on SQLite.
- **Acceptance evidence:** Guide accurately shows caller-owned adapter lifecycle
  and the Postgres limitation; all existing no-kernel example calls remain
  valid.
- **Verification:** `uv run pytest -q`,
  `uv run pytest tests/test_kernel_contract.py -q --kernel=all`,
  `uv run ruff check src examples tests`, `uv run mypy src/keelflow`,
  `git diff --check`.
- **Ownership:** `docs/guide.md`, focused tests, plan result.
- **Dependencies:** T1.

Dependency graph: T1 → T2.

## Risks and recovery

- **Resource ownership:** Closing a caller-owned kernel inside the helper can
  break application-managed lifecycles. The explicit-kernel path will only stop
  its worker; the caller owns kernel/client closure.
- **Queue API mismatch:** Chancy Queue objects cannot configure native/SQLite.
  Reject that combination instead of silently ignoring it.
- **Blocking event loop:** Existing `run_worker` uses `asyncio.run` and is a
  blocking entry point. This plan preserves that behavior; async embedding is
  out of scope.
- **Postgres coupling:** Native/SQLite worker bootstrap does not make
  `dispatch()` SQLite-capable; docs/tests must state this boundary.
- **Rollback:** `kernel=None` remains the unchanged default path; callers can
  omit the new argument to return to Chancy behavior.

## Progress

- [x] Inspected current `run_worker`, Kernel lifecycle protocol, and example
      call sites.
- [x] T1 — optional Kernel bootstrap implemented
      (`worker.py:336-408`; both paths stop the worker in `finally`).
- [x] T2 — documented in `docs/guide.md` §16; tests:
      `tests/test_run_worker_bootstrap.py` (3 fakes tests).

## Decisions

- 2026-11-23: Keep the Chancy default and caller-owned explicit Kernel
  lifecycle; preserve Chancy Queue configuration only on the default path.
- 2026-11-23: Do not promise non-Postgres engine dispatch from this bootstrap
  change.

## Validation

- Planning only; implementation not started.


## Result

`run_worker` now accepts `kernel=` (any Kernel port implementation)
while `kernel=None` keeps the exact Chancy default. Explicit kernels
are caller-owned — configured, migrated, started, stopped; never
closed. `queues=` with a non-Chancy kernel raises `ValueError`.
Evidence: 223 tests pass (+3 bootstrap fakes), contract suite
24 passed/3 skipped on all three backends, ruff+mypy clean.
