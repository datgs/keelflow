# Hardening completion: retention sweep, DLQ resume, dedup, size cap, release

Date: 2026-11-20

## Status

Active — ready for implementation approval (except T7, which has its own gate).

## Outcome

Close the remaining known gaps from the hardening/retention work and prepare a
first public release:

1. documentation housekeeping (plan lifecycle, PII/secrets audit);
2. time-based retention sweep for engine tables;
3. DLQ replay that resumes the owning workflow run instead of re-running a
   step job standalone;
4. idempotent enqueue via `unique_key`;
5. size cap on `jobs.result` / `step_runs.outputs`;
6. validated release path (separate approval gate).

## Context and verified evidence

- Baseline is green: `uv run pytest -q` → 110 passed; ruff + mypy clean; CI run
  `44693417583` green on `main` (commit `96681eb`).
- Retention work already landed (`src/keelflow/engine/bulk.py` `_fan_in`,
  `keelflow_row_archive`, `row_disposition`) but **no time-based sweep exists**:
  `keelflow_jobs`, `keelflow_outbox`, and `keelflow_row_archive` grow without
  bound. `MonitoringService` (`src/keelflow/engine/monitoring.py:11`) has no
  delete/sweep API.
- `keelflow_dlq` (`src/keelflow/postgres/schema.sql:174-183`) stores only
  `id, type, payload, context, error, attempts, dead_at` — **no `run_id` /
  `step_run_id`** — so `dlq_replay`
  (`src/keelflow/engine/monitoring.py:152-167`) can only enqueue a standalone
  job. This is the documented limitation.
- `JobBuilder` (`src/keelflow/engine/enqueue.py:38-131`) has no `unique_key`;
  `enqueue` validates payload and `payload_max_bytes` but nothing dedups.
- Worker writes results via `_write_finish`/`_finish`
  (`src/keelflow/engine/worker.py:406-433`) with **no size cap** on `result`
  or step `outputs`.
- `pyproject.toml:3` is `version = "0.1.0a1"`; `release.yml` publishes on `v*`
  tags via PyPI trusted publishing; `docs/plans/active/demo-suite.md` tracks
  the remaining `tag 0.1.0a1 + PyPI release` item as its last unchecked step.
- FK behaviour that the sweep must respect: `keelflow_chunks.job_id` and
  `keelflow_row_status.job_id` are `REFERENCES keelflow_jobs(id) ON DELETE
  CASCADE`; `keelflow_row_archive` intentionally has **no FK**.

## Decisions (confirmed with user)

- **Dedup (item 5):** duplicate `unique_key` enqueue **returns the existing
  job id** (idempotent), it does not raise.
- **Size cap (item 6):** two-tier.
  - `result_max_bytes: int | None = None` — **hard cap, opt-in, default OFF**;
    when set, an oversized result fails loudly (same philosophy as
    `payload_max_bytes`).
  - `result_warning_bytes: int | None = 32768` — **soft threshold, default ON**;
    never fails a job, emits a warning and records the measured size so
    applications can find and fix their hot job types.
  - Applies to **both** `jobs.result` and `step_runs.outputs` (both feed the
    DAG interpolation scope and both bloat engine tables).
  - Warning is delivered via the per-job logger (`jlog`, which already carries
    job id / type / correlation) **and** persisted as a queryable size column.
- **Release (item 7):** kept as a **separate final task with its own approval
  gate**; it needs a PyPI pending-publisher registration that only the user
  can create.

## Proposed defaults (confirm at approval)

- `JobTypeSpec.result_max_bytes: int | None = None` (no hard cap by default —
  zero regression risk; opt-in per job type), alongside the existing
  `payload_max_bytes: int = 8192`.
- `JobTypeSpec.result_warning_bytes: int | None = 32768` (soft 32 KiB warning,
  on by default; harmless because it never fails a job).
- Sweep retention defaults: terminal jobs `30 days`; published outbox rows
  `7 days`; `keelflow_row_archive` rows `90 days`; failed `row_status` rows
  `30 days`. All as parameters with these defaults, no hardcoding.
- `unique_key` uniqueness scope: unique per `(type, unique_key)` among
  **non-terminal** jobs (`pending/claimed/running/blocked`); terminal jobs
  release the key.

## Scope

In scope: `MonitoringService` sweep API, `keelflow_dlq` schema + replay path,
`JobBuilder`/`JobTypeSpec` dedup and size cap, alembic migrations, spec files,
CHANGELOG, plan housekeeping, release workflow validation.

Out of scope: partitioning, per-job-type tables, ANN/vector work, demo UI
changes, changing existing retry/claim semantics, auto-running destructive
sweeps, and anything that changes committed history.

## Approach

### Phase A — Housekeeping (low risk)

**T1 — Plan lifecycle**
- Move `docs/plans/active/demo-readmes-and-rag-content.md` →
  `docs/plans/completed/` (its Result section is recorded and CI is green).
- Keep `demo-suite.md` active until T7 completes (its last item is the
  release); then move it too.
- Move `history/20261120-bulk-row-retention/` references intact; do not
  rewrite history content.
- Evidence: `ls docs/plans/active docs/plans/completed`; plan files still
  internally consistent after move.

**T2 — PII / secrets audit of pushed content**
- Audit the pushed tree (not just the current diff) for: credentials/keys,
  real personal data, real bank/tax identifiers, internal URLs.
- Verify `.env` and local artifacts are gitignored and untracked; check
  `git ls-files` for accidental secrets.
- Confirm demo fixture data is synthetic (names, tax ids, bank details).
- Evidence: explicit command output (`git ls-files`, targeted greps for
  key/token/password patterns), plus a written statement of what was and was
  not verified. This is an audit, not a guarantee.

### Phase B — Retention sweep (completes the approved retention plan)

**T3 — `sweep()` API + spec**
- Add a maintenance API (proposed: `MonitoringService.sweep(conn, *, jobs_days,
  outbox_days, archive_days, failed_rows_days, batch)` returning per-table
  counts) that deletes, oldest-first, in batches:
  - terminal `keelflow_jobs` older than `jobs_days` (cascades to `chunks`,
    `row_status` via FK);
  - `keelflow_outbox` rows `status='published'` older than `outbox_days`;
  - `keelflow_row_archive` rows older than `archive_days`;
  - leftover `keelflow_row_status` rows `status='failed'` older than
    `failed_rows_days` whose parent job is already gone.
- Must be **caller-invoked** (an ops job / cron handler), never implicit.
- Must be idempotent and bounded (`batch` limit per call) so it cannot hold a
  long transaction or delete unbounded rows in one statement.
- Update `spec/01-schema.md` (retention semantics) and `docs/guide.md`.
- Acceptance: sweep removes only rows older than the cutoff; in-flight jobs are
  never touched; a second sweep is a no-op; `row_archive` survives job deletion
  unless past `archive_days`.
- Risk: **high blast radius** (deletes data) — mitigated by caller-invoked-only,
  batched deletes, parameterised cutoffs, and tests that assert what is *not*
  deleted.

### Phase C — Engine gaps

**T4 — DLQ replay resumes the owning run** (needs migration `0005`)
- Add nullable `run_id` and `step_run_id` to `keelflow_dlq`; populate them in
  `_write_dlq` when the failed job is a workflow step.
- `dlq_replay`: if `step_run_id` is present, reopen the step and re-drive the
  run (same semantics as `resume_step`/`resume_run`) instead of enqueuing a
  standalone job; otherwise keep today's behaviour.
- Keep today's standalone replay for non-step jobs.
- Update `spec/02-job-lifecycle.md` + `spec/03-workflow.md`; remove the
  "known limitation" note once fixed.
- Acceptance: replaying a failed DAG-step DLQ entry advances the original run
  to completion; replaying a standalone job still enqueues a fresh job; DLQ
  entries created before the migration still replay standalone.
- Depends on: nothing, but shares files with T6 — sequence them.

**T5 — `unique_key` idempotent enqueue** (needs migration `0006`)
- `JobBuilder.unique_key(key)` + `keelflow_jobs.unique_key` column + partial
  unique index on `(type, unique_key)` where status is non-terminal.
- On conflict: return the existing job's `JobRef` (no second kernel push, no
  second outbox row).
- Must run inside the caller's TX; conflict resolution must not deadlock under
  concurrent enqueue of the same key.
- Update `spec/02-job-lifecycle.md` and `docs/guide.md`.
- Acceptance: two concurrent enqueues with the same key yield one job and two
  identical `JobRef`s; a different key enqueues normally; once the first job is
  terminal a new enqueue with the same key creates a new job.
- Risk: **medium** — a unique index on a large table needs care; confirm index
  creation strategy on the demo DB before finalising the migration.

**T6 — Result/outputs size visibility (two-tier)** (needs migration)
- Add to `JobTypeSpec`:
  - `result_max_bytes: int | None = None` — hard cap, opt-in, default off;
  - `result_warning_bytes: int | None = 32768` — soft warning, default on.
- Add nullable size columns so the signal is queryable:
  `keelflow_jobs.result_bytes` and `keelflow_step_runs.outputs_bytes`,
  written in the same statement as the value itself (`_write_finish`, the
  step-`outputs` write at `workflow.py:393`) — no extra round trip.
- Enforce in the worker:
  - measure the serialized size once;
  - `result_warning_bytes` exceeded → `jlog.warning(...)` with job id, type,
    measured bytes and threshold (never fails the job);
  - `result_max_bytes` set and exceeded → fail the job/step with a diagnostic
    naming the cap and the job type, mirroring `payload_max_bytes` wording.
- Extend `MonitoringService` so apps can act on the signal — expose
  `result_bytes`/`outputs_bytes` in `jobs_list` and add a focused
  `results_oversized(conn, *, threshold, limit)` returning the worst offenders
  by type. Target query:

  ```sql
  SELECT type,
         count(*) FILTER (WHERE result_bytes > %s) AS oversized,
         max(result_bytes) AS worst
    FROM keelflow_jobs
   GROUP BY type
   ORDER BY worst DESC;
  ```

- Document in `docs/guide.md` + `spec/02-job-lifecycle.md`: handlers should
  return references, not blobs; how to tune both thresholds; how to find hot
  job types.
- Acceptance: a handler over the warning threshold logs a warning and stores
  the size but still succeeds; a handler over an explicit `result_max_bytes`
  fails with a diagnostic naming the cap; normal results unaffected; the
  oversized query reports the correct type/count/max; full suite green;
  every existing demo still passes with defaults (no hard cap).
- Risk: **low–medium** — the default is non-breaking (`None` hard cap), so the
  main risk is the extra column write and a noisy warning threshold; validate
  the 32 KiB default against actual demo output sizes.

### Phase D — Release (separate gate)

**T7 — Validate release path, then tag (separate approval)**
- Preconditions: all Phase A–C tasks complete and the full suite + CI green.
- Confirm `release.yml` builds the correct distribution (root `keelflow`
  package only — verify `uv build` contents exclude demo packages) and that
  the PyPI pending publisher exists.
- Update `CHANGELOG.md` (`[Unreleased]` → `0.1.0a1`) and the acceptance-evidence
  / demo-suite items.
- **Requires explicit user approval** to create the tag and publish; this is a
  public, hard-to-reverse action. Creating the PyPI pending publisher is a
  user-only step and must be done before the first publish.
- If the user has not registered the publisher, T7 stops at "release path
  validated locally" and reports that.

## Task graph

```text
Phase A:  T1 ── T2            (independent, docs/audit only)
Phase B:  T3                  (independent; high blast radius)
Phase C:  T4 ── T6 ── T5      (share worker/specs; migrations sequential)
Phase D:  ────────────────►  T7  (after A–C green, own approval)
```

Notes on ordering:
- T4 and T5 each add a migration — implement **sequentially** (single alembic
  head); T5's index strategy must be validated on a populated table first.
- T4 and T6 both touch `worker.py`; sequence them to avoid conflicting edits.
- T3 is independent but must not run in a session where other tasks are
  mutating demo data.

## Risks and recovery

- **Data loss (T3, high):** sweep deletes rows. Mitigated by caller-invoked-only
  API, parameterised cutoffs, batched deletes, and negative tests. Recovery:
  stop invoking the sweep; the API is additive and can be removed without
  migration.
- **Migration on populated table (T5, medium):** unique-index build may lock or
  fail on duplicates. Mitigated by validating the index strategy on the demo DB
  and using `CONCURRENTLY` where appropriate.
- **Behavioural breakage (T6, medium):** existing handlers with large outputs
  start failing. Mitigated by choosing the default from measured demo output
  sizes and by running the full suite.
- **Release irreversibility (T7, high):** a tag/PyPI publish is public. Gate on
  explicit approval and validated publish content.
- **Recovery general:** every task is additive (new column, new API, new spec
  paragraph). No task rewrites existing engine semantics; each can be reverted
  independently.

## Acceptance evidence

- T1: plans moved; `docs/plans/active` contains only genuinely active work.
- T2: written audit record with the exact commands run and their output, plus an
  explicit statement of what the audit cannot prove.
- T3: sweep tests prove old terminal rows are removed, recent/in-flight rows are
  not, second call is a no-op, and archived rows survive job deletion.
- T4: a DAG-step DLQ entry replays into the owning run and completes it; a
  standalone DLQ entry still replays standalone.
- T5: concurrent same-key enqueue yields exactly one job; key reuse after
  terminal state is allowed.
- T6: over-warning-threshold result logs a warning and records the size but
  still succeeds; over-`result_max_bytes` result fails with a diagnostic naming
  the cap; `results_oversized` reports the correct type/count/max; all demos
  still pass under the default (no hard cap).
- T7: release path validated (`uv build` contents inspected); publish only with
  explicit approval.

## Validation

Focused (per task, as implemented):

```bash
uv run pytest tests/test_workflow.py tests/test_engine_hardening.py -q   # T4
uv run pytest tests/test_enqueue.py -q                                  # T5
uv run pytest tests/test_worker.py tests/test_monitoring_admin.py -q    # T3, T6
```

Repository-required:

```bash
uv run pytest -q
uv run ruff check src examples tests
uv run mypy src/keelflow
git diff --check
uv run alembic upgrade head && uv run alembic downgrade -1   # T4/T5 migrations
```

CI must be green on `main` after each pushed batch.

## Progress

- [x] Baseline confirmed green (110 tests, CI `44693417583`).
- [x] Evidence gathered for items 1–7; three policy decisions confirmed.
- [x] Phase A — T1 plan housekeeping.
- [x] Phase A — T2 PII/secrets audit.
- [ ] Phase B — T3 retention sweep.
- [ ] Phase C — T4 DLQ replay resumes owning run.
- [ ] Phase C — T6 result/outputs size visibility (hard cap opt-in + soft warning).
- [ ] Phase C — T5 `unique_key` idempotent enqueue.
- [ ] Phase D — T7 release validation (own approval gate).

## Phase A audit record (2026-11-20)

Commands run: `git ls-files` (env/secret file names, `.pyc`/
`__pycache__`), targeted greps for `sk-*`/AWS `AKIA*`/private-key PEM/
`password=`, real-domain emails, bank/tax/PII tokens, non-localhost
URLs; `git check-ignore .env.local`; `strings` on `scripts/bin/harness`.

Findings:

- **No credentials, keys, tokens, or tracked `.env*` files** — `.env`,
  `.env.*` are gitignored (`.env.example` allowed but absent).
- **All emails are `*.example` fixtures** and all tax IDs / bank refs are
  synthetic strings in `supplier_onboarding/seed.py` (e.g. `XX0000`).
- **No `__pycache__`/`.pyc` tracked.** Only external URLs are
  docs/badge links (shields.io) — benign.
- **Observation (not a secret):** `scripts/bin/harness` is a tracked 1.6 MB
  macOS arm64 binary committed in the initial scaffold — agent tooling,
  not user data. Candidate for `.gitignore`/removal in a separate
  cleanup; left untouched because deletion is destructive.
- **Cannot prove:** contents of live developer machines, untracked files,
  or historical commits outside `main`.

## Approval gate

No implementation starts until the user approves this plan. T7 additionally
requires its own explicit approval at the time of tagging/publishing, and it
depends on a PyPI pending-publisher registration that only the user can create.
