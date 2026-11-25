# Reliability guarantees

What keelflow guarantees, what it does not, and what the application
must own. Proofs referenced are executable tests — see
`tests/test_recovery.py`, `tests/test_e2e.py`, `tests/test_events.py`,
`tests/test_engine_hardening.py`, `tests/test_alembic_upgrade.py`.

## Guarantee matrix

| Guarantee | Scope | Proof / mechanism |
|---|---|---|
| **Atomic intent** — a job, event, or workflow step intent either commits with your data or does not exist | UoW participants on one Postgres transaction (`Session.begin()`) | `test_uow_contract.py` |
| **Exactly-once state change** — writes inside `ctx.uow` commit once and roll back on failure | The dispatch unit of work only | `test_e2e`, `test_recovery::test_db_disconnect_…` |
| **At-least-once execution** — a job may run more than once after crashes or kernel redelivery | All machine jobs | kernel recovery + `test_e2e` kill-resume |
| **At-least-once event delivery** — outbox events may redeliver to a subscriber | `job.enqueued` and domain outbox events | per-subscriber dedup (`test_events`) |
| **Idempotent enqueue** — same `(type, deduplication_key)` lands at most one live job | non-terminal jobs | partial unique index |
| **No lost intent** — enqueue inside a committed TX is durably claimable | transactional outbox model | `test_recovery` |
| **Cancel is cooperative** — `cancel()` terminalizes pending jobs and flags running ones; only `cooperative_cancel` types stop mid-run | job lifecycle | `test_engine_hardening` |

## What is NOT guaranteed

- **Progress is not proof of work.** Opt-in job progress is an application-reported snapshot visible independently of the handler transaction. A report is not proof that the corresponding business work committed. No-progress timeouts cancel the local attempt and use normal retries; they cannot cancel an external request already in flight. Configure automatic retry only with a stable provider idempotency key, a transactional outbox plus receiver deduplication, or an explicit reconciliation strategy. If an external outcome is ambiguous and cannot be deduplicated or reconciled, do not configure blind automatic retry; use alerting/manual reconciliation.
- **Stale dispatches cannot overwrite a newer claim.** Each claim mints a non-reusable `execution_token`; progress, finish, and retry-reschedule writes carrying an outdated token are rejected (`ExecutionLostError`) and the attempt's UoW rolls back. Fencing prevents stale local commits — it does not undo external side effects already issued.
- **Exactly-once external effects.** Email, payment calls, webhooks,
  and any write outside the UoW are **not rolled back** and may repeat
  on redelivery. Applications must use idempotency keys, dedup
  endpoints, or exactly-once APIs at the provider. The engine's
  idempotent patterns (natural keys, upserts, `ON CONFLICT`) are the
  model to copy — see `examples/` handlers.
- **Cross-database atomicity.** A UoW spans one backend transaction.
  Domain data in a different database commits independently.
- **Permission revocation on queued work.** Permissions are snapshotted
  into `jobs.context` at enqueue; a queued job keeps the permissions it
  was authorized with (`spec/06-context.md`). Design rule, not a bug —
  audit must match the authorizing context. Apps needing revocation
  must cancel and re-enqueue.
- **Tenant isolation.** `tenant_id` is routing/reporting context, not a
  security boundary — shared tables are visible to every query. Use
  separate deployments or databases where isolation is required.
- **`ConcurrencySpec.max_workers`** is declared but not enforced;
  registration rejects it until consistent enforcement exists.

## Recovery behavior

| Scenario | What happens | Operator action |
|---|---|---|
| Worker dies mid-handler | UoW rolls back; kernel lease expires; job redelivered | none (at-least-once) |
| Worker dies after commit | job is terminal; dispatch may see a stale claim and skip | none |
| DB disconnect mid-handler | TX aborts; job redelivered; idempotent write lands once | none |
| Scheduler restart | ticks replay; `cron:<type>:<slot>` unique keys dedup | none |
| Outbox dispatcher restart | events re-claimed; subscriber dedup skips delivered ones | none |
| Job exhausts attempts | `failed` + DLQ row | `dlq_replay` after fixing |
| `pg_terminate_backend` on worker conn | same as disconnect | none |

## Upgrade policy

- Schema changes ship as **Alembic revisions** (`alembic upgrade head`).
  `postgres/schema.py::apply_async` is for **fresh** databases only —
  `CREATE IF NOT EXISTS` is not a versioned upgrade and must not be
  reported as one.
- Upgrades run forward-only against a copied/restored database first;
  schema `downgrade` does not restore data.
- **Persisted keys are not API names.** A rename in code does not rewrite
  stored data — persisted payloads (`trigger_config`, `jobs.context`)
  need an explicit data migration or a reader fallback.
- **Live runs pin behavior.** An in-flight run resolves the workflow
  definition and job types registered under its recorded names —
  upgrade means deploying compatible registrations, not renaming types
  mid-flight.
- Backup/restore rehearsal: `CREATE DATABASE <snap> TEMPLATE <db>` (or
  `pg_dump`/restore) — proven in
  `tests/test_recovery.py::test_database_snapshot_restore_path`.

## Unsupported / experimental surface

- `NativeKernel` is experimental: no rate limits, queue pause/resume,
  worker tags, priority ordering, or plugin metrics
  (`engine/native_kernel.py` header).
- `SQLiteKernel` is local/proof-level: single-writer, no cross-process
  notify, no advisory locks — not a production durability target.
