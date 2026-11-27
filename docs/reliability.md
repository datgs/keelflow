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
| **Bounded crash recovery** — every Native claim consumes one `attempts` slot, including claims abandoned before handler start; claims beyond `max_attempts` settle `failed` + DLQ | Native only; equivalent Chancy behavior is required before parity gate passes | `test_reliability::test_repeated_crash_abandoned_claims_exhaust_to_dlq` |
| **Lease-safe drain** — `stop(timeout)` keeps renewing in-flight leases through the drain window; a sibling worker cannot steal a draining job | native kernel | `test_reliability::test_graceful_stop_renews_lease_through_drain` |
| **Worker survives DB loss** — Native claim/maintenance loops reconnect; `run_worker` supervises backend workers | tested paths only; not proof of claim/attempt parity | `test_reliability::test_native_worker_recovers_after_connection_drop`, `test_chancy_pipeline_recovers_after_connection_drop` |

## What is NOT guaranteed

- **Progress is not proof of work.** Opt-in job progress is an application-reported snapshot visible independently of the handler transaction. A report is not proof that the corresponding business work committed. No-progress timeouts cancel the local attempt and use normal retries; they cannot cancel an external request already in flight. Configure automatic retry only with a stable provider idempotency key, a transactional outbox plus receiver deduplication, or an explicit reconciliation strategy. If an external outcome is ambiguous and cannot be deduplicated or reconciled, do not configure blind automatic retry; use alerting/manual reconciliation.
- **Native claim fencing.** Native claims mint a non-reusable `execution_token`; stale dispatch status writes carrying an outdated token are rejected. This does not undo external side effects. Chancy does not currently expose a proven supported claim-to-dispatch fencing seam, so equivalent fencing is not guaranteed there.
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

## Backend operation

NativeKernel is the current default and has tested pre-dispatch claim-token
fencing and attempt accounting. Chancy remains an explicitly usable adapter,
but equivalent pre-dispatch fencing and bounded attempt accounting have not
been proven. Native/Chancy reliability parity is a release gate and is not yet
passed; the current default does not imply equivalent guarantees. SQLite is a
local/proof backend, not a production durability target.

Mixed Chancy/Native fleets are unsupported. Stop schedulers and workers, drain
in-flight work, and inspect/reconcile outstanding jobs before changing every
worker and scheduler together. Preserve old backend rows. This operation has
not been rehearsed end-to-end; treat it as a conservative procedure, not a
verified migration guarantee.

## Recovery behavior

| Scenario | What happens | Operator action |
|---|---|---|
| Worker dies mid-handler | UoW rolls back; kernel lease expires; job redelivered | none (at-least-once) |
| Worker dies before its dispatch starts (Native) | claim token retires on reclaim; stale dispatch cannot start | none |
| Repeated crash-before-handler (Native) | each claim counts an attempt; `attempts > max_attempts` → `failed` + DLQ | `dlq_replay` after fixing |
| Chancy worker dies before engine dispatch starts | Chancy may redeliver, but bounded engine attempts and claim fencing are unproven | monitor and reconcile stuck or repeated work |
| Worker dies after commit | job is terminal; dispatch may see a stale claim and skip | none |
| DB disconnect mid-handler | TX aborts; job redelivered; idempotent write lands once | none |
| DB connectivity lost (worker idle) | claim/maintenance loops drop the dead conn and reconnect; `run_worker` supervision replaces a dead chancy worker | none |
| Native `stop()` during a long handler | claiming stops; leases renew while draining. If the handler remains after timeout plus cancellation grace, `stop()` returns `False` and retains worker ownership and resources; retry stop later | keep the worker alive while it owns handlers |
| Chancy worker loses a connection or abandons a pre-dispatch claim | behavior depends on Chancy recovery; no bounded engine claim accounting or stale-claim fence is promised | supervise/restart and inspect job state |
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

- Chancy is explicitly usable, but pre-dispatch claim fencing and bounded
  attempt accounting before `mark_running` are unproven. This leaves the
  Native/Chancy reliability parity release gate unsatisfied; do not claim
  equivalent crash guarantees until Chancy behavior is implemented and tested.
- NativeKernel is the current default Postgres backend. It does not support rate
  limits, queue pause/resume, worker tags, priority ordering, or Chancy plugin
  metrics (`engine/native_kernel.py` header).
- `SQLiteKernel` is local/proof-level: single-writer, no cross-process
  notify, no advisory locks — not a production durability target.
