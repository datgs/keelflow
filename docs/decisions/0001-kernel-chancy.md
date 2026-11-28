# 0001 Adopt Chancy as the job kernel

Date: 2026-11-18

## Status

Accepted for the original Chancy adoption decision.

This record preserves the original Chancy adoption choice and its rationale.
Native is the current code default. Both adapters remain supported, with
backend-specific reliability guarantees; Chancy's documented pre-dispatch gaps
are accepted for release. Do not infer reliability parity.

## Context

The design brief (§2.4–2.5, decision #2) requires adopting a Postgres job-queue
kernel for claim/retry/lease mechanics rather than building one, with Chancy and
PgQueuer as the candidates, decided by a benchmark spike (plan task T2).
Benchmark code and raw numbers: `.spikes/kernel/README.md`.

What keelflow needs from a kernel: claim with SKIP LOCKED, retries with backoff and
max-attempts, job cancellation, scheduling (`run_at`, cron), rate limits,
priority, kill-resume (dead worker → work reclaimed), dedup key, and
transactional enqueue in the caller's transaction. Everything else (DAG,
chunk/row, handler registry, JobContext) is built by us regardless.

## Decision

Use **Chancy 0.26.0** as the kernel. Jobs enqueue via `Job`/`push_many` inside
the caller's transaction; workers run `Worker` with `ThreadedExecutor` (and
process/sub-interpreter/asyncio executors available per queue), and queues use
`eager_polling=True` for drain throughput.

## Alternatives Considered

1. **PgQueuer 2.0.0** — excellent kernel (much higher raw throughput: ~20k vs
   ~2k drained jobs/s, single-statement batched enqueue at ~60k/s, BIGINT PK
   fixed in 2.0, inline stale-job reclaim). Rejected not for correctness but
   feature coverage: it ships claim/heartbeat/dedupe/defer/cron but no retries
   with attempts/backoff policy, no cancellation, no priority, no rate limits —
   all of which decision #2 expects the kernel to provide, and which we would
   otherwise have to build and own. Its throughput advantage is far beyond the
   project's requirements (per-job DB-writing handlers cap at ~400/s anyway, and
   real ERP jobs are seconds-long, not microseconds).
2. **Build the kernel** — rejected earlier (approach.md); keeps door open via
   the `Storage` protocol boundary.

## Consequences

Positive:

- Retries (`max_attempts`), priorities, `scheduled_at`, cancellation,
  per-queue rate limits, queue pausing/resume-at, worker tags, and plugins
  (cron, workflow DAG, metrics, dashboard) are provided out of the box —
  less code to own in T4/T5/T10.
- `unique_key` gives atomic dedup (`ON CONFLICT … DO UPDATE`), verified:
  100 pushes with one key → 1 row, 1 execution.
- `LISTEN/NOTIFY` dispatch confirmed (median ~12ms enqueue→handler);
  `notifications=True` default with polling fallback.
- Kill-resume verified: SIGKILL'd worker's job re-claimed by the leader's
  Recovery plugin and completed (at-least-once: 2 starts / 1 done).
- Transactional enqueue: `push`/`push_many` run inside the caller's psycopg
  transaction — satisfies "durable intent" (must-prove #2).
- UUID job PKs; ~14k LOC, psycopg3-only dependency (already in our stack).

Tradeoffs:

- Lower raw throughput (~2k jobs/s drain vs pgq ~20k) — acceptable at our scale;
  per-queue rate limits are global, not per-worker; no per-job rate limits.
- Claim ordering is `id DESC` on UUIDs — FIFO ordering is unreliable; our
  key-serialization layer (T10, must-prove #8) must not depend on it.
- Recovery latency is `heartbeat_timeout` + Recovery `poll_interval`
  (defaults 90s+60s) — tune down per deployment (spike used 10s/5s → ~60s
  observed); document in the worker runbook.
- `eager_polling=True` is required for good drain throughput; default
  `polling_interval`-based fetch caps at `concurrency/interval` (~15/s).
  Must be set in our worker bootstrap.
- Chancy's own DAG plugin exists but we build our own T7 engine — plugin not
  used (no `needs`/`if:`/outputs semantics).

## Follow-Up

- Accepted Chancy limitation: pre-dispatch claim fencing and bounded engine
  attempt accounting are not proven. Chancy remains a supported opt-in backend
  with an explicit warning; release does not require parity with Native. Do not
  describe the backends as reliability-equivalent.
- T4 enqueue pipeline: wrap `push` in caller TX + outbox record.
- T5 worker: queue `eager_polling=True`, `heartbeat_poll_interval`/timeout
  tuned, Recovery plugin enabled (default).
- T10: key-serialization and per-type `max_workers` built on top; do not rely
  on kernel ordering.
- If Chancy ever becomes a blocker, swap behind `Storage` + worker glue only.
