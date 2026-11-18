"""Cron scheduler (T10).

Job types declare `schedule: CronSpec` (cron expression + timezone +
optional window + overlap policy). `run_scheduler` loops registered
scheduled types, computes the next fire time, adjusts for `window`, and
enqueues a job whose `unique_key` is `cron:<type>:<iso-fire-time>` — so a
scheduler crash/restart can never double-enqueue the same slot.

Overlap policies (design decision #10):
- `skip` (default): do nothing if the type's previous job is still
  pending/running.
- `queue`: always enqueue (unique_key still prevents same-slot dupes).
- `kill_previous`: cancel the previous job, then enqueue.
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import UTC, datetime, timedelta
from datetime import time as dtime
from zoneinfo import ZoneInfo

import psycopg
from croniter import croniter

from ..sdk import Actor, JobContext, Registry

log = logging.getLogger("pyerp.scheduler")


def next_fire(spec_cron, *, after: datetime | None = None) -> datetime:
    """Next fire time for a CronSpec, honoring timezone + window."""
    tz = ZoneInfo(spec_cron.timezone)
    now = (after or datetime.now(UTC)).astimezone(tz)
    t = croniter(spec_cron.cron, now).get_next(datetime)
    return _apply_window(t, spec_cron.window, tz)


def _apply_window(fire: datetime, window: str | None, tz) -> datetime:
    """Push `fire` to the next allowed hour inside `window` ("22:00-06:00").

    If fire is already inside the window, unchanged. Otherwise it is
    deferred to the next window start (never fired early/outside).
    """
    if not window:
        return fire
    start_s, end_s = window.split("-")
    sh, sm = map(int, start_s.split(":"))
    eh, em = map(int, end_s.split(":"))
    start = dtime(sh, sm, tzinfo=tz)
    end = dtime(eh, em, tzinfo=tz)
    t = fire.timetz()
    inside = (
        (start <= end and start <= t <= end)
        or (start > end and (t >= start or t <= end))
    )
    if inside:
        return fire
    # next occurrence of `start`
    candidate = datetime.combine(fire.date(), start)
    if candidate <= fire:
        candidate += timedelta(days=1)
    return candidate


async def tick(conn, registry: Registry, kernel, *, now: datetime | None = None) -> int:
    """Enqueue due scheduled jobs for all registered scheduled types.

    Returns number of jobs enqueued. Intended to be called in a loop
    (`run_scheduler`) or from tests.
    """
    now = now or datetime.now(UTC)
    enqueued = 0
    for spec in _scheduled_types(registry):
        cron = spec.schedule
        fire = next_fire(cron, after=now - timedelta(seconds=1))
        # fire is the *next* slot; enqueue it once (unique_key dedupes).
        slot = fire.astimezone(UTC).isoformat()
        unique_key = f"cron:{spec.type}:{slot}"

        # overlap policy vs previous job for this type
        if cron.overlap != "queue":
            cur = await conn.execute(
                "SELECT id, status FROM hd_flow_jobs"
                " WHERE type=%s AND status IN ('pending','claimed','running')"
                " ORDER BY created_at DESC LIMIT 1",
                (spec.type,),
            )
            live = await cur.fetchone()
            if live and cron.overlap == "skip":
                continue
            if live and cron.overlap == "kill_previous":
                await conn.execute(
                    "UPDATE hd_flow_jobs SET status='cancelled',"
                    " cancel_requested=true, updated_at=now() WHERE id=%s",
                    (live[0],),
                )

        job_id = uuid.uuid4()
        ctx = JobContext(actor=Actor.system("scheduler"))
        from psycopg.rows import dict_row

        from .context import ctx_to_dict

        async with conn.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(
                """
                INSERT INTO hd_flow_jobs
                    (id, type, status, payload, context, max_attempts, run_at,
                     created_at, updated_at)
                VALUES (%s,%s,'pending','{}',%s,%s,%s,%s,%s)
                """,
                (
                    job_id, spec.type, json.dumps(ctx_to_dict(ctx)),
                    spec.retry.max_attempts if spec.retry else 3,
                    fire, now, now,
                ),
            )
            await kernel.push_ex(
                cursor,
                _kernel_job(job_id, spec.type, fire, unique_key),
            )
            await cursor.execute(
                "INSERT INTO hd_flow_outbox (event_type, payload) VALUES"
                " ('job.scheduled', %s)",
                (json.dumps({"job_id": str(job_id), "type": spec.type}),),
            )
        await conn.commit()
        enqueued += 1
    return enqueued


async def run_scheduler(
    dsn: str, registry: Registry, kernel, *, interval: float = 30.0, stop=None
) -> None:
    conn = await psycopg.AsyncConnection.connect(dsn)
    try:
        while not (stop and stop.is_set()):
            try:
                n = await tick(conn, registry, kernel)
            except Exception:
                log.exception("scheduler tick failed")
                n = 0
            await asyncio.sleep(interval if n == 0 else 0)
    finally:
        await conn.close()


# ── helpers ───────────────────────────────────────────────────────────────


def _scheduled_types(registry) -> list:
    reg = getattr(registry, "_job_types", None)
    if reg is None:
        return []
    return [s for s in reg.values() if s.schedule]


def _kernel_job(job_id, type_name: str, run_at: datetime, unique_key: str):
    import chancy

    from .enqueue import DEFAULT_QUEUE, DISPATCH_FUNC

    return chancy.Job(
        func=DISPATCH_FUNC,
        queue=DEFAULT_QUEUE,
        kwargs={"job_id": str(job_id)},
        max_attempts=3,
        scheduled_at=run_at,
        unique_key=unique_key,
        meta={"job_id": str(job_id), "type": type_name, "scheduled": True},
    )
