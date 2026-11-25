"""Cron scheduler (T10).

Job types declare `schedule: CronSpec` (cron expression + timezone +
optional window + overlap policy). `run_scheduler` loops registered
scheduled types, computes the next fire time, adjusts for `window`, and
enqueues a job whose `unique_key` is `cron:<type>:<iso-fire-time>` — so
a scheduler crash/restart can never double-enqueue the same slot.

Overlap policies (design decision #10):
- `skip` (default): do nothing if the type's previous job is still
  pending/running.
- `queue`: always enqueue (unique_key still prevents same-slot dupes).
- `kill_previous`: cooperative cancellation of the previous job, then
  enqueue. A pending job is cancelled outright; a claimed/running job
  is flagged `cancel_requested` and runs to completion — this is a
  drain, not a force-kill.

Each job type's work (overlap check → insert → kernel push → outbox
event) commits in its own unit of work.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime, timedelta
from datetime import time as dtime
from zoneinfo import ZoneInfo

from croniter import croniter

from ..sdk import Actor, JobContext, JobInsert, Registry, Storage
from .context import ctx_to_dict

log = logging.getLogger("keelflow.scheduler")


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


async def tick(
    uow, storage: Storage, registry: Registry, kernel,
    *, now: datetime | None = None, interval: float = 30.0,
) -> int:
    """Enqueue due scheduled jobs for all registered scheduled types.

    Returns number of jobs enqueued. Intended to be called in a loop
    (`run_scheduler`) or from tests. All writes join `uow`.
    """
    now = now or datetime.now(UTC)
    from .kernel import as_kernel

    kernel = as_kernel(kernel)
    # liveness: a heartbeat row per scheduler process, inside the tick's
    # uow so ops can alert on a silently dead scheduler. The tick IS
    # the heartbeat, so the freshness window needs headroom over the
    # caller's interval — ~3 missed ticks before reporting stale.
    await storage.heartbeat_upsert(
        uow, _scheduler_name(), "scheduler",
        stale_after_s=max(interval * 3.0, 30.0))
    enqueued = 0
    for spec in _scheduled_types(registry):
        cron = spec.schedule
        fire = next_fire(cron, after=now - timedelta(seconds=1))
        # fire is the *next* slot; enqueue it once (unique_key dedupes).
        slot = fire.astimezone(UTC).isoformat()
        unique_key = f"cron:{spec.type}:{slot}"

        # overlap policy vs previous job for this type
        if cron.overlap != "queue":
            live_jobs = await storage.jobs_live_by_type(uow, spec.type)
            live = live_jobs[0] if live_jobs else None
            if live and cron.overlap == "skip":
                continue
            if live and cron.overlap == "kill_previous":
                # Cooperative cancel: pending jobs are cancelled before
                # dispatch can run them; claimed/running jobs only get
                # the flag and run to completion (cancel does not
                # interrupt an in-flight handler).
                await storage.job_cancel_request(uow, live["id"])

        job_id = uuid.uuid4()
        ctx = JobContext(actor=Actor.system("scheduler"))
        await storage.job_insert(uow, JobInsert(
            id=str(job_id), type=spec.type, payload={},
            context=ctx_to_dict(ctx),
            max_attempts=spec.retry.max_attempts if spec.retry else 3,
            run_at=fire,
        ))
        await kernel.push_ex(
            uow,
            _kernel_job(job_id, spec.type, fire, unique_key),
        )
        await storage.outbox_insert(uow, "job.scheduled", {
            "job_id": str(job_id), "type": spec.type})
        enqueued += 1
    return enqueued


async def run_scheduler(
    storage: Storage, registry: Registry, kernel,
    *, interval: float = 30.0, stop=None, session=None,
) -> None:
    own = session is None
    if session is None:
        session = await storage.connect()
    try:
        while not (stop and stop.is_set()):
            try:
                async with session.begin() as uow:
                    n = await tick(uow, storage, registry, kernel,
                                   interval=interval)
            except Exception:
                log.exception("scheduler tick failed")
                n = 0
            await asyncio.sleep(interval if n == 0 else 0)
    finally:
        if own:
            await session.close()


# ── helpers ───────────────────────────────────────────────────────────────

import os
import socket

_scheduler_id: str | None = None


def _scheduler_name() -> str:
    global _scheduler_id
    if _scheduler_id is None:
        _scheduler_id = f"scheduler-{socket.gethostname()}-{os.getpid()}"
    return _scheduler_id


def _scheduled_types(registry) -> list:
    reg = getattr(registry, "_job_types", None)
    if reg is None:
        return []
    return [s for s in reg.values() if s.schedule]


def _kernel_job(job_id, type_name: str, run_at: datetime, unique_key: str):
    from .kernel import dispatch_spec

    return dispatch_spec(
        job_id=job_id,
        max_attempts=3,
        scheduled_at=run_at,
        unique_key=unique_key,
        meta={"job_id": str(job_id), "type": type_name,
              "scheduled": True},
    )
