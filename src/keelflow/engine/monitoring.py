"""Monitoring data APIs (T15) — read side for ops UIs / SuperAdmin.

All methods take a UnitOfWork; callers own permissions and the UoW's
commit scope. Reads join `uow` so a Mongo adapter can reuse the session.
"""
from __future__ import annotations

from typing import Any

from ..sdk import Storage
from ..sdk.uow import nested_scope


class MonitoringService:
    def __init__(self, jobs=None, registry=None,
                 storage: Storage | None = None) -> None:
        # Optional JobService — required only for dlq_replay().
        # Optional registry — required to resume a workflow run from a
        # DLQ entry that links to a step (otherwise replay stays standalone).
        self._jobs = jobs
        self._registry = registry
        self._storage = storage

    def _store(self) -> Storage:
        if self._storage is None:
            from .worker import _get_storage

            self._storage = _get_storage()
        return self._storage

    async def list_jobs(
        self, uow, *, types=None, statuses=None, limit: int = 50,
        cursor: Any | None = None,
    ) -> dict[str, Any]:
        return await self._store().ops_jobs_list(
            uow, types=types, statuses=statuses, limit=limit, cursor=cursor)

    async def get_job(self, uow, job_id: str) -> dict[str, Any] | None:
        return await self._store().job_load(uow, job_id)

    async def list_stale_progress(self, uow) -> list[dict[str, Any]]:
        """Return running jobs past their configured no-progress window."""
        return await self._store().ops_stale_progress(uow)

    async def get_run(self, uow, run_id: str) -> dict[str, Any] | None:
        storage = self._store()
        run = await storage.run_get(uow, run_id)
        if not run:
            return None
        run["steps"] = await storage.steps_get(uow, run_id)
        return run

    async def list_runs(self, uow, *, limit: int = 50) -> list[dict[str, Any]]:
        return await self._store().ops_runs_list(uow, limit=limit)

    async def rows_list_failed(self, uow, job_id: str) -> list[dict[str, Any]]:
        return await self._store().ops_rows_failed(uow, job_id)

    async def rows_list_archived(
        self,
        uow,
        job_id: str,
        *,
        limit: int = 50,
        after_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """Completed rows moved to the row archive at fan-in.

        Keyset pagination — pass the last returned `id` as `after_id`
        for the next page. Archive rows have no FK to live jobs, so this
        stays readable after job retention.
        """
        return await self._store().ops_rows_archived(
            uow, job_id, limit=limit, after_id=after_id)

    async def results_oversized(
        self, uow, *, threshold: int = 32768, limit: int = 50
    ) -> list[dict[str, Any]]:
        """Job types ranked by worst result size — the `result_bytes`
        signal in action so ops can find hot job types to optimize."""
        return await self._store().ops_results_oversized(
            uow, threshold=threshold, limit=limit)

    async def metrics_snapshot(self, uow) -> dict[str, Any]:
        return await self._store().ops_metrics_snapshot(uow)

    async def retry_job(self, uow, job_id: str) -> None:
        """Ops action: put a failed job back to pending (kernel re-claims)."""
        await self._store().job_retry_reset(uow, job_id)

    async def purge_expired_records(
        self,
        uow,
        *,
        jobs_days: float | None = 30,
        outbox_days: float | None = 7,
        archive_days: float | None = 90,
        dlq_days: float | None = 90,
        batch: int = 500,
    ) -> dict[str, int]:
        """Time-based retention for engine state — **caller-invoked**.

        Deletes, oldest-first and bounded by `batch` per table per call:
        terminal workflow runs older than `jobs_days` (cascades to their
        step runs, releasing the job link); terminal jobs older than
        `jobs_days` that no step run or child still references; published
        outbox events older than `outbox_days`; archived rows older than
        `archive_days`; DLQ entries older than `dlq_days`.

        Never touched: non-terminal jobs/runs, pending or failed outbox
        events, audit, entity state. `None` disables a category. Returns
        per-table deleted counts; callers typically run it periodically
        until counts reach 0.
        """
        return await self._store().ops_sweep(
            uow, jobs_days=jobs_days, outbox_days=outbox_days,
            archive_days=archive_days, dlq_days=dlq_days, batch=batch)

    async def dlq_list(self, uow, *, limit: int = 50) -> list[dict[str, Any]]:
        """Terminal-failure copies, newest first."""
        return await self._store().dlq_list(uow, limit=limit)

    async def dlq_replay(self, uow, dlq_id: str, ctx) -> str:
        """Replay a DLQ entry.

        If the dead job was a DAG step (`run_id`/`step_run_id` populated,
        the step_run still exists, and a registry is available), resume
        the owning run via `resume_step` — the step is re-enqueued in
        place and the run advances when it finishes. Returns the run id.
        Otherwise enqueue a fresh standalone job and return its job id.
        Writes join `uow`.
        """
        if self._jobs is None:
            raise RuntimeError(
                "dlq_replay requires MonitoringService(jobs=JobService(...))"
            )
        storage = self._store()
        row = await storage.dlq_get(uow, dlq_id)
        if row is None:
            raise KeyError(f"no dlq entry {dlq_id}")

        step = None
        if row["step_run_id"] and self._registry is not None:
            step = await storage.step_run_get(uow, str(row["step_run_id"]))
        if step is not None and step["status"] == "failed":
            from .workflow import WorkflowService

            wf = WorkflowService(self._registry, self._jobs)
            async with nested_scope(uow) as nuow:
                await wf.resume_step(
                    nuow, str(step["run_id"]), step["step_id"], ctx)
            return str(step["run_id"])

        async with nested_scope(uow) as nuow:
            ref = await self._jobs.builder(row["type"]) \
                .payload(row["payload"] or {}) \
                .context(ctx) \
                .enqueue(nuow)
        return str(ref.id)
