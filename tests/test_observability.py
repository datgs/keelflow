"""T-A1 observability proofs."""

import logging

import pytest

from keelflow.observability import (
    LOG_FIELDS,
    _child_traceparent,
    _new_traceparent,
    extract_traceparent,
    format_metrics_prometheus,
    inject_traceparent,
    job_logger,
    tracer,
)
from keelflow.sdk import Actor, JobContext


def test_traceparent_chain():
    tp = _new_traceparent()
    assert tp.startswith("00-") and len(tp.split("-")) == 4
    child = _child_traceparent(tp)
    assert child.split("-")[1] == tp.split("-")[1]   # same trace id
    assert child != tp                               # new span id


def test_inject_extract_on_ctx():
    ctx = JobContext(actor=Actor.user("u"), correlation_id="c1")
    inject_traceparent(ctx)
    tp = extract_traceparent(ctx)
    assert tp is not None
    # second inject produces a child span under the same trace
    inject_traceparent(ctx)
    assert extract_traceparent(ctx).split("-")[1] == tp.split("-")[1]


def test_noop_tracer_safe_without_otel():
    tr = tracer()
    with tr.start_as_current_span("x"):
        pass  # must not raise with or without OTel installed


def test_job_logger_contract_fields(caplog):
    base = logging.getLogger("keelflow.test")
    jlog = job_logger(base, job_id="j1", job_type="t@v1",
                      correlation_id="c", tenant_id="t9",
                      payload="SECRET", context={"k": "v"})
    with caplog.at_level(logging.INFO, logger="keelflow.test"):
        jlog.info("hello")
    rec = caplog.records[-1]
    assert rec.job_id == "j1" and rec.correlation_id == "c"
    assert rec.tenant_id == "t9"
    # privacy: non-contract fields are dropped, never logged
    assert not hasattr(rec, "payload") and not hasattr(rec, "context")
    for f in LOG_FIELDS:
        assert isinstance(f, str)


def test_prometheus_format_golden():
    out = format_metrics_prometheus({
        "queue_depth": 3, "claimed": 1, "running": 2, "waiting": 4,
        "failed_24h": 5, "oldest_pending_s": 12.5,
        "avg_done_duration_s_24h": 3.2, "dlq_depth": 1,
        "workers_alive": 2, "scheduler_last_seen_age_s": 30.0,
        "db_connections": 7,
    })
    assert "# TYPE keelflow_queue_depth gauge" in out
    assert "keelflow_queue_depth 3" in out
    assert "keelflow_waiting 4" in out          # waits ≠ stuck
    assert "keelflow_dlq_depth 1" in out
    assert "keelflow_workers_alive 2" in out
    # no payload-ish fields leaked
    assert "payload" not in out and "context" not in out


def _has_sample(out: str, metric: str) -> bool:
    """True when the exposition contains a sample line (not HELP/TYPE)."""
    return any(line.startswith(f"{metric} ") for line in out.splitlines())


def test_prometheus_handles_missing_values():
    """Absent snapshot keys are omitted, not rendered as zero — an
    absent scheduler heartbeat age is unknown, not '0 seconds'."""
    out = format_metrics_prometheus({"queue_depth": 0})
    assert _has_sample(out, "keelflow_queue_depth")
    assert not _has_sample(out, "keelflow_running")
    assert not _has_sample(out, "keelflow_scheduler_last_seen_age_seconds")


def test_prometheus_none_age_not_zero():
    """Snapshot with no scheduler rows carries age=None — it must not
    emit a misleading 0 sample."""
    out = format_metrics_prometheus({
        "schedulers_alive": 0, "scheduler_last_seen_age_s": None})
    assert _has_sample(out, "keelflow_schedulers_alive")
    assert not _has_sample(out, "keelflow_scheduler_last_seen_age_seconds")


@pytest.mark.asyncio
async def test_workers_alive_counts_only_fresh_heartbeats():
    """D3: stale worker rows must not count alive; fresh ones do."""
    import os

    import psycopg
    from psycopg.rows import dict_row

    from keelflow.postgres.storage import PostgresStorage

    dsn = os.environ.get(
        "KEELFLOW_TEST_DSN",
        "postgresql://postgres:postgres@localhost:5544/keelflow_test",
    )
    conn = await psycopg.AsyncConnection.connect(
        dsn, autocommit=True, row_factory=dict_row
    )
    try:
        await conn.execute("DELETE FROM keelflow_process_health")
        await conn.execute(
            "INSERT INTO keelflow_process_health"
            " (name, kind, last_seen, stale_after_s)"
            " VALUES ('fresh-1','worker', now(), 30),"
            "        ('fresh-2','worker', now() - interval '5 seconds', 30),"
            "        ('stale-1','worker', now() - interval '10 minutes', 30),"
            "        ('stale-2','worker', now() - interval '1 hour', 30),"
            "        ('sched-live','scheduler', now(), 90),"
            "        ('sched-idle','scheduler',"
            "         now() - interval '60 seconds', 90),"
            "        ('sched-dead','scheduler', now() - interval '1 day', 90)"
        )
        storage = PostgresStorage(dsn)
        session = await storage.connect()
        try:
            async with session.begin() as uow:
                snap = await storage.ops_metrics_snapshot(uow)
        finally:
            await session.close()
        assert snap["workers_alive"] == 2
        assert snap["workers_stale"] == 2
        assert snap["schedulers_alive"] == 2   # 60s-old row is within its 90s window
        text = format_metrics_prometheus(snap)
        assert "keelflow_workers_alive 2" in text
        assert "keelflow_workers_stale 2" in text
        assert "keelflow_schedulers_alive 2" in text
    finally:
        await conn.execute("DELETE FROM keelflow_process_health")
        await conn.close()


@pytest.mark.asyncio
async def test_worker_heartbeat_loop_marks_idle_worker_alive():
    """An app-owned worker (own kernel lifecycle) can reuse
    `worker_heartbeat_loop` for idle liveness — the row lands without
    any dispatch running."""
    import asyncio
    import os

    import psycopg
    from psycopg.rows import dict_row

    from keelflow.engine.worker import worker_heartbeat_loop
    from keelflow.postgres.storage import PostgresStorage

    dsn = os.environ.get(
        "KEELFLOW_TEST_DSN",
        "postgresql://postgres:postgres@localhost:5544/keelflow_test",
    )
    conn = await psycopg.AsyncConnection.connect(
        dsn, autocommit=True, row_factory=dict_row
    )
    try:
        await conn.execute("DELETE FROM keelflow_process_health")
        stop = asyncio.Event()
        storage = PostgresStorage(dsn)
        task = asyncio.create_task(
            worker_heartbeat_loop(storage, interval=0.1, stop=stop))
        await asyncio.sleep(0.35)
        stop.set()
        await asyncio.wait_for(task, timeout=2)
        row = await (await conn.execute(
            "SELECT kind, stale_after_s FROM keelflow_process_health"
        )).fetchone()
        assert row is not None
        assert row["kind"] == "worker" and row["stale_after_s"] == 30.0
    finally:
        await conn.execute("DELETE FROM keelflow_process_health")
        await conn.close()


@pytest.mark.asyncio
async def test_worker_heartbeat_custom_interval_widens_window():
    """A caller heartbeating slower than the default 10s cadence must
    not be reported stale while healthy — the declared freshness
    window derives from the interval (3 missed ticks, min 30s)."""
    import asyncio
    import os

    from keelflow.engine.worker import worker_heartbeat_loop
    from keelflow.postgres.storage import PostgresStorage

    dsn = os.environ.get(
        "KEELFLOW_TEST_DSN",
        "postgresql://postgres:postgres@localhost:5544/keelflow_test",
    )
    captured: list[float] = []
    storage = PostgresStorage(dsn)
    orig = PostgresStorage.heartbeat_upsert

    async def capture(self, uow, name, kind, *, stale_after_s=30.0):
        captured.append(stale_after_s)
        return await orig(self, uow, name, kind,
                          stale_after_s=stale_after_s)

    PostgresStorage.heartbeat_upsert = capture
    try:
        stop = asyncio.Event()
        task = asyncio.create_task(
            worker_heartbeat_loop(storage, interval=60.0, stop=stop))
        await asyncio.sleep(0.3)   # first upsert has landed
        stop.set()
        await asyncio.wait_for(task, timeout=2)
    finally:
        PostgresStorage.heartbeat_upsert = orig
    assert captured == [180.0]     # max(60*3, 30), not the 30s default
