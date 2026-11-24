"""T-A1 observability proofs."""

import logging

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


def test_prometheus_handles_missing_values():
    out = format_metrics_prometheus({"queue_depth": 0})
    assert "keelflow_queue_depth 0" in out
    assert "keelflow_running 0" in out
