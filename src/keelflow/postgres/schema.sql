-- keelflow core schema
-- Engine tables live in the app's database. Names use keelflow_ prefix.

CREATE TABLE IF NOT EXISTS keelflow_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','claimed','running','done','failed','cancelled','blocked')),
    payload         JSONB NOT NULL DEFAULT '{}',
    context         JSONB NOT NULL DEFAULT '{}',
    attempts        INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 3,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_by      TEXT,
    lease_until     TIMESTAMPTZ,
    concurrency_key TEXT,
    unique_key      TEXT,              -- idempotent-enqueue dedup (nullable)
    assignee        TEXT,
    due_at          TIMESTAMPTZ,          -- human task SLA (nullable)
    parent_job_id   UUID REFERENCES keelflow_jobs(id),
    result          JSONB,
    result_bytes    INT,              -- serialized size for size-visibility
    error           TEXT,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    progress         JSONB,
    progress_at      TIMESTAMPTZ,
    progress_timeout_seconds DOUBLE PRECISION,
    execution_token TEXT,           -- non-reusable fencing token per claim
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE keelflow_jobs ADD COLUMN IF NOT EXISTS progress JSONB;
ALTER TABLE keelflow_jobs ADD COLUMN IF NOT EXISTS progress_at TIMESTAMPTZ;
ALTER TABLE keelflow_jobs ADD COLUMN IF NOT EXISTS progress_timeout_seconds DOUBLE PRECISION;
ALTER TABLE keelflow_jobs ADD COLUMN IF NOT EXISTS execution_token TEXT;
CREATE INDEX IF NOT EXISTS ix_jobs_claim
    ON keelflow_jobs (status, run_at, type)
    WHERE status = 'pending';

-- Native-kernel claim marker: push_ex flips it true; the claim loop only
-- selects flagged rows (human/wait rows never get a dispatch push).
ALTER TABLE keelflow_jobs
    ADD COLUMN IF NOT EXISTS dispatch_queued BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX IF NOT EXISTS ix_jobs_native_claim
    ON keelflow_jobs (run_at, id)
    WHERE status = 'pending' AND dispatch_queued;
CREATE INDEX IF NOT EXISTS ix_jobs_key
    ON keelflow_jobs (concurrency_key) WHERE status = 'running';
-- Idempotent enqueue: at most one non-terminal job per (type, unique_key).
CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_unique_key
    ON keelflow_jobs (type, unique_key)
    WHERE unique_key IS NOT NULL
      AND status IN ('pending','claimed','running','blocked');
CREATE INDEX IF NOT EXISTS ix_jobs_assignee ON keelflow_jobs (assignee)
    WHERE status IN ('pending','claimed');


CREATE TABLE IF NOT EXISTS keelflow_workflow_runs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow    TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'running'
                CHECK (status IN ('running','done','failed','cancelled','blocked')),
    inputs      JSONB NOT NULL DEFAULT '{}',
    -- Workflow-trigger metadata: set when a state-machine transition
    -- started this run so the terminal callback can find its target.
    triggered_by_entity_type TEXT,
    triggered_by_entity_id   TEXT,
    trigger_config           JSONB,           -- on_succeeded/on_rejected/on_failed/failure_job
    trigger_ctx              JSONB,           -- serialized initiating JobContext
    terminal_outcome         TEXT CHECK (terminal_outcome IN
                                 ('succeeded','rejected','failed','cancelled')),
    callback_state           TEXT NOT NULL DEFAULT 'none'
                           CHECK (callback_state IN
                                  ('none','pending','done','failed')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE IF NOT EXISTS keelflow_step_runs (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id       UUID NOT NULL REFERENCES keelflow_workflow_runs(id) ON DELETE CASCADE,
    step_id      TEXT NOT NULL,
    type         TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending','claimed','running','done','failed','cancelled','blocked','skipped')),
    job_id       UUID REFERENCES keelflow_jobs(id),
    outputs      JSONB NOT NULL DEFAULT '{}',
    outputs_bytes INT,                -- serialized size for size-visibility
    attempts     INT NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, step_id)
);
CREATE INDEX IF NOT EXISTS ix_step_runs_run ON keelflow_step_runs (run_id, status);


CREATE TABLE IF NOT EXISTS keelflow_chunks (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id       UUID NOT NULL REFERENCES keelflow_jobs(id) ON DELETE CASCADE,
    idx          INT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending','claimed','running','done','failed','cancelled')),
    row_range    INT4RANGE,
    claimed_by   TEXT,
    lease_until  TIMESTAMPTZ,
    attempts     INT NOT NULL DEFAULT 0,
    result       JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_id, idx)
);


CREATE TABLE IF NOT EXISTS keelflow_row_status (
    id         BIGSERIAL PRIMARY KEY,
    job_id     UUID NOT NULL REFERENCES keelflow_jobs(id) ON DELETE CASCADE,
    chunk_id   UUID REFERENCES keelflow_chunks(id) ON DELETE SET NULL,
    row_key    TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'pending'
               CHECK (status IN ('pending','processing','done','failed')),
    error      TEXT,
    data       JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_row_status_job ON keelflow_row_status (job_id, status);
CREATE INDEX IF NOT EXISTS ix_row_status_chunk ON keelflow_row_status (chunk_id, status);


-- Cold store for completed bulk rows moved at fan-in. No FKs to live
-- jobs/chunks: archive rows must survive job retention. `row_id` is the
-- source keelflow_row_status.id — a stable identity for idempotent moves.
CREATE TABLE IF NOT EXISTS keelflow_row_archive (
    id          BIGSERIAL PRIMARY KEY,
    job_id      UUID NOT NULL,
    row_id      BIGINT NOT NULL,
    row_key     TEXT NOT NULL,
    chunk_id    UUID,
    data        JSONB,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (job_id, row_id)
);
CREATE INDEX IF NOT EXISTS ix_row_archive_job ON keelflow_row_archive (job_id, id);
CREATE INDEX IF NOT EXISTS ix_row_archive_key ON keelflow_row_archive (job_id, row_key);


CREATE TABLE IF NOT EXISTS keelflow_outbox (
    id         BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload    JSONB NOT NULL,
    status     TEXT NOT NULL DEFAULT 'pending'
               CHECK (status IN ('pending','published','failed')),
    attempts   INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_outbox_pending ON keelflow_outbox (id)
    WHERE status = 'pending';


-- Per-subscriber delivery log: at-least-once dedup across crashes.
CREATE TABLE IF NOT EXISTS keelflow_outbox_deliveries (
    outbox_id  BIGINT NOT NULL REFERENCES keelflow_outbox(id) ON DELETE CASCADE,
    subscriber TEXT NOT NULL,
    delivered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (outbox_id, subscriber)
);


CREATE TABLE IF NOT EXISTS keelflow_audit (
    id           BIGSERIAL PRIMARY KEY,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor        JSONB NOT NULL,
    action       TEXT NOT NULL,
    entity_type  TEXT NOT NULL,
    entity_id    TEXT NOT NULL,
    changes      JSONB NOT NULL DEFAULT '{}',
    source       TEXT,
    correlation_id TEXT
);
CREATE INDEX IF NOT EXISTS ix_audit_entity ON keelflow_audit (entity_type, entity_id);
CREATE INDEX IF NOT EXISTS ix_audit_corr ON keelflow_audit (correlation_id);


-- Entity lifecycle state for the state-machine service (T12). Domain
-- entities may keep their own status column; this is the engine's record.
CREATE TABLE IF NOT EXISTS keelflow_entity_state (
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    state       TEXT NOT NULL,
    version     INT  NOT NULL DEFAULT 1,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_type, entity_id)
);


CREATE TABLE IF NOT EXISTS keelflow_dlq (
    id         UUID PRIMARY KEY,
    type       TEXT NOT NULL,
    payload    JSONB NOT NULL,
    context    JSONB NOT NULL,
    error      TEXT,
    attempts   INT NOT NULL,
    -- workflow link (nullable): set when the dead job was a DAG step, so
    -- dlq_replay can resume the owning run instead of a standalone job.
    -- no FK — a DLQ entry may outlive its run.
    run_id     UUID,
    step_run_id UUID,
    dead_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- Process liveness for observability: workers and schedulers upsert a
-- heartbeat row; ops reads last_seen to tell alive from silently dead.
CREATE TABLE IF NOT EXISTS keelflow_process_health (
    name       TEXT PRIMARY KEY,
    kind       TEXT NOT NULL CHECK (kind IN ('worker','scheduler')),
    last_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
    stale_after_s DOUBLE PRECISION NOT NULL DEFAULT 30
);
ALTER TABLE keelflow_process_health
    ADD COLUMN IF NOT EXISTS stale_after_s DOUBLE PRECISION NOT NULL DEFAULT 30;
