-- hd-flow core schema
-- Engine tables live in the app's database. Names use hd_flow_ prefix.

CREATE TABLE IF NOT EXISTS hd_flow_jobs (
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
    assignee        TEXT,
    parent_job_id   UUID REFERENCES hd_flow_jobs(id),
    result          JSONB,
    error           TEXT,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_jobs_claim
    ON hd_flow_jobs (status, run_at, type)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS ix_jobs_key
    ON hd_flow_jobs (concurrency_key) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS ix_jobs_assignee ON hd_flow_jobs (assignee)
    WHERE status IN ('pending','claimed');


CREATE TABLE IF NOT EXISTS hd_flow_workflow_runs (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow    TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'running'
                CHECK (status IN ('running','done','failed','cancelled','blocked')),
    inputs      JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE IF NOT EXISTS hd_flow_step_runs (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id       UUID NOT NULL REFERENCES hd_flow_workflow_runs(id) ON DELETE CASCADE,
    step_id      TEXT NOT NULL,
    type         TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending'
                 CHECK (status IN ('pending','claimed','running','done','failed','cancelled','blocked','skipped')),
    job_id       UUID REFERENCES hd_flow_jobs(id),
    outputs      JSONB NOT NULL DEFAULT '{}',
    attempts     INT NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, step_id)
);
CREATE INDEX IF NOT EXISTS ix_step_runs_run ON hd_flow_step_runs (run_id, status);


CREATE TABLE IF NOT EXISTS hd_flow_chunks (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id       UUID NOT NULL REFERENCES hd_flow_jobs(id) ON DELETE CASCADE,
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


CREATE TABLE IF NOT EXISTS hd_flow_row_status (
    id         BIGSERIAL PRIMARY KEY,
    job_id     UUID NOT NULL REFERENCES hd_flow_jobs(id) ON DELETE CASCADE,
    chunk_id   UUID REFERENCES hd_flow_chunks(id) ON DELETE SET NULL,
    row_key    TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'pending'
               CHECK (status IN ('pending','processing','done','failed')),
    error      TEXT,
    data       JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_row_status_job ON hd_flow_row_status (job_id, status);
CREATE INDEX IF NOT EXISTS ix_row_status_chunk ON hd_flow_row_status (chunk_id, status);


CREATE TABLE IF NOT EXISTS hd_flow_outbox (
    id         BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload    JSONB NOT NULL,
    status     TEXT NOT NULL DEFAULT 'pending'
               CHECK (status IN ('pending','published','failed')),
    attempts   INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_outbox_pending ON hd_flow_outbox (id)
    WHERE status = 'pending';


CREATE TABLE IF NOT EXISTS hd_flow_audit (
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
CREATE INDEX IF NOT EXISTS ix_audit_entity ON hd_flow_audit (entity_type, entity_id);
CREATE INDEX IF NOT EXISTS ix_audit_corr ON hd_flow_audit (correlation_id);


CREATE TABLE IF NOT EXISTS hd_flow_dlq (
    id         UUID PRIMARY KEY,
    type       TEXT NOT NULL,
    payload    JSONB NOT NULL,
    context    JSONB NOT NULL,
    error      TEXT,
    attempts   INT NOT NULL,
    dead_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
