-- keelflow demo 4 — rag_demo domain schema (employee helpdesk).
CREATE TABLE IF NOT EXISTS demo_rag_docs (
    id          serial PRIMARY KEY,
    title       text NOT NULL,
    body        text NOT NULL,
    status      text NOT NULL DEFAULT 'pending',      -- pending|indexed|failed
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS demo_rag_chunks (
    id          serial PRIMARY KEY,
    doc_id      int NOT NULL REFERENCES demo_rag_docs(id) ON DELETE CASCADE,
    idx         int NOT NULL,
    text        text NOT NULL,
    embedding   float8[] NOT NULL DEFAULT '{}',
    status      text NOT NULL DEFAULT 'pending',      -- pending|done|failed
    error       text,
    UNIQUE (doc_id, idx)                              -- idempotent re-index
);
CREATE INDEX IF NOT EXISTS ix_rag_chunks_doc ON demo_rag_chunks (doc_id, idx);

CREATE TABLE IF NOT EXISTS demo_rag_questions (
    id           serial PRIMARY KEY,
    q            text NOT NULL,
    status       text NOT NULL DEFAULT 'asked',       -- asked|answered|reviewed|rejected
    answer       text,
    confidence   numeric,
    reviewed_by  text,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- chaos knobs for the demo (flaky LLM, slow LLM, weak retrieval).
CREATE TABLE IF NOT EXISTS demo_rag_settings (
    key    text PRIMARY KEY,
    value  jsonb NOT NULL
);
