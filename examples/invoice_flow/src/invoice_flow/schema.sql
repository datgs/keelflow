-- invoice_flow demo domain tables (keelflow_* engine tables come from
-- keelflow.postgres.schema.apply_async).
CREATE TABLE IF NOT EXISTS demo_invoices (
    number      TEXT PRIMARY KEY,            -- entity id for the SM
    vendor      TEXT NOT NULL,
    amount      NUMERIC(12,2) NOT NULL,
    currency    TEXT NOT NULL DEFAULT 'USD',
    fail_post   BOOLEAN NOT NULL DEFAULT FALSE,  -- chaos toggle
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS demo_ledger_postings (
    id          BIGSERIAL PRIMARY KEY,
    invoice_no  TEXT NOT NULL UNIQUE REFERENCES demo_invoices(number),
    amount      NUMERIC(12,2) NOT NULL,
    posted_by   TEXT NOT NULL,
    posted_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS demo_gl_records (
    invoice_no  TEXT PRIMARY KEY REFERENCES demo_invoices(number),
    journal_ref TEXT NOT NULL,
    recorded_by TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
