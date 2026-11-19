-- keelflow demo 3 — bulk import domain schema.
-- Raw rows live here; per-row engine status lives in keelflow_row_status
-- (keyed by the bulk parent job_id) — validation and processing are two
-- separate bulk passes over copies of the raw rows.
CREATE TABLE IF NOT EXISTS demo_imports (
    id          serial PRIMARY KEY,
    filename    text NOT NULL,
    status      text NOT NULL DEFAULT 'uploaded',
                -- uploaded|importing|imported|aborted|failed
    mapping     jsonb,
    rows_total  int,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS demo_import_raw (
    id          serial PRIMARY KEY,
    import_id   int NOT NULL REFERENCES demo_imports(id) ON DELETE CASCADE,
    row_no      int NOT NULL,
    data        jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_demo_raw_import ON demo_import_raw (import_id);

CREATE TABLE IF NOT EXISTS demo_products (
    id          serial PRIMARY KEY,
    sku         text NOT NULL UNIQUE,    -- natural key → idempotent import
    name        text NOT NULL,
    price       numeric(10,2) NOT NULL,
    stock       int NOT NULL,
    imported_by int REFERENCES demo_imports(id),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
