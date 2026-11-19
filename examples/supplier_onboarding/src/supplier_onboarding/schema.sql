-- keelflow demo 2 — supplier_onboarding domain schema.
CREATE TABLE IF NOT EXISTS demo_suppliers (
    id            serial PRIMARY KEY,
    name          text NOT NULL,
    country       text NOT NULL,
    tax_id        text NOT NULL,
    contact_email text NOT NULL,
    bank_ref      text NOT NULL,
    risk          text,
    status        text NOT NULL DEFAULT 'requested',  -- requested|onboarding|active|rejected
    fail_sanction boolean NOT NULL DEFAULT false,      -- chaos knob: sanction check raises
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS demo_vendor_accounts (
    id            serial PRIMARY KEY,
    supplier_id   int NOT NULL REFERENCES demo_suppliers(id),
    vendor_no     text NOT NULL UNIQUE,                -- natural key → idempotent ERP create
    legal_name    text,
    tax_id        text,
    payment_terms text NOT NULL DEFAULT 'NET30',
    bank_verified boolean NOT NULL DEFAULT false,
    activated_at  timestamptz,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS demo_check_results (
    id          serial PRIMARY KEY,
    supplier_id int NOT NULL REFERENCES demo_suppliers(id),
    check_type  text NOT NULL,
    result      jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (supplier_id, check_type)   -- idempotent per supplier+check
);

CREATE INDEX IF NOT EXISTS ix_demo_checks_sup ON demo_check_results (supplier_id);
