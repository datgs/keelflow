-- keelflow demo 5 — financial_approval domain schema.
-- A payment RUN (header + many lines) driven through a ~26-step DAG.

CREATE TABLE IF NOT EXISTS demo_pay_requests (
    id          serial PRIMARY KEY,
    ref         text NOT NULL UNIQUE,             -- e.g. PAY-0001
    vendor_id   int  NOT NULL,
    currency    text NOT NULL DEFAULT 'USD',      -- non-USD → fx quote wait step
    amount      numeric(14,2) NOT NULL,           -- header amount
    urgent      boolean NOT NULL DEFAULT false,   -- urgent → treasury step
    fail_sanction boolean NOT NULL DEFAULT false, -- chaos knob: sanction raises
    risk        text,                             -- low|medium|high (scored)
    status      text NOT NULL DEFAULT 'draft',    -- draft|submitted|approved|
                                                -- scheduled|executing|paid|
                                                -- rejected|cancelled
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS demo_pay_lines (
    id         serial PRIMARY KEY,
    request_id int NOT NULL REFERENCES demo_pay_requests(id) ON DELETE CASCADE,
    line_no    int NOT NULL,
    invoice_no text,                              -- blank invoice → fails at pay_lines
    gl_account text NOT NULL DEFAULT '6100',
    amount     numeric(14,2) NOT NULL,
    UNIQUE (request_id, line_no)
);

CREATE TABLE IF NOT EXISTS demo_vendors (
    id       serial PRIMARY KEY,
    name     text NOT NULL,
    country  text NOT NULL,
    bank_ref text NOT NULL UNIQUE
);

-- Fan-out check results (sanction/budget/tax/…) keyed per request+check,
-- same shape as the supplier_onboarding demo's check table.
CREATE TABLE IF NOT EXISTS demo_fin_checks (
    id          serial PRIMARY KEY,
    request_id  int NOT NULL REFERENCES demo_pay_requests(id),
    check_type  text NOT NULL,
    result      jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (request_id, check_type)
);
CREATE INDEX IF NOT EXISTS ix_demo_fin_checks_req
    ON demo_fin_checks (request_id);

-- App-owned correlation for the external providers (fx desk, bank
-- pre-verify, bank execute). NOT engine state: the app correlates
-- provider_ref → keelflow wait job id when the provider calls back.
CREATE TABLE IF NOT EXISTS demo_fin_provider_requests (
    provider_ref text PRIMARY KEY,
    request_id   int NOT NULL REFERENCES demo_pay_requests(id),
    wait_type    text NOT NULL,                   -- wait.fx_quote|wait.bank_preverify|wait.bank_execute
    job_id       uuid NOT NULL,
    status       text NOT NULL DEFAULT 'sent'     -- sent|verified|failed
);
