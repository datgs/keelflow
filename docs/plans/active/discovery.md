---
status: proposed
last_reviewed: 2026-11-18
superseded_by: []
related_code: []
---

# Discovery: pyerp — reusable Postgres-backed workflow engine

## Outcome and Scope
- Desired behavior: a reusable Python library (`pyerp`) providing a durable job/workflow engine: DAG workflows, bulk chunk+row processing, human-in-the-loop tasks, entity state machines, transactional outbox, RBAC hooks, audit, monitoring data APIs.
- Users: engineering teams building internal business applications (internal ERP and other projects).
- Non-goals: no domain logic (inventory, invoices, RBAC rules, host screens), no UI product, no standalone service, no external broker, no dynamic UI workflow designer.

## Evidence
- `docs/architecture-design-brief.md` — full agreed design: unified execution model (§1.1), job engine (§2), build-vs-adopt + library comparison (§2.4–2.5), JobContext (§2.6), human tasks (§2.7), bulk model (§3), module/state machine (§4.3), packaging + SDK interfaces (§4.1, §4.1.1), auth/RBAC/audit (§5–7), monitoring (§8), 12 internal decisions (§10), 8 prototype must-prove items (§9).
- Research findings captured in §2.5/§2.5.1: Chancy ≈14k LOC Python, has retries/scheduling/cancel/DAG plugin but no YAML/ordering/key-serialization; PgQueuer minimal SKIP LOCKED but int4 PK cap and no scheduling; Procrastinate has defer-in-transaction but maintenance risk; DBOS/pgflow rejected (model mismatch / lock-in).

## Existing Contracts and Patterns
- Repo has no application code — this is a greenfield library inside `eta` workspace docs context; actual code repo TBD (new repo or packages directory).
- SDK interface list already specified at `../../architecture-design-brief.md` §4.1.1 (Registry, JobTypeSpec, JobHandler, JobBuilder, JobService, WorkflowService, EventBus, StateMachineService, PermissionService, AuditService, Storage).

## Constraints
- Technical: Python; Postgres as single source of truth; SKIP LOCKED claim; transactional intent; read-only ctx in handlers; strict job-state enum; minimal `if:` expression syntax.
- Operations: worker = plain Python process (systemd/Docker managed), embedded library (no separate service).
- Security/data: no PII requirements in framework; RBAC is generic.
- UI standard: none — only a deliberately rough server-rendered SuperAdmin; app UIs are out of scope.

## Facts, Assumptions, Questions
- Verified: all design decisions in the brief (recorded as internal R&D decisions).
- Assumed: Python 3.11+, Postgres 14+, Pydantic v2, async workers.
- Open: kernel choice (Chancy vs PgQueuer) — resolved by benchmark spike T2; remote-worker HTTP adapter deferred; workflow YAML compiler details finalized in T7.

## Clarifications
- Scope confirmed with user: generic framework, host-specific content excluded, no FIFO/inventory implementation.
