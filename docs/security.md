# Security and data-handling boundaries

Minimum rules for internal deployments. Keelflow is a library; it does
not run a control plane — the application supplies authentication,
authorization, and data policy.

## Data classified as sensitive

| Data | Where | Sensitivity |
|---|---|---|
| `jobs.payload`, `jobs.context`, `step_runs.outputs` | engine tables | business data; may contain PII/secrets the app passes |
| `keelflow_audit` | audit table | who did what; may contain user ids, diffs |
| `keelflow_outbox.payload` | outbox | domain event payloads |
| `keelflow_dlq.payload` | DLQ | same as job payload — dead jobs keep it |

Retention: `purge_expired_records` deletes terminal jobs/runs, published
outbox, row archive, and DLQ by age — audit and entity state are never
purged by the sweep (the app sets audit retention policy).

## Access boundaries

- **SuperAdmin (`keelflow.admin.app`)**: unauthenticated, HTTP. Safe
  only bound to loopback or behind a proxy that enforces
  **authentication, authorization, and write protection** — the retry
  endpoint mutates state. A reverse proxy without those controls is not
  protection.
- **MCP server (`keelflow.mcp`)**: read-only tools, but they expose
  jobs, runs, failed rows, and audit — restrict to trusted agents/users
  like any DB read access.
- **Monitoring APIs**: payload/context/audit read access — treat as
  sensitive data endpoints in the app that mounts them.

## Telemetry

- Logs, traces, and metric labels must never include `payload`,
  `context`, audit changes, or secrets. Engine logs carry only
  `job_id`, `job_type`, `run_id`, `step_id`, `correlation_id`,
  `tenant_id`, `worker_id`, `attempt`.
- `correlation_id`/`tenant_id` are context identifiers; do not put raw
  user data in them if your log pipeline is less trusted than the DB.

## Permissions

`ctx.require(perm)` checks the *snapshot* taken at enqueue
(`jobs.context`). Queued and in-flight work keeps the permissions it
was created with — permission changes apply to new work only. Apps that
need revocation must `cancel()` and re-enqueue.

## Multi-tenancy

`tenant_id` is **not** an isolation boundary. All tenants share
`keelflow_*` tables; every query can see every row. For tenant
isolation use separate databases or deployments — do not rely on
application-level filtering of shared tables.
