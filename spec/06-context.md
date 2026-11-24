# keelflow spec — JobContext & actor model

## JobContext (JSON, serialized into `jobs.context`)

```json
{
  "actor": {"type": "user|service|system|job", "id": "u1",
            "via": {"type": "user", "id": "approver-1"}},
  "tenant_id": "t1",
  "correlation_id": "c-…",
  "request_id": "r-…",
  "permissions": ["inv.approve", "perm:own"],
  "extra": {"job_id": "…"}
}
```

Rules:

- **Mandatory.** `enqueue()` without `.context(ctx)` → error. Ambient
  data never travels inside `payload`.
- **Snapshot at enqueue.** Permission changes after queueing do not
  affect the job — the audit must match the authorizing context.
- **Read-only to handlers.** Handlers receive `ctx`; downstream context
  passes through step `outputs`, never by mutating `ctx`.
- **Child inheritance.** Engine-created children set
  `actor=job:<id> via=<parent actor>`; `correlation_id`, `tenant_id`,
  `permissions` inherited.
- **`extra` is the extension surface.** `locale`, `cost_center`,
  `job_id` (injected by dispatch). Unvalidated by engine.
- **`ctx.uow` — handler-only, never serialized.** During dispatch the
  engine sets `ctx.uow` to the unit of work that finishes the job
  and advances the run; domain writes through it commit or roll back
  with the job outcome. Excluded from `jobs.context` JSON.
- `ctx.require(perm)` resolves from `ctx.actor` — never thread-local.

## Permission scope syntax

`perm:all` — unrestricted · `perm:own` — own rows only ·
`perm:<scope>` — arbitrary named scopes; host app maps scope→filter.

## Audit actor

`keelflow_audit.actor` stores `{type, id, via?}` — a `via` chain
`user u1 → job j7 → job j8` answers "which human authorized the chain".
