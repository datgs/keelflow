# keelflow spec — conformance checklist

An implementation is keelflow-conformant when it demonstrates all eight
items. (Python reference: `tests/` in `keelflow` —
`docs/plans/active/acceptance-evidence.md` maps each to a test.)

| # | Claim | How to prove |
|---|---|---|
| 1 | Durable intent | Kill a worker mid-chunk → new worker resumes pending rows only; no lost work, no duplicates |
| 2 | Transactional triggers | Rollback after `enqueue`/`publish`/`transition` → no phantom job/event; commit → visible |
| 3 | Bulk throughput | ≥10k rows/s engine overhead for noop handler at 100k rows; all row statuses observable |
| 4 | Ordering safety | Parallel chunks on disjoint `concurrency_key`s; serialized execution on same key |
| 5 | Workflow DAG | `needs`, `if:` guards, output refs, resume-from-failed-step, human-step parking |
| 6 | Module boundary | Engine exposes only service interfaces + `keelflow_*` schema — host owns domain isolation |
| 7 | RBAC | `perm:own`/`perm:all` scope enforcement; `via` chain in audit actor; transition guard `permission` enforced |
| 8 | Key-serialized execution | Two jobs with same `concurrency_key` never run in parallel; different keys run freely |

## Interop requirements

- YAML definition format (`03-workflow.md`) MUST parse identically —
  a definition file is portable across implementations.
- `JobContext` JSON (`06-context.md`) MUST round-trip — a job enqueued
  by a Python caller is dispatchable by a .NET worker reading
  `jobs.context` (future interop goal).
- Status enums (`01-schema.md`) MUST match verbatim — UIs and tooling
  rely on the strict enum.
