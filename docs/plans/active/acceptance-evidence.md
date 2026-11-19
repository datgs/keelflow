# Acceptance evidence — 8 must-prove items

From `docs/architecture-design-brief.md` §9. Each item names the test or
artifact that demonstrates it. Run: `uv run pytest -q` (56 tests).

| # | Claim | Evidence |
|---|---|---|
| 1 | Durable intent — kill worker mid-chunk, resume pending only | `tests/test_e2e.py::test_e2e_scenario` §4 (slow job → worker stop → new worker finishes); kernel kill-resume verified in `.spikes/kernel/README.md` |
| 2 | Transactional triggers — data + outbox commit atomically | `tests/test_enqueue.py` (rollback → no job/outbox row); `tests/test_events.py` |
| 3 | 500k-row throughput | `.spikes/bulk_bench.py` + `.spikes/bulk_bench.md` — 100k rows in ~10 s noop; extrapolated ~50 s for 500k |
| 4 | Ordering safety — parallel disjoint keys, serialized shared key | `tests/test_concurrency_scheduler.py::test_same_key_never_parallel_different_keys_free` (MAX_SEEN==1 for shared key) |
| 5 | Workflow DAG — `needs`, `if:` guards, outputs, resume-from-failed-step | `tests/test_workflow.py` (multi-step DAG, guard eval, output refs); `tests/test_human_sm.py::test_human_step_parks_then_unblocks_run` |
| 6 | Module boundary — modules can't reach each other's tables | Host-app concern, not engine: engine exposes only service interfaces + `keelflow_*` schema; host apps apply their own module isolation. **Deferred to host integration.** |
| 7 | RBAC — `scope: own` enforced; unknown permissions fail | `tests/test_audit_permissions.py` (`perm:own`/`perm:all` scope syntax, `_effective_actor` via-chain). "Fail at load" is partially covered: SM transition guards resolve at runtime; compile-time permission lint is a future enhancement. |
| 8 | Key-serialized execution | `tests/test_concurrency_scheduler.py` — `concurrency_key` → `pg_advisory_lock(hashtext(key))`; same key never parallel |

Additional contract coverage: YAML compiler (`test_compiler`), scheduler
windows/dedup (`test_concurrency_scheduler`), human inbox claim/escalation
(`test_human_sm`), state machine hooks (`test_human_sm`), monitoring +
SuperAdmin (`test_monitoring_admin`), MCP tools (`test_mcp`), audit
rollback atomicity (`test_audit_permissions`).
