# Chancy issue review (S0) — 2026-11-19

Pinned version: `chancy==0.26.0` (`chancy>=0.26.0` in pyproject).
Upstream repo: `TkTech/chancy`. Open issues scanned 2026-11-19.

## Material to keelflow

| Issue | Status | Impact | Action |
|---|---|---|---|
| #104 Leadership crashes the worker after a brief PG disconnect (0.26.0) | OPEN — fix in PR #106, expected in 0.27.0 | A 5-second DB outage kills the worker until restarted. Our worker/dispatch relies on chancy Workers staying alive. | **Accepted risk / deployment constraint.** Document that workers must run under a supervisor (systemd/docker/k8s restart policy). Re-pin to `>=0.27.0` once released with the plugin-API fix (breaking change — review adapter code then). |
| #105 `TaskManager.cancel_all` KeyError regression in 0.26.0 | OPEN — maintainer acknowledged; simple `discard` fix | Crash on shutdown edge case: `cancel_all` + same-tick done tasks. Our `run_worker`/worker stop path could hit this during graceful shutdown. | **Defer to upstream patch** — small fix, likely lands in same release as #104. Re-pin to `>=0.27.0` when available. |
| #69 Fail loudly on push to undeclared queue | OPEN | We declare "default" in `run_worker`; callers using custom `Queue`s must declare them. | Noted in `run_worker` docstring — callers must pass `queues=` when using non-default queues. |

## Not applicable / deferred

#86 psycopg2 cursors (we use psycopg3 only), #81 concurrency cap (nice-to-have), #78 LISTEN/NOTIFY close on stop (cosmetic), #77/#75/#73/#74/#70 (sync support, SA models, typing, `_ex` functions, schema config — not needed), #69 covered above, #66 Django (n/a), #65/#64/#60 API plugin (unused), #53 arg validation (we validate at enqueue), #38 migration helpers (n/a), #14/#13 progress/logging (n/a), #11 rate limiting (Chancy has `rate_limit`).

## Decision

- **Pin note:** stay on `0.26.x` for now; re-review and bump to `>=0.27.0`
  when the plugin-API fix ships (its breaking change requires updating
  our worker/dispatch adapters — not a drop-in).
- **Ops requirement:** document supervisor-managed workers as a
  deployment requirement (matches Chancy's intended model).
- No adapter workarounds needed today; no kernel fork.
