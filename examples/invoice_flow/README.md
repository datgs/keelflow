# invoice_flow — keelflow demo

Invoice lifecycle exercising **state machine + human-in-the-loop + DAG
workflow + workflow triggers** in one flow:

```
draft ──submit──► in_review ──approve──► posting ──on_done──► posted
   │                │                      │
   │                │ creates_job:         │ triggers post.wf@v1:
   │                │ human.review@v1      │  ledger → record → gl_check(human)
   │                │ (role:finance)       │
   │                │                      ├─ GL reject → posting_rejected
   │                └─ reject → needs_fix  │   → repost resumes run
   └── resubmit ◄───┘                      └─ tech fail → posting_failed
                                              + human.posting_fix@v1 (role:gl)
```

- **Business rejection vs technical failure**: a GL `reject` completes the
  step normally (`decision=reject`) → run terminal outcome `rejected` →
  `on_rejected` transition. A handler exception burns retries → outcome
  `failed` → `on_failed` transition + `failure_job` fix-up task.
- **Single TX everywhere**: handler domain writes (`demo_ledger_postings`),
  job finish, run advance, and the SM callback all commit atomically. A
  failure leaves zero partial writes.
- **Exactly-once callback**: `callback_state` guards the SM transition on
  run completion; `resume_run` reopens the offending step in place (same
  run id, successful steps stay done).

## Screens

| Route | Screen |
|---|---|
| `GET /` | Invoice list + create |
| `GET /invoices/{no}` | State, allowed actions, runs, audit trail, chaos toggle |
| `GET /inbox` | Inbox — claim → approve/reject/re-post |
| `GET /runs/{id}` | Run DAG — step nodes + statuses |
| `GET /jobs/{id}` | Job drawer — payload/result/ctx `via` chain, retry |

Actors: `u1` (clerk: submit/resubmit), `fin1` (finance: approve),
`gl1` (GL check + fix-ups), `mgr` (all). Switch via `?as=` in the header.

## Run

```bash
export KEELFLOW_DSN=postgresql://postgres:postgres@localhost:5544/keelflow
uv sync                                # workspace member — uses local keelflow
uv run python -m invoice_flow.seed     # demo schema + keelflow schema + invoices
uv run python -m invoice_flow.worker & # kernel worker (dispatch DAG steps)
uv run uvicorn invoice_flow.app:app --port 8501
```

→ http://localhost:8501 — demo only, no auth, manual refresh.

## Test

```bash
uv run pytest examples/invoice_flow
```
