# invoice_flow — keelflow demo

Invoice lifecycle demo exercising **state machine + human-in-the-loop +
DAG workflow** in one flow:

```
draft ──submit──► in_review ──approve──► posted
                    │                     │
                    └─ creates_job:       └─ triggers_workflow:
                       human.review@v1       post.wf@v1 (p1 → p2)
                       (role:finance)
```

## Screens

| Route | Screen |
|---|---|
| `GET /` | Invoice list — entity state + per-user allowed actions |
| `GET /invoices/{id}` | Detail — state, actions, audit timeline |
| `GET /inbox` | Reviewer inbox — claim / approve / reject |
| `GET /runs/{id}` | Workflow run — DAG steps live status |
| `GET /jobs/{id}` | Job drawer — ctx actor + `via` chain |

Switch the acting user with `?as=u1` (submitter) or `?as=fin1` (finance).

## Run

```bash
export KEELFLOW_DSN=postgresql://postgres:postgres@localhost:5544/keelflow
uv sync --dev                       # workspace member — pulls keelflow local
uv run python -m invoice_flow.seed  # schema + demo invoices + registry
uv run python -m invoice_flow.worker &   # kernel worker (dispatch DAG steps)
uv run uvicorn invoice_flow.app:app --port 8501
```

→ http://localhost:8501
