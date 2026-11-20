# Demo 4 — Employee helpdesk (RAG)

A deliberately "unreliable LLM" wrapped in durable keelflow workflows.
Domain: an **employee helpdesk** answering questions over 16 synthetic
knowledge-base articles (password reset, annual leave, expenses, VPN…) —
chosen so a reader can tell a correct answer from a wrong one without
domain knowledge.

## Workflows

Two independent pipelines:

```mermaid
flowchart TD
    subgraph rag.index@v1
        split --> embed[[embed · bulk]] --> mark_docs
    end
```

`split` chunks each pending doc (~60 words) → `embed` computes a
deterministic hash embedding via **bulk chunked processing**
(`atomic='row'`, so a failing chunk row lands in the error queue without
blocking siblings) → `mark_docs` flips docs to `indexed`.

```mermaid
flowchart TD
    subgraph rag.answer@v1
        retrieve --> generate --> score
        score -->|confidence >= 0.55| publish
        score -->|confidence < 0.55| review[[review · human]]
        review --> review_done
    end
```

`retrieve` finds the top-3 chunks by cosine over the hash embeddings →
`generate` calls the fake LLM (`timeout_seconds=10`,
`RetryPolicy(4, backoff base_seconds=2)`) → `score` gates on confidence →
either auto-`publish`, or `human.review` (assignee `role:reviewer`,
`due_in_seconds=300`) → `review_done` records the approved answer.

**No reinforcement loop.** Human review is an approval gate, not a
training loop: completing it updates only that question's row — it does
not touch embeddings, model weights, prompts, or the knowledge base.
Retrieval is deterministic bag-of-words hashing + cosine in Python
("toy ANN") — **no pgvector, no API keys, no real model**.

## Chaos knobs (the demo story)

Toggle on the home page to simulate an unreliable model/vendor:

| knob | what it does | what you see |
|---|---|---|
| `flaky_llm` | generate 503s ~half the time | engine-scheduled retry (`run_at +2s`) until success |
| `slow_llm` | generate sleeps 30s | `timeout_seconds=10` kills it → retries → **DLQ** |
| `flaky_embed` | ~1 in 4 embed rows 429s | row-level error queue inside the bulk pass |
| `weak_retrieval` | zero retrieved chunks | low confidence → **human review gate** |

Fix the root cause (turn the knob off), then **replay** the DLQ entry
from `/dlq` — `MonitoringService.dlq_replay` re-enqueues the payload as
a fresh job.

## Run

```bash
export KEELFLOW_DSN=postgresql://postgres:postgres@localhost:5544/keelflow

# seed 16 synthetic helpdesk docs
uv run --package keelflow-demo-rag python -m rag_demo.seed

# worker (machine steps; human.review is UI-side)
uv run --package keelflow-demo-rag python -m rag_demo.worker

# UI — http://127.0.0.1:8003/?as=u-ops
uv run --package keelflow-demo-rag uvicorn rag_demo.app:app --port 8003
```

**Reset the fixture** (clears demo-owned docs/chunks/questions/settings —
use after switching demo content; plain `seed` appends and keeps history):

```bash
uv run --package keelflow-demo-rag python -m rag_demo.seed --reset
```

## Actors (`?as=`)

| id | role | can | workflow step |
|---|---|---|---|
| `u-ask` | requester | ask questions | — |
| `u-ops` | requester + reviewer | ask, index, review, dlq replay | `review` |
| `u-mgr` | all | everything | any human step |

## The 60-second demo

1. `/docs` → **index pending docs** → watch `rag.index` split → bulk
   embed chunks → docs flip to `indexed`.
2. `/` → ask *"How do I reset my password?"* → retrieve hits the
   `Password reset` article → answer auto-publishes (confidence ≥ 0.55).
3. Turn on **flaky_llm**, ask again → generate job shows attempts
   climbing with `run_at` pushed forward (engine backoff) → done.
4. Turn on **slow_llm**, ask again → timeout kills the handler after
   10s → retries exhaust → job lands in `/dlq` → turn the knob off →
   **replay**.
5. Turn on **weak_retrieval**, ask anything → confidence 0.2 →
   `human.review` task appears in `/inbox` (role:reviewer, due_at set)
   → claim + approve (optionally editing the draft) → `review_done`
   marks the question `reviewed` with your name.

Everything is deterministic and local — no API keys, no vector DB, no
real model. Synthetic docs only.
