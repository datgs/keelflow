# keelflow spec — workflow DAG

## Definition

```yaml
workflow: name@vN
steps:
  - id: a
    type: type@vN
    needs: [x, y]          # optional
    if: "steps.x.outputs.flag == true"   # optional guard
    input: {k: "${{ steps.x.outputs.v }}"}   # optional refs
    retry: {max_attempts: 3}
```

Load-time validation MUST reject: unknown `needs`, unknown types,
dependency cycles, and guards outside the grammar below.

## Guard grammar (whitelisted, no eval)

`${{ }}` refs · `== != > < >= <= in` · `and or not` · literals only.
Anything else → `DefinitionError` at load.

## Runtime semantics

- `start(name, inputs, ctx, tx)` creates run + all step_runs `pending`,
  in `tx`.
- `advance_run` (called on each step completion): for every `pending`
  step whose `needs` are all `done` and whose `if:` is true, enqueue its
  job — for human types, insert job row only and mark step `blocked`.
- A step whose `if:` is false → `done` with skipped marker.
- Run flips `done` when no step is `pending|claimed|running|blocked`;
  `failed` if any step `failed` (others may finish).
- Child job context: `actor = job:<job_id> via <parent ctx actor>`;
  `correlation_id` inherited.
- Resume-from-failed-step: re-running the failed step job must not
  re-enqueue already-done siblings (idempotent via step status checks).
