"""Mermaid diagram generators for workflow DAGs and state machines.

Pure text — no engine dependency beyond the spec dataclasses. Apps embed
`MERMAID_SNIPPET` once per page and render `..._mermaid()` output inside
`<pre class="mermaid">…</pre>`.

Live overlay: pass per-step/entity status dicts and the generator styles
nodes accordingly (done/running/blocked/failed/skipped + who-acts-next).
"""
from __future__ import annotations

from typing import Any

from ..sdk.specs import StateMachineDefinition, WorkflowDefinition

# CDN include — demos and simple UIs embed this once. Apps that need
# offline rendering can host mermaid.js themselves and skip the script tag.
MERMAID_SNIPPET = (
    '<script type="module">'
    'import mermaid from'
    ' "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";'
    'mermaid.initialize({startOnLoad:true, theme:"neutral"});'
    '</script>'
)

_WF_STYLE = {
    "done": ":::kfDone",
    "skipped": ":::kfSkipped",
    "failed": ":::kfFailed",
    "running": ":::kfActive",
    "claimed": ":::kfActive",
    "pending": ":::kfActive",
    "blocked": ":::kfBlocked",
}
_WF_CLASSDEF = (
    "classDef kfDone fill:#1d3a24,color:#b8e6c2;\n"
    "classDef kfActive fill:#1c3252,color:#bcd6f5;\n"
    "classDef kfBlocked fill:#3a321a,color:#f0dcae;\n"
    "classDef kfFailed fill:#3a1c1c,color:#f5bcbc;\n"
    "classDef kfSkipped fill:#262626,color:#8a8a8a,stroke-dasharray:3 3;\n"
)


def workflow_mermaid(
    defn: WorkflowDefinition,
    *,
    step_status: dict[str, str] | None = None,
    job_type_meta: dict[str, dict[str, Any]] | None = None,
) -> str:
    """Mermaid flowchart of a workflow DAG.

    `step_status` — `{step_id: status}` from `get_run` for live overlay.
    `job_type_meta` — optional `{type: {"assignee": "role:finance"}}` to
    annotate human steps with who handles them.
    """
    lines = ["flowchart TD", "  %% " + defn.name]
    step_status = step_status or {}
    job_type_meta = job_type_meta or {}

    for s in defn.steps:
        human = s.type.startswith("human.")
        label = s.id
        if human:
            meta = job_type_meta.get(s.type, {})
            who = meta.get("assignee") or "anyone"
            label = f"{s.id}<br/>👤 {who}"
        node = f'  {s.id}["{label}"]'
        if human:
            node = f'  {s.id}{{{{"{label}"}}}}'  # rounded = human gate
        status = step_status.get(s.id)
        if status in _WF_STYLE:
            node += _WF_STYLE[status]
        lines.append(node)
        if s.if_expr:
            lines.append(
                f'  {s.id}_if["if: {_short_if(s.if_expr)}"]:::kfSkipped')
    edges = []
    for s in defn.steps:
        for n in s.needs:
            edges.append(f"  {n} --> {s.id}")
        if s.if_expr:
            edges.append(f'  {s.id}_if --> {s.id}')
    lines.extend(sorted(set(edges)) or ["  _none[\"(no edges)\"]"])
    lines.append(_WF_CLASSDEF)
    return "\n".join(lines)


def state_machine_mermaid(
    defn: StateMachineDefinition,
    *,
    current: str | None = None,
    allowed_actions: set[str] | None = None,
) -> str:
    """Mermaid stateDiagram-v2 of an entity state machine.

    `current` highlights the live state; `allowed_actions` dims the
    transitions the actor may not take (shown but de-emphasized).
    """
    lines = ["stateDiagram-v2", "  %% " + defn.name]
    lines.append(f"  [*] --> {defn.initial}")
    for t in defn.transitions:
        label = t.action
        extra = []
        if t.guard.get("permission"):
            extra.append(f"perm:{t.guard['permission']}")
        if t.creates_job:
            who = t.guard.get("assignee_role", "anyone")
            extra.append(f"task→{who}")
        if t.triggers_workflow:
            wf = (t.triggers_workflow.workflow
                  if hasattr(t.triggers_workflow, "workflow")
                  else str(t.triggers_workflow))
            extra.append(f"wf:{wf}")
        if extra:
            label += " [" + ", ".join(extra) + "]"
        style = ""
        if allowed_actions is not None and t.action not in allowed_actions:
            style = "  %% dimmed (not allowed for actor)"
        lines.append(f"  {t.from_state} --> {t.to_state} : {label}{style}")
    if current:
        lines.append(f"  classDef kfCur fill:#3a321a,stroke:#f0dcae,"
                     "stroke-width:3px;")
        lines.append(f"  class {current} kfCur")
    return "\n".join(lines)


def _short_if(expr: str) -> str:
    """Compact an `if:` guard for a node label."""
    return (expr.replace("${{", "").replace("}}", "")
            .replace("steps.", "").replace("outputs.", ""))[:48]
