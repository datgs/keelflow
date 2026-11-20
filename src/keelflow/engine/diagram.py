"""Diagram generators for workflow DAGs and state machines.

Two renderers, both pure text — no engine dependency beyond the spec
dataclasses:

- `workflow_svg` / `state_machine_svg` — hand-laid-out inline SVG
  (status-colored nodes, high-contrast edges, bezier connectors). Prefer for
  polished UIs: full visual control, no client JS.
- `workflow_mermaid` / `state_machine_mermaid` — Mermaid text kept for
  quick/CLI use; embed `MERMAID_SNIPPET` once per page to render.

Live overlay: pass per-step/entity status dicts and the renderers style
nodes accordingly (done/running/blocked/failed/skipped + who-acts-next).
"""
from __future__ import annotations

from typing import Any
from xml.sax.saxutils import escape

from ..sdk.specs import StateMachineDefinition, WorkflowDefinition

# ── status palette for inline SVG on light demo pages ────────────────────────
_FILL = {
    "done": ("#1d3a24", "#b8e6c2"),
    "skipped": ("#262626", "#8a8a8a"),
    "failed": ("#3a1c1c", "#f5bcbc"),
    "running": ("#1c3252", "#bcd6f5"),
    "claimed": ("#1c3252", "#bcd6f5"),
    "pending": ("#1c3252", "#bcd6f5"),
    "blocked": ("#3a321a", "#f0dcae"),
    "default": ("#2a3040", "#c8d0e0"),
}
_EDGE = "#7c8698"
_ARROW = "#475569"


def _fill(status: str | None) -> tuple[str, str]:
    return _FILL.get(status or "default", _FILL["default"])


def _esc(s: Any) -> str:
    return escape(str(s))


# ── layout helpers ───────────────────────────────────────────────────────────

def _layers(steps) -> list[list[str]]:
    """Kahn layering: step is in layer k when all `needs` are in < k."""
    depth: dict[str, int] = {}
    remaining = {s.id: set(s.needs) for s in steps}
    while remaining:
        ready = [sid for sid, needs in remaining.items()
                 if all(n in depth for n in needs)]
        if not ready:                      # cycle → dump into last layer
            for sid in remaining:
                depth[sid] = max(depth.values(), default=0) + 1
            break
        for sid in ready:
            needs = remaining.pop(sid)
            depth[sid] = (max((depth[n] for n in needs), default=-1) + 1)
    out: dict[int, list[str]] = {}
    for sid, d in depth.items():
        out.setdefault(d, []).append(sid)
    return [out[k] for k in sorted(out)]


def workflow_svg(
    defn: WorkflowDefinition,
    *,
    step_status: dict[str, str] | None = None,
    job_type_meta: dict[str, dict[str, Any]] | None = None,
) -> str:
    """Inline SVG of a workflow DAG, layered left→right.

    Human steps render as dashed-border pills with a `who: role` label;
    `if:`-guarded steps show the guard beneath the id. Edges are bezier
    curves with arrowheads. `step_status` colors each node live.
    """
    step_status = step_status or {}
    job_type_meta = job_type_meta or {}
    layers = _layers(defn.steps)

    NW, NH = 200, 64          # node box
    HGAP, VGAP = 90, 34       # gaps
    ML, MT = 30, 26
    LEGEND = 44               # legend strip height
    width = ML * 2 + len(layers) * NW + (len(layers) - 1) * HGAP
    max_rows = max(len(l) for l in layers)
    content_h = max_rows * NH + (max_rows - 1) * VGAP
    height = MT * 2 + content_h + LEGEND

    pos: dict[str, tuple[float, float]] = {}
    for li, layer in enumerate(layers):
        total_h = len(layer) * NH + (len(layer) - 1) * VGAP
        y0 = MT + (content_h - total_h) / 2
        for ri, sid in enumerate(layer):
            pos[sid] = (ML + li * (NW + HGAP), y0 + ri * (NH + VGAP))

    out = [(f'<svg viewBox="0 0 {width} {height}" width="100%" '
            'height="auto" xmlns="http://www.w3.org/2000/svg" '
            'role="img" aria-label="workflow DAG" '
            'preserveAspectRatio="xMinYMin meet" '
            'style="display:block;margin:8px 0;min-width:900px">'),
           ('<defs><marker id="kfArr" viewBox="0 0 10 10" refX="9" refY="5"'
            ' markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
            f'<path d="M0,0 L10,5 L0,10 z" fill="{_ARROW}"/></marker></defs>')]

    for s in defn.steps:                       # edges first (under nodes)
        for n in s.needs:
            if n not in pos:
                continue
            x1, y1 = pos[n][0] + NW, pos[n][1] + NH / 2
            x2, y2 = pos[s.id][0], pos[s.id][1] + NH / 2
            mx = (x1 + x2) / 2
            out.append(
                f'<path d="M{x1},{y1} C{mx},{y1} {mx},{y2} {x2},{y2}" '
                f'fill="none" stroke="{_EDGE}" stroke-width="1.6" '
                'marker-end="url(#kfArr)"/>')

    for s in defn.steps:
        x, y = pos[s.id]
        status = step_status.get(s.id)
        fill, txt = _fill(status)
        human = s.type.startswith("human.")
        stroke = "#f0dcae" if human else txt
        dash = ' stroke-dasharray="5 4"' if human else ""
        out.append(
            f'<g><rect x="{x}" y="{y}" width="{NW}" height="{NH}" rx="12"'
            f' fill="{fill}" stroke="{stroke}" stroke-width="1.4"{dash}/>')
        out.append(
            f'<text x="{x + NW/2}" y="{y + 22}" text-anchor="middle"'
            f' font-family="ui-monospace,monospace" font-size="13"'
            f' font-weight="600" fill="{txt}">{_esc(s.id)}</text>')
        sub = s.type
        if s.if_expr:
            sub = "if " + _short_if(s.if_expr)
        if human:
            who = (job_type_meta.get(s.type) or {}).get("assignee") or "anyone"
            sub = f"who: {who}"
        out.append(
            f'<text x="{x + NW/2}" y="{y + 44}" text-anchor="middle"'
            f' font-family="ui-monospace,monospace" font-size="10"'
            f' fill="{txt}">{_esc(sub)}</text></g>')
    out.append(_legend(step_status, height - LEGEND + 26, ML))
    out.append("</svg>")
    return "".join(out)


def state_machine_svg(
    defn: StateMachineDefinition,
    *,
    current: str | None = None,
    allowed_actions: set[str] | None = None,
) -> str:
    """Inline SVG of a state machine, main path left→right.

    States are layered by shortest distance from `initial`; same-depth
    states stack vertically. Transitions are labeled arcs — forward
    edges above, back edges below. `current` is outlined; actions not
    in `allowed_actions` render dimmed.
    """
    # depth = shortest hops from initial (BFS, ignoring repeats)
    depth = {defn.initial: 0}
    frontier = [defn.initial]
    while frontier:
        nxt = []
        for st in frontier:
            for t in defn.transitions:
                if t.from_state == st and t.to_state not in depth:
                    depth[t.to_state] = depth[st] + 1
                    nxt.append(t.to_state)
        frontier = nxt
    for st in defn.states:
        depth.setdefault(st, max(depth.values(), default=0) + 1)

    NW, NH = 150, 46
    HGAP, VGAP = 60, 70
    ML, MT = 30, 40
    cols: dict[int, list[str]] = {}
    for st, d in depth.items():
        cols.setdefault(d, []).append(st)
    max_rows = max(len(c) for c in cols.values())
    width = ML * 2 + len(cols) * NW + (len(cols) - 1) * HGAP
    height = MT * 2 + max_rows * NH + (max_rows - 1) * VGAP

    pos: dict[str, tuple[float, float]] = {}
    for d, sts in sorted(cols.items()):
        total_h = len(sts) * NH + (len(sts) - 1) * VGAP
        y0 = MT + (height - MT * 2 - total_h) / 2
        for i, st in enumerate(sts):
            pos[st] = (ML + d * (NW + HGAP), y0 + i * (NH + VGAP))

    out = [(f'<svg viewBox="0 0 {width} {height}" width="100%" '
            'height="auto" xmlns="http://www.w3.org/2000/svg" '
            'role="img" aria-label="state machine" '
            'preserveAspectRatio="xMinYMin meet" '
            'style="display:block;margin:8px 0;min-width:900px">'),
           ('<defs><marker id="kfArrS" viewBox="0 0 10 10" refX="9" refY="5"'
            ' markerWidth="6" markerHeight="6" orient="auto-start-reverse">'
            f'<path d="M0,0 L10,5 L0,10 z" fill="{_ARROW}"/></marker></defs>')]

    for t in defn.transitions:
        if t.from_state not in pos or t.to_state not in pos:
            continue
        x1, y1 = pos[t.from_state]
        x2, y2 = pos[t.to_state]
        cx1, cy1 = x1 + NW, y1 + NH / 2
        cx2, cy2 = x2, y2 + NH / 2
        allowed = allowed_actions is None or t.action in allowed_actions
        color = _ARROW if allowed else "#64748b"
        dash = "" if allowed else ' stroke-dasharray="4 4"'
        if depth[t.to_state] > depth[t.from_state]:   # forward: top edge
            my = min(y1, y2) - 14
            lx, ly = (x1 + NW + x2) / 2, my - 8
            path = (f"M{cx1},{cy1 - 6} C{cx1 + 30},{my} {cx2 - 30},{my} "
                    f"{cx2},{cy2 - 6}")
        else:                                          # back/side: below
            my = max(y1, y2) + NH + 22
            lx, ly = (x1 + NW + x2) / 2, my + 12
            path = (f"M{cx1},{cy1 + 6} C{cx1 + 30},{my} {cx2 - 30},{my} "
                    f"{cx2},{cy2 + 6}")
        out.append(
            f'<path d="{path}" fill="none" stroke="{color}"'
            f' stroke-width="1.5"{dash} marker-end="url(#kfArrS)"/>')
        lbl = t.action
        if t.guard.get("permission"):
            lbl += f" ({t.guard['permission']})"
        out.append(
            f'<text x="{lx}" y="{ly}" text-anchor="middle"'
            f' font-family="ui-monospace,monospace" font-size="10"'
            f' fill="{color}">{_esc(lbl)}</text>')

    for st, (x, y) in pos.items():
        is_cur = st == current
        fill, txt = _fill("blocked" if is_cur else None)
        w = 2.6 if is_cur else 1.4
        out.append(
            f'<g><rect x="{x}" y="{y}" width="{NW}" height="{NH}" rx="12"'
            f' fill="{fill}" stroke="{txt}" stroke-width="{w}"/>')
        out.append(
            f'<text x="{x + NW/2}" y="{y + NH/2 + 4}" text-anchor="middle"'
            f' font-family="ui-monospace,monospace" font-size="12"'
            f' font-weight="600" fill="#e6e9f0">{_esc(st)}</text></g>')
    # entry arrow
    ix, iy = pos[defn.initial]
    out.append(
        f'<path d="M{ix - 20},{iy + NH/2} L{ix - 2},{iy + NH/2}"'
        f' stroke="{_ARROW}" stroke-width="1.6" marker-end="url(#kfArrS)"/>')
    out.append("</svg>")
    return "".join(out)


def _legend(step_status: dict[str, str], y: float, x0: float) -> str:
    """One-line color key for the statuses actually present."""
    present = []
    for st in ("done", "blocked", "running", "failed", "skipped", "pending"):
        if st in step_status.values() and st not in present:
            present.append(st)
    parts = []
    x = x0
    for st in present:
        fill, txt = _fill(st)
        parts.append(
            f'<rect x="{x}" y="{y - 11}" width="14" height="14" rx="4"'
            f' fill="{fill}" stroke="{txt}" stroke-width="1"/>'
            f'<text x="{x + 20}" y="{y}" font-family="ui-monospace,monospace"'
            f' font-size="11" fill="#55606e">{st}</text>')
        x += 20 + len(st) * 7 + 24
    return "".join(parts)


# ── mermaid (kept for quick / CLI use) ───────────────────────────────────────

MERMAID_SNIPPET = (
    '<script type="module">'
    'import mermaid from'
    ' "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";'
    'mermaid.initialize({startOnLoad:true, theme:"dark"});'
    '</script>'
)

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
    """Mermaid flowchart of a workflow DAG (see module docstring)."""
    lines = ["flowchart TD", "  %% " + defn.name]
    step_status = step_status or {}
    job_type_meta = job_type_meta or {}
    style = {"done": ":::kfDone", "skipped": ":::kfSkipped",
             "failed": ":::kfFailed", "running": ":::kfActive",
             "claimed": ":::kfActive", "pending": ":::kfActive",
             "blocked": ":::kfBlocked"}
    for s in defn.steps:
        human = s.type.startswith("human.")
        label = s.id
        if human:
            who = (job_type_meta.get(s.type) or {}).get("assignee") or "anyone"
            label = f"{s.id}<br/>who: {who}"
        node = (f'  {s.id}{{{{"{label}"}}}}' if human
                else f'  {s.id}["{label}"]')
        st = step_status.get(s.id)
        if st in style:
            node += style[st]
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
    """Mermaid stateDiagram-v2 of an entity state machine."""
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
        lines.append("  classDef kfCur fill:#3a321a,stroke:#f0dcae,"
                     "stroke-width:3px;")
        lines.append(f"  class {current} kfCur")
    return "\n".join(lines)


def _short_if(expr: str) -> str:
    """Compact an `if:` guard for a node label."""
    return (expr.replace("${{", "").replace("}}", "")
            .replace("steps.", "").replace("outputs.", ""))[:48]
