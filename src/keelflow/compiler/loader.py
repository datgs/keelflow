"""YAML → WorkflowDefinition / StateMachineDefinition with validation.

Validation at load time (not at run):
- every `needs` references a declared step;
- every `type` is a registered job type in the registry;
- every `creates_job`/`triggers_workflow` references registered names;
- `needs` graph is acyclic;
- `if:` guards parse under the minimal expression grammar;
- SM transitions use declared states; actions are unique.

Definitions are versioned by their `name@vN` — the name in the file is
what callers reference.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from ..engine.workflow import validate_guard
from ..sdk import (
    StateMachineDefinition,
    StepSpec,
    TransitionSpec,
    WorkflowDefinition,
    WorkflowTriggerSpec,
)

_NAME = re.compile(r"^[\w.\-]+@v\d+$")


class DefinitionError(ValueError):
    """A definition file failed validation at load."""


def load_workflow(path: str | Path, registry) -> WorkflowDefinition:
    data = yaml.safe_load(Path(path).read_text())
    if not isinstance(data, dict):
        raise DefinitionError(f"{path}: expected a mapping")
    name = data.get("workflow") or data.get("name")
    _check_name(name, path)
    assert name is not None  # narrowed by _check_name
    steps_raw = data.get("steps")
    if not isinstance(steps_raw, list) or not steps_raw:
        raise DefinitionError(f"{name}: 'steps' must be a non-empty list")

    steps: list[StepSpec] = []
    seen: set[str] = set()
    for i, s in enumerate(steps_raw):
        if not isinstance(s, dict) or "id" not in s or "type" not in s:
            raise DefinitionError(f"{name}: step[{i}] needs 'id' and 'type'")
        sid = s["id"]
        if sid in seen:
            raise DefinitionError(f"{name}: duplicate step id {sid!r}")
        seen.add(sid)
        needs = tuple(s.get("needs") or ())
        for n in needs:
            if n not in seen:
                raise DefinitionError(
                    f"{name}: step {sid!r} needs unknown step {n!r}"
                )
        if_expr = s.get("if") or s.get("if_expr")
        if if_expr is not None:
            try:
                validate_guard(if_expr)
            except Exception as exc:
                raise DefinitionError(
                    f"{name}: step {sid!r} has invalid if: {exc}"
                ) from exc
        _check_known_type(s["type"], registry, f"{name} step {sid!r}")
        steps.append(
            StepSpec(
                id=sid,
                type=s["type"],
                input=dict(s.get("input") or {}),
                needs=needs,
                if_expr=if_expr,
            )
        )
    _check_acyclic(steps, name)
    defn = WorkflowDefinition(name=name, steps=tuple(steps))
    registry.register_workflow(defn)
    return defn


def load_state_machine(path: str | Path, registry) -> StateMachineDefinition:
    data = yaml.safe_load(Path(path).read_text())
    if not isinstance(data, dict):
        raise DefinitionError(f"{path}: expected a mapping")
    name = data.get("name") or data.get("state_machine")
    _check_name(name, path)
    assert name is not None
    entity = data.get("entity")
    if not entity:
        raise DefinitionError(f"{name}: missing 'entity'")
    states = tuple(data.get("states") or ())
    if not states:
        raise DefinitionError(f"{name}: 'states' must be a non-empty list")
    transitions = []
    actions: set[str] = set()
    for i, t in enumerate(data.get("transitions") or []):
        for field in ("from", "to", "action"):
            if field not in t:
                raise DefinitionError(
                    f"{name}: transition[{i}] missing {field!r}"
                )
        if t["from"] not in states or t["to"] not in states:
            raise DefinitionError(
                f"{name}: transition[{i}] uses undeclared state"
            )
        if t["action"] in actions:
            raise DefinitionError(
                f"{name}: duplicate action {t['action']!r}"
            )
        actions.add(t["action"])
        if t.get("creates_job"):
            _check_known_type(t["creates_job"], registry, f"{name} transition")
        tw = t.get("triggers_workflow")
        trigger = _parse_trigger(tw, name, i)
        if trigger and not _is_registered_workflow(trigger.workflow, registry):
            raise DefinitionError(
                f"{name}: triggers_workflow {trigger.workflow!r} not registered"
            )
        if trigger and trigger.failure_job:
            _check_known_type(trigger.failure_job, registry,
                              f"{name} transition trigger")
        transitions.append(
            TransitionSpec(
                from_state=t["from"],
                to_state=t["to"],
                action=t["action"],
                guard=dict(t.get("guard") or {}),
                creates_job=t.get("creates_job"),
                triggers_workflow=trigger,
                notify=t.get("notify"),
            )
        )
    initial = data.get("initial") or states[0]
    if initial not in states:
        raise DefinitionError(f"{name}: initial {initial!r} not in states")
    defn = StateMachineDefinition(
        name=name,
        entity=entity,
        states=states,
        transitions=tuple(transitions),
        initial=initial,
    )
    registry.register_state_machine(defn)
    return defn


def load_definitions(dir_: str | Path, registry) -> dict[str, int]:
    """Load every *.yaml under `workflows/` and `state_machines/` inside `dir_`.

    Returns counts per kind.
    """
    dir_ = Path(dir_)
    counts = {"workflows": 0, "state_machines": 0}
    for f in sorted((dir_ / "workflows").glob("*.yaml")):
        load_workflow(f, registry)
        counts["workflows"] += 1
    for f in sorted((dir_ / "state_machines").glob("*.yaml")):
        load_state_machine(f, registry)
        counts["state_machines"] += 1
    return counts


# ── helpers ────────────────────────────────────────────────────────────────


def _parse_trigger(tw: Any, name: str, i: int) -> WorkflowTriggerSpec | None:
    """`triggers_workflow` may be a string or a typed mapping."""
    if tw is None:
        return None
    if isinstance(tw, str):
        return WorkflowTriggerSpec(workflow=tw)
    if isinstance(tw, dict):
        unknown = set(tw) - {"workflow", "on_done", "on_rejected",
                             "on_failed", "failure_job"}
        if unknown:
            raise DefinitionError(
                f"{name}: transition[{i}] triggers_workflow unknown keys "
                f"{sorted(unknown)}"
            )
        if not tw.get("workflow"):
            raise DefinitionError(
                f"{name}: transition[{i}] triggers_workflow needs 'workflow'"
            )
        return WorkflowTriggerSpec(
            workflow=tw["workflow"],
            on_done=tw.get("on_done"),
            on_rejected=tw.get("on_rejected"),
            on_failed=tw.get("on_failed"),
            failure_job=tw.get("failure_job"),
        )
    raise DefinitionError(
        f"{name}: transition[{i}] triggers_workflow must be str or mapping"
    )


def _check_name(name: Any, path) -> None:
    if not name or not _NAME.match(str(name)):
        raise DefinitionError(f"{path}: name must be 'domain.action@vN', got {name!r}")


def _check_known_type(type_name: str, registry, where: str) -> None:
    try:
        registry.job_type(type_name)
    except KeyError as exc:
        raise DefinitionError(f"{where}: unknown job type {type_name!r}") from exc


def _is_registered_workflow(name: str, registry) -> bool:
    try:
        registry.workflow(name)
    except KeyError:
        return False
    return True


def _check_acyclic(steps: list[StepSpec], name: str) -> None:
    by_id = {s.id: s for s in steps}
    visiting: set[str] = set()
    done: set[str] = set()

    def visit(sid: str, trail: list[str]) -> None:
        if sid in done:
            return
        if sid in visiting:
            raise DefinitionError(
                f"{name}: cyclic dependency {' → '.join(trail + [sid])}"
            )
        visiting.add(sid)
        for n in by_id[sid].needs:
            visit(n, trail + [sid])
        visiting.discard(sid)
        done.add(sid)

    for s in steps:
        visit(s.id, [])
