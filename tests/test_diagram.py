from __future__ import annotations

import re

from keelflow.engine.diagram import state_machine_svg, workflow_svg
from keelflow.sdk import (
    StateMachineDefinition,
    StepSpec,
    TransitionSpec,
    WorkflowDefinition,
)


def _boxes(svg: str) -> list[tuple[float, float, float, float]]:
    return [
        (float(x), float(y), float(w), float(h))
        for x, y, w, h in re.findall(
            r'<rect x="([\d.]+)" y="([\d.]+)" width="([\d.]+)" height="([\d.]+)"',
            svg,
        )
    ]


def test_workflow_svg_layers_parallel_steps_and_keeps_legend_clear():
    definition = WorkflowDefinition(
        name="test@v1",
        steps=(
            StepSpec("start", "machine.start@v1"),
            StepSpec("check_a", "machine.a@v1", needs=("start",)),
            StepSpec("check_b", "machine.b@v1", needs=("start",)),
            StepSpec("review", "human.review@v1", needs=("check_a", "check_b")),
        ),
    )
    svg = workflow_svg(
        definition,
        step_status={"start": "done", "check_a": "done", "check_b": "failed",
                     "review": "blocked"},
        job_type_meta={"human.review@v1": {"assignee": "finance"}},
    )
    boxes = _boxes(svg)
    node_boxes = [box for box in boxes if box[2] == 200]
    legend_boxes = [box for box in boxes if box[2] == 14]
    assert len(node_boxes) == 4
    assert legend_boxes
    legend_y = legend_boxes[0][1]

    # Same depth steps share a lane; no two boxes overlap.
    assert node_boxes[1][0] == node_boxes[2][0]
    for i, (x, y, w, h) in enumerate(node_boxes):
        assert y + h < legend_y
        for ox, oy, ow, oh in node_boxes[i + 1:]:
            assert x + w <= ox or ox + ow <= x or y + h <= oy or oy + oh <= y
    assert "who: finance" in svg
    assert ">done</text>" in svg and ">failed</text>" in svg
    assert "width=\"100%\"" in svg


def test_state_machine_svg_escapes_labels_and_marks_current_state():
    definition = StateMachineDefinition(
        name="test@v1",
        entity="test",
        states=("new", "review", "done"),
        initial="new",
        transitions=(
            TransitionSpec("new", "review", "submit"),
            TransitionSpec("review", "done", "approve"),
            TransitionSpec("review", "new", "back <fix>"),
        ),
    )
    svg = state_machine_svg(definition, current="review", allowed_actions={"approve"})
    assert 'aria-label="state machine"' in svg
    assert "back &lt;fix&gt;" in svg
    assert 'stroke-width="2.6"' in svg
    assert 'stroke-dasharray="4 4"' in svg
