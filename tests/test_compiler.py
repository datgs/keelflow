"""T9 contract tests: YAML compiler load-time validation."""
from __future__ import annotations

import pytest
from pydantic import BaseModel

from pyerp.compiler import DefinitionError, load_state_machine, load_workflow
from pyerp.engine.registry import InMemoryRegistry
from pyerp.sdk import JobHandler, JobResult, JobTypeSpec


class P(BaseModel):
    pass


class H(JobHandler):
    async def run(self, ctx, payload):
        return JobResult(status="done")


@pytest.fixture
def registry():
    reg = InMemoryRegistry()
    for t in ("a.do@v1", "b.do@v1", "human.review@v1"):
        reg.register_job_type(
            JobTypeSpec(type=t, input_schema=P, handler=H, is_human=t.startswith("human."))
        )
    return reg


def test_loads_valid_workflow(tmp_path, registry):
    f = tmp_path / "wf.yaml"
    f.write_text(
        """
workflow: inv.process@v1
steps:
  - id: a
    type: a.do@v1
    input: {x: "${{ inputs.x }}"}
  - id: b
    type: b.do@v1
    needs: [a]
    if: "${{ steps.a.outputs.n }} > 0"
"""
    )
    defn = load_workflow(f, registry)
    assert defn.name == "inv.process@v1"
    assert defn.steps[1].needs == ("a",)
    assert defn.steps[1].if_expr == "${{ steps.a.outputs.n }} > 0"
    # registered
    assert registry.workflow("inv.process@v1") is defn


def test_rejects_unknown_needs(tmp_path, registry):
    f = tmp_path / "wf.yaml"
    f.write_text(
        """
workflow: bad.needs@v1
steps:
  - id: a
    type: a.do@v1
    needs: [ghost]
"""
    )
    with pytest.raises(DefinitionError, match="unknown step"):
        load_workflow(f, registry)


def test_rejects_unknown_job_type(tmp_path, registry):
    f = tmp_path / "wf.yaml"
    f.write_text(
        """
workflow: bad.type@v1
steps:
  - id: a
    type: ghost.do@v1
"""
    )
    with pytest.raises(DefinitionError, match="unknown job type"):
        load_workflow(f, registry)


def test_rejects_cycle(tmp_path, registry):
    f = tmp_path / "wf.yaml"
    f.write_text(
        """
workflow: cyclic@v1
steps:
  - id: a
    type: a.do@v1
    needs: [b]
  - id: b
    type: b.do@v1
    needs: [a]
"""
    )
    with pytest.raises(DefinitionError, match="needs unknown step"):
        load_workflow(f, registry)


def test_rejects_bad_guard(tmp_path, registry):
    f = tmp_path / "wf.yaml"
    f.write_text(
        """
workflow: bad.guard@v1
steps:
  - id: a
    type: a.do@v1
  - id: b
    type: b.do@v1
    needs: [a]
    if: "os.system('rm -rf /')"
"""
    )
    with pytest.raises(DefinitionError):
        load_workflow(f, registry)


def test_loads_state_machine(tmp_path, registry):
    f = tmp_path / "sm.yaml"
    f.write_text(
        """
name: invoice@v1
entity: invoice
states: [draft, in_review, approved, rejected]
transitions:
  - {from: draft, to: in_review, action: submit}
  - {from: in_review, to: approved, action: approve, creates_job: human.review@v1}
  - {from: in_review, to: rejected, action: reject}
"""
    )
    defn = load_state_machine(f, registry)
    assert defn.entity == "invoice"
    assert len(defn.transitions) == 3
    assert defn.transitions[1].creates_job == "human.review@v1"


def test_sm_rejects_unknown_state(tmp_path, registry):
    f = tmp_path / "sm.yaml"
    f.write_text(
        """
name: bad@v1
entity: x
states: [a, b]
transitions:
  - {from: a, to: ghost, action: go}
"""
    )
    with pytest.raises(DefinitionError, match="undeclared state"):
        load_state_machine(f, registry)
