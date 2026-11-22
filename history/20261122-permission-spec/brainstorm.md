---
status: proposed
last_reviewed: 2026-11-22
superseded_by: []
related_code:
  - src/keelflow/engine/permissions.py
  - src/keelflow/sdk/services.py
  - src/keelflow/sdk/context.py
  - src/keelflow/sdk/actor.py
---

# Brainstorm: keelpolicy — Portable Policy Specification

## Decision

- **Recommendation:** Define `keelpolicy` as a separate, implementation-neutral policy specification and later build a library/SDK that declares and evaluates authorization policies, with YAML as the application's single source of truth. The model is IAM-inspired, not AWS IAM-compatible unless explicitly decided later.
- **Decision owner:** User.
- **Confidence:** Medium — cross-system reuse, name, YAML policy-as-code, and responsibility boundaries are clear; evaluation edge cases and distribution form remain open.

## Problem and Outcome

The user is building Keelflow and an ERP Framework and wants to explore a reusable policy SDK that can be used by different systems. The SDK concept under discussion is deliberately narrow: YAML policy input plus a processor that validates/evaluates it against caller-supplied request data. It has no database, identity store, login, sessions, role assignment, or relationship persistence. Applications/frameworks supply trusted actor/context and integrate the result into their own operations.

Whether the SDK should support policy-rule and relationship-based evaluation in one library remains an architectural question to investigate, not a decision.

## Constraints and Non-Goals

- Current exploration: `keelpolicy` may own policy schema/semantics, validation, and evaluation — not login, user storage, sessions, role assignment, or identity lifecycle.
- Application would own YAML policy files and provide trusted actor/role/context. No decision yet on policy composition or defaults.
- IAM is a reference point only. The sample policy uses `effect`, roles, resource, actions, record conditions, and field read/write rules; it does not establish AWS IAM schema or compatibility.
- Whether to use declarative bounded conditions, and whether relationship-based checks belong in the same SDK, remain open design questions.
- The SDK may return an authorization decision plus record/field constraints; integrating frameworks would enforce them. Exact result shape is undecided.
- KeelModel would translate record rules into query/write constraints. Field-level authorization, if supported, must be enforced in data access/write rather than only masking after retrieval.
- Protected/unprotected access paths (e.g. raw SQL or direct SQLAlchemy access) need explicit documentation; SDK enforcement does not automatically protect bypass paths.
- No claim of universal fit and no wholesale copy of AWS IAM features.

## Evidence and Exploration

- `src/keelflow/engine/permissions.py:28-65`: current default implementation checks `ctx.permissions`, supports wildcard and `all`/`own`, and resolves job actors through their `via` chain. It is a Keelflow convention, not a portable policy engine.
- `src/keelflow/sdk/services.py:203-207`: `PermissionService` accepts `JobContext` and returns Keelflow's `ScopeFilter`, so the existing seam is SDK-coupled.
- `src/keelflow/sdk/context.py:87-92`: `JobContext.require()` contains a separate, simpler permission check; the authorization path is not unified.
- Cedar illustrates a policy evaluator with principal/action/resource/context requests, schema validation, allow/deny decisions, and diagnostics. OPA illustrates a general policy engine that accepts caller-supplied structured input and can return structured output. OpenFGA illustrates a relationship model evaluated against relationship tuples. These are different design points; research did not establish that one is universally best. Sources: https://docs.cedarpolicy.com/auth/authorization.html, https://docs.cedarpolicy.com/policies/validation.html, https://www.openpolicyagent.org/docs, https://openfga.dev/docs/concepts, https://openfga.dev/docs/learn/policy-engine.
- AWS IAM is a policy-language reference (effect/action/resource/condition and explicit versus implicit deny), but its many policy types and cross-policy evaluation rules exceed the discussed scope. `keelpolicy` is not decided to be AWS-compatible. Sources: https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html, https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic_policy-eval-denyallow.html, and https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_evaluation-logic_AccessPolicyLanguage_Interplay.html.

### Illustrative KeelModel Scenario (not a schema decision)

The user proposed `sales.Invoice` with `created_by`, `status`, `total`, `customer_name`, and `internal_note`, with the Application passing authenticated `actor_id` and roles into KeelModel. The sample YAML explored:

- `sales_clerk` may read own invoices (`created_by == actor_id`) and read a selected field set, excluding `internal_note`.
- `sales_clerk` may update own invoices but only `customer_name`; `status`, `total`, and `internal_note` are denied.
- `sales_manager` may read all invoices and manage all invoices.

This scenario suggests a possible evaluator result containing a decision plus record and field constraints. KeelModel, not the SDK, would need to apply record constraints in query/write and field constraints during read/write. It remains unresolved whether a forbidden read field is omitted or the whole operation is rejected, and whether a write containing any forbidden field rejects the whole mutation or drops those fields.

## Meaningful Alternatives

### Keep permission conventions in each framework

- **When it is better:** There is no cross-system reuse goal, or workflow and ERP semantics cannot share a stable core.
- **Why not now:** The user wants a common policy design reference for Keelflow and ERP.

### Adopt AWS IAM schema and semantics

- **When it is better:** AWS policy interoperability is a requirement.
- **Why not now:** No such compatibility requirement was stated. IAM's policy types and evaluation interactions would widen scope and create compatibility obligations.

## Trade-offs and Risks

| Item | Impact | Response |
|---|---|---|
| Portable core may be too abstract | medium | Keep a small request vocabulary and show separate Workflow and ERP profiles |
| Allow/deny evaluation ambiguity | high | Decide default deny, explicit deny precedence, and policy combination before SDK implementation |
| Record rules may be returned but not enforced | high | KeelModel must apply them to query/write paths; document bypass routes and trust boundaries |
| Field permissions may become post-query masking only | high | Specify enforcement for reads and writes at the data access boundary; field behavior remains a decision |
| Arbitrary conditions/functions create unsafe or inconsistent evaluation | high | V1 uses a bounded declarative condition set; no custom code execution |
| YAML policy drift across implementations | medium | Add language-neutral test vectors and conformance cases before treating SDKs as equivalent |
| Keelflow's JobContext permission snapshot is framework-specific | medium | Define a Keelflow adapter; do not make snapshot-vs-live policy behavior a universal rule |

## Assumptions and Open Questions

No design choice below is confirmed. The discussion is exploratory and the examples are not approved contracts.

- **Open:** Is the desired artifact initially a specification, an SDK package, or both? Should it start in a shared repository or a separate repository/package?
- **Open:** Should one SDK support both policy-rule evaluation (Cedar/OPA-like) and relationship evaluation (OpenFGA/Zanzibar-like), or should those be separate modules with a shared request/result vocabulary?
- **Open:** How are relationship facts represented and supplied if the SDK has no database or persistence?
- **Open:** Should policies use `Allow`/`Deny`, and what are default-deny, explicit-deny precedence, and multi-policy combination rules?
- **Open:** Is role resolution always performed by the Application? Is role inheritance supported or excluded?
- **Open:** For field access, should a forbidden read field be omitted or reject the whole operation? For writes, should any forbidden field reject the mutation or be ignored? Read and write behavior may differ.
- **Open:** Exact YAML schema, resource identifiers/wildcards, condition operators, missing-attribute and evaluation-error behavior, and explanation/constraint result shape.
- **Open:** Which access paths are protected and explicitly excluded (raw SQL/direct ORM)?
- **Open:** Which are the first real consumers? KeelModel appears as an intended consumer, while workflow is another potential consumer; their requirements need comparison.
- **Falsifying signal:** If consumers cannot share a request/result core without hiding materially different evaluation semantics, use a shared base vocabulary with distinct modules/profiles rather than one blended evaluator.

## Security Considerations

Authorization is security-sensitive. The spec must distinguish authentication from authorization; fail closed on missing/unknown attributes and evaluator errors; prevent untrusted policy/context injection; and state that a decision has no protective effect unless all relevant enforcement points apply it. Raw SQL/ORM bypasses must be documented. Require focused security review before a production SDK is claimed to enforce policy.

## Revisit Trigger

Revisit the core when KeelModel access patterns are concrete enough to test record and field enforcement, or when a second consumer reveals incompatible evaluation assumptions. Decide package/repository split when there is a real independent consumer or release need.

## Next Step

Continue discovery by mapping concrete authorization cases from Keelflow and KeelModel against candidate SDK shapes (policy evaluator, relationship evaluator, or both). Then present alternatives and trade-offs for the user's direction. Do not write a technical design or treat any proposed semantics/schema as decided until the user explicitly chooses them.
