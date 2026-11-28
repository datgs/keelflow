# Framework principles inventory

This page is an index for reviewing the principles already stated in the
repository. It does not create new product policy or override its cited
sources. If sources conflict, the conflict remains open until the repository
owner resolves it.

The language-agnostic contract is in `../spec/`, whose overview currently says
`draft v0.1`. Runtime behavior is established by the implementation and
behavior-level evidence; reliability and security limits are recorded in the
operational docs below. A shared interface does not imply equal backend
capabilities or guarantees.

| Principle | Current source | Scope and qualification |
|---|---|---|
| Transactional intent | [`spec/00-overview.md`](../spec/00-overview.md#core-invariants), [`spec/02-job-lifecycle.md`](../spec/02-job-lifecycle.md#enqueue-in-caller-tx) | Caller transaction owns durable intent; rollback must leave no phantom job/event. The spec overview identifies this as its most important property. |
| Caller-owned transaction handle | [`spec/00-overview.md`](../spec/00-overview.md#core-invariants) | APIs accepting a transaction/session use that handle; engine-owned connections are reserved for already-committed work. |
| One relational database for atomic UoW | [`spec/00-overview.md`](../spec/00-overview.md), [`docs/reliability.md`](reliability.md#what-is-not-guaranteed) | The UoW is one backend transaction. Cross-database atomicity is not promised. |
| Strict lifecycle state | [`spec/00-overview.md`](../spec/00-overview.md#core-invariants), [`spec/01-schema.md`](../spec/01-schema.md#status-enums-normative), [`spec/02-job-lifecycle.md`](../spec/02-job-lifecycle.md) | Engine states are controlled enums; business state belongs in payload/result. Backend claim and retry mechanics can differ. |
| Kernel boundary, not backend parity | [`spec/00-overview.md`](../spec/00-overview.md#core-invariants), [`docs/reliability.md`](reliability.md#backend-operation), [`docs/decisions/0001-kernel-chancy.md`](decisions/0001-kernel-chancy.md#status) | Backends implement the dispatch boundary, but Chancy's accepted pre-dispatch fencing and attempt-accounting gaps are not Native-equivalent guarantees. SQLite is local/proof-only. |
| Actor context is explicit | [`spec/00-overview.md`](../spec/00-overview.md#core-invariants), [`spec/06-context.md`](../spec/06-context.md) | Mutating work carries caller-resolved context; permissions are snapshotted at enqueue. `tenant_id` is not an isolation boundary. |
| At-least-once execution; transactional handler effects | [`docs/reliability.md`](reliability.md#guarantee-matrix), [`spec/03-workflow.md`](../spec/03-workflow.md#dispatch-transaction) | Handler database writes in the dispatch UoW roll back with the job outcome. Redelivery can rerun handlers; effects outside the UoW can repeat and require application idempotency/reconciliation. |
| Workflow intent and progress are separate from business truth | [`spec/03-workflow.md`](../spec/03-workflow.md#runtime-semantics), [`spec/02-job-lifecycle.md`](../spec/02-job-lifecycle.md#dispatch-worker-new-tx), [`docs/reliability.md`](reliability.md#what-is-not-guaranteed) | Workflow state and progress are engine records; progress is observational and does not prove committed business work. |
| Security and data policy are application-owned | [`docs/security.md`](security.md), [`spec/06-context.md`](../spec/06-context.md) | Keelflow is a library, not a control plane. The host supplies authentication, authorization, and tenant isolation; shared tables do not isolate tenants. |
| Unsupported capabilities must not be assumed | [`docs/reliability.md`](reliability.md#unsupported--experimental-surface), [`src/keelflow/engine/native_kernel.py`](../src/keelflow/engine/native_kernel.py) | Capability availability is backend-specific; documented unsupported behavior is not a shared guarantee. Requests that the public API accepts need a separate code-and-test audit to verify they fail clearly rather than being ignored. |

## Review flags

These are documentation/authority questions, not new rules:

- The spec overview is labeled draft even though the repository map calls the
  spec a contract. Decide whether the spec is an accepted normative authority
  before treating every `MUST` there as a release policy.
- `README.md` uses the phrase “Exactly-once effects” in its design principles,
  while `docs/reliability.md` scopes exactly-once database state change to the
  dispatch UoW and explicitly excludes external effects. Review that wording
  against the intended principle.
- The spec's kernel-agnostic statement must be read together with the accepted
  backend-specific reliability limits; the interface alone does not establish
  conformance or guarantee parity.
- Capability handling is not established by this inventory. Each requested
  capability must be traced through the public API, each backend, and tests
  before claiming unsupported requests fail explicitly.
