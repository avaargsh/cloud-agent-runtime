# Durable external event inbox (Issue #43, slice 1)

The runtime can persist reference-only external events before any Temporal
Signal is sent, using `EventEnvelope` and `SQLiteEventInbox`.
Initialize `SQLiteStore` and save the canonical Session and Run first, then
open `SQLiteEventInbox` using the **same SQLite database path**.

```python
from cloud_agent_runtime import EventEnvelope, SQLiteEventInbox

inbox = SQLiteEventInbox("runtime.sqlite")
received = inbox.accept(EventEnvelope(
    event_id="provider-transaction-123",
    tenant_id="tenant-a",
    run_id="canonical-run-456",
    event_type="human.reviewed",
    correlation_id="task-789",
    causation_id="approval-request-10",
    payload_ref="evidence://sha256/decision",
))
```

`accept()` returns `True` on the first committed event and `False`
on an exact retry; reusing the same ID with changed content raises
`EventConflictError`. The inbox transaction verifies canonical Run ownership
through its Session tenant, rejects unknown/cross-tenant targets and new
events for terminal Runs, and stores immutable envelopes with ordered per-Run
retrieval. Large payloads and secrets belong outside the inbox; persist only
content references.

**Boundary:** This is the durable ingress contract, **not** an event
dispatcher, outbox, Temporal Signal bridge, automatic Workflow wake-up,
scheduled timer, compute handoff or production multi-process authorization
service. A later slice must deliver stored events with stable Signal IDs and
reconcile ambiguous signal acknowledgements; store persistence alone does
not prove delivery. SQLite is the local reference storage boundary, not
production multi-tenant database acceptance.
