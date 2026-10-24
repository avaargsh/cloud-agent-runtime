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

## Slice 2: explicit Temporal Signal delivery and reconciliation

`TemporalEventDispatcher` is an **opt-in, caller-driven** adapter. It
does not run a process, continuously poll, schedule retries or authorize an
external user. After creating the canonical Session and Run, persist the Run's
`workflow_ref` from a successfully verified Temporal start before calling it:

```python
from cloud_agent_runtime import (
    SQLiteEventInbox,
    TemporalEventDispatcher,
    TemporalWorkflowDriver,
)

inbox = SQLiteEventInbox("runtime.sqlite")
driver = await TemporalWorkflowDriver.connect(
    "localhost:7233", task_queue="agent-runs"
)
dispatcher = TemporalEventDispatcher(inbox=inbox, driver=driver)
confirmed = await dispatcher.deliver_pending(
    run_id="canonical-run-456", tenant_id="tenant-a"
)
```

For each unacknowledged event, the dispatcher requires a **persisted, nonempty**
Temporal Workflow ID **and execution Run ID**, not caller-supplied IDs. An older
Run without a pinned provider execution ID must be reconciled through a trusted
provider observation step first; dispatch fails closed rather than signalling an
unscoped workflow handle. The dispatcher verifies
that the server-reported canonical Run and Session match, and then checks for
the exact immutable event payload in `run_state.external_events` **before**
attempting another Signal. New events are signalled as
`external_event_received`. Only after Temporal reports the exact event does
SQLite commit a delivery receipt. An ambiguous Signal transport failure, a
missing Worker, a mismatched Workflow identity, an altered event with the
same ID, a terminal Workflow without an acknowledged event, or a missing
confirmation leaves the event pending or fails closed. Calling the dispatcher
again after a failure reconciles against the original workflow and does not
create a new execution.

`external_event_received` is a reference-only audit inbox in the Workflow.
It **does not approve**, invoke tools, mutate provider resources or execute
an event's alleged intent. Applying a `human.reviewed` decision requires a
separate authenticated approval path. The handler keeps at most 1,000 external
events in the current Workflow history; beyond that it intentionally does not
acknowledge more messages. A future version must use retention policies,
Continue-As-New / external checkpoints with replay-safe migration to support
unbounded streams. As with all Temporal signals, an accepted Signal does
**not** mean a downstream business action completed.

**Acceptance boundary:** tests cover SQLite recovery, ambiguous ACK,
tenant/Session/Run isolation, duplicate IDs and Temporal SDK Worker replacement
on its local test service. A production deployment still needs authenticated
event ingress, restrictive Temporal network credentials/TLS, multiple
dispatchers with concurrency control, recovery polling with backoff, a real
persisted-server restart/replay and external audit retention. The SQLite local
reference store is not the production multi-tenant event service.


## Slice 3: bounded single-flight dispatch on one SQLite database

Multiple caller-driven dispatchers may now share the **same local SQLite database**.
Each unacknowledged event is atomically claimed with `BEGIN IMMEDIATE`, a unique
owner token and a default **60-second** lease before any Temporal query/Signal.
Competing live dispatchers skip the claimed event; only its current, unexpired
owner can commit a server-observed receipt. A stale owner cannot delete its
replacement's claim. Normal completion or caught errors release the claim.
After process death, the database lease expires and another invocation can
reconcile Temporal's observed event before considering a re-Signal.

This is **at-least-once transport, idempotent Workflow recording**, not exactly-once
Signal delivery. A driver request delayed beyond the lease can overlap a
replacement and may issue a duplicate Signal; its stale receipt is rejected.
The reference adapter has **no daemon, retry timer, automatic backoff, lease
renewal or database-distributed security boundary**. Operators must bound
driver RPC times below the configured lease and re-invoke after failures. Only
use this implementation for the local SQLite reference setup; production
deployments still need authorized persistent retry coordination, telemetry,
bounded RPC deadlines, provider credentials and a retained-history policy.
