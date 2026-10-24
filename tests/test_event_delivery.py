import asyncio
from dataclasses import replace

import pytest

from cloud_agent_runtime.event_delivery import EventDeliveryUncertain, TemporalEventDispatcher
from cloud_agent_runtime.event_inbox import EventConflictError, EventEnvelope, SQLiteEventInbox
from cloud_agent_runtime.models import Run, RunStatus, Session
from cloud_agent_runtime.store import SQLiteStore
from cloud_agent_runtime.workflow import WorkflowRef
from cloud_agent_runtime.workflow import WorkflowRef


def seeded_inbox(tmp_path):
    path = tmp_path / "events.sqlite"
    store = SQLiteStore(path)
    store.save_session(Session("session-a", "agent-a", "release-a", "tenant-a"))
    store.save_session(Session("session-b", "agent-b", "release-b", "tenant-b"))
    store.save_run(
        Run(
            "run-a",
            "session-a",
            status=RunStatus.RUNNING,
            workflow_ref=WorkflowRef(
                provider="temporal",
                workflow_id="agent-run-run-a",
                run_id="temporal-execution-a",
            ),
        )
    )
    store.save_run(Run("run-b", "session-b", status=RunStatus.RUNNING))
    inbox = SQLiteEventInbox(path)
    event = EventEnvelope(
        event_id="provider-event-1",
        tenant_id="tenant-a",
        run_id="run-a",
        event_type="human.reviewed",
        correlation_id="task-1",
        payload_ref="evidence://decision/1",
        evidence_refs=("evidence://decision/1",),
    )
    assert inbox.accept(event)
    return store, inbox, event, path


class FakeTemporalDriver:
    name = "temporal"

    def __init__(self):
        self.state = {
            "runtime_run_id": "run-a",
            "session_id": "session-a",
            "phase": "waiting",
            "terminal": False,
            "external_events": [],
        }
        self.signals = []
        self.record_signal = True
        self.raise_after_signal = False

    async def query(self, workflow, *, name):
        assert workflow.workflow_id == "agent-run-run-a"
        assert workflow.run_id == "temporal-execution-a"
        assert name == "run_state"
        return {
            **self.state,
            "external_events": [dict(x) for x in self.state["external_events"]],
        }

    async def signal(self, workflow, *, name, payload):
        assert workflow.workflow_id == "agent-run-run-a"
        assert workflow.run_id == "temporal-execution-a"
        assert name == "external_event_received"
        self.signals.append(dict(payload))
        if self.record_signal:
            self.state["external_events"].append(dict(payload))
        if self.raise_after_signal:
            raise ConnectionError("ambiguous Temporal Signal ACK")


def test_delivery_records_receipt_only_after_workflow_state_confirms(tmp_path):
    store, inbox, event, path = seeded_inbox(tmp_path)
    driver = FakeTemporalDriver()
    dispatcher = TemporalEventDispatcher(inbox=inbox, driver=driver)
    assert asyncio.run(dispatcher.deliver_pending(run_id="run-a", tenant_id="tenant-a")) == 1
    assert driver.signals == [event.to_dict()]
    assert inbox.list_pending(run_id="run-a", tenant_id="tenant-a") == []
    inbox.close()

    reopened = SQLiteEventInbox(path)
    dispatcher = TemporalEventDispatcher(inbox=reopened, driver=driver)
    assert asyncio.run(dispatcher.deliver_pending(run_id="run-a", tenant_id="tenant-a")) == 0
    assert len(driver.signals) == 1
    reopened.close()
    store.close()


def test_lost_ack_retains_pending_and_reconciles_without_duplicate_send(tmp_path):
    store, inbox, event, _ = seeded_inbox(tmp_path)
    driver = FakeTemporalDriver()
    driver.raise_after_signal = True
    dispatcher = TemporalEventDispatcher(inbox=inbox, driver=driver)
    with pytest.raises(EventDeliveryUncertain):
        asyncio.run(dispatcher.deliver_pending(run_id="run-a", tenant_id="tenant-a"))
    assert inbox.list_pending(run_id="run-a", tenant_id="tenant-a") == [event]
    driver.raise_after_signal = False
    assert asyncio.run(dispatcher.deliver_pending(run_id="run-a", tenant_id="tenant-a")) == 1
    assert driver.signals == [event.to_dict()]
    inbox.close()
    store.close()


def test_signal_ack_without_workflow_processing_is_not_delivery(tmp_path):
    store, inbox, event, _ = seeded_inbox(tmp_path)
    driver = FakeTemporalDriver()
    driver.record_signal = False
    dispatcher = TemporalEventDispatcher(
        inbox=inbox,
        driver=driver,
        confirmation_timeout_seconds=0,
        confirmation_poll_interval_seconds=0,
    )
    with pytest.raises(EventDeliveryUncertain, match="not confirmed"):
        asyncio.run(dispatcher.deliver_pending(run_id="run-a", tenant_id="tenant-a"))
    assert inbox.list_pending(run_id="run-a", tenant_id="tenant-a") == [event]
    inbox.close()
    store.close()


def test_terminal_unconfirmed_event_is_not_silently_dropped(tmp_path):
    store, inbox, event, _ = seeded_inbox(tmp_path)
    driver = FakeTemporalDriver()
    driver.state.update({"terminal": True, "phase": "failed"})
    with pytest.raises(EventDeliveryUncertain, match="terminal"):
        asyncio.run(TemporalEventDispatcher(inbox, driver).deliver_pending(
            run_id="run-a", tenant_id="tenant-a"
        ))
    assert driver.signals == []
    assert inbox.list_pending(run_id="run-a", tenant_id="tenant-a") == [event]
    inbox.close()
    store.close()


def test_foreign_workflow_and_conflicting_remote_event_fail_closed(tmp_path):
    store, inbox, event, _ = seeded_inbox(tmp_path)
    driver = FakeTemporalDriver()
    driver.state["session_id"] = "session-b"
    with pytest.raises(PermissionError):
        asyncio.run(TemporalEventDispatcher(inbox, driver).deliver_pending(
            run_id="run-a", tenant_id="tenant-a"
        ))
    driver.state["session_id"] = "session-a"
    driver.state["external_events"].append(replace(event, event_type="forged").to_dict())
    with pytest.raises(EventConflictError):
        asyncio.run(TemporalEventDispatcher(inbox, driver).deliver_pending(
            run_id="run-a", tenant_id="tenant-a"
        ))
    assert driver.signals == []
    assert inbox.list_pending(run_id="run-a", tenant_id="tenant-a") == [event]
    inbox.close()
    store.close()


def test_unbound_run_or_cross_tenant_cannot_dispatch(tmp_path):
    store, inbox, event, _ = seeded_inbox(tmp_path)
    driver = FakeTemporalDriver()
    dispatcher = TemporalEventDispatcher(inbox, driver)
    for run_id, tenant_id in (("run-a", "tenant-b"), ("run-b", "tenant-b")):
        with pytest.raises(PermissionError):
            asyncio.run(dispatcher.deliver_pending(run_id=run_id, tenant_id=tenant_id))
    assert driver.signals == []
    assert inbox.list_pending(run_id="run-a", tenant_id="tenant-a") == [event]
    inbox.close()
    store.close()


@pytest.mark.parametrize("run_id", [None, "", " "])
def test_unpinned_temporal_execution_is_rejected_before_signal(tmp_path, run_id):
    """A logical workflow ID alone must not authorize an execution Signal."""
    store, inbox, event, _ = seeded_inbox(tmp_path)
    original = store.get_run("run-a")
    original.workflow_ref = WorkflowRef(
        provider="temporal",
        workflow_id="agent-run-run-a",
        run_id=run_id,
    )
    store.save_run(original)

    driver = FakeTemporalDriver()
    with pytest.raises(PermissionError, match="binding is unavailable"):
        asyncio.run(
            TemporalEventDispatcher(inbox, driver).deliver_pending(
                run_id="run-a", tenant_id="tenant-a"
            )
        )
    assert driver.signals == []
    assert inbox.list_pending(run_id="run-a", tenant_id="tenant-a") == [event]
    inbox.close()
    store.close()
