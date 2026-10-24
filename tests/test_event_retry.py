import asyncio

import pytest

from cloud_agent_runtime import (
    EventEnvelope,
    Run,
    RunStatus,
    Session,
    SQLiteEventInbox,
    SQLiteStore,
)
from cloud_agent_runtime.event_delivery import TemporalEventDispatcher
from cloud_agent_runtime.event_retry import EventRetryCoordinator
from cloud_agent_runtime.workflow import WorkflowRef


class FakeDriver:
    name = "temporal"

    def __init__(self):
        self.events = []
        self.signals = []
        self.record_signal = True
        self.drop_ack = False
        self.terminal = False
        self.session_id = "session-a"

    async def query(self, workflow, *, name):
        assert name == "run_state"
        assert workflow.workflow_id == "agent-run-a"
        assert workflow.run_id == "execution-a"
        return {
            "runtime_run_id": "run-a",
            "session_id": self.session_id,
            "terminal": self.terminal,
            "external_events": [dict(event) for event in self.events],
        }

    async def signal(self, workflow, *, name, payload):
        assert name == "external_event_received"
        assert workflow.run_id == "execution-a"
        self.signals.append(dict(payload))
        if self.record_signal:
            self.events.append(dict(payload))
        if self.drop_ack:
            raise ConnectionError("response lost after Signal was accepted")


@pytest.fixture
def persisted_event(tmp_path):
    db = tmp_path / "event-retry.sqlite"
    store = SQLiteStore(db)
    store.save_session(Session("session-a", "agent-a", "release-a", "tenant-a"))
    store.save_run(Run(
        "run-a", "session-a", status=RunStatus.RUNNING,
        workflow_ref=WorkflowRef(
            provider="temporal", workflow_id="agent-run-a", run_id="execution-a"
        ),
    ))
    inbox = SQLiteEventInbox(db)
    event = EventEnvelope(
        event_id="event-a", tenant_id="tenant-a", run_id="run-a",
        event_type="human.reviewed", correlation_id="task-a",
        payload_ref="evidence://signed/a",
    )
    assert inbox.accept(event)
    yield db, store, inbox, event
    inbox.close()
    store.close()


def runner(inbox, driver, now, *, max_attempts=4):
    dispatcher = TemporalEventDispatcher(
        inbox, driver, confirmation_timeout_seconds=0,
        confirmation_poll_interval_seconds=0,
    )
    return EventRetryCoordinator(
        inbox, dispatcher, clock=lambda: now[0], jitter=lambda: 1.0,
        base_delay_seconds=2.0, max_delay_seconds=30.0,
        max_attempts=max_attempts,
    )


def tick(runner_instance):
    return asyncio.run(runner_instance.reconcile_once(
        run_id="run-a", tenant_id="tenant-a"
    ))


def test_inbox_survives_process_restart_before_first_signal(persisted_event):
    db, _, inbox, event = persisted_event
    inbox.close()
    reopened = SQLiteEventInbox(db)
    driver = FakeDriver()
    try:
        outcome = tick(runner(reopened, driver, [100.0]))
        assert outcome.status == "confirmed"
        assert driver.signals == [event.to_dict()]
        assert reopened.list_pending(run_id="run-a", tenant_id="tenant-a") == []
        assert tick(runner(reopened, driver, [100.0])).status == "idle"
    finally:
        reopened.close()


def test_lost_ack_backoff_survives_restart_and_reconciles_without_resend(persisted_event):
    db, _, inbox, event = persisted_event
    driver = FakeDriver()
    driver.drop_ack = True
    now = [100.0]
    first = tick(runner(inbox, driver, now))
    assert first.status == "retry_scheduled"
    assert first.attempts == 1
    assert first.next_attempt_at == 102.0
    assert inbox.list_pending(run_id="run-a", tenant_id="tenant-a") == [event]
    assert len(driver.signals) == 1

    inbox.close()
    reopened = SQLiteEventInbox(db)
    try:
        retry = runner(reopened, driver, now)
        assert tick(retry).status == "deferred"
        assert len(driver.signals) == 1
        now[0] = 102.0
        driver.drop_ack = False
        assert tick(retry).status == "confirmed"
        assert driver.signals == [event.to_dict()]
        assert len(driver.events) == 1
        assert reopened.get_retry_state(event) is None
        assert reopened.list_pending(run_id="run-a", tenant_id="tenant-a") == []
    finally:
        reopened.close()


def test_signal_ack_is_not_workflow_confirmation(persisted_event):
    _, _, inbox, event = persisted_event
    driver = FakeDriver()
    driver.record_signal = False
    now = [100.0]
    retry = runner(inbox, driver, now)
    assert tick(retry).status == "retry_scheduled"
    assert inbox.list_pending(run_id="run-a", tenant_id="tenant-a") == [event]
    assert len(driver.signals) == 1
    now[0] = 102.0
    driver.record_signal = True
    assert tick(retry).status == "confirmed"
    assert len(driver.signals) == 2
    assert inbox.get_retry_state(event) is None


def test_terminal_and_foreign_workflow_fail_closed(persisted_event):
    _, _, inbox, event = persisted_event
    now = [100.0]
    driver = FakeDriver()
    driver.terminal = True
    assert tick(runner(inbox, driver, now)).status == "blocked"
    state = inbox.get_retry_state(event)
    assert state is not None and state.blocked and state.last_error_class == "terminal_unconfirmed"
    assert tick(runner(inbox, driver, [1000.0])).status == "blocked"
    assert driver.signals == []
    assert inbox.list_pending(run_id="run-a", tenant_id="tenant-a") == [event]

    inbox.reset_retry_state(event)
    driver.terminal = False
    driver.session_id = "foreign-session"
    assert tick(runner(inbox, driver, now)).status == "blocked"
    assert driver.signals == []


def test_attempt_budget_blocks_without_deleting_event(persisted_event):
    _, _, inbox, event = persisted_event
    driver = FakeDriver()
    driver.record_signal = False
    now = [100.0]
    retry = runner(inbox, driver, now, max_attempts=2)
    assert tick(retry).status == "retry_scheduled"
    now[0] = 102.0
    outcome = tick(retry)
    assert outcome.status == "blocked"
    assert outcome.attempts == 2
    assert tick(retry).status == "blocked"
    assert len(driver.signals) == 2
    assert inbox.list_pending(run_id="run-a", tenant_id="tenant-a") == [event]
    assert inbox.get_retry_state(event).blocked


def test_cross_tenant_cannot_observe_retry_state_or_dispatch(persisted_event):
    _, _, inbox, event = persisted_event
    driver = FakeDriver()
    with pytest.raises(PermissionError):
        asyncio.run(runner(inbox, driver, [100.0]).reconcile_once(
            run_id="run-a", tenant_id="tenant-b"
        ))
    with pytest.raises(PermissionError):
        inbox.get_retry_state(EventEnvelope(
            event_id=event.event_id, tenant_id="tenant-b",
            run_id="run-a", event_type=event.event_type,
            correlation_id=event.correlation_id,
        ))
    assert driver.signals == []


def test_unavailable_workflow_query_has_bounded_attempt_duration(persisted_event):
    _, _, inbox, event = persisted_event

    class HungQueryDriver(FakeDriver):
        async def query(self, workflow, *, name):
            await asyncio.sleep(60)

    driver = HungQueryDriver()
    dispatcher = TemporalEventDispatcher(inbox, driver)
    coordinator = EventRetryCoordinator(
        inbox, dispatcher, clock=lambda: 100.0, jitter=lambda: 1.0,
        attempt_timeout_seconds=0.01,
    )
    outcome = tick(coordinator)
    assert outcome.status == "retry_scheduled"
    assert inbox.get_retry_state(event).last_error_class == "transport_unknown"
    assert inbox.list_pending(run_id="run-a", tenant_id="tenant-a") == [event]
    assert driver.signals == []


def test_invalid_retry_configuration_is_rejected(persisted_event):
    _, _, inbox, _ = persisted_event
    driver = FakeDriver()
    dispatcher = TemporalEventDispatcher(inbox, driver)
    with pytest.raises(ValueError):
        EventRetryCoordinator(inbox, dispatcher, max_attempts=0)
    with pytest.raises(ValueError):
        EventRetryCoordinator(inbox, dispatcher, base_delay_seconds=0)
    with pytest.raises(ValueError):
        EventRetryCoordinator(inbox, dispatcher, attempt_timeout_seconds=0)


def test_competing_dispatcher_claim_does_not_consume_retry_budget(persisted_event):
    _, _, inbox, event = persisted_event
    driver = FakeDriver()
    assert inbox.try_claim(event, owner_token="another-dispatcher")
    retry = runner(inbox, driver, [100.0])
    outcome = tick(retry)
    assert outcome.status == "inflight"
    assert outcome.attempts == 0
    assert inbox.get_retry_state(event) is None
    assert driver.signals == []
    assert inbox.release_claim(event, owner_token="another-dispatcher")
    assert tick(retry).status == "confirmed"
