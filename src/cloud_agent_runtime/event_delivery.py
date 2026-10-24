from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from uuid import uuid4
from typing import Any

from .event_inbox import EventConflictError, EventEnvelope, SQLiteEventInbox
from .temporal_driver import TemporalWorkflowDriver
from .workflow import WorkflowRef


class EventDeliveryUncertain(RuntimeError):
    """Event remains in the inbox; do not treat Signal transport ACK as delivery."""


@dataclass
class TemporalEventDispatcher:
    """Reference-only inbox -> Temporal Signal with observed acknowledgment.

    No background polling, tool execution, approval authorization or business
    effect is performed. Reinvoke after transport errors or a Worker restart.
    """

    inbox: SQLiteEventInbox
    driver: TemporalWorkflowDriver
    confirmation_timeout_seconds: float = 5.0
    confirmation_poll_interval_seconds: float = 0.05
    delivery_lease_seconds: int = 60

    @staticmethod
    def _observed(
        state: dict[str, Any],
        event: EventEnvelope,
        session_id: str,
    ) -> bool:
        if (
            not isinstance(state, dict)
            or state.get("runtime_run_id") != event.run_id
            or state.get("session_id") != session_id
        ):
            raise PermissionError("Temporal workflow identity does not match canonical Run")
        events = state.get("external_events", [])
        if not isinstance(events, list):
            raise EventDeliveryUncertain("Temporal event observation is unavailable")
        for received in events:
            if isinstance(received, dict) and received.get("event_id") == event.event_id:
                if json.dumps(received, sort_keys=True) != json.dumps(
                    event.to_dict(), sort_keys=True
                ):
                    raise EventConflictError("Temporal event ID is bound to different content")
                return True
        return False

    async def deliver_pending(
        self, *, run_id: str, tenant_id: str, limit: int = 100
    ) -> int:
        workflow, session_id = self.inbox.bound_workflow(
            run_id=run_id, tenant_id=tenant_id
        )
        if workflow.provider != self.driver.name:
            raise PermissionError("Temporal provider does not match canonical binding")
        if type(self.delivery_lease_seconds) is not int or not (
            1 <= self.delivery_lease_seconds <= 3600
        ):
            raise ValueError("delivery_lease_seconds must be between 1 and 3600")
        pending = self.inbox.list_pending(
            run_id=run_id, tenant_id=tenant_id, limit=limit
        )
        confirmed = 0
        for event in pending:
            claim_token = uuid4().hex
            if not self.inbox.try_claim(
                event, owner_token=claim_token, lease_seconds=self.delivery_lease_seconds
            ):
                continue
            try:
                state = await self.driver.query(workflow, name="run_state")
                if self._observed(state, event, session_id):
                    self.inbox.mark_delivered(event, workflow, owner_token=claim_token)
                    confirmed += 1
                    continue
                if state.get("terminal"):
                    raise EventDeliveryUncertain(
                        f"workflow terminal before event {event.event_id} was confirmed"
                    )
                try:
                    await self.driver.signal(
                        workflow,
                        name="external_event_received",
                        payload=event.to_dict(),
                    )
                except Exception as exc:
                    # Server may have accepted the Signal before transport failure.
                    # Leave it pending: the next call observes History before resending.
                    raise EventDeliveryUncertain(
                        f"Temporal Signal outcome is uncertain for {event.event_id}"
                    ) from exc

                loop = asyncio.get_running_loop()
                deadline = loop.time() + max(0.0, self.confirmation_timeout_seconds)
                while True:
                    state = await self.driver.query(workflow, name="run_state")
                    if self._observed(state, event, session_id):
                        self.inbox.mark_delivered(event, workflow, owner_token=claim_token)
                        confirmed += 1
                        break
                    if state.get("terminal"):
                        raise EventDeliveryUncertain(
                            f"workflow terminal before event {event.event_id} was confirmed"
                        )
                    if loop.time() >= deadline:
                        raise EventDeliveryUncertain(
                            f"event {event.event_id} was not confirmed by Temporal workflow"
                        )
                    await asyncio.sleep(max(0.0, self.confirmation_poll_interval_seconds))
            finally:
                self.inbox.release_claim(event, owner_token=claim_token)
        return confirmed
