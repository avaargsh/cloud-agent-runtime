from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
import math
from typing import Any

from temporalio import workflow


@dataclass
class AgentRunWorkflowState:
    runtime_run_id: str
    session_id: str
    phase: str = "pending"
    paused: bool = False
    terminal: bool = False
    approval_events: list[dict[str, Any]] = field(
        default_factory=list
    )
    processed_approval_ids: set[str] = field(
        default_factory=set
    )
    evidence_refs: list[str] = field(
        default_factory=list
    )
    result: dict[str, Any] | None = None
    error: str | None = None
    transition_events: list[dict[str, Any]] = field(
        default_factory=list
    )


@workflow.defn(name="AgentRunWorkflow")
class AgentRunWorkflow:
    """Durable control-flow shell for a canonical runtime Run.

    Product state remains in the runtime store. Temporal owns durable
    waiting and signal delivery. Signals are handled idempotently where
    the caller supplies a stable domain identifier.
    """

    def __init__(self) -> None:
        self._state: AgentRunWorkflowState | None = None

    def _transition(
        self,
        *,
        to_phase: str,
        event_type: str,
        event_id: str | None = None,
        evidence_refs: list[str] | None = None,
    ) -> None:
        """Record a correlated audit event for each actual phase change."""
        state = self._state
        if state is None or state.phase == to_phase:
            return
        sequence = len(state.transition_events) + 1
        state.transition_events.append(
            {
                "transition_id": f"{state.runtime_run_id}:transition:{sequence}",
                "runtime_run_id": state.runtime_run_id,
                "session_id": state.session_id,
                "sequence": sequence,
                "event_type": event_type,
                "event_id": event_id,
                "from_phase": state.phase,
                "to_phase": to_phase,
                "evidence_refs": list(evidence_refs or []),
            }
        )
        state.phase = to_phase

    @workflow.run
    async def run(
        self,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        self._state = AgentRunWorkflowState(
            runtime_run_id=str(
                payload["runtime_run_id"]
            ),
            session_id=str(payload["session_id"]),
        )
        self._transition(
            to_phase="running",
            event_type="run_started",
            event_id=str(payload.get("event_id") or "") or None,
            evidence_refs=list(payload.get("evidence_refs", [])),
        )
        for ref in payload.get("evidence_refs", []):
            if ref not in self._state.evidence_refs:
                self._state.evidence_refs.append(ref)

        is_terminal = lambda: (
            self._state is not None and self._state.terminal
        )
        timeout_raw = payload.get("timeout_seconds")
        if timeout_raw is None:
            await workflow.wait_condition(is_terminal)
        else:
            if isinstance(timeout_raw, bool) or not isinstance(
                timeout_raw, (int, float)
            ):
                raise ValueError("timeout_seconds must be a finite positive number")
            timeout_seconds = float(timeout_raw)
            if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
                raise ValueError("timeout_seconds must be a finite positive number")
            try:
                await workflow.wait_condition(
                    is_terminal,
                    timeout=timedelta(seconds=timeout_seconds),
                    timeout_summary="Agent Run deadline exceeded",
                )
            except TimeoutError:
                # The Temporal timer is part of workflow history, so replaying
                # after a worker restart reaches the same deterministic result.
                if not is_terminal():
                    self._transition(
                        to_phase="failed",
                        event_type="run_timed_out",
                        event_id=str(
                            payload.get("timeout_event_id")
                            or f"timeout:{self._state.runtime_run_id}"
                        ),
                        evidence_refs=list(self._state.evidence_refs),
                    )
                    self._state.error = (
                        f"run timed out after {timeout_seconds:g} seconds"
                    )
                    self._state.terminal = True

        assert self._state is not None
        return {
            "runtime_run_id": (
                self._state.runtime_run_id
            ),
            "session_id": self._state.session_id,
            "phase": self._state.phase,
            "result": self._state.result,
            "error": self._state.error,
            "evidence_refs": list(
                self._state.evidence_refs
            ),
            "transition_events": list(
                self._state.transition_events
            ),
        }

    @workflow.signal(name="approval_resolved")
    def approval_resolved(
        self,
        payload: dict[str, Any],
    ) -> None:
        if self._state is None or self._state.terminal:
            return

        approval_id = str(
            payload.get("approval_id") or ""
        )
        if (
            approval_id
            and approval_id
            in self._state.processed_approval_ids
        ):
            return

        event = dict(payload)
        self._state.approval_events.append(event)

        if approval_id:
            self._state.processed_approval_ids.add(
                approval_id
            )

        for ref in event.get(
            "evidence_refs",
            [],
        ):
            if ref not in self._state.evidence_refs:
                self._state.evidence_refs.append(ref)

        if event.get("approved") is False:
            self._transition(
                to_phase="failed",
                event_type="approval_denied",
                event_id=approval_id or None,
                evidence_refs=list(event.get("evidence_refs", [])),
            )
            self._state.error = (
                event.get("reason")
                or "approval denied"
            )
            self._state.terminal = True
        elif not self._state.paused:
            # Resolving an approval must not override a manual pause.
            self._transition(
                to_phase="running",
                event_type="approval_resolved",
                event_id=approval_id or None,
                evidence_refs=list(event.get("evidence_refs", [])),
            )

    @workflow.signal(name="wait_for_input")
    def wait_for_input(self, payload: dict[str, Any] | None = None) -> None:
        if self._state is None or self._state.terminal:
            return
        event = payload or {}
        self._transition(
            to_phase="waiting",
            event_type="wait_for_input",
            event_id=str(event.get("event_id") or "") or None,
            evidence_refs=list(event.get("evidence_refs", [])),
        )
        for ref in event.get("evidence_refs", []):
            if ref not in self._state.evidence_refs:
                self._state.evidence_refs.append(ref)

    @workflow.signal(name="pause_run")
    def pause_run(
        self,
        payload: dict[str, Any] | None = None,
    ) -> None:
        if self._state is None or self._state.terminal:
            return
        event = payload or {}
        self._state.paused = True
        self._transition(
            to_phase="paused",
            event_type="pause_run",
            event_id=str(event.get("event_id") or "") or None,
            evidence_refs=list(event.get("evidence_refs", [])),
        )
        for ref in event.get("evidence_refs", []):
            if ref not in self._state.evidence_refs:
                self._state.evidence_refs.append(ref)

    @workflow.signal(name="resume_run")
    def resume_run(
        self,
        payload: dict[str, Any] | None = None,
    ) -> None:
        if self._state is None or self._state.terminal:
            return
        event = payload or {}
        self._state.paused = False
        self._transition(
            to_phase="running",
            event_type="resume_run",
            event_id=str(event.get("event_id") or "") or None,
            evidence_refs=list(event.get("evidence_refs", [])),
        )
        for ref in event.get("evidence_refs", []):
            if ref not in self._state.evidence_refs:
                self._state.evidence_refs.append(ref)

    @workflow.signal(name="complete_run")
    def complete_run(
        self,
        payload: dict[str, Any],
    ) -> None:
        if self._state is None or self._state.terminal:
            return

        self._transition(
            to_phase="succeeded",
            event_type="complete_run",
            event_id=str(payload.get("event_id") or "") or None,
            evidence_refs=list(payload.get("evidence_refs", [])),
        )
        self._state.result = dict(
            payload.get("result") or {}
        )

        for ref in payload.get(
            "evidence_refs",
            [],
        ):
            if ref not in self._state.evidence_refs:
                self._state.evidence_refs.append(ref)

        self._state.terminal = True

    @workflow.signal(name="fail_run")
    def fail_run(
        self,
        payload: dict[str, Any],
    ) -> None:
        if self._state is None or self._state.terminal:
            return

        self._transition(
            to_phase="failed",
            event_type="fail_run",
            event_id=str(payload.get("event_id") or "") or None,
            evidence_refs=list(payload.get("evidence_refs", [])),
        )
        self._state.error = str(
            payload.get("error")
            or "runtime failure"
        )
        for ref in payload.get("evidence_refs", []):
            if ref not in self._state.evidence_refs:
                self._state.evidence_refs.append(ref)
        self._state.terminal = True

    @workflow.signal(name="cancel_run")
    def cancel_run(self, payload: dict[str, Any] | None = None) -> None:
        if self._state is None or self._state.terminal:
            return
        event = payload or {}
        self._transition(
            to_phase="cancelled",
            event_type="cancel_run",
            event_id=str(event.get("event_id") or "") or None,
            evidence_refs=list(event.get("evidence_refs", [])),
        )
        self._state.error = str(event.get("reason") or "cancelled")
        for ref in event.get("evidence_refs", []):
            if ref not in self._state.evidence_refs:
                self._state.evidence_refs.append(ref)
        self._state.terminal = True

    @workflow.query(name="run_state")
    def run_state(self) -> dict[str, Any]:
        if self._state is None:
            return {
                "phase": "initializing",
            }

        return {
            "runtime_run_id": (
                self._state.runtime_run_id
            ),
            "session_id": self._state.session_id,
            "phase": self._state.phase,
            "paused": self._state.paused,
            "terminal": self._state.terminal,
            "approval_events": list(
                self._state.approval_events
            ),
            "transition_events": list(
                self._state.transition_events
            ),
            "evidence_refs": list(
                self._state.evidence_refs
            ),
            "error": self._state.error,
        }
