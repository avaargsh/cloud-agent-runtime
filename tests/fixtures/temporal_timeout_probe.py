from __future__ import annotations

from typing import Any

from temporalio import workflow


@workflow.defn(name="AgentRunTimeoutProbe")
class AgentRunTimeoutProbe:
    """Minimal workflow used to verify Temporal timeout terminal events."""

    def __init__(self) -> None:
        self._state: dict[str, Any] | None = None

    @workflow.run
    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._state = {
            "runtime_run_id": str(payload["runtime_run_id"]),
            "session_id": str(payload["session_id"]),
            "phase": "running",
            "terminal": False,
            "transition_events": [],
        }

        timeout_seconds = payload.get("timeout_seconds")
        if timeout_seconds is None:
            await workflow.wait_condition(lambda: bool(self._state and self._state["terminal"]))
        else:
            try:
                await workflow.wait_condition(
                    lambda: bool(self._state and self._state["terminal"]),
                    timeout=float(timeout_seconds),
                )
            except TimeoutError:
                assert self._state is not None
                self._state["phase"] = "failed"
                self._state["terminal"] = True
                self._state["transition_events"].append({
                    "event_id": f"timeout:{self._state['runtime_run_id']}",
                    "event_type": "run_timed_out",
                    "evidence_refs": [],
                })

        assert self._state is not None
        return dict(self._state)

    @workflow.signal(name="complete_run")
    def complete_run(self, payload: dict[str, Any]) -> None:
        if self._state is None or self._state["terminal"]:
            return
        self._state["phase"] = "succeeded"
        self._state["terminal"] = True
        self._state["transition_events"].append({
            "event_id": str(payload.get("event_id") or "completed"),
            "event_type": "run_completed",
            "evidence_refs": list(payload.get("evidence_refs") or []),
        })
