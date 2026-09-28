import asyncio

from cloud_agent_runtime.temporal_bridge import TemporalRunBridge
from cloud_agent_runtime.temporal_driver import TemporalWorkflowDriver


class FakeHandle:
    first_execution_run_id = "temporal-1"

    def __init__(self):
        self.signals = []
        self.state = {"runtime_run_id": "run-1", "phase": "paused", "paused": True, "terminal": False, "evidence_refs": ["evidence://sha256/abc"], "error": None}
        self.complete_queries_remaining = 0

    async def signal(self, name, payload):
        self.signals.append((name, payload))
        if name == "complete_run":
            self.complete_queries_remaining = 2

    async def query(self, name, *args):
        assert name == "run_state"
        if self.complete_queries_remaining:
            self.complete_queries_remaining -= 1
            if self.complete_queries_remaining == 0:
                self.state.update({"phase": "succeeded", "paused": False, "terminal": True})
        return self.state


class FakeClient:
    def __init__(self):
        self.handle = FakeHandle()

    async def start_workflow(self, workflow_type, *, args, id, task_queue, id_reuse_policy):
        return self.handle

    def get_workflow_handle(self, workflow_id, **kwargs):
        return self.handle


def test_bridge_signals_stable_approval_and_reads_evidence_refs():
    async def scenario():
        client = FakeClient()
        bridge = TemporalRunBridge(TemporalWorkflowDriver(client=client, task_queue="agent-runs"))
        ref = await bridge.start_or_attach(runtime_run_id="run-1", session_id="session-1")
        await bridge.signal_approval(ref, approval_id="approval-42", approved=True, evidence_refs=("evidence://sha256/abc",))
        status = await bridge.get_run_status(ref)
        assert client.handle.signals[0] == ("approval_resolved", {"approval_id": "approval-42", "approved": True, "evidence_refs": ["evidence://sha256/abc"]})
        assert status.runtime_run_id == "run-1"
        assert status.phase == "paused"
        assert status.evidence_refs == ("evidence://sha256/abc",)
    asyncio.run(scenario())


def test_bridge_complete_waits_for_terminal_ack_and_attaches_final_evidence_refs():
    async def scenario():
        client = FakeClient()
        bridge = TemporalRunBridge(
            TemporalWorkflowDriver(client=client, task_queue="agent-runs"),
            completion_timeout_seconds=1,
            completion_poll_interval_seconds=0,
        )
        ref = await bridge.start_or_attach(runtime_run_id="run-1", session_id="session-1")
        await bridge.complete(ref, result={"status": "VERIFIED"}, evidence_refs=("evidence://sha256/final",))
        assert client.handle.signals[-1] == ("complete_run", {"result": {"status": "VERIFIED"}, "evidence_refs": ["evidence://sha256/final"]})
        assert client.handle.state["terminal"] is True
        assert client.handle.state["phase"] == "succeeded"
    asyncio.run(scenario())
