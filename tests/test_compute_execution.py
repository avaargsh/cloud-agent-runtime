from __future__ import annotations

import asyncio
import copy
import re
from typing import Any

import pytest

from cloud_agent_runtime import AgentRuntime, InMemoryStore, SQLiteStore
from cloud_agent_runtime.compute_execution import (
    ComputeProfileConflict,
    ExecutionProfileRouter,
)


IMAGE = "registry.example.test/worker@sha256:" + ("a" * 64)
PROFILE_RESOURCES = {
    "cpuMilli": 500,
    "memoryMiB": 512,
    "ephemeralStorageMiB": 1024,
    "timeoutSeconds": 300,
    "maxConcurrency": 1,
}


def profile(environment: str, accelerator: dict[str, Any]) -> dict[str, Any]:
    return {
        "apiVersion": "agent.execution-profile/v0.1",
        "profileId": "gpu-worker",
        "revision": 7,
        "projectId": "project-a",
        "environment": environment,
        "resources": dict(PROFILE_RESOURCES),
        "accelerator": accelerator,
        "persistence": "ephemeral",
        "networkPolicyRef": "default-deny",
        "credentialBindingRefs": [],
    }


class RecordingComputeClient:
    def __init__(self, store: Any | None = None) -> None:
        self.store = store
        self.requests: list[dict[str, Any]] = []

    async def submit_or_reconcile(
        self,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        # The operation intent must be durably stored before the first
        # Control Plane side effect. This is deliberately observable here.
        if self.store is not None:
            run = self.store.get_run(request["correlation"]["runId"])
            assert run.compute_request is not None
            assert run.compute_request["requestId"] == request["requestId"]
        self.requests.append(copy.deepcopy(request))
        return {
            "requestId": request["requestId"],
            "projectId": request["projectId"],
            "computePoolId": request["computePoolId"],
            "workloadId": request["workload"]["id"],
            "generation": 1,
            "state": "REQUESTED",
        }


def runtime_with_run(store: Any | None = None):
    runtime = AgentRuntime(store=store or InMemoryStore())
    session = runtime.create_session(
        agent_id="agent-a",
        release_id="release-a",
        tenant_id="tenant-a",
    )
    run = runtime.start_run(session_id=session.session_id)
    return runtime, session, run


def test_ordinary_worker_profile_never_calls_compute_api() -> None:
    async def scenario() -> None:
        store = InMemoryStore()
        _runtime, _session, run = runtime_with_run(store)
        client = RecordingComputeClient(store)
        router = ExecutionProfileRouter(store=store, compute_client=client)

        result = await router.dispatch(
            run.run_id,
            profile=profile("worker", {"mode": "none"}),
        )

        assert result is None
        assert client.requests == []
        assert store.get_run(run.run_id).compute_request is None

    asyncio.run(scenario())


@pytest.mark.parametrize("environment", ["worker", "sandbox", "workspace"])
def test_non_gpu_profiles_reject_accelerator_requirements(environment: str) -> None:
    async def scenario() -> None:
        store = InMemoryStore()
        _runtime, _session, run = runtime_with_run(store)
        client = RecordingComputeClient(store)
        router = ExecutionProfileRouter(store=store, compute_client=client)

        with pytest.raises(ValueError, match="must declare accelerator.mode=none"):
            await router.dispatch(
                run.run_id,
                profile=profile(
                    environment,
                    {"mode": "required", "class": "h100-80g", "quota": 1},
                ),
                compute_pool_id="pool-a",
                image=IMAGE,
                command=["python", "-m", "worker"],
            )

        assert client.requests == []
        assert store.get_run(run.run_id).compute_request is None

    asyncio.run(scenario())


def test_gpu_profile_preserves_canonical_run_and_session_identity() -> None:
    async def scenario() -> None:
        store = InMemoryStore()
        _runtime, session, run = runtime_with_run(store)
        client = RecordingComputeClient(store)
        router = ExecutionProfileRouter(store=store, compute_client=client)

        allocation = await router.dispatch(
            run.run_id,
            profile=profile(
                "gpu-workload",
                {"mode": "required", "class": "h100-80g", "quota": 2},
            ),
            compute_pool_id="pool-a",
            image=IMAGE,
            command=["python", "-m", "train"],
        )

        assert allocation is not None
        assert len(client.requests) == 1
        request = client.requests[0]
        assert request["apiVersion"] == "compute.request/v0.1"
        assert request["profileRef"] == {"profileId": "gpu-worker", "revision": 7}
        assert request["correlation"] == {
            "agentId": session.agent_id,
            "sessionId": session.session_id,
            "runId": run.run_id,
        }
        assert request["workload"]["accelerator"] == {"class": "h100-80g", "quota": 2}
        assert request["workload"]["expectedGeneration"] == 0
        assert request["workload"]["image"] == IMAGE
        assert re.fullmatch(r"[0-9a-f]{32}", request["requestId"])
        assert re.fullmatch(r"sha256:[0-9a-f]{64}", request["intentDigest"])
        assert allocation["requestId"] == request["requestId"]
        assert allocation["state"] == "REQUESTED"  # Never promote ACK to ALLOCATED.

    asyncio.run(scenario())


def test_gpu_request_and_idempotency_key_survive_sqlite_runtime_restart(tmp_path) -> None:
    async def scenario() -> None:
        db_path = tmp_path / "compute-run.sqlite"
        store_before = SQLiteStore(db_path)
        runtime_before, session, run = runtime_with_run(store_before)
        client = RecordingComputeClient(store_before)
        router_before = ExecutionProfileRouter(store=store_before, compute_client=client)
        gpu_profile = profile(
            "gpu-workload",
            {"mode": "required", "class": "h100-80g", "quota": 1},
        )

        first = await router_before.dispatch(
            run.run_id,
            profile=gpu_profile,
            compute_pool_id="pool-a",
            image=IMAGE,
            command=["python", "-m", "train"],
        )
        first_request = copy.deepcopy(store_before.get_run(run.run_id).compute_request)
        assert first is not None and first_request is not None
        store_before.close()

        store_after = SQLiteStore(db_path)
        persisted_run = store_after.get_run(run.run_id)
        assert persisted_run.run_id == run.run_id
        assert persisted_run.session_id == session.session_id
        assert persisted_run.compute_request == first_request

        client_after = RecordingComputeClient(store_after)
        router_after = ExecutionProfileRouter(store=store_after, compute_client=client_after)
        await router_after.dispatch(
            run.run_id,
            profile=gpu_profile,
            compute_pool_id="pool-a",
            image=IMAGE,
            command=["python", "-m", "train"],
        )
        assert client_after.requests == [first_request]
        assert client_after.requests[0]["requestId"] == first_request["requestId"]
        store_after.close()

    asyncio.run(scenario())


def test_replaying_gpu_run_with_changed_intent_fails_closed() -> None:
    async def scenario() -> None:
        store = InMemoryStore()
        _runtime, _session, run = runtime_with_run(store)
        client = RecordingComputeClient(store)
        router = ExecutionProfileRouter(store=store, compute_client=client)
        gpu_profile = profile(
            "gpu-workload",
            {"mode": "required", "class": "h100-80g", "quota": 1},
        )

        await router.dispatch(
            run.run_id,
            profile=gpu_profile,
            compute_pool_id="pool-a",
            image=IMAGE,
            command=["python", "-m", "train"],
        )
        before = copy.deepcopy(store.get_run(run.run_id).compute_request)

        with pytest.raises(ComputeProfileConflict, match="immutable"):
            await router.dispatch(
                run.run_id,
                profile=gpu_profile,
                compute_pool_id="pool-b",
                image=IMAGE,
                command=["python", "-m", "train"],
            )

        assert store.get_run(run.run_id).compute_request == before
        assert len(client.requests) == 1

    asyncio.run(scenario())


def test_compute_allocation_identity_mismatch_fails_closed() -> None:
    class WrongClient:
        async def submit_or_reconcile(self, request: dict[str, Any]) -> dict[str, Any]:
            return {
                "requestId": request["requestId"],
                "projectId": "another-project",
                "computePoolId": request["computePoolId"],
                "workloadId": request["workload"]["id"],
                "generation": 1,
                "state": "REQUESTED",
            }

    async def scenario() -> None:
        store = InMemoryStore()
        _runtime, _session, run = runtime_with_run(store)
        router = ExecutionProfileRouter(store=store, compute_client=WrongClient())
        with pytest.raises(RuntimeError, match="identity mismatch"):
            await router.dispatch(
                run.run_id,
                profile=profile(
                    "gpu-workload",
                    {"mode": "required", "class": "h100-80g", "quota": 1},
                ),
                compute_pool_id="pool-a",
                image=IMAGE,
                command=["python", "-m", "train"],
            )

    asyncio.run(scenario())



class CommitThenLoseAckClient:
    def __init__(self) -> None:
        self.committed: dict[str, dict[str, Any]] = {}
        self.calls: list[dict[str, Any]] = []
        self.lose_first_ack = True

    async def submit_or_reconcile(
        self,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        self.calls.append(copy.deepcopy(request))
        request_id = request["requestId"]
        if request_id not in self.committed:
            self.committed[request_id] = {
                "requestId": request_id,
                "projectId": request["projectId"],
                "computePoolId": request["computePoolId"],
                "workloadId": request["workload"]["id"],
                "generation": 1,
                "state": "REQUESTED",
            }
        if self.lose_first_ack:
            self.lose_first_ack = False
            raise ConnectionError("simulated response loss after durable Control Plane commit")
        return copy.deepcopy(self.committed[request_id])


def test_lost_ack_retries_the_persisted_request_id() -> None:
    async def scenario() -> None:
        store = InMemoryStore()
        _runtime, _session, run = runtime_with_run(store)
        client = CommitThenLoseAckClient()
        router = ExecutionProfileRouter(store=store, compute_client=client)
        gpu_profile = profile(
            "gpu-workload",
            {"mode": "required", "class": "h100-80g", "quota": 1},
        )

        with pytest.raises(ConnectionError, match="response loss"):
            await router.dispatch(
                run.run_id,
                profile=gpu_profile,
                compute_pool_id="pool-a",
                image=IMAGE,
                command=["python", "-m", "train"],
            )
        persisted = store.get_run(run.run_id).compute_request
        assert persisted is not None

        recovered = await router.dispatch(
            run.run_id,
            profile=gpu_profile,
            compute_pool_id="pool-a",
            image=IMAGE,
            command=["python", "-m", "train"],
        )

        assert recovered is not None
        assert len(client.committed) == 1
        assert len(client.calls) == 2
        assert client.calls[0] == client.calls[1] == persisted
        assert recovered["requestId"] == persisted["requestId"]

    asyncio.run(scenario())


def test_concurrent_gpu_dispatches_share_one_atomic_request_key() -> None:
    async def scenario() -> None:
        store = InMemoryStore()
        _runtime, _session, run = runtime_with_run(store)
        client = RecordingComputeClient(store)
        router_a = ExecutionProfileRouter(store=store, compute_client=client)
        router_b = ExecutionProfileRouter(store=store, compute_client=client)
        gpu_profile = profile(
            "gpu-workload",
            {"mode": "required", "class": "h100-80g", "quota": 1},
        )

        first, second = await asyncio.gather(
            router_a.dispatch(
                run.run_id,
                profile=gpu_profile,
                compute_pool_id="pool-a",
                image=IMAGE,
                command=["python", "-m", "train"],
            ),
            router_b.dispatch(
                run.run_id,
                profile=gpu_profile,
                compute_pool_id="pool-a",
                image=IMAGE,
                command=["python", "-m", "train"],
            ),
        )

        assert first is not None and second is not None
        assert len(client.requests) == 2  # at-least-once client calls are expected
        assert client.requests[0] == client.requests[1]
        assert client.requests[0]["requestId"] == store.get_run(run.run_id).compute_request["requestId"]

    asyncio.run(scenario())


def test_stale_run_save_cannot_erase_compute_request_intent() -> None:
    async def scenario() -> None:
        store = InMemoryStore()
        _runtime, _session, run = runtime_with_run(store)
        client = RecordingComputeClient(store)
        router = ExecutionProfileRouter(store=store, compute_client=client)

        await router.dispatch(
            run.run_id,
            profile=profile(
                "gpu-workload",
                {"mode": "required", "class": "h100-80g", "quota": 1},
            ),
            compute_pool_id="pool-a",
            image=IMAGE,
            command=["python", "-m", "train"],
        )
        before = store.get_run(run.run_id).compute_request

        with pytest.raises(ValueError, match="atomic persistence"):
            store.save_run(run)  # stale snapshot lacks the committed compute intent

        assert store.get_run(run.run_id).compute_request == before

    asyncio.run(scenario())
