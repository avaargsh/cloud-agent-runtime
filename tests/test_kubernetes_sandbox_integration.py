"""Real Kubernetes API recovery test, enabled in the disposable kind CI job.

This is an API-server-backed test of the repository's Kubernetes adapter, not
a claim that the adapter is a production-complete sandbox implementation.
"""
from __future__ import annotations

import os
import subprocess
import time
from uuid import uuid4

import pytest

from cloud_agent_runtime import AgentRuntime, InMemoryWorkflowDriver, SQLiteStore
from cloud_agent_runtime.kubernetes_sandbox import KubernetesSandboxProvider


pytestmark = pytest.mark.skipif(
    os.environ.get("CLOUD_AGENT_RUNTIME_KUBERNETES_INTEGRATION") != "1",
    reason="requires the disposable Kubernetes cluster configured by CI",
)


def _wait_until_pod_is_absent(namespace: str, pod_name: str, timeout_seconds: float = 30):
    deadline = time.monotonic() + timeout_seconds
    last_stderr = ""
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["kubectl", "-n", namespace, "get", "pod", pod_name],
            text=True,
            capture_output=True,
            check=False,
            timeout=10,
        )
        if result.returncode != 0:
            last_stderr = result.stderr
            if "(NotFound)" in result.stderr or "not found" in result.stderr.lower():
                return
            raise AssertionError(
                f"kubectl get failed for a reason other than Pod absence: "
                f"{result.stderr.strip()}"
            )
        time.sleep(0.25)
    raise AssertionError(
        f"Pod {namespace}/{pod_name} was not deleted in time: {last_stderr}"
    )


def test_kubernetes_cleanup_timeout_recovers_after_runtime_restart(tmp_path):
    namespace = f"agent-recovery-{uuid4().hex[:8]}"
    database = tmp_path / "kubernetes-cleanup-recovery.db"
    first_store = None
    second_store = None
    try:
        provider = KubernetesSandboxProvider(
            namespace=namespace,
            image=os.environ.get(
                "CLOUD_AGENT_RUNTIME_SANDBOX_IMAGE",
                "busybox:1.36",
            ),
            command_timeout_seconds=20,
        )
        real_kubectl = provider._kubectl

        class TimeoutAfterDeleteAccepted:
            """Apply a real delete, then lose its acknowledgement once."""

            def __init__(self):
                self.injected = False
                self.delete_requests = 0

            def __call__(self, args, *, stdin=None):
                result = real_kubectl(args, stdin=stdin)
                if "delete" in args and "pod" in args:
                    self.delete_requests += 1
                    if not self.injected:
                        self.injected = True
                        raise subprocess.TimeoutExpired(
                            cmd=["kubectl", *args],
                            timeout=0.01,
                        )
                return result

        ambiguous_kubectl = TimeoutAfterDeleteAccepted()
        provider._kubectl = ambiguous_kubectl
        first_store = SQLiteStore(database)
        first_runtime = AgentRuntime(
            store=first_store,
            sandbox_provider=provider,
            workflow_driver=InMemoryWorkflowDriver(),
        )
        session = first_runtime.create_session(
            agent_id="recovery-test-agent",
            release_id="recovery-test-release",
            tenant_id="recovery-test-tenant",
        )
        run = first_runtime.start_run(session_id=session.session_id)
        original = {
            "run_id": run.run_id,
            "session_id": run.session_id,
            "workflow_ref": run.workflow_ref,
            "sandbox_ref": run.sandbox_ref,
            "sandbox_id": run.sandbox_binding.sandbox_id,
        }

        cancelled = first_runtime.cancel_run(
            run.run_id,
            reason="integration cleanup timeout",
            event_id="integration-cleanup-timeout",
        )
        assert ambiguous_kubectl.injected is True
        assert cancelled.status.value == "cancelled"
        assert cancelled.run_id == original["run_id"]
        assert cancelled.session_id == original["session_id"]
        assert cancelled.workflow_ref == original["workflow_ref"]
        assert cancelled.sandbox_ref == original["sandbox_ref"]
        assert cancelled.sandbox_binding.pending_cleanup_refs == [
            original["sandbox_ref"]
        ]
        assert cancelled.metadata.get("cancel_cleanup_completed_ref") is None
        assert ambiguous_kubectl.delete_requests == 1

        # The API server accepted deletion even though the caller saw a timeout.
        # The canonical Run must retain retry intent until a later request confirms
        # cleanup; it must not allocate another sandbox to recover this Run.
        _wait_until_pod_is_absent(namespace, original["sandbox_id"])
        first_store.close()
        first_store = None

        second_store = SQLiteStore(database)
        restarted_provider = KubernetesSandboxProvider(
            namespace=namespace,
            image=provider.image,
            command_timeout_seconds=20,
        )
        restarted = AgentRuntime(
            store=second_store,
            sandbox_provider=restarted_provider,
            workflow_driver=InMemoryWorkflowDriver(),
        )
        durable_before_retry = second_store.get_run(original["run_id"])
        durable_session = second_store.get_session(original["session_id"])
        assert durable_before_retry.run_id == original["run_id"]
        assert durable_before_retry.session_id == original["session_id"]
        assert durable_before_retry.workflow_ref == original["workflow_ref"]
        assert durable_before_retry.sandbox_ref == original["sandbox_ref"]
        assert durable_before_retry.sandbox_binding.sandbox_id == original["sandbox_id"]
        assert durable_before_retry.sandbox_binding.pending_cleanup_refs == [
            original["sandbox_ref"]
        ]
        assert durable_session.session_id == original["session_id"]
        assert durable_session.agent_id == "recovery-test-agent"
        assert durable_session.release_id == "recovery-test-release"
        assert durable_session.tenant_id == "recovery-test-tenant"

        # Retry exactly the same idempotency intent after reconstructing the
        # Runtime, SQLite connection, and Kubernetes provider adapter.
        recovered = restarted.cancel_run(
            original["run_id"],
            reason="integration cleanup timeout",
            event_id="integration-cleanup-timeout",
        )
        assert recovered.status.value == "cancelled"
        assert recovered.run_id == original["run_id"]
        assert recovered.session_id == original["session_id"]
        assert recovered.workflow_ref == original["workflow_ref"]
        assert recovered.sandbox_ref == original["sandbox_ref"]
        assert recovered.sandbox_binding.sandbox_id == original["sandbox_id"]
        assert recovered.sandbox_binding.pending_cleanup_refs == []
        assert (
            recovered.metadata["cancel_cleanup_completed_ref"]
            == original["sandbox_ref"]
        )
        assert [
            (event["from_status"], event["to_status"])
            for event in recovered.transition_events
        ] == [
            ("created", "running"),
            ("running", "cancelled"),
        ]
        _wait_until_pod_is_absent(namespace, original["sandbox_id"])
    finally:
        if first_store is not None:
            first_store.close()
        if second_store is not None:
            second_store.close()
        subprocess.run(
            [
                "kubectl",
                "delete",
                "namespace",
                namespace,
                "--ignore-not-found=true",
                "--wait=false",
            ],
            text=True,
            capture_output=True,
            check=False,
            timeout=10,
        )
