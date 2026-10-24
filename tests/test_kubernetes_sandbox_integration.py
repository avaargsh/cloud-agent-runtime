"""Real Kubernetes API recovery test, enabled in the disposable kind CI job.

This exercises namespace-scoped sandbox destruction against a real API server;
it does not claim production-cluster or production-provider acceptance.
"""
from __future__ import annotations

import os
import subprocess
import time
from uuid import uuid4

import pytest

from cloud_agent_runtime import AgentRuntime, InMemoryWorkflowDriver, SQLiteStore
from cloud_agent_runtime.kubernetes_sandbox import KubernetesSandboxProvider
from cloud_agent_runtime.temporal_driver import deterministic_cleanup_operation_id


pytestmark = pytest.mark.skipif(
    os.environ.get("CLOUD_AGENT_RUNTIME_KUBERNETES_INTEGRATION") != "1",
    reason="requires the disposable Kubernetes cluster configured by CI",
)


def _wait_until_namespace_is_absent(namespace: str, timeout_seconds: float = 30):
    deadline = time.monotonic() + timeout_seconds
    last_stderr = ""
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["kubectl", "get", "namespace", namespace, "-o", "name"],
            text=True,
            capture_output=True,
            check=False,
            timeout=10,
        )
        if result.returncode != 0:
            last_stderr = result.stderr
            if "not found" in result.stderr.lower() or "notfound" in result.stderr.lower():
                return
            raise AssertionError(
                f"kubectl get namespace failed for a reason other than absence: "
                f"{result.stderr.strip()}"
            )
        time.sleep(0.25)
    raise AssertionError(
        f"Namespace {namespace} was not deleted in time: {last_stderr}"
    )


def test_kubernetes_cleanup_timeout_recovers_after_runtime_restart(tmp_path):
    namespace_prefix = f"agent-recovery-{uuid4().hex[:8]}"
    database = tmp_path / "kubernetes-cleanup-recovery.db"
    first_store = None
    second_store = None
    provider = KubernetesSandboxProvider(
        namespace=namespace_prefix,
        image=os.environ.get(
            "CLOUD_AGENT_RUNTIME_SANDBOX_IMAGE",
            "busybox:1.36",
        ),
    )
    real_kubectl = provider._kubectl

    class TimeoutAfterNamespaceDeleteAccepted:
        """Delete the real per-sandbox Namespace, then lose its acknowledgement."""

        def __init__(self):
            self.injected = False
            self.delete_requests = 0

        def __call__(self, args, *, stdin=None):
            result = real_kubectl(args, stdin=stdin)
            is_sandbox_namespace_delete = (
                len(args) >= 3
                and args[:2] == ["delete", "namespace"]
                and args[2].startswith(namespace_prefix + "-sbx-")
            )
            if is_sandbox_namespace_delete:
                self.delete_requests += 1
                if not self.injected:
                    self.injected = True
                    # The real kubectl command has completed successfully and
                    # the Namespace is gone; only the response to the caller
                    # is lost, leaving the durable Run uncertain.
                    raise subprocess.TimeoutExpired(
                        cmd=["kubectl", *args],
                        timeout=0.01,
                    )
            return result

    ambiguous_kubectl = TimeoutAfterNamespaceDeleteAccepted()
    provider._kubectl = ambiguous_kubectl
    try:
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
        original_namespace = provider._namespace_for(original["sandbox_id"])

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

        cleanup_operation_id = deterministic_cleanup_operation_id(
            original["run_id"], original["sandbox_ref"]
        )
        before_restart = first_store.get_run(original["run_id"])
        first_cleanup_events = [
            event for event in before_restart.recovery_evidence
            if event["operation_id"] == cleanup_operation_id
        ]
        assert before_restart.recovery_operation_id == cleanup_operation_id
        assert before_restart.recovery_state == "unknown"
        assert [event["outcome"] for event in first_cleanup_events] == [
            "intent_persisted", "unknown"
        ]
        assert [event["attempt"] for event in first_cleanup_events] == ["1", "1"]

        # The API server deleted the complete Namespace, including emptyDir
        # workspace and policy, even though the caller lost the acknowledgement.
        _wait_until_namespace_is_absent(original_namespace)
        first_store.close()
        first_store = None

        second_store = SQLiteStore(database)
        restarted_provider = KubernetesSandboxProvider(
            namespace=namespace_prefix,
            image=provider.image,
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
        durable_cleanup_events = [
            event for event in durable_before_retry.recovery_evidence
            if event["operation_id"] == cleanup_operation_id
        ]
        assert durable_before_retry.recovery_operation_id == cleanup_operation_id
        assert durable_before_retry.recovery_state == "unknown"
        assert [event["outcome"] for event in durable_cleanup_events] == [
            "intent_persisted", "unknown"
        ]
        assert [event["attempt"] for event in durable_cleanup_events] == ["1", "1"]
        assert durable_session.session_id == original["session_id"]
        assert durable_session.agent_id == "recovery-test-agent"
        assert durable_session.release_id == "recovery-test-release"
        assert durable_session.tenant_id == "recovery-test-tenant"

        # Retry exactly the same cancellation/cleanup intent after reconstructing
        # Runtime, SQLite, and Provider. It must not allocate a replacement.
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
        recovered_cleanup_events = [
            event for event in recovered.recovery_evidence
            if event["operation_id"] == cleanup_operation_id
        ]
        assert recovered.recovery_operation_id == cleanup_operation_id
        assert recovered.recovery_state == "cleanup_acknowledged"
        assert [event["outcome"] for event in recovered_cleanup_events] == [
            "intent_persisted",
            "unknown",
            "intent_persisted",
            "observed_gone",
            "acknowledged",
        ]
        assert [event["attempt"] for event in recovered_cleanup_events] == [
            "1", "1", "2", "2", "2",
        ]
        assert [
            (event["from_status"], event["to_status"])
            for event in recovered.transition_events
        ] == [
            ("created", "running"),
            ("running", "cancelled"),
        ]
        _wait_until_namespace_is_absent(original_namespace)
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
                namespace_prefix,
                "--ignore-not-found=true",
                "--wait=false",
            ],
            text=True,
            capture_output=True,
            check=False,
            timeout=10,
        )
