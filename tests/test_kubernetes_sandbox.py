import subprocess

import pytest

from cloud_agent_runtime import AgentRuntime, InMemoryWorkflowDriver
from cloud_agent_runtime.kubernetes_sandbox import KubernetesSandboxProvider, KubectlResult


class FakeKubectl:
    def __init__(self):
        self.calls = []
        self.namespace_exists = False
        self.namespaces = set()
        self.pods = set()

    def __call__(self, args, *, stdin=None):
        self.calls.append((tuple(args), stdin))
        if args[:2] == ["get", "namespace"]:
            namespace = args[2]
            exists = namespace in self.namespaces
            if exists:
                return KubectlResult(0, f"namespace/{namespace}", "")
            return KubectlResult(1, "", f'Error from server (NotFound): namespaces "{namespace}" not found')
        if "get" in args and "pod" in args:
            pod = args[args.index("pod") + 1]
            if pod in self.pods:
                return KubectlResult(0, f"pod/{pod}", "")
            return KubectlResult(
                1,
                "",
                f'Error from server (NotFound): pods "{pod}" not found',
            )
        if args[:2] == ["create", "namespace"]:
            self.namespaces.add(args[2])
            self.namespace_exists = True
            return KubectlResult(0, "", "")
        if args[:2] == ["apply", "-f"]:
            assert stdin
            import json
            manifest = json.loads(stdin)
            if manifest["kind"] == "Pod":
                self.pods.add(manifest["metadata"]["name"])
            return KubectlResult(0, "", "")
        if "label" in args:
            return KubectlResult(0, "", "")
        if "delete" in args:
            if "pod" in args:
                pod = args[args.index("pod") + 1]
                self.pods.discard(pod)
            elif "namespace" in args:
                namespace = args[args.index("namespace") + 1]
                self.namespaces.discard(namespace)
                self.namespace_exists = bool(self.namespaces)
                self.pods.clear()
            elif "networkpolicy" in args:
                return KubectlResult(0, "", "")
            return KubectlResult(0, "", "")
        return KubectlResult(1, "", "unexpected kubectl call")


def test_kubernetes_provider_replaces_pod_without_changing_run_identity():
    kubectl = FakeKubectl()
    provider = KubernetesSandboxProvider(kubectl=kubectl)
    runtime = AgentRuntime(
        sandbox_provider=provider,
        workflow_driver=InMemoryWorkflowDriver(),
    )
    session = runtime.create_session(
        agent_id="sre-agent",
        release_id="checkout-sre-golden-v1",
        tenant_id="golden-demo",
    )
    run = runtime.start_run(session_id=session.session_id)
    run_id = run.run_id
    workflow_ref = run.workflow_ref
    sandbox_a = run.sandbox_ref

    active = runtime._sandboxes[run_id]
    checkpoint = provider.snapshot(active).snapshot_ref
    assert checkpoint is not None

    # Simulate provider-observed loss before recovery. delete is idempotent.
    provider.terminate(active)
    active.status = active.status.BOUND

    rebound = runtime.rebind_run_sandbox(run_id, snapshot_ref=checkpoint)

    assert rebound.run_id == run_id
    assert rebound.workflow_ref == workflow_ref
    assert rebound.sandbox_ref != sandbox_a
    assert len(kubectl.pods) == 1


def test_terminate_is_orphan_cleanup_safe():
    kubectl = FakeKubectl()
    provider = KubernetesSandboxProvider(kubectl=kubectl)
    sandbox = provider.allocate()
    provider.bind(sandbox, session_id="session-1")

    provider.terminate(sandbox)
    # A second cleanup attempt must remain safe because kubectl uses
    # --ignore-not-found=true.
    provider.terminate(sandbox)

    delete_calls = [args for args, _ in kubectl.calls if "delete" in args]
    assert [args[args.index("delete") + 1] for args in delete_calls] == [
        "pod", "networkpolicy", "namespace", "pod", "networkpolicy", "namespace"
    ]
    assert all("--ignore-not-found=true" in args for args in delete_calls)



def test_delete_timeout_is_ambiguous_and_retry_uses_same_sandbox_id():
    class TimeoutAfterDelete(FakeKubectl):
        def __init__(self):
            super().__init__()
            self.injected = False

        def __call__(self, args, *, stdin=None):
            result = super().__call__(args, stdin=stdin)
            if "delete" in args and "pod" in args and not self.injected:
                # Model the API accepting deletion while the caller loses ACK.
                self.injected = True
                raise subprocess.TimeoutExpired(
                    cmd=["kubectl", *args],
                    timeout=0.01,
                )
            return result

    kubectl = TimeoutAfterDelete()
    provider = KubernetesSandboxProvider(
        command_timeout_seconds=0.25,
        kubectl=kubectl,
    )
    sandbox = provider.bind(provider.allocate(), session_id="session-stable")
    original_id = sandbox.sandbox_id

    with pytest.raises(RuntimeError, match="outcome may be unknown"):
        provider.terminate(sandbox)

    assert kubectl.injected is True
    assert original_id not in kubectl.pods
    assert sandbox.sandbox_id == original_id
    assert sandbox.status.value == "bound"

    # Retry the same idempotent delete after the ACK was lost.
    terminated = provider.terminate(sandbox)
    assert terminated.sandbox_id == original_id
    assert terminated.status.value == "terminated"
    delete_calls = [args for args, _ in kubectl.calls if "delete" in args]
    assert [args[args.index("delete") + 1] for args in delete_calls] == [
        "pod", "pod", "networkpolicy", "namespace"
    ]
    assert all("--ignore-not-found=true" in args for args in delete_calls)


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan"), True])
def test_kubectl_command_timeout_must_be_finite_and_positive(timeout):
    with pytest.raises(ValueError, match="command_timeout_seconds"):
        KubernetesSandboxProvider(command_timeout_seconds=timeout)


def test_kubernetes_provider_confirms_gone_with_independent_get():
    kubectl = FakeKubectl()
    provider = KubernetesSandboxProvider(kubectl=kubectl)
    sandbox = provider.bind(provider.allocate(), session_id="session-gone")
    assert provider.is_gone(sandbox.sandbox_id) is False

    provider.terminate(sandbox)
    assert provider.is_gone(sandbox.sandbox_id) is True


def test_kubernetes_provider_unknown_gone_status_fails_closed():
    def failed_get(args, *, stdin=None):
        if "get" in args and "pod" in args:
            return KubectlResult(1, "", "connection refused")
        return KubectlResult(1, "", "unexpected kubectl call")

    provider = KubernetesSandboxProvider(kubectl=failed_get)
    with pytest.raises(RuntimeError, match="could not independently establish"):
        provider.is_gone("pod-unknown")
