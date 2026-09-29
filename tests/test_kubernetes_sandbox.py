from cloud_agent_runtime import AgentRuntime, InMemoryWorkflowDriver
from cloud_agent_runtime.kubernetes_sandbox import KubernetesSandboxProvider, KubectlResult


class FakeKubectl:
    def __init__(self):
        self.calls = []
        self.namespace_exists = False
        self.pods = set()

    def __call__(self, args, *, stdin=None):
        self.calls.append((tuple(args), stdin))
        if args[:2] == ["get", "namespace"]:
            return KubectlResult(0 if self.namespace_exists else 1, "", "")
        if args[:2] == ["create", "namespace"]:
            self.namespace_exists = True
            return KubectlResult(0, "", "")
        if args[:2] == ["apply", "-f"]:
            assert stdin and '"kind": "Pod"' in stdin
            import json
            pod = json.loads(stdin)["metadata"]["name"]
            self.pods.add(pod)
            return KubectlResult(0, "", "")
        if "label" in args:
            return KubectlResult(0, "", "")
        if "delete" in args:
            pod = args[args.index("pod") + 1]
            self.pods.discard(pod)
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
    assert len(delete_calls) == 2
    assert all("--ignore-not-found=true" in args for args in delete_calls)
