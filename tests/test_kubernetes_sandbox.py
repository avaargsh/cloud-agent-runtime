import json

from cloud_agent_runtime import AgentRuntime, InMemoryWorkflowDriver
from cloud_agent_runtime.kubernetes_sandbox import KubernetesSandboxProvider, KubectlResult


class FakeKubectl:
    def __init__(self):
        self.calls = []
        self.namespace_exists = False
        self.pods = set()
        self.network_policies = set()
        self.manifests = []

    def __call__(self, args, *, stdin=None):
        self.calls.append((tuple(args), stdin))
        if args[:2] == ["get", "namespace"]:
            return KubectlResult(0 if self.namespace_exists else 1, "", "")
        if args[:2] == ["create", "namespace"]:
            self.namespace_exists = True
            return KubectlResult(0, "", "")
        if args[:2] == ["apply", "-f"]:
            assert stdin
            manifest = json.loads(stdin)
            self.manifests.append(manifest)
            name = manifest["metadata"]["name"]
            if manifest["kind"] == "Pod":
                self.pods.add(name)
            elif manifest["kind"] == "NetworkPolicy":
                self.network_policies.add(name)
            else:
                return KubectlResult(1, "", "unexpected manifest kind")
            return KubectlResult(0, "", "")
        if "label" in args:
            return KubectlResult(0, "", "")
        if "delete" in args:
            kind = args[args.index("delete") + 1]
            name = args[args.index(kind) + 1]
            if kind == "pod":
                self.pods.discard(name)
            elif kind == "networkpolicy":
                self.network_policies.discard(name)
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
    assert len(kubectl.network_policies) == 1


def test_terminate_is_orphan_cleanup_safe():
    kubectl = FakeKubectl()
    provider = KubernetesSandboxProvider(kubectl=kubectl)
    sandbox = provider.allocate()
    provider.bind(sandbox, session_id="session-1")

    provider.terminate(sandbox)
    # A second cleanup attempt must remain safe because both resources use
    # --ignore-not-found=true.
    provider.terminate(sandbox)

    pod_deletes = [
        args for args, _ in kubectl.calls
        if "delete" in args and args[args.index("delete") + 1] == "pod"
    ]
    policy_deletes = [
        args for args, _ in kubectl.calls
        if "delete" in args and args[args.index("delete") + 1] == "networkpolicy"
    ]
    assert len(pod_deletes) == 2
    assert len(policy_deletes) == 2
    assert all("--ignore-not-found=true" in args for args in pod_deletes + policy_deletes)
    assert not kubectl.pods
    assert not kubectl.network_policies


def test_sandbox_manifest_is_non_root_ephemeral_and_network_is_deny_all():
    kubectl = FakeKubectl()
    provider = KubernetesSandboxProvider(kubectl=kubectl)
    sandbox = provider.allocate()

    policy = next(item for item in kubectl.manifests if item["kind"] == "NetworkPolicy")
    assert policy["spec"]["policyTypes"] == ["Ingress", "Egress"]
    assert policy["spec"]["ingress"] == []
    assert policy["spec"]["egress"] == []
    assert policy["spec"]["podSelector"]["matchLabels"]["agent.sandbox-id"] == sandbox.sandbox_id

    pod = next(item for item in kubectl.manifests if item["kind"] == "Pod")
    spec = pod["spec"]
    assert spec["automountServiceAccountToken"] is False
    assert spec["securityContext"]["runAsNonRoot"] is True
    container = spec["containers"][0]
    assert container["securityContext"]["allowPrivilegeEscalation"] is False
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["securityContext"]["capabilities"]["drop"] == ["ALL"]
    mounts = {mount["mountPath"]: mount["name"] for mount in container["volumeMounts"]}
    assert mounts == {"/workspace": "workspace", "/tmp": "tmp"}
    volume_kinds = {
        volume["name"]: next(iter(set(volume) - {"name"}))
        for volume in spec["volumes"]
    }
    assert volume_kinds == {"workspace": "emptyDir", "tmp": "emptyDir"}
    assert "persistentVolumeClaim" not in json.dumps(pod)
    assert "hostPath" not in json.dumps(pod)

    namespace_labels = [
        args for args, _ in kubectl.calls
        if "label" in args and "namespace" in args
    ]
    assert namespace_labels
    assert any("pod-security.kubernetes.io/enforce=restricted" in arg for arg in namespace_labels[0])


def test_policy_creation_failure_blocks_pod_allocation():
    class PolicyFailKubectl(FakeKubectl):
        def __call__(self, args, *, stdin=None):
            if args[:2] == ["apply", "-f"] and stdin and json.loads(stdin)["kind"] == "NetworkPolicy":
                self.calls.append((tuple(args), stdin))
                return KubectlResult(1, "", "policy denied")
            return super().__call__(args, stdin=stdin)

    kubectl = PolicyFailKubectl()
    provider = KubernetesSandboxProvider(kubectl=kubectl)
    try:
        provider.allocate()
    except RuntimeError as exc:
        assert "policy denied" in str(exc)
    else:
        raise AssertionError("sandbox allocated without its network policy")
    assert not kubectl.pods
