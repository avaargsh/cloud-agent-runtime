import json

from cloud_agent_runtime import AgentRuntime, InMemoryWorkflowDriver
from cloud_agent_runtime.kubernetes_sandbox import KubernetesSandboxProvider, KubectlResult


class FakeKubectl:
    def __init__(self):
        self.calls = []
        self.namespaces = set()
        self.pods = {}
        self.network_policies = {}
        self.manifests = []

    def __call__(self, args, *, stdin=None):
        self.calls.append((tuple(args), stdin))
        if args[:2] == ["get", "namespace"]:
            if args[2] in self.namespaces:
                return KubectlResult(0, args[2], "")
            return KubectlResult(
                1,
                "",
                f'Error from server (NotFound): namespaces "{args[2]}" not found',
            )
        if args[:2] == ["create", "namespace"]:
            self.namespaces.add(args[2])
            return KubectlResult(0, "", "")
        if args[:2] == ["label", "namespace"]:
            return KubectlResult(
                0 if args[2] in self.namespaces else 1,
                "",
                "",
            )
        if args[:2] == ["apply", "-f"]:
            assert stdin
            manifest = json.loads(stdin)
            self.manifests.append(manifest)
            name = manifest["metadata"]["name"]
            namespace = manifest["metadata"]["namespace"]
            if manifest["kind"] == "Pod":
                self.pods[(namespace, name)] = namespace
            elif manifest["kind"] == "NetworkPolicy":
                self.network_policies[(namespace, name)] = namespace
            else:
                return KubectlResult(1, "", "unexpected manifest kind")
            return KubectlResult(0, "", "")
        if "label" in args:
            namespace = args[args.index("-n") + 1]
            return KubectlResult(
                0 if namespace in self.namespaces else 1,
                "",
                "",
            )
        if args[:2] == ["delete", "namespace"]:
            namespace = args[2]
            self.namespaces.discard(namespace)
            self.pods = {
                key: ns for key, ns in self.pods.items() if ns != namespace
            }
            self.network_policies = {
                key: ns for key, ns in self.network_policies.items()
                if ns != namespace
            }
            return KubectlResult(0, "", "")
        if "delete" in args:
            kind = args[args.index("delete") + 1]
            name = args[args.index(kind) + 1]
            namespace = args[args.index("-n") + 1]
            if kind == "pod":
                self.pods.pop((namespace, name), None)
            elif kind == "networkpolicy":
                self.network_policies.pop((namespace, name), None)
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
    assert len(kubectl.namespaces) == 1


def test_terminate_is_orphan_cleanup_safe():
    kubectl = FakeKubectl()
    provider = KubernetesSandboxProvider(kubectl=kubectl)
    sandbox = provider.allocate()
    provider.bind(sandbox, session_id="session-1")

    namespace = provider._namespace_for(sandbox.sandbox_id)
    provider.terminate(sandbox)
    # The second call observes an absent namespace and is idempotent.
    provider.terminate(sandbox)

    namespace_deletes = [
        args for args, _ in kubectl.calls
        if args[:2] == ("delete", "namespace")
    ]
    child_resource_deletes = [
        args for args, _ in kubectl.calls
        if "delete" in args
        and len(args) > args.index("delete") + 1
        and args[args.index("delete") + 1] in {"pod", "networkpolicy"}
    ]
    assert len(namespace_deletes) == 1
    assert child_resource_deletes == []
    assert "--wait=true" in namespace_deletes[0]
    assert "--ignore-not-found=true" in namespace_deletes[0]
    assert not kubectl.pods
    assert not kubectl.network_policies
    assert namespace not in kubectl.namespaces

def test_sandbox_manifest_is_non_root_ephemeral_and_network_is_deny_all():
    kubectl = FakeKubectl()
    provider = KubernetesSandboxProvider(kubectl=kubectl)
    sandbox = provider.allocate()

    policy = next(
        item for item in kubectl.manifests
        if item["kind"] == "NetworkPolicy"
    )
    pod = next(item for item in kubectl.manifests if item["kind"] == "Pod")
    assert policy["metadata"]["namespace"] == pod["metadata"]["namespace"]
    assert policy["metadata"]["name"] == "default-deny-all"
    assert policy["spec"]["policyTypes"] == ["Ingress", "Egress"]
    assert policy["spec"]["ingress"] == []
    assert policy["spec"]["egress"] == []
    assert policy["spec"]["podSelector"] == {}

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
        if args[:2] == ("label", "namespace")
    ]
    assert namespace_labels
    assert any(
        "pod-security.kubernetes.io/enforce=restricted" in arg
        for arg in namespace_labels[0]
    )
    assert any("agent.sandbox/managed=per-run" in arg for arg in namespace_labels[0])
    assert sandbox.sandbox_id in pod["metadata"]["name"]


def test_sandboxes_receive_distinct_namespaces_and_deny_policies():
    kubectl = FakeKubectl()
    provider = KubernetesSandboxProvider(kubectl=kubectl)
    first = provider.allocate()
    second = provider.allocate()

    first_pod = next(
        item for item in kubectl.manifests
        if item["kind"] == "Pod" and item["metadata"]["name"] == first.sandbox_id
    )
    second_pod = next(
        item for item in kubectl.manifests
        if item["kind"] == "Pod" and item["metadata"]["name"] == second.sandbox_id
    )
    assert first_pod["metadata"]["namespace"] != second_pod["metadata"]["namespace"]
    assert first_pod["metadata"]["namespace"] in kubectl.namespaces
    assert second_pod["metadata"]["namespace"] in kubectl.namespaces
    assert len(kubectl.network_policies) == 2
    policies = [
        policy for policy in kubectl.manifests
        if policy["kind"] == "NetworkPolicy"
    ]
    assert len(policies) == 2
    assert all(policy["spec"]["podSelector"] == {} for policy in policies)
    assert len({policy["metadata"]["namespace"] for policy in policies}) == 2


def test_policy_creation_failure_blocks_pod_allocation():
    class PolicyFailKubectl(FakeKubectl):
        def __call__(self, args, *, stdin=None):
            if (
                args[:2] == ["apply", "-f"]
                and stdin
                and json.loads(stdin)["kind"] == "NetworkPolicy"
            ):
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
    assert not kubectl.network_policies
    assert not kubectl.namespaces
