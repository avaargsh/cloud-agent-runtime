import json
from subprocess import CompletedProcess

import pytest

from cloud_agent_runtime import AgentRuntime, InMemoryStore, SQLiteStore
from cloud_agent_runtime.kubernetes_sandbox import KubectlResult, KubernetesSandboxProvider
from cloud_agent_runtime.store import SandboxAllocationIntent


class FakeKubectl:
    def __init__(self):
        self.namespaces = set()
        self.pods = {}
        self.policies = {}
        self.calls = []
        self.manifests = []
        self.fail_policy = False
        self.fail_namespace_delete_once = False

    def __call__(self, args, *, stdin=None):
        self.calls.append((tuple(args), stdin))
        if args[:2] == ["get", "namespace"]:
            ns = args[2]
            if ns in self.namespaces:
                return KubectlResult(0, f"namespace/{ns}", "")
            return KubectlResult(1, "", f'Error from server (NotFound): namespaces "{ns}" not found')
        if args[:2] == ["create", "namespace"]:
            self.namespaces.add(args[2])
            return KubectlResult(0, "", "")
        if args[:2] == ["label", "namespace"]:
            return KubectlResult(0 if args[2] in self.namespaces else 1, "", "")
        if args[:3] == ["apply", "-f", "-"]:
            manifest = json.loads(stdin or "{}")
            self.manifests.append(manifest)
            kind = manifest["kind"]
            ns = manifest["metadata"]["namespace"]
            name = manifest["metadata"]["name"]
            if kind == "NetworkPolicy":
                if self.fail_policy:
                    return KubectlResult(1, "", "network policy admission denied")
                self.policies[(ns, name)] = manifest
            elif kind == "Pod":
                self.pods[(ns, name)] = manifest
            else:
                return KubectlResult(1, "", f"unsupported kind {kind}")
            return KubectlResult(0, "", "")
        if "label" in args and "-n" in args:
            return KubectlResult(0, "", "")
        if args[:2] == ["delete", "namespace"]:
            ns = args[2]
            if self.fail_namespace_delete_once:
                self.fail_namespace_delete_once = False
                return KubectlResult(1, "", "injected namespace deletion failure")
            self.namespaces.discard(ns)
            self.pods = {k:v for k,v in self.pods.items() if k[0] != ns}
            self.policies = {k:v for k,v in self.policies.items() if k[0] != ns}
            return KubectlResult(0, "", "")
        if "delete" in args and "-n" in args:
            ns = args[args.index("-n") + 1]
            kind = args[args.index("delete") + 1]
            name = args[args.index(kind) + 1]
            if kind == "pod":
                self.pods.pop((ns, name), None)
            elif kind == "networkpolicy":
                self.policies.pop((ns, name), None)
            return KubectlResult(0, "", "")
        if "get" in args and "pod" in args and "-n" in args:
            ns = args[args.index("-n") + 1]
            name = args[args.index("pod") + 1]
            if (ns, name) in self.pods:
                return KubectlResult(0, f"pod/{name}", "")
            return KubectlResult(1, "", f'Error from server (NotFound): pods "{name}" not found')
        return KubectlResult(1, "", "unexpected kubectl invocation")


def test_per_sandbox_namespace_policy_and_workspace_are_restricted_and_ephemeral():
    kubectl = FakeKubectl()
    provider = KubernetesSandboxProvider(kubectl=kubectl)
    first = provider.allocate()
    second = provider.allocate()

    first_ns = provider._namespace_for(first.sandbox_id)
    second_ns = provider._namespace_for(second.sandbox_id)
    assert first_ns != second_ns
    assert {first_ns, second_ns}.issubset(kubectl.namespaces)
    assert [m["kind"] for m in kubectl.manifests] == ["NetworkPolicy", "Pod", "NetworkPolicy", "Pod"]

    for sandbox in (first, second):
        ns = provider._namespace_for(sandbox.sandbox_id)
        policy = kubectl.policies[(ns, "default-deny-all")]
        assert policy["spec"]["policyTypes"] == ["Ingress", "Egress"]
        assert policy["spec"]["ingress"] == []
        assert policy["spec"]["egress"] == []
        assert policy["spec"]["podSelector"] == {}
        pod = kubectl.pods[(ns, sandbox.sandbox_id)]
        spec = pod["spec"]
        assert spec["automountServiceAccountToken"] is False
        assert spec["securityContext"]["runAsNonRoot"] is True
        sec = spec["containers"][0]["securityContext"]
        assert sec["allowPrivilegeEscalation"] is False
        assert sec["readOnlyRootFilesystem"] is True
        assert sec["capabilities"]["drop"] == ["ALL"]
        assert {v["mountPath"]: v["name"] for v in spec["containers"][0]["volumeMounts"]} == {
            "/workspace": "workspace", "/tmp": "tmp"
        }
        assert {v["name"]: next(iter(set(v) - {"name"})) for v in spec["volumes"]} == {
            "workspace": "emptyDir", "tmp": "emptyDir"
        }
        assert "persistentVolumeClaim" not in json.dumps(pod)
        assert "hostPath" not in json.dumps(pod)

def test_network_policy_admission_failure_fails_closed():
    kubectl = FakeKubectl()
    kubectl.fail_policy = True
    provider = KubernetesSandboxProvider(kubectl=kubectl)
    try:
        provider.allocate()
    except RuntimeError as exc:
        assert "network policy admission denied" in str(exc)
    else:
        raise AssertionError("Pod allocated without a deny-all NetworkPolicy")
    assert not kubectl.pods
    assert not kubectl.policies
    assert not kubectl.namespaces


def test_namespace_destruction_failure_is_retryable_and_idempotent():
    kubectl = FakeKubectl()
    provider = KubernetesSandboxProvider(kubectl=kubectl)
    sandbox = provider.bind(provider.allocate(), session_id="session-a")
    sandbox_id = sandbox.sandbox_id
    ns = provider._namespace_for(sandbox_id)
    kubectl.fail_namespace_delete_once = True
    try:
        provider.terminate(sandbox)
    except RuntimeError as exc:
        assert "injected namespace deletion failure" in str(exc)
    else:
        raise AssertionError("namespace deletion failure was reported as success")
    assert sandbox.status.value == "bound"
    assert ns in kubectl.namespaces
    # A failed namespace deletion must not first remove the deny-all policy.
    # Otherwise an orphan Pod/resource could run without its isolation boundary.
    assert (ns, "default-deny-all") in kubectl.policies
    assert (ns, sandbox_id) in kubectl.pods
    assert provider.is_gone(sandbox_id) is False
    result = provider.terminate(sandbox)
    assert result.sandbox_id == sandbox_id
    assert result.status.value == "terminated"
    assert ns not in kubectl.namespaces
    assert provider.is_gone(sandbox_id) is True



def test_is_gone_requires_namespace_absence_not_only_pod_absence():
    kubectl = FakeKubectl()
    provider = KubernetesSandboxProvider(kubectl=kubectl)
    sandbox = provider.allocate()
    namespace = provider._namespace_for(sandbox.sandbox_id)

    # Simulate a pod already deleted while its namespace and policy remain.
    # That is not cleanup completion: namespace-scoped objects may still exist.
    kubectl.pods.pop((namespace, sandbox.sandbox_id))

    assert namespace in kubectl.namespaces
    assert (namespace, "default-deny-all") in kubectl.policies
    assert provider.is_gone(sandbox.sandbox_id) is False



def test_kubectl_process_deadline_covers_server_wait_timeout(monkeypatch):
    observed = {}

    def fake_run(command, **kwargs):
        observed["timeout"] = kwargs["timeout"]
        return CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(
        "cloud_agent_runtime.kubernetes_sandbox.subprocess.run",
        fake_run,
    )
    provider = KubernetesSandboxProvider()

    provider._run_kubectl([
        "delete", "namespace", "sandbox-a", "--timeout=60s",
    ])
    assert observed["timeout"] == 65.0

    provider._run_kubectl(["get", "namespace", "sandbox-a", "-o", "name"])
    assert observed["timeout"] == 15.0



def test_failed_allocation_cleanup_survives_store_reopen(tmp_path):
    kubectl = FakeKubectl()
    provider = KubernetesSandboxProvider(kubectl=kubectl)
    database = tmp_path / "orphan-allocation.db"
    store = SQLiteStore(database)
    runtime = AgentRuntime(store=store, sandbox_provider=provider)
    session = runtime.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
    )

    # NetworkPolicy admission fails after Namespace creation, and the
    # provider's best-effort Namespace deletion also fails. The intent must
    # remain durable and address the exact same Sandbox ID.
    kubectl.fail_policy = True
    kubectl.fail_namespace_delete_once = True
    with pytest.raises(RuntimeError, match="network policy admission denied"):
        runtime.start_run(session_id=session.session_id)

    intents = store.list_allocation_intents()
    assert len(intents) == 1
    intent = intents[0]
    assert intent.run_id
    assert intent.session_id == session.session_id
    assert intent.provider == "kubernetes"
    assert intent.state == "cleanup_pending"
    namespace = provider._namespace_for(intent.sandbox_id)
    assert namespace in kubectl.namespaces
    assert not kubectl.pods

    store.close()
    reopened = SQLiteStore(database)
    # Runtime startup retries the exact durable cleanup identity. It removes
    # the intent only after independently observing Namespace absence.
    recovered_runtime = AgentRuntime(store=reopened, sandbox_provider=provider)
    assert namespace not in kubectl.namespaces
    assert provider.is_gone(intent.sandbox_id) is True
    assert reopened.list_allocation_intents() == []
    # A second reconciliation is an idempotent no-op after the durable intent
    # was cleared; it cannot delete or otherwise mutate an unrelated resource.
    assert recovered_runtime.reconcile_sandbox_allocation_intents() == 0
    reopened.close()


def test_recent_allocation_intent_is_not_misclassified_as_orphan(tmp_path):
    kubectl = FakeKubectl()
    provider = KubernetesSandboxProvider(kubectl=kubectl)
    runtime = AgentRuntime(
        store=SQLiteStore(tmp_path / "concurrent-allocation.db"),
        sandbox_provider=provider,
    )
    sandbox_id = "agent-sandbox-concurrent1"
    sandbox = provider.allocate_with_id(sandbox_id)
    intent = SandboxAllocationIntent(
        run_id="run-in-flight",
        session_id="session-in-flight",
        provider="kubernetes",
        sandbox_id=sandbox_id,
        state="allocating",
        updated_at=100.0,
    )
    runtime.store.save_allocation_intent(intent)

    # Simulated competing allocation still within its bounded creation window:
    # no delete call is allowed, and the ownership intent remains untouched.
    reconciled = runtime.reconcile_sandbox_allocation_intents(
        stale_after_seconds=300.0,
        now=120.0,
    )
    assert reconciled == 0
    assert provider._namespace_for(sandbox_id) in kubectl.namespaces
    assert runtime.store.list_allocation_intents() == [intent]
    assert not any(
        args[:2] == ("delete", "namespace")
        for args, _ in kubectl.calls
    )
    provider.terminate(sandbox)
    runtime.store.close()



def test_recoverable_provider_fails_closed_without_intent_store_contract():
    class LegacyStore:
        """Pre-intent RuntimeStore wrapper that deliberately omits new methods."""

        def __init__(self):
            self.inner = InMemoryStore()

        def __getattr__(self, name):
            if name in {
                "save_allocation_intent",
                "list_allocation_intents",
                "delete_allocation_intent",
            }:
                raise AttributeError(name)
            return getattr(self.inner, name)

    kubectl = FakeKubectl()
    provider = KubernetesSandboxProvider(kubectl=kubectl)
    runtime = AgentRuntime(
        store=LegacyStore(),
        sandbox_provider=provider,
    )
    session = runtime.create_session(
        agent_id="coding-agent",
        release_id="coding-agent-v1",
        tenant_id="tenant-a",
    )

    with pytest.raises(RuntimeError, match="durable Sandbox allocation intents"):
        runtime.start_run(session_id=session.session_id)

    # Fail before any Namespace or Pod side effect, rather than silently
    # falling back to allocate() and losing the cleanup owner on a crash.
    assert not kubectl.namespaces
    assert not kubectl.pods
    assert not any(
        args[:2] == ("create", "namespace")
        for args, _ in kubectl.calls
    )
