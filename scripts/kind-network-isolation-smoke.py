"""Live Calico NetworkPolicy + sandbox destruction/recovery proof.

Run only in a disposable cluster with a NetworkPolicy-enforcing CNI installed.
The script intentionally fails once during retired-namespace cleanup, then
retries the same durable Run cleanup state.
"""
from __future__ import annotations

import subprocess
import time

from cloud_agent_runtime import (
    AgentRuntime,
    InMemoryWorkflowDriver,
    KubernetesSandboxProvider,
)
from cloud_agent_runtime.kubernetes_sandbox import KubectlResult


def command(args: list[str], *, stdin: str | None = None, timeout: int = 30):
    result = subprocess.run(
        args,
        text=True,
        input=stdin,
        capture_output=True,
        check=False,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"command failed: {' '.join(args)}: {result.stderr.strip()}"
        )
    return result.stdout.strip()


def k(*args: str, timeout: int = 30) -> str:
    return command(["kubectl", *args], timeout=timeout)


def wait_namespace_gone(namespace: str) -> None:
    for _ in range(75):
        probe = subprocess.run(
            ["kubectl", "get", "namespace", namespace, "-o", "name"],
            text=True,
            capture_output=True,
            check=False,
            timeout=10,
        )
        if probe.returncode != 0:
            if "not found" in probe.stderr.lower():
                return
            raise RuntimeError(
                f"cannot verify namespace cleanup for {namespace}: {probe.stderr.strip()}"
            )
        time.sleep(1)
    raise RuntimeError(f"namespace deletion did not converge: {namespace}")


def denied_by_policy(namespace: str, pod: str, url: str) -> None:
    # First establish that the target's server is healthy locally. This helper
    # is only called after the positive localhost probe succeeds.
    out = k(
        "-n", namespace, "exec", pod, "--", "sh", "-c",
        f"wget -T 3 -qO- '{url}' >/dev/null 2>&1; rc=$?; test $rc -ne 0",
        timeout=12,
    )
    del out


failure = {
    "namespace": "",
    "armed": False,
    "injected": False,
}


def kubectl_for_provider(args: list[str], *, stdin: str | None = None) -> KubectlResult:
    if (
        failure["armed"]
        and not failure["injected"]
        and len(args) >= 3
        and args[0:2] == ["delete", "namespace"]
        and args[2] == failure["namespace"]
    ):
        failure["injected"] = True
        return KubectlResult(1, "", "injected one-time namespace deletion failure")
    result = subprocess.run(
        ["kubectl", *args],
        text=True,
        input=stdin,
        capture_output=True,
        check=False,
        timeout=75,
    )
    return KubectlResult(result.returncode, result.stdout, result.stderr)


provider = KubernetesSandboxProvider(
    namespace="agent-sandbox-live-proof",
    image="busybox:1.36",
    kubectl=kubectl_for_provider,
)
runtime = AgentRuntime(
    sandbox_provider=provider,
    workflow_driver=InMemoryWorkflowDriver(),
)

tenant_b_namespace = "tenant-b-network-probe"
tenant_b_pod = "tenant-b-probe"
run = None
sandbox_b = None

try:
    session = runtime.create_session(
        agent_id="network-proof-agent-a",
        release_id="network-proof-v1",
        tenant_id="tenant-a",
    )
    run = runtime.start_run(session_id=session.session_id)
    sandbox_a = runtime._sandboxes[run.run_id]
    sandbox_a_id = sandbox_a.sandbox_id
    sandbox_a_ref = run.sandbox_ref
    sandbox_a_namespace = provider._namespace_for(sandbox_a_id)
    k(
        "-n", sandbox_a_namespace, "wait",
        "--for=condition=Ready", f"pod/{sandbox_a_id}", "--timeout=120s",
    )

    # A reachable loopback response proves the server is listening before
    # treating cross-tenant timeout as policy enforcement evidence.
    k(
        "-n", sandbox_a_namespace, "exec", sandbox_a_id, "--", "sh", "-c",
        "printf 'tenant-a-secret\\n' > /workspace/secret.txt && "
        "busybox httpd -p 8080 -h /workspace",
        timeout=15,
    )
    local = k(
        "-n", sandbox_a_namespace, "exec", sandbox_a_id, "--",
        "wget", "-T", "3", "-qO-", "http://127.0.0.1:8080/secret.txt",
        timeout=12,
    )
    if local.strip() != "tenant-a-secret":
        raise RuntimeError("sandbox A positive localhost HTTP probe did not return its marker")

    # A separate, restricted tenant namespace has no NetworkPolicy of its own;
    # the destination sandbox ingress rule must still deny the connection.
    k("create", "namespace", tenant_b_namespace)
    k(
        "label", "namespace", tenant_b_namespace,
        "pod-security.kubernetes.io/enforce=restricted",
        "pod-security.kubernetes.io/audit=restricted",
        "pod-security.kubernetes.io/warn=restricted",
        "--overwrite",
    )
    probe_manifest = """{
      "apiVersion":"v1",
      "kind":"Pod",
      "metadata":{"name":"tenant-b-probe","namespace":"tenant-b-network-probe","labels":{"app":"tenant-b-probe"}},
      "spec":{
        "automountServiceAccountToken":false,
        "restartPolicy":"Never",
        "securityContext":{"runAsNonRoot":true,"runAsUser":10001,"runAsGroup":10001,"fsGroup":10001,"seccompProfile":{"type":"RuntimeDefault"}},
        "containers":[{
          "name":"probe","image":"busybox:1.36",
          "command":["sh","-c","mkdir -p /tmp/www; printf 'tenant-b\\n' > /tmp/www/index.html; busybox httpd -p 8081 -h /tmp/www; sleep 3600"],
          "securityContext":{"allowPrivilegeEscalation":false,"readOnlyRootFilesystem":true,"capabilities":{"drop":["ALL"]}},
          "volumeMounts":[{"name":"tmp","mountPath":"/tmp"}],
          "resources":{"requests":{"cpu":"50m","memory":"32Mi","ephemeral-storage":"32Mi"},"limits":{"cpu":"200m","memory":"128Mi","ephemeral-storage":"256Mi"}}
        }],
        "volumes":[{"name":"tmp","emptyDir":{"sizeLimit":"256Mi"}}]
      }
    }"""
    k("apply", "-f", "-", stdin=probe_manifest)
    k(
        "-n", tenant_b_namespace, "wait",
        "--for=condition=Ready", f"pod/{tenant_b_pod}", "--timeout=120s",
    )
    tenant_b_ip = k(
        "-n", tenant_b_namespace, "get", "pod", tenant_b_pod,
        "-o", "jsonpath={.status.podIP}",
    )
    if not tenant_b_ip:
        raise RuntimeError("tenant B probe Pod has no IP")
    probe_local = k(
        "-n", tenant_b_namespace, "exec", tenant_b_pod, "--",
        "wget", "-T", "3", "-qO-", "http://127.0.0.1:8081/",
        timeout=12,
    )
    if probe_local.strip() != "tenant-b":
        raise RuntimeError("tenant B positive localhost HTTP probe failed")

    sandbox_a_ip = k(
        "-n", sandbox_a_namespace, "get", "pod", sandbox_a_id,
        "-o", "jsonpath={.status.podIP}",
    )
    denied_by_policy(
        tenant_b_namespace,
        tenant_b_pod,
        f"http://{sandbox_a_ip}:8080/secret.txt",
    )
    # Sandbox egress must also be isolated from a healthy server in tenant B.
    denied_by_policy(
        sandbox_a_namespace,
        sandbox_a_id,
        f"http://{tenant_b_ip}:8081/",
    )

    # Delete only the old Pod to model provider loss. Keep the namespace and
    # policy so the runtime's normal durable retired-binding cleanup can act.
    k(
        "-n", sandbox_a_namespace, "delete", "pod", sandbox_a_id,
        "--ignore-not-found=true", "--wait=true", "--timeout=60s",
    )
    failure["namespace"] = sandbox_a_namespace
    failure["armed"] = True
    rebound = runtime.rebind_run_sandbox(
        run.run_id,
        rebind_key="calico-network-isolation-recovery-v1",
    )
    sandbox_b = runtime._sandboxes[run.run_id]
    sandbox_b_id = sandbox_b.sandbox_id
    sandbox_b_namespace = provider._namespace_for(sandbox_b_id)
    if not failure["injected"]:
        raise RuntimeError("expected cleanup failure injection was not exercised")
    if rebound.run_id != run.run_id or rebound.session_id != session.session_id:
        raise RuntimeError("recovery changed canonical Run or Session identity")
    if rebound.sandbox_ref == sandbox_a_ref:
        raise RuntimeError("recovery did not select a new sandbox binding")
    if (
        rebound.sandbox_binding is None
        or rebound.sandbox_ref not in (sandbox_b.sandbox_id, f"sandbox://kubernetes/{sandbox_b_id}")
        or f"sandbox://kubernetes/{sandbox_a_id}" not in rebound.sandbox_binding.pending_cleanup_refs
    ):
        raise RuntimeError("failed cleanup was not durably recorded for retry")

    # The failed delete must be visible as pending cleanup, not reported as
    # success. Replaying cleanup converges the old namespace without replacing B.
    k(
        "-n", sandbox_b_namespace, "wait",
        "--for=condition=Ready", f"pod/{sandbox_b_id}", "--timeout=120s",
    )
    replacement_ref = rebound.sandbox_ref
    cleaned = runtime.cleanup_retired_sandboxes(run.run_id)
    if cleaned.sandbox_binding is None or cleaned.sandbox_binding.pending_cleanup_refs:
        raise RuntimeError("retired workspace cleanup did not converge on retry")
    if cleaned.sandbox_ref != replacement_ref:
        raise RuntimeError("cleanup retry changed the selected replacement sandbox")
    wait_namespace_gone(sandbox_a_namespace)

    # The newly created emptyDir workspace must not inherit the previous
    # tenant's marker. Verify the new server is healthy before cross-tenant
    # denial assertions for the new namespace.
    k(
        "-n", sandbox_b_namespace, "exec", sandbox_b_id, "--", "sh", "-c",
        "test ! -e /workspace/secret.txt && "
        "printf 'sandbox-b-ready\\n' > /workspace/health.txt && "
        "busybox httpd -p 8080 -h /workspace",
        timeout=15,
    )
    health = k(
        "-n", sandbox_b_namespace, "exec", sandbox_b_id, "--",
        "wget", "-T", "3", "-qO-", "http://127.0.0.1:8080/health.txt",
        timeout=12,
    )
    if health.strip() != "sandbox-b-ready":
        raise RuntimeError("replacement workspace positive localhost probe failed")
    sandbox_b_ip = k(
        "-n", sandbox_b_namespace, "get", "pod", sandbox_b_id,
        "-o", "jsonpath={.status.podIP}",
    )
    denied_by_policy(
        tenant_b_namespace,
        tenant_b_pod,
        f"http://{sandbox_b_ip}:8080/health.txt",
    )
    denied_by_policy(
        sandbox_b_namespace,
        sandbox_b_id,
        f"http://{tenant_b_ip}:8081/",
    )

    # Finally verify ordinary termination destroys the replacement namespace.
    provider.terminate(sandbox_b)
    wait_namespace_gone(sandbox_b_namespace)
    k("delete", "namespace", tenant_b_namespace, "--wait=true", "--timeout=60s")
    print("PASS")
    print(f"run_id={run.run_id}")
    print(f"session_id={session.session_id}")
    print(f"sandbox_a_id={sandbox_a_id}")
    print(f"sandbox_a_namespace={sandbox_a_namespace}")
    print(f"sandbox_b_id={sandbox_b_id}")
    print(f"sandbox_b_namespace={sandbox_b_namespace}")
    print("cni=calico")
    print("cross_tenant_ingress=denied")
    print("sandbox_egress=denied")
    print("workspace_isolation=passed")
    print("cleanup_retry=passed")
    print("namespace_destruction=passed")
finally:
    # Best effort for failure paths; the cluster is disposable and workflow
    # cleanup deletes it even if a provider fault intentionally leaves residue.
    if sandbox_b is not None and sandbox_b.status.value != "terminated":
        try:
            provider.terminate(sandbox_b)
        except Exception:
            pass
    try:
        k("delete", "namespace", tenant_b_namespace, "--ignore-not-found=true", "--wait=false")
    except Exception:
        pass
