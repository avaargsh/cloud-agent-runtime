from __future__ import annotations

import json
import math
import subprocess
from dataclasses import dataclass
from typing import Callable
from uuid import uuid4

from .sandbox import Sandbox, SandboxStatus


@dataclass(frozen=True)
class KubectlResult:
    returncode: int
    stdout: str
    stderr: str


class KubernetesSandboxProvider:
    """Kubectl-backed, ephemeral sandbox for recovery and isolation proofs.

    Each Sandbox receives a dedicated namespace, reducing cross-Run resource
    adjacency and preventing policies in another sandbox's namespace from
    applying to it. Egress is deny-all until an explicitly reviewed policy
    extension is added. PVC/hostPath persistence is unsupported.
    """

    name = "kubernetes"

    def __init__(
        self,
        *,
        namespace: str = "agent-sandbox",
        image: str = "busybox:1.36",
        command_timeout_seconds: float = 30.0,
        kubectl: Callable[..., KubectlResult] | None = None,
    ) -> None:
        if (
            isinstance(command_timeout_seconds, bool)
            or not isinstance(command_timeout_seconds, (int, float))
            or not math.isfinite(float(command_timeout_seconds))
            or command_timeout_seconds <= 0
        ):
            raise ValueError("command_timeout_seconds must be a finite positive number")
        self.namespace = namespace
        self.image = image
        self.command_timeout_seconds = float(command_timeout_seconds)
        self._kubectl = kubectl or self._run_kubectl

    def _namespace_for(self, sandbox_id: str) -> str:
        # Keep the namespace within Kubernetes' DNS-label limit.
        prefix = self.namespace[:38].rstrip("-")
        suffix = sandbox_id.rsplit("-", 1)[-1]
        return f"{prefix}-sbx-{suffix}"

    def _invoke_kubectl(
        self,
        args: list[str],
        *,
        stdin: str | None = None,
    ) -> KubectlResult:
        try:
            return self._kubectl(args, stdin=stdin)
        except subprocess.TimeoutExpired as exc:
            command = "kubectl " + " ".join(args)
            raise RuntimeError(
                f"{command} timed out after "
                f"{self.command_timeout_seconds:g} seconds; outcome may be unknown"
            ) from exc

    def _run_kubectl(self, args: list[str], *, stdin: str | None = None) -> KubectlResult:
        try:
            result = subprocess.run(
                ["kubectl", *args],
                text=True,
                input=stdin,
                capture_output=True,
                check=False,
                timeout=self.command_timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"kubectl {' '.join(args)} timed out after "
                f"{self.command_timeout_seconds:g} seconds; outcome may be unknown"
            ) from exc
        return KubectlResult(result.returncode, result.stdout, result.stderr)

    def ensure_namespace(self, namespace: str | None = None) -> None:
        target = namespace or self.namespace
        probe = self._invoke_kubectl(["get", "namespace", target, "-o", "name"])
        created = False
        if probe.returncode != 0:
            self._check(["create", "namespace", target])
            created = True
        try:
            # A failure to apply the restricted admission profile must block
            # sandbox creation rather than silently weakening isolation.
            self._check([
                "label",
                "namespace",
                target,
                "pod-security.kubernetes.io/enforce=restricted",
                "pod-security.kubernetes.io/audit=restricted",
                "pod-security.kubernetes.io/warn=restricted",
                "agent.sandbox/managed=per-run",
                "--overwrite",
            ])
        except Exception:
            if created:
                self._delete_namespace_best_effort(target)
            raise

    def _network_policy(self, sandbox_id: str, namespace: str) -> dict[str, object]:
        # A namespace is dedicated to one Sandbox, so select every Pod in it.
        # NetworkPolicies are additive; a competing allow rule in this same
        # namespace could widen access, therefore this namespace must remain
        # under provider-only RBAC and must not receive unrelated allow policies.
        return {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {
                "name": "default-deny-all",
                "namespace": namespace,
                "labels": {"agent.sandbox/managed": "per-run"},
            },
            "spec": {
                "podSelector": {},
                "policyTypes": ["Ingress", "Egress"],
                "ingress": [],
                "egress": [],
            },
        }

    def _pod(self, sandbox_id: str, namespace: str) -> dict[str, object]:
        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": sandbox_id,
                "namespace": namespace,
                "labels": {
                    "app.kubernetes.io/name": "agent-sandbox",
                    "agent.sandbox-id": sandbox_id,
                },
            },
            "spec": {
                "restartPolicy": "Never",
                "automountServiceAccountToken": False,
                "securityContext": {
                    "runAsNonRoot": True,
                    "runAsUser": 10001,
                    "runAsGroup": 10001,
                    "fsGroup": 10001,
                    "seccompProfile": {"type": "RuntimeDefault"},
                },
                "containers": [{
                    "name": "sandbox",
                    "image": self.image,
                    "command": ["sh", "-c", "trap : TERM INT; sleep infinity & wait"],
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "readOnlyRootFilesystem": True,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "resources": {
                        "requests": {
                            "cpu": "100m",
                            "memory": "64Mi",
                            "ephemeral-storage": "64Mi",
                        },
                        "limits": {
                            "cpu": "500m",
                            "memory": "256Mi",
                            "ephemeral-storage": "1Gi",
                        },
                    },
                    "volumeMounts": [
                        {"name": "workspace", "mountPath": "/workspace"},
                        {"name": "tmp", "mountPath": "/tmp"},
                    ],
                }],
                "volumes": [
                    {"name": "workspace", "emptyDir": {"sizeLimit": "1Gi"}},
                    {"name": "tmp", "emptyDir": {"sizeLimit": "256Mi"}},
                ],
            },
        }

    def allocate(self) -> Sandbox:
        sandbox_id = f"agent-sandbox-{uuid4().hex[:12]}"
        namespace = self._namespace_for(sandbox_id)
        self.ensure_namespace(namespace)

        # Create deny-all before the Pod can start. A policy error leaves no
        # Pod and triggers best-effort namespace cleanup.
        try:
            self._check(
                ["apply", "-f", "-"],
                stdin=json.dumps(self._network_policy(sandbox_id, namespace)),
            )
        except Exception:
            self._delete_namespace_best_effort(namespace)
            raise

        try:
            self._check(
                ["apply", "-f", "-"],
                stdin=json.dumps(self._pod(sandbox_id, namespace)),
            )
        except Exception:
            self._delete_namespace_best_effort(namespace)
            raise
        return Sandbox(sandbox_id, self.name, SandboxStatus.ALLOCATED)

    def bind(self, sandbox: Sandbox, *, session_id: str) -> Sandbox:
        namespace = self._namespace_for(sandbox.sandbox_id)
        self._check([
            "-n", namespace, "label", "pod", sandbox.sandbox_id,
            f"agent.session={session_id}", "--overwrite",
        ])
        sandbox.session_id = session_id
        sandbox.status = SandboxStatus.BOUND
        return sandbox

    def snapshot(self, sandbox: Sandbox) -> Sandbox:
        # This provider has no filesystem snapshot implementation. The ref is
        # a lifecycle marker only; durable artifacts belong in the separately
        # authorized artifact/state store.
        sandbox.snapshot_ref = f"k8s-lifecycle://{sandbox.sandbox_id}"
        sandbox.status = SandboxStatus.PAUSED
        return sandbox

    def resume(self, snapshot_ref: str, *, session_id: str) -> Sandbox:
        sandbox = self.allocate()
        sandbox.snapshot_ref = snapshot_ref
        return self.bind(sandbox, session_id=session_id)

    def terminate(self, sandbox: Sandbox) -> Sandbox:
        namespace = self._namespace_for(sandbox.sandbox_id)
        probe = self._invoke_kubectl(["get", "namespace", namespace, "-o", "name"])
        if probe.returncode != 0:
            if "not found" in probe.stderr.lower():
                # Idempotent cleanup after an earlier namespace deletion.
                sandbox.status = SandboxStatus.TERMINATED
                return sandbox
            raise RuntimeError(
                f"kubectl get namespace {namespace} failed: {probe.stderr.strip()}"
            )

        # Explicit deletes make provider cleanup observable and retryable;
        # namespace deletion then removes unexpected leftover resources.
        # API ACK is not proof that async namespace finalization ended.
        self._check([
            "-n", namespace, "delete", "pod", sandbox.sandbox_id,
            "--ignore-not-found=true", "--wait=false",
        ])
        self._check([
            "-n", namespace, "delete", "networkpolicy", "default-deny-all",
            "--ignore-not-found=true", "--wait=false",
        ])
        # Do not acknowledge termination until Kubernetes confirms the
        # namespace (and its ephemeral workspace) has disappeared. A timeout
        # raises so the runtime can retain cleanup intent and retry.
        self._check([
            "delete", "namespace", namespace,
            "--ignore-not-found=true", "--wait=true", "--timeout=60s",
        ])
        sandbox.status = SandboxStatus.TERMINATED
        return sandbox

    def is_gone(self, sandbox_id: str) -> bool:
        """Confirm sandbox namespace absence; ambiguous API errors fail closed."""
        namespace = self._namespace_for(sandbox_id)
        result = self._invoke_kubectl(
            ["get", "namespace", namespace, "-o", "name"]
        )
        if result.returncode == 0:
            return False
        detail = f"{result.stdout}\n{result.stderr}".lower()
        if "not found" in detail or "notfound" in detail:
            return True
        raise RuntimeError(
            f"could not independently establish whether sandbox {sandbox_id} is Gone: "
            f"{result.stderr.strip()}"
        )
