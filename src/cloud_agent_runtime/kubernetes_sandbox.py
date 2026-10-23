from __future__ import annotations

import json
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
    """Kubectl-backed disposable sandbox for recovery and isolation proofs.

    Network egress is deny-all until an explicitly reviewed policy extension is
    added. Each sandbox has its own emptyDir workspace; PVC/hostPath persistence
    is deliberately unsupported by this reference provider.
    """

    name = "kubernetes"

    def __init__(
        self,
        *,
        namespace: str = "agent-sandbox",
        image: str = "busybox:1.36",
        kubectl: Callable[..., KubectlResult] | None = None,
    ) -> None:
        self.namespace = namespace
        self.image = image
        self._kubectl = kubectl or self._run_kubectl

    @staticmethod
    def _run_kubectl(args: list[str], *, stdin: str | None = None) -> KubectlResult:
        result = subprocess.run(
            ["kubectl", *args],
            text=True,
            input=stdin,
            capture_output=True,
            check=False,
        )
        return KubectlResult(result.returncode, result.stdout, result.stderr)

    def _check(self, args: list[str], *, stdin: str | None = None) -> KubectlResult:
        result = self._kubectl(args, stdin=stdin)
        if result.returncode != 0:
            raise RuntimeError(
                f"kubectl {' '.join(args)} failed: {result.stderr.strip()}"
            )
        return result

    def ensure_namespace(self) -> None:
        probe = self._kubectl(["get", "namespace", self.namespace, "-o", "name"])
        if probe.returncode != 0:
            self._check(["create", "namespace", self.namespace])
        # Never silently use a namespace without the intended Pod Security
        # admission profile. Policy-label failure blocks sandbox allocation.
        self._check([
            "label",
            "namespace",
            self.namespace,
            "pod-security.kubernetes.io/enforce=restricted",
            "pod-security.kubernetes.io/audit=restricted",
            "pod-security.kubernetes.io/warn=restricted",
            "--overwrite",
        ])

    def _network_policy(self, sandbox_id: str) -> dict[str, object]:
        labels = {
            "app.kubernetes.io/name": "agent-sandbox",
            "agent.sandbox-id": sandbox_id,
        }
        return {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {
                "name": sandbox_id,
                "namespace": self.namespace,
                "labels": labels,
            },
            "spec": {
                "podSelector": {
                    "matchLabels": {"agent.sandbox-id": sandbox_id},
                },
                "policyTypes": ["Ingress", "Egress"],
                "ingress": [],
                "egress": [],
            },
        }

    def _pod(self, sandbox_id: str) -> dict[str, object]:
        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": sandbox_id,
                "namespace": self.namespace,
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
        self.ensure_namespace()
        sandbox_id = f"agent-sandbox-{uuid4().hex[:12]}"

        # Install the deny-all policy before the Pod can start. If Pod creation
        # fails, best-effort deletion removes the unused policy without hiding
        # the original allocation failure.
        self._check(
            ["apply", "-f", "-"],
            stdin=json.dumps(self._network_policy(sandbox_id)),
        )
        try:
            self._check(["apply", "-f", "-"], stdin=json.dumps(self._pod(sandbox_id)))
        except Exception:
            try:
                self._kubectl([
                    "-n", self.namespace, "delete", "networkpolicy", sandbox_id,
                    "--ignore-not-found=true", "--wait=false",
                ])
            except Exception:
                pass
            raise
        return Sandbox(sandbox_id, self.name, SandboxStatus.ALLOCATED)

    def bind(self, sandbox: Sandbox, *, session_id: str) -> Sandbox:
        self._check([
            "-n", self.namespace, "label", "pod", sandbox.sandbox_id,
            f"agent.session={session_id}", "--overwrite",
        ])
        sandbox.session_id = session_id
        sandbox.status = SandboxStatus.BOUND
        return sandbox

    def snapshot(self, sandbox: Sandbox) -> Sandbox:
        # This provider has no filesystem snapshot implementation. The ref is
        # a lifecycle marker only and must not be treated as durable workspace
        # content; durable artifacts belong in the separately authorized store.
        sandbox.snapshot_ref = f"k8s-lifecycle://{sandbox.sandbox_id}"
        sandbox.status = SandboxStatus.PAUSED
        return sandbox

    def resume(self, snapshot_ref: str, *, session_id: str) -> Sandbox:
        sandbox = self.allocate()
        sandbox.snapshot_ref = snapshot_ref
        return self.bind(sandbox, session_id=session_id)

    def terminate(self, sandbox: Sandbox) -> Sandbox:
        # Delete the Pod before its policy. If policy deletion fails, retry is
        # safe and the already-deleted Pod cannot retain workspace data.
        self._check([
            "-n", self.namespace, "delete", "pod", sandbox.sandbox_id,
            "--ignore-not-found=true", "--wait=false",
        ])
        self._check([
            "-n", self.namespace, "delete", "networkpolicy", sandbox.sandbox_id,
            "--ignore-not-found=true", "--wait=false",
        ])
        sandbox.status = SandboxStatus.TERMINATED
        return sandbox
