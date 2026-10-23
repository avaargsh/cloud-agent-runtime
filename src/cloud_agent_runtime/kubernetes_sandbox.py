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
    """Small kubectl-backed provider for disposable kind recovery proofs.

    This is intentionally not a general production sandbox implementation.
    It creates one Pod per sandbox and uses provider identity/loss as the
    integration boundary for the Run recovery contract.
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
            raise ValueError(
                "command_timeout_seconds must be a finite positive number"
            )
        self.namespace = namespace
        self.image = image
        self.command_timeout_seconds = float(command_timeout_seconds)
        self._kubectl = kubectl or self._run_kubectl

    def _run_kubectl(
        self,
        args: list[str],
        *,
        stdin: str | None = None,
    ) -> KubectlResult:
        result = subprocess.run(
            ["kubectl", *args],
            text=True,
            input=stdin,
            capture_output=True,
            check=False,
            timeout=self.command_timeout_seconds,
        )
        return KubectlResult(result.returncode, result.stdout, result.stderr)

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

    def _check(self, args: list[str], *, stdin: str | None = None) -> KubectlResult:
        result = self._invoke_kubectl(args, stdin=stdin)
        if result.returncode != 0:
            raise RuntimeError(
                f"kubectl {' '.join(args)} failed: {result.stderr.strip()}"
            )
        return result

    def ensure_namespace(self) -> None:
        probe = self._invoke_kubectl(
            ["get", "namespace", self.namespace, "-o", "name"]
        )
        if probe.returncode != 0:
            self._check(["create", "namespace", self.namespace])

    def allocate(self) -> Sandbox:
        self.ensure_namespace()
        sandbox_id = f"agent-sandbox-{uuid4().hex[:12]}"
        manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": sandbox_id,
                "namespace": self.namespace,
                "labels": {"app.kubernetes.io/name": "agent-sandbox"},
            },
            "spec": {
                "restartPolicy": "Never",
                "containers": [{
                    "name": "sandbox",
                    "image": self.image,
                    "command": ["sh", "-c", "trap : TERM INT; sleep infinity & wait"],
                }],
            },
        }
        self._check(["apply", "-f", "-"], stdin=json.dumps(manifest))
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
        # Kubernetes Pod state is intentionally not treated as durable state.
        # The snapshot ref names the durable Run checkpoint supplied by the
        # upper runtime layer; a replacement Pod is always newly allocated.
        sandbox.snapshot_ref = f"k8s-checkpoint://{sandbox.sandbox_id}"
        sandbox.status = SandboxStatus.PAUSED
        return sandbox

    def resume(self, snapshot_ref: str, *, session_id: str) -> Sandbox:
        sandbox = self.allocate()
        sandbox.snapshot_ref = snapshot_ref
        return self.bind(sandbox, session_id=session_id)

    def terminate(self, sandbox: Sandbox) -> Sandbox:
        # A timed-out kubectl command has an ambiguous outcome: the API server
        # may already have accepted deletion. Leave the object unmarked so the
        # durable caller can retry the same idempotent delete request.
        result = self._invoke_kubectl([
            "-n", self.namespace, "delete", "pod", sandbox.sandbox_id,
            "--ignore-not-found=true", "--wait=false",
        ])
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip())
        sandbox.status = SandboxStatus.TERMINATED
        return sandbox

    def is_gone(self, sandbox_id: str) -> bool:
        """Independently observe Pod absence; ambiguous API errors fail closed."""
        result = self._invoke_kubectl([
            "-n", self.namespace, "get", "pod", sandbox_id, "-o", "name",
        ])
        if result.returncode == 0:
            return False
        detail = f"{result.stdout}\n{result.stderr}".lower()
        if "not found" in detail or "notfound" in detail:
            return True
        raise RuntimeError(
            f"could not independently establish whether Pod {sandbox_id} is Gone: "
            f"{result.stderr.strip()}"
        )
