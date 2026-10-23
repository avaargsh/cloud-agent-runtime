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
        kubectl: Callable[..., KubectlResult] | None = None,
    ) -> None:
        # namespace is a prefix, not a shared namespace for multiple sandboxes.
        self.namespace = namespace
        self.image = image
        self._kubectl = kubectl or self._run_kubectl


