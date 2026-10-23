from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol
from uuid import uuid4


class SandboxStatus(str, Enum):
    ALLOCATED = "allocated"
    WARM = "warm"
    BOUND = "bound"
    PAUSED = "paused"
    TERMINATED = "terminated"


@dataclass
class Sandbox:
    sandbox_id: str
    provider: str
    status: SandboxStatus
    snapshot_ref: str | None = None
    session_id: str | None = None


class SandboxProvider(Protocol):
    name: str

    def allocate(self) -> Sandbox:
        ...

    def bind(self, sandbox: Sandbox, *, session_id: str) -> Sandbox:
        ...

    def snapshot(self, sandbox: Sandbox) -> Sandbox:
        ...

    def resume(self, snapshot_ref: str, *, session_id: str) -> Sandbox:
        ...

    def terminate(self, sandbox: Sandbox) -> Sandbox:
        ...

    def is_gone(self, sandbox_id: str) -> bool:
        """Independently observe that a sandbox no longer exists."""
        ...


class InMemorySandboxProvider:
    name = "in-memory"

    def __init__(self) -> None:
        self._sandboxes: dict[str, Sandbox] = {}

    def allocate(self) -> Sandbox:
        sandbox = Sandbox(
            sandbox_id=str(uuid4()),
            provider=self.name,
            status=SandboxStatus.ALLOCATED,
        )
        self._sandboxes[sandbox.sandbox_id] = sandbox
        return sandbox

    def bind(self, sandbox: Sandbox, *, session_id: str) -> Sandbox:
        if sandbox.status == SandboxStatus.TERMINATED:
            raise ValueError("terminated sandbox cannot be bound")
        sandbox.session_id = session_id
        sandbox.status = SandboxStatus.BOUND
        self._sandboxes[sandbox.sandbox_id] = sandbox
        return sandbox

    def snapshot(self, sandbox: Sandbox) -> Sandbox:
        if sandbox.status not in {
            SandboxStatus.BOUND,
            SandboxStatus.WARM,
        }:
            raise ValueError("sandbox must be active before snapshot")
        sandbox.snapshot_ref = f"snapshot://{sandbox.sandbox_id}"
        sandbox.status = SandboxStatus.PAUSED
        self._sandboxes[sandbox.sandbox_id] = sandbox
        return sandbox

    def resume(self, snapshot_ref: str, *, session_id: str) -> Sandbox:
        sandbox = Sandbox(
            sandbox_id=str(uuid4()),
            provider=self.name,
            status=SandboxStatus.BOUND,
            snapshot_ref=snapshot_ref,
            session_id=session_id,
        )
        self._sandboxes[sandbox.sandbox_id] = sandbox
        return sandbox

    def terminate(self, sandbox: Sandbox) -> Sandbox:
        current = self._sandboxes.pop(sandbox.sandbox_id, None)
        if current is not None:
            current.status = SandboxStatus.TERMINATED
        sandbox.status = SandboxStatus.TERMINATED
        return sandbox

    def is_gone(self, sandbox_id: str) -> bool:
        """Return True only when the provider registry no longer has the sandbox."""
        return sandbox_id not in self._sandboxes
