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


class InMemorySandboxProvider:
    name = "in-memory"

    def allocate(self) -> Sandbox:
        return Sandbox(
            sandbox_id=str(uuid4()),
            provider=self.name,
            status=SandboxStatus.ALLOCATED,
        )

    def bind(self, sandbox: Sandbox, *, session_id: str) -> Sandbox:
        if sandbox.status == SandboxStatus.TERMINATED:
            raise ValueError("terminated sandbox cannot be bound")
        sandbox.session_id = session_id
        sandbox.status = SandboxStatus.BOUND
        return sandbox

    def snapshot(self, sandbox: Sandbox) -> Sandbox:
        if sandbox.status not in {
            SandboxStatus.BOUND,
            SandboxStatus.WARM,
        }:
            raise ValueError("sandbox must be active before snapshot")
        sandbox.snapshot_ref = f"snapshot://{sandbox.sandbox_id}"
        sandbox.status = SandboxStatus.PAUSED
        return sandbox

    def resume(self, snapshot_ref: str, *, session_id: str) -> Sandbox:
        return Sandbox(
            sandbox_id=str(uuid4()),
            provider=self.name,
            status=SandboxStatus.BOUND,
            snapshot_ref=snapshot_ref,
            session_id=session_id,
        )

    def terminate(self, sandbox: Sandbox) -> Sandbox:
        sandbox.status = SandboxStatus.TERMINATED
        return sandbox
