from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .contracts import ArtifactRef


class ArtifactStore(Protocol):
    def put_bytes(
        self,
        *,
        key: str,
        content: bytes,
        media_type: str,
    ) -> ArtifactRef:
        ...


@dataclass
class LocalArtifactStore:
    root: Path

    def __post_init__(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def put_bytes(
        self,
        *,
        key: str,
        content: bytes,
        media_type: str,
    ) -> ArtifactRef:
        path = (self.root / key).resolve()
        root = self.root.resolve()

        if root not in path.parents and path != root:
            raise ValueError("artifact key escapes store root")

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

        checksum = hashlib.sha256(content).hexdigest()
        return ArtifactRef(
            uri=path.as_uri(),
            media_type=media_type,
            checksum=f"sha256:{checksum}",
        )

    def put_text(
        self,
        *,
        key: str,
        content: str,
        media_type: str = "text/plain",
    ) -> ArtifactRef:
        return self.put_bytes(
            key=key,
            content=content.encode("utf-8"),
            media_type=media_type,
        )
