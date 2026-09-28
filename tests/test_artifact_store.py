from pathlib import Path

import pytest

from cloud_agent_runtime.artifact_store import LocalArtifactStore


def test_local_artifact_store_returns_content_addressed_ref(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path)
    ref = store.put_text(
        key="runs/run-1/patch.diff",
        content="hello",
        media_type="text/x-diff",
    )

    assert ref.uri.startswith("file:")
    assert ref.media_type == "text/x-diff"
    assert ref.checksum is not None
    assert (tmp_path / "runs/run-1/patch.diff").read_text() == "hello"


def test_artifact_store_rejects_path_escape(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path)

    with pytest.raises(ValueError):
        store.put_text(
            key="../escape.txt",
            content="no",
        )
