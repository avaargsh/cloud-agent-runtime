from io import BytesIO

import pytest

from cloud_agent_runtime.evidence_object_store import (
    EvidenceObjectIntegrityError,
)
from cloud_agent_runtime.s3_object_store import S3ObjectStore


class MissingObject(Exception):
    def __init__(self):
        self.response = {"Error": {"Code": "NoSuchKey"}}


class FakeS3Client:
    def __init__(self):
        self.objects = {}
        self.put_calls = []

    def put_object(self, **kwargs):
        key = (kwargs["Bucket"], kwargs["Key"])
        self.objects[key] = bytes(kwargs["Body"])
        self.put_calls.append(kwargs)
        return {"ETag": "test"}

    def get_object(self, **kwargs):
        key = (kwargs["Bucket"], kwargs["Key"])
        if key not in self.objects:
            raise MissingObject()
        return {"Body": BytesIO(self.objects[key])}


def test_s3_object_store_writes_and_reads_with_prefix() -> None:
    client = FakeS3Client()
    store = S3ObjectStore(
        bucket="agent-evidence",
        prefix="prod",
        client=client,
    )

    digest = store.put(
        "evidence/run-1/event.json",
        b'{"ok":true}',
    )

    assert digest.startswith("sha256:")
    assert store.get("evidence/run-1/event.json") == b'{"ok":true}'
    assert client.put_calls[0]["Bucket"] == "agent-evidence"
    assert client.put_calls[0]["Key"] == "prod/evidence/run-1/event.json"
    assert client.put_calls[0]["ContentType"] == "application/json"


def test_s3_object_store_is_idempotent_for_identical_bytes() -> None:
    client = FakeS3Client()
    store = S3ObjectStore(bucket="agent-evidence", client=client)

    first = store.put("evidence/run-1/x.json", b"same")
    second = store.put("evidence/run-1/x.json", b"same")

    assert first == second
    assert len(client.put_calls) == 1


def test_s3_object_store_rejects_rewrite_at_same_key() -> None:
    client = FakeS3Client()
    store = S3ObjectStore(bucket="agent-evidence", client=client)

    store.put("evidence/run-1/x.json", b"original")
    client.objects[("agent-evidence", "evidence/run-1/x.json")] = b"tampered"

    with pytest.raises(
        EvidenceObjectIntegrityError,
        match="different bytes",
    ):
        store.put("evidence/run-1/x.json", b"original")
