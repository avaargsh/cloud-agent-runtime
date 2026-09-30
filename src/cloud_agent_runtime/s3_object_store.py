from __future__ import annotations

from io import BytesIO
from typing import Any

from .evidence_object_store import (
    EvidenceObjectIntegrityError,
    _sha256,
)


_NOT_FOUND_CODES = {
    "404",
    "NoSuchKey",
    "NoSuchObject",
    "NotFound",
}


class S3ObjectStore:
    """Thin S3-compatible ObjectStore implementation.

    The provider works with AWS S3 and S3-compatible endpoints such as MinIO
    or Ceph RGW. Content-addressed keys plus bucket Object Lock/versioning are
    expected to provide the production immutability boundary.
    """

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "",
        endpoint_url: str | None = None,
        region_name: str | None = None,
        client: Any | None = None,
        **client_kwargs: Any,
    ) -> None:
        if not bucket:
            raise ValueError("bucket is required")

        if client is None:
            try:
                import boto3
            except ImportError as exc:
                raise RuntimeError(
                    'install S3 support with: pip install -e ".[s3]"'
                ) from exc

            client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                region_name=region_name,
                **client_kwargs,
            )

        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = client

    def _key(self, key: str) -> str:
        normalized = key.lstrip("/")
        if not self.prefix:
            return normalized
        return f"{self.prefix}/{normalized}"

    def put(self, key: str, body: bytes) -> str:
        """Create an immutable evidence object.

        A pre-existing object at the same content-addressed key is accepted only
        when its bytes are identical. Production buckets should additionally
        enable Object Lock/retention because this client-side check is not a
        substitute for storage-side immutability.
        """
        try:
            existing = self.get(key)
        except KeyError:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self._key(key),
                Body=body,
                ContentType="application/json",
            )
        else:
            if existing != body:
                raise EvidenceObjectIntegrityError(
                    "content-addressed S3 key already contains different bytes"
                )

        return _sha256(body)

    def get(self, key: str) -> bytes:
        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=self._key(key),
            )
        except Exception as exc:
            code = (
                getattr(exc, "response", {})
                .get("Error", {})
                .get("Code")
            )
            if str(code) in _NOT_FOUND_CODES:
                raise KeyError(
                    f"unknown S3 evidence object: {self._key(key)}"
                ) from exc
            raise

        body = response["Body"]
        if isinstance(body, bytes):
            return body
        if isinstance(body, BytesIO):
            return body.getvalue()
        if hasattr(body, "read"):
            return body.read()
        return bytes(body)
