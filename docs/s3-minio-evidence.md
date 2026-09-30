# S3 / MinIO Evidence Provider

The v3.2 Evidence Plane uses the generic ObjectStore contract. The
`S3ObjectStore` adapter supplies a production-facing implementation for AWS S3
and S3-compatible endpoints such as MinIO or Ceph RGW.

The adapter is intentionally thin:

```text
EvidenceArchive
      |
      v
EvidenceObjectStore
      |
      v
S3ObjectStore
      |
      +--> AWS S3
      +--> MinIO
      +--> Ceph RGW
```

It does not implement an object database, retention engine, or replication
system inside AgentOS.

## Immutability

Evidence keys are content-addressed. If a key already exists, the adapter accepts
it only when the stored bytes are identical.

For production, storage-side controls are still mandatory:

- bucket versioning;
- Object Lock / retention;
- deny-delete IAM for the runtime writer;
- separate auditor/read role;
- independently retained trusted manifest or chain-head checkpoint.

The client-side equality check prevents accidental rewrite through the runtime
adapter, but is not a substitute for storage-side enforcement.

## Configuration example

```python
from cloud_agent_runtime.s3_object_store import S3ObjectStore

store = S3ObjectStore(
    bucket="agent-evidence",
    prefix="prod",
    endpoint_url="http://minio:9000",
    aws_access_key_id="...",
    aws_secret_access_key="...",
)
```

The same adapter can omit `endpoint_url` and use normal AWS credential
resolution for Amazon S3.
