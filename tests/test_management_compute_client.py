from __future__ import annotations

import asyncio
import hashlib
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from cloud_agent_runtime.management_compute_client import (
    ManagementComputeClient,
    ManagementProtocolError,
    ManagementRequestOutcomeUnknown,
)


IMAGE = "registry.example.test/worker@sha256:" + ("a" * 64)


def server_mutation_fingerprint(body: dict[str, Any], workload_id: str) -> str:
    """Independent test implementation of the Go Management fingerprint contract."""
    spec = {key: body["spec"][key] for key in sorted(body["spec"])}
    if isinstance(spec.get("accelerator"), dict):
        spec["accelerator"] = {
            key: spec["accelerator"][key]
            for key in sorted(spec["accelerator"])
        }
    payload = {
        "method": "PUT",
        "cluster": "cluster-a",
        "project": "project-a",
        "kind": "Workload",
        "resourceId": workload_id,
        "expectedGeneration": body["expectedGeneration"],
        "poolId": body["poolId"],
        "spec": spec,
    }
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    for plain, escaped in (
        ("&", r"\u0026"),
        ("<", r"\u003c"),
        (">", r"\u003e"),
        ("\u2028", r"\u2028"),
        ("\u2029", r"\u2029"),
    ):
        encoded = encoded.replace(plain, escaped)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def server_delete_fingerprint(workload_id: str, generation: int) -> str:
    payload = {
        "method": "DELETE",
        "cluster": "cluster-a",
        "project": "project-a",
        "kind": "Workload",
        "resourceId": workload_id,
        "expectedGeneration": generation,
        "poolId": "",
    }
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    for plain, escaped in (
        ("&", r"\u0026"),
        ("<", r"\u003c"),
        (">", r"\u003e"),
        ("\u2028", r"\u2028"),
        ("\u2029", r"\u2029"),
    ):
        encoded = encoded.replace(plain, escaped)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class MemoryAuthority:
    async def access_token_for_project(self, project_id: str) -> str:
        assert project_id == "project-a"
        return "short-lived-test-access-token"


class MemoryBindingResolver:
    def __init__(self) -> None:
        self.calls = 0

    async def resolve(self, *, project_id: str, compute_pool_id: str) -> dict[str, Any]:
        self.calls += 1
        assert project_id == "project-a"
        assert compute_pool_id == "pool-a"
        return {
            "clusterId": "cluster-a" if self.calls == 1 else "cluster-b",
            "provider": "kueue",
            "providerBindingGeneration": self.calls,
        }


class LocalManagementServer:
    def __init__(
        self,
        *,
        drop_first_put_ack: bool = False,
        drop_first_delete_ack: bool = False,
        receipt_digest_override: str | None = None,
        release_digest_override: str | None = None,
    ) -> None:
        self.drop_first_put_ack = drop_first_put_ack
        self.drop_first_delete_ack = drop_first_delete_ack
        self.receipt_digest_override = receipt_digest_override
        self.release_digest_override = release_digest_override
        self.put_count = 0
        self.delete_count = 0
        self.receipt: dict[str, Any] | None = None
        self.receipts: dict[str, dict[str, Any]] = {}
        self.desired: dict[str, Any] | None = None
        self.requests: list[dict[str, Any]] = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, _format: str, *_args: object) -> None:
                return

            def _json(self, status: int, value: dict[str, Any]) -> None:
                body = json.dumps(value).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:  # noqa: N802
                owner.requests.append({
                    "method": "GET",
                    "path": self.path,
                    "authorization": self.headers.get("Authorization"),
                })
                if "/operations/" in self.path:
                    request_id = self.path.rsplit("/", 1)[1]
                    receipt = owner.receipts.get(request_id)
                    if receipt is None:
                        self.send_response(404)
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    self._json(200, receipt)
                    return
                if "/resources/Workload/" in self.path:
                    if owner.desired is None:
                        self.send_response(404)
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    self._json(200, owner.desired)
                    return
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_PUT(self) -> None:  # noqa: N802
                owner.put_count += 1
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length))
                request_id = self.headers.get("Idempotency-Key")
                owner.requests.append({
                    "method": "PUT",
                    "path": self.path,
                    "authorization": self.headers.get("Authorization"),
                    "request_id": request_id,
                    "body": body,
                })
                assert self.headers.get("Authorization") == "Bearer short-lived-test-access-token"
                assert request_id and len(request_id) == 32
                workload_id = self.path.rsplit("/", 1)[1]
                owner.receipt = {
                    "requestId": request_id,
                    "status": "desired-committed",
                    "clusterId": "cluster-a",
                    "projectId": "project-a",
                    "kind": "Workload",
                    "resourceId": workload_id,
                    "action": "create",
                    "expectedGeneration": 0,
                    "resultingGeneration": 1,
                    "requestDigest": owner.receipt_digest_override or server_mutation_fingerprint(body, workload_id),
                    "committedAt": "2026-10-24T00:00:00Z",
                }
                owner.receipts[request_id] = owner.receipt
                owner.desired = {
                    "id": workload_id,
                    "clusterId": "cluster-a",
                    "projectId": "project-a",
                    "generation": 1,
                    "spec": {
                        "poolID": "pool-a",
                        "image": IMAGE,
                        "command": ["python", "-m", "train"],
                        "accelerator": {"class": "h100-80g", "quota": 1},
                    },
                }
                if owner.drop_first_put_ack and owner.put_count == 1:
                    # The server persists Desired + Receipt, then closes the
                    # socket without returning the committed HTTP 204.
                    self.close_connection = True
                    try:
                        self.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    self.connection.close()
                    return
                self.send_response(204)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_DELETE(self) -> None:  # noqa: N802
                owner.delete_count += 1
                request_id = self.headers.get("Idempotency-Key")
                if_match = self.headers.get("If-Match")
                owner.requests.append({
                    "method": "DELETE",
                    "path": self.path,
                    "authorization": self.headers.get("Authorization"),
                    "request_id": request_id,
                    "if_match": if_match,
                })
                assert self.headers.get("Authorization") == "Bearer short-lived-test-access-token"
                assert request_id and len(request_id) == 32
                if owner.desired is None:
                    self.send_response(404)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                generation = owner.desired["generation"]
                if if_match != f'"{generation}"':
                    self.send_response(412)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                workload_id = self.path.rsplit("/", 1)[1]
                receipt = {
                    "requestId": request_id,
                    "status": "desired-committed",
                    "clusterId": "cluster-a",
                    "projectId": "project-a",
                    "kind": "Workload",
                    "resourceId": workload_id,
                    "action": "delete-request",
                    "expectedGeneration": generation,
                    "resultingGeneration": generation,
                    "requestDigest": owner.release_digest_override or server_delete_fingerprint(workload_id, generation),
                    "committedAt": "2026-10-24T00:05:00Z",
                }
                owner.receipts[request_id] = receipt
                owner.desired["deleting"] = True
                if owner.drop_first_delete_ack and owner.delete_count == 1:
                    # The server commits the delete request receipt then drops
                    # the response, reproducing a mutation ACK lost after commit.
                    self.close_connection = True
                    try:
                        self.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    self.connection.close()
                    return
                self.send_response(202)
                self.send_header("Content-Length", "0")
                self.end_headers()

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def request() -> dict[str, Any]:
    return {
        "apiVersion": "compute.request/v0.1",
        "requestId": "a" * 32,
        "intentDigest": "sha256:" + ("b" * 64),
        "projectId": "project-a",
        "computePoolId": "pool-a",
        "profileRef": {"profileId": "gpu-worker", "revision": 7},
        "correlation": {
            "agentId": "agent-a",
            "sessionId": "session-a",
            "runId": "run-a",
        },
        "workload": {
            "id": "run-123",
            "expectedGeneration": 0,
            "image": IMAGE,
            "command": ["python", "-m", "train"],
            "accelerator": {"class": "h100-80g", "quota": 1},
        },
    }


def test_management_client_uses_public_route_and_server_receipt() -> None:
    async def scenario() -> None:
        server = LocalManagementServer()
        try:
            resolver = MemoryBindingResolver()
            client = ManagementComputeClient(
                base_url=server.base_url,
                token_provider=MemoryAuthority(),
                binding_resolver=resolver,
                allow_loopback_http=True,
                timeout_seconds=2,
            )
            binding = await client.resolve_binding(project_id="project-a", compute_pool_id="pool-a")
            allocation = await client.submit_or_reconcile(request(), binding=binding)
            assert resolver.calls == 1

            assert server.put_count == 1
            assert server.receipt is not None
            assert allocation["state"] == "REQUESTED"
            assert allocation["generation"] == 1
            assert allocation["observedGeneration"] == 0
            assert allocation["provider"] == "kueue"
            assert allocation["requestId"] == "a" * 32
            put = next(item for item in server.requests if item["method"] == "PUT")
            assert put["path"] == (
                "/api/v1/management/clusters/cluster-a/projects/project-a/"
                "resources/Workload/run-123"
            )
            assert put["request_id"] == "a" * 32
            assert "operations/" not in put["path"]
            assert not any("/internal/" in item["path"] or "/api/v1/agent/" in item["path"] for item in server.requests)
        finally:
            server.close()

    asyncio.run(scenario())


def test_management_client_recovers_commit_after_lost_ack_without_second_put() -> None:
    async def scenario() -> None:
        server = LocalManagementServer(drop_first_put_ack=True)
        try:
            resolver = MemoryBindingResolver()
            client = ManagementComputeClient(
                base_url=server.base_url,
                token_provider=MemoryAuthority(),
                binding_resolver=resolver,
                allow_loopback_http=True,
                timeout_seconds=2,
            )
            binding = await client.resolve_binding(project_id="project-a", compute_pool_id="pool-a")
            result = await client.submit_or_reconcile(request(), binding=binding)
            assert resolver.calls == 1
            assert result["state"] == "REQUESTED"
            assert result["generation"] == 1
            assert server.put_count == 1, "receipt recovery must not issue a second PUT"
            assert [item["method"] for item in server.requests].count("GET") >= 2
        finally:
            server.close()

    asyncio.run(scenario())


def test_management_client_refuses_mutable_image_and_non_loopback_plain_http() -> None:
    async def scenario() -> None:
        server = LocalManagementServer()
        try:
            with pytest.raises(ValueError, match="HTTPS"):
                ManagementComputeClient(
                    base_url="http://example.test",
                    token_provider=MemoryAuthority(),
                    binding_resolver=MemoryBindingResolver(),
                )
            invalid = request()
            invalid["workload"]["image"] = "registry.example.test/model:latest"
            resolver = MemoryBindingResolver()
            client = ManagementComputeClient(
                base_url=server.base_url,
                token_provider=MemoryAuthority(),
                binding_resolver=resolver,
                allow_loopback_http=True,
                timeout_seconds=2,
            )
            binding = await client.resolve_binding(project_id="project-a", compute_pool_id="pool-a")
            with pytest.raises(ValueError, match="sha256"):
                await client.submit_or_reconcile(invalid, binding=binding)
            assert server.put_count == 0
        finally:
            server.close()

    asyncio.run(scenario())


def test_management_client_stops_after_unknown_commit_and_uses_same_key_on_retry() -> None:
    async def scenario() -> None:
        class LostReceiptServer(LocalManagementServer):
            pass

        server = LostReceiptServer(drop_first_put_ack=True)
        # Simulate temporary read outage after a committed write. The client
        # must return UNKNOWN and not mint/reissue another idempotency key.
        original = server.server.RequestHandlerClass.do_GET

        def get_with_temporary_outage(handler) -> None:
            if "/operations/" in handler.path and server.receipt is not None:
                handler.send_response(503)
                handler.send_header("Content-Length", "0")
                handler.end_headers()
                return
            original(handler)

        server.server.RequestHandlerClass.do_GET = get_with_temporary_outage
        try:
            resolver = MemoryBindingResolver()
            client = ManagementComputeClient(
                base_url=server.base_url,
                token_provider=MemoryAuthority(),
                binding_resolver=resolver,
                allow_loopback_http=True,
                timeout_seconds=2,
            )
            binding = await client.resolve_binding(project_id="project-a", compute_pool_id="pool-a")
            with pytest.raises(ManagementRequestOutcomeUnknown):
                await client.submit_or_reconcile(request(), binding=binding)
            assert server.put_count == 1
            # Restore receipt endpoint and retry. The same request ID finds the
            # immutable receipt first and returns REQUESTED without PUT, still
            # pinned to the originally persisted Pool -> Cluster route.
            server.server.RequestHandlerClass.do_GET = original
            result = await client.submit_or_reconcile(request(), binding=binding)
            assert result["requestId"] == "a" * 32
            assert server.put_count == 1
            assert resolver.calls == 1
            assert result["clusterId"] == "cluster-a"
        finally:
            server.close()

    asyncio.run(scenario())



def test_management_client_rejects_receipt_digest_for_another_mutation() -> None:
    async def scenario() -> None:
        server = LocalManagementServer(receipt_digest_override="f" * 64)
        try:
            client = ManagementComputeClient(
                base_url=server.base_url,
                token_provider=MemoryAuthority(),
                binding_resolver=MemoryBindingResolver(),
                allow_loopback_http=True,
                timeout_seconds=2,
            )
            binding = await client.resolve_binding(project_id="project-a", compute_pool_id="pool-a")
            with pytest.raises(ManagementProtocolError, match="requestDigest does not match"):
                await client.submit_or_reconcile(request(), binding=binding)
        finally:
            server.close()

    asyncio.run(scenario())
