from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Mapping
from typing import Any, Protocol
from urllib.error import HTTPError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


_REQUEST_ID = re.compile(r"^[0-9a-f]{32}$")
_DIGEST_IMAGE = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
_DNS_ID = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_MAX_RESPONSE_BYTES = 2 * 1024 * 1024


class ManagementRequestOutcomeUnknown(RuntimeError):
    """A public Management mutation may have committed, but no receipt is available."""

    def __init__(self, request_id: str, message: str) -> None:
        super().__init__(message)
        self.request_id = request_id
        self.code = "UNAVAILABLE"
        self.outcome = "UNKNOWN"
        self.retryable = True


class ManagementRequestRejected(RuntimeError):
    """A public Management request was explicitly rejected by its authority boundary."""

    def __init__(self, *, status: int, request_id: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.request_id = request_id
        self.code = "PERMISSION_DENIED" if status in (401, 403) else "MANAGEMENT_REJECTED"
        self.outcome = "REJECTED"
        self.retryable = False


class ManagementProtocolError(RuntimeError):
    """A successful HTTP response violated the public Management API contract."""


class AccessTokenProvider(Protocol):
    async def access_token_for_project(self, project_id: str) -> str:
        """Return a short-lived OIDC access token from the trusted identity layer."""
        ...


class ComputePoolBindingResolver(Protocol):
    async def resolve(
        self,
        *,
        project_id: str,
        compute_pool_id: str,
    ) -> Mapping[str, Any]:
        """Resolve current Pool -> Cluster/Provider binding from trusted control-plane config."""
        ...


class ComputeAllocationObserver(Protocol):
    async def observe(
        self,
        *,
        request: dict[str, Any],
        binding: Mapping[str, Any],
        receipt: Mapping[str, Any],
        desired: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        """Return fresh independent allocation evidence, or None when not yet observable."""
        ...


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        # Never forward a bearer token or idempotency key to a redirected origin.
        return None


class ManagementComputeClient:
    """Authenticated adapter to the public, project-scoped Management API.

    The route-binding resolver supplies the authoritative Pool -> Cluster/
    Provider binding needed by the public route; the Cluster ID is never
    accepted from the ComputeRequest. Access tokens come from a separate
    trusted identity provider. The optional observer supplies independent
    Kueue/Kubernetes evidence; without it the client returns REQUESTED and
    never invents ALLOCATED or RELEASED.

    Plain HTTP is accepted only when explicitly enabled for IPv4 loopback
    integration tests.
    """

    def __init__(
        self,
        *,
        base_url: str,
        token_provider: AccessTokenProvider,
        binding_resolver: ComputePoolBindingResolver,
        observer: ComputeAllocationObserver | None = None,
        timeout_seconds: float = 10,
        allow_loopback_http: bool = False,
    ) -> None:
        if not (0 < timeout_seconds <= 120):
            raise ValueError("timeout_seconds must be greater than 0 and at most 120")
        normalized = base_url.rstrip("/")
        parsed = urlsplit(normalized)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in ("", "/")
        ):
            raise ValueError("base_url must be an HTTP(S) origin without credentials, path, query or fragment")
        if parsed.scheme != "https":
            if not allow_loopback_http:
                raise ValueError("Management API base_url must use HTTPS outside explicit loopback tests")
            if parsed.hostname != "127.0.0.1" or not parsed.port or not (1 <= parsed.port <= 65535):
                raise ValueError("plain HTTP is allowed only for explicit 127.0.0.1:<port> tests")
        self.base_url = normalized
        self.token_provider = token_provider
        self.binding_resolver = binding_resolver
        self.observer = observer
        self.timeout_seconds = timeout_seconds
        self._opener = build_opener(_NoRedirect())

    @staticmethod
    def _safe_path_segment(value: str, name: str) -> str:
        if not isinstance(value, str) or not value or len(value) > 128 or value.strip() != value:
            raise ValueError(f"{name} must be a non-empty canonical path identifier")
        if any(char.isspace() for char in value):
            raise ValueError(f"{name} must not contain whitespace")
        return quote(value, safe="")

    @staticmethod
    def _validate_request(request: Mapping[str, Any]) -> None:
        request_id = request.get("requestId")
        if not isinstance(request_id, str) or not _REQUEST_ID.fullmatch(request_id):
            raise ValueError("requestId must be exactly 32 lowercase hexadecimal characters")
        if request.get("apiVersion") != "compute.request/v0.1":
            raise ValueError("unsupported ComputeRequest apiVersion")
        for field in ("projectId", "computePoolId"):
            value = request.get(field)
            if not isinstance(value, str) or not value or any(c.isspace() for c in value):
                raise ValueError(f"{field} must be a non-empty canonical identifier")
        workload = request.get("workload")
        if not isinstance(workload, Mapping):
            raise ValueError("workload must be an object")
        workload_id = workload.get("id")
        if not isinstance(workload_id, str) or not _DNS_ID.fullmatch(workload_id):
            raise ValueError("workload.id must be DNS-safe")
        if workload.get("expectedGeneration") != 0:
            raise ValueError("the current public Management create path requires expectedGeneration=0")
        image = workload.get("image")
        if not isinstance(image, str) or not _DIGEST_IMAGE.fullmatch(image):
            raise ValueError("workload image must be immutable and pinned to @sha256:<64 lowercase hex>")
        accelerator = workload.get("accelerator")
        if not isinstance(accelerator, Mapping):
            raise ValueError("workload.accelerator must be an object")
        if (
            not isinstance(accelerator.get("class"), str)
            or not accelerator["class"]
            or type(accelerator.get("quota")) is not int
            or accelerator["quota"] < 1
        ):
            raise ValueError("workload accelerator class/quota is invalid")
        if "command" in workload:
            command = workload["command"]
            if not isinstance(command, list) or not command or not all(isinstance(item, str) for item in command):
                raise ValueError("workload.command must be a non-empty array of strings")

    def _request_sync(
        self,
        *,
        method: str,
        url: str,
        token: str,
        request_id: str,
        body: bytes | None = None,
    ) -> tuple[int, bytes]:
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Cache-Control": "no-store",
            "Idempotency-Key": request_id,
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        req = Request(url, data=body, headers=headers, method=method)
        try:
            with self._opener.open(req, timeout=self.timeout_seconds) as response:
                payload = response.read(_MAX_RESPONSE_BYTES + 1)
                if len(payload) > _MAX_RESPONSE_BYTES:
                    raise ManagementProtocolError("Management API response exceeded the 2 MiB limit")
                return response.status, payload
        except HTTPError as response:
            payload = response.read(_MAX_RESPONSE_BYTES + 1)
            if len(payload) > _MAX_RESPONSE_BYTES:
                raise ManagementProtocolError("Management API error response exceeded the 2 MiB limit")
            return response.code, payload
        except (TimeoutError, ConnectionError, OSError) as exc:
            # Do not include exception text or request URL; it may contain
            # internal routing details. A mutation transport error is UNKNOWN.
            raise _ManagementTransportError(type(exc).__name__) from None

    async def _request(
        self,
        *,
        method: str,
        url: str,
        token: str,
        request_id: str,
        body: bytes | None = None,
    ) -> tuple[int, bytes]:
        return await asyncio.to_thread(
            self._request_sync,
            method=method,
            url=url,
            token=token,
            request_id=request_id,
            body=body,
        )

    async def _json_read(
        self,
        *,
        url: str,
        token: str,
        request_id: str,
        missing_ok: bool = False,
    ) -> dict[str, Any] | None:
        try:
            status, body = await self._request(
                method="GET",
                url=url,
                token=token,
                request_id=request_id,
            )
        except _ManagementTransportError as exc:
            raise ManagementRequestOutcomeUnknown(
                request_id,
                f"Management read is unavailable ({exc.kind}); retry with the same requestId",
            ) from None
        if status == 404 and missing_ok:
            return None
        if status in (401, 403):
            raise ManagementRequestRejected(
                status=status,
                request_id=request_id,
                message=f"Management authorization rejected request (HTTP {status})",
            )
        if status != 200:
            raise ManagementRequestOutcomeUnknown(
                request_id,
                f"Management read returned HTTP {status}; reconcile with the same requestId",
            )
        try:
            document = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ManagementProtocolError("Management API returned invalid JSON") from None
        if not isinstance(document, dict):
            raise ManagementProtocolError("Management API returned a non-object JSON response")
        return document

    def _paths(
        self,
        *,
        cluster_id: str,
        project_id: str,
        workload_id: str,
        request_id: str,
    ) -> tuple[str, str]:
        cluster = self._safe_path_segment(cluster_id, "clusterId")
        project = self._safe_path_segment(project_id, "projectId")
        workload = self._safe_path_segment(workload_id, "workloadId")
        operation = self._safe_path_segment(request_id, "requestId")
        root = f"{self.base_url}/api/v1/management/clusters/{cluster}/projects/{project}"
        return (
            f"{root}/resources/Workload/{workload}",
            f"{root}/operations/{operation}",
        )

    def _validate_receipt(
        self,
        receipt: Mapping[str, Any],
        *,
        request: Mapping[str, Any],
        cluster_id: str,
    ) -> int:
        required = {
            "requestId": request["requestId"],
            "status": "desired-committed",
            "clusterId": cluster_id,
            "projectId": request["projectId"],
            "kind": "Workload",
            "resourceId": request["workload"]["id"],
            "action": "create",
            "expectedGeneration": 0,
        }
        for field, expected in required.items():
            if receipt.get(field) != expected:
                raise ManagementProtocolError(f"Management operation receipt mismatch for {field}")
        generation = receipt.get("resultingGeneration")
        if type(generation) is not int or generation < 1:
            raise ManagementProtocolError("Management operation receipt has an invalid resultingGeneration")
        digest = receipt.get("requestDigest")
        if not isinstance(digest, str) or not digest:
            raise ManagementProtocolError("Management operation receipt is missing requestDigest")
        return generation

    def _validate_desired(
        self,
        desired: Mapping[str, Any],
        *,
        request: Mapping[str, Any],
        cluster_id: str,
        generation: int,
    ) -> None:
        expected = {
            "id": request["workload"]["id"],
            "clusterId": cluster_id,
            "projectId": request["projectId"],
            "generation": generation,
        }
        for field, value in expected.items():
            if desired.get(field) != value:
                raise ManagementProtocolError(f"Authorized Desired state mismatch for {field}")
        spec = desired.get("spec")
        if not isinstance(spec, Mapping):
            raise ManagementProtocolError("Authorized Desired state is missing spec")
        workload = request["workload"]
        expected_spec = {
            "poolID": request["computePoolId"],
            "image": workload["image"],
            "accelerator": dict(workload["accelerator"]),
        }
        if "command" in workload:
            expected_spec["command"] = workload["command"]
        for field, value in expected_spec.items():
            if spec.get(field) != value:
                raise ManagementProtocolError(f"Authorized Desired spec mismatch for {field}")

    async def submit_or_reconcile(self, request: dict[str, Any]) -> dict[str, Any]:
        """Commit/reconcile through the public Management route using the same key."""
        self._validate_request(request)
        request = copy_json(request)
        request_id = request["requestId"]
        project_id = request["projectId"]
        pool_id = request["computePoolId"]
        workload_id = request["workload"]["id"]

        token = await self.token_provider.access_token_for_project(project_id)
        if not isinstance(token, str) or not token or any(c in token for c in "\r\n"):
            raise ManagementRequestRejected(
                status=401,
                request_id=request_id,
                message="No valid short-lived project access token was supplied",
            )

        try:
            binding = await self.binding_resolver.resolve(
                project_id=project_id,
                compute_pool_id=pool_id,
            )
        except Exception as exc:
            raise ManagementRequestOutcomeUnknown(
                request_id,
                f"Trusted ComputePool binding resolver failed ({type(exc).__name__}); no mutation sent",
            ) from None
        if not isinstance(binding, Mapping):
            raise ManagementProtocolError("ComputePool binding resolver returned a non-object")
        cluster_id = binding.get("clusterId")
        provider = binding.get("provider")
        binding_generation = binding.get("providerBindingGeneration")
        if not isinstance(cluster_id, str) or not cluster_id:
            raise ManagementProtocolError("Trusted ComputePool binding is missing clusterId")
        if not isinstance(provider, str) or not provider:
            raise ManagementProtocolError("Trusted ComputePool binding is missing provider")
        if type(binding_generation) is not int or binding_generation < 1:
            raise ManagementProtocolError("Trusted ComputePool binding has invalid providerBindingGeneration")

        resource_url, receipt_url = self._paths(
            cluster_id=cluster_id,
            project_id=project_id,
            workload_id=workload_id,
            request_id=request_id,
        )
        receipt = await self._json_read(
            url=receipt_url,
            token=token,
            request_id=request_id,
            missing_ok=True,
        )
        if receipt is None:
            body_doc = {
                "expectedGeneration": 0,
                "poolId": pool_id,
                "spec": {
                    "image": request["workload"]["image"],
                    **(
                        {"command": request["workload"]["command"]}
                        if "command" in request["workload"]
                        else {}
                    ),
                    "accelerator": dict(request["workload"]["accelerator"]),
                },
            }
            body = json.dumps(
                body_doc,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
            try:
                status, _ = await self._request(
                    method="PUT",
                    url=resource_url,
                    token=token,
                    request_id=request_id,
                    body=body,
                )
            except _ManagementTransportError as exc:
                # The write may have committed. Read the authoritative receipt
                # before returning UNKNOWN; never create a new key.
                try:
                    receipt = await self._json_read(
                        url=receipt_url,
                        token=token,
                        request_id=request_id,
                        missing_ok=True,
                    )
                except Exception as read_error:
                    raise ManagementRequestOutcomeUnknown(
                        request_id,
                        f"Management PUT transport failed ({exc.kind}); receipt recovery is inconclusive ({type(read_error).__name__})",
                    ) from None
                if receipt is None:
                    raise ManagementRequestOutcomeUnknown(
                        request_id,
                        f"Management PUT transport failed ({exc.kind}); receipt is absent at recovery read",
                    ) from None
            else:
                if status not in (200, 204):
                    # 5xx/429 may happen after a commit. Check the receipt
                    # first, but do not repeat the mutation in this call.
                    try:
                        receipt = await self._json_read(
                            url=receipt_url,
                            token=token,
                            request_id=request_id,
                            missing_ok=True,
                        )
                    except Exception as read_error:
                        if status in (400, 401, 403, 404, 409, 412, 422):
                            raise ManagementRequestRejected(
                                status=status,
                                request_id=request_id,
                                message=f"Management rejected the request with HTTP {status}",
                            ) from None
                        raise ManagementRequestOutcomeUnknown(
                            request_id,
                            f"Management PUT returned HTTP {status}; receipt recovery is inconclusive ({type(read_error).__name__})",
                        ) from None
                    if receipt is None:
                        if status in (400, 401, 403, 404, 409, 412, 422):
                            raise ManagementRequestRejected(
                                status=status,
                                request_id=request_id,
                                message=f"Management rejected the request with HTTP {status}",
                            )
                        raise ManagementRequestOutcomeUnknown(
                            request_id,
                            f"Management PUT returned HTTP {status} without an authoritative receipt",
                        )
                else:
                    receipt = await self._json_read(
                        url=receipt_url,
                        token=token,
                        request_id=request_id,
                        missing_ok=True,
                    )
                    if receipt is None:
                        raise ManagementRequestOutcomeUnknown(
                            request_id,
                            "Management PUT returned success but operation receipt is not visible yet",
                        )

        assert receipt is not None
        generation = self._validate_receipt(receipt, request=request, cluster_id=cluster_id)
        desired = await self._json_read(
            url=resource_url,
            token=token,
            request_id=request_id,
            missing_ok=False,
        )
        assert desired is not None
        self._validate_desired(
            desired,
            request=request,
            cluster_id=cluster_id,
            generation=generation,
        )

        allocation: dict[str, Any] = {
            "apiVersion": "compute.allocation/v0.1",
            "requestId": request_id,
            "projectId": project_id,
            "computePoolId": pool_id,
            "workloadId": workload_id,
            "generation": generation,
            "clusterId": cluster_id,
            "provider": provider,
            "providerBindingGeneration": binding_generation,
            "state": "REQUESTED",
            "observedGeneration": 0,
            "providerRefs": [],
            "conditions": [],
            "evidenceRefs": [],
        }
        if self.observer is not None:
            observation = await self.observer.observe(
                request=copy_json(request),
                binding=dict(binding),
                receipt=dict(receipt),
                desired=dict(desired),
            )
            if observation is not None:
                if not isinstance(observation, Mapping):
                    raise ManagementProtocolError("Compute observation reader returned a non-object")
                identity = {
                    "requestId": request_id,
                    "projectId": project_id,
                    "computePoolId": pool_id,
                    "workloadId": workload_id,
                    "generation": generation,
                    "clusterId": cluster_id,
                    "provider": provider,
                    "providerBindingGeneration": binding_generation,
                }
                for key, expected in identity.items():
                    if key in observation and observation[key] != expected:
                        raise ManagementProtocolError(f"Independent observation identity mismatch for {key}")
                for key in ("state", "observedGeneration", "providerRefs", "conditions", "evidenceRefs"):
                    if key in observation:
                        allocation[key] = copy_json(observation[key])
        return allocation


class _ManagementTransportError(RuntimeError):
    def __init__(self, kind: str) -> None:
        super().__init__(kind)
        self.kind = kind


def copy_json(value: Any) -> Any:
    """Return a JSON-shaped defensive copy, rejecting unserializable authority data."""
    return json.loads(json.dumps(value, separators=(",", ":"), sort_keys=True))
