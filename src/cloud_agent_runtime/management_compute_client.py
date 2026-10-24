from __future__ import annotations

import asyncio
import hashlib
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
_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")


def _go_json_dumps(value: Any, *, sort_keys: bool = False) -> str:
    """Match encoding/json's compact HTML-escaping for receipt fingerprints."""
    encoded = json.dumps(
        value,
        sort_keys=sort_keys,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return (
        encoded.replace("&", r"\u0026")
        .replace("<", r"\u003c")
        .replace(">", r"\u003e")
        .replace("\u2028", r"\u2028")
        .replace("\u2029", r"\u2029")
    )


def _management_request_digest(
    request: Mapping[str, Any],
    *,
    cluster_id: str,
) -> str:
    """Reproduce Management mutationFingerprint for this public PUT payload.

    The server fingerprints method, resolved route/resource identity, CAS
    generation, pool ID and the exact canonicalized spec; it intentionally does
    not use the adapter-local ComputeRequest intentDigest or caller token.
    """
    workload = request["workload"]
    spec: dict[str, Any] = {
        "image": workload["image"],
        "accelerator": dict(workload["accelerator"]),
    }
    if "command" in workload:
        spec["command"] = list(workload["command"])
    # The HTTP adapter emits the body with sort_keys=True, and Go's
    # json.RawMessage compacts but preserves that field ordering in the digest.
    spec = {key: spec[key] for key in sorted(spec)}
    spec["accelerator"] = {
        key: spec["accelerator"][key]
        for key in sorted(spec["accelerator"])
    }
    fingerprint = {
        "method": "PUT",
        "cluster": cluster_id,
        "project": request["projectId"],
        "kind": "Workload",
        "resourceId": workload["id"],
        "expectedGeneration": workload["expectedGeneration"],
        "poolId": request["computePoolId"],
        "spec": spec,
    }
    return hashlib.sha256(_go_json_dumps(fingerprint).encode("utf-8")).hexdigest()


def _management_release_digest(
    release_request: Mapping[str, Any],
    *,
    cluster_id: str,
) -> str:
    """Match Management mutationFingerprint for DELETE (no pool/spec input)."""
    fingerprint = {
        "method": "DELETE",
        "cluster": cluster_id,
        "project": release_request["projectId"],
        "kind": "Workload",
        "resourceId": release_request["workloadId"],
        "expectedGeneration": release_request["expectedGeneration"],
        "poolId": "",
    }
    return hashlib.sha256(_go_json_dumps(fingerprint).encode("utf-8")).hexdigest()


def _release_intent_digest(release_request: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        {key: value for key, value in release_request.items() if key != "intentDigest"},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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
        desired: Mapping[str, Any] | None,
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
        if_match: str | None = None,
    ) -> tuple[int, bytes]:
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Cache-Control": "no-store",
            "Idempotency-Key": request_id,
        }
        if body is not None:
            headers["Content-Type"] = "application/json"
        if if_match is not None:
            headers["If-Match"] = if_match
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
        if_match: str | None = None,
    ) -> tuple[int, bytes]:
        return await asyncio.to_thread(
            self._request_sync,
            method=method,
            url=url,
            token=token,
            request_id=request_id,
            body=body,
            if_match=if_match,
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
        if not isinstance(digest, str) or not _SHA256_HEX.fullmatch(digest):
            raise ManagementProtocolError("Management operation receipt has an invalid requestDigest")
        expected_digest = _management_request_digest(request, cluster_id=cluster_id)
        if digest != expected_digest:
            raise ManagementProtocolError(
                "Management operation receipt requestDigest does not match this request"
            )
        return generation

    def _validate_release_request(
        self,
        release_request: Mapping[str, Any],
        *,
        request: Mapping[str, Any],
    ) -> None:
        request_id = release_request.get("requestId")
        source_request_id = release_request.get("sourceRequestId")
        if not isinstance(request_id, str) or not _REQUEST_ID.fullmatch(request_id):
            raise ManagementProtocolError("release requestId must be exactly 32 lowercase hexadecimal characters")
        if not isinstance(source_request_id, str) or source_request_id != request.get("requestId"):
            raise ManagementProtocolError("release sourceRequestId does not match the immutable create request")
        if request_id == source_request_id:
            raise ManagementProtocolError("release requestId must differ from the create requestId")
        if release_request.get("apiVersion") != "compute.release-request/v0.1":
            raise ManagementProtocolError("unsupported ComputeReleaseRequest apiVersion")
        source_workload = request.get("workload")
        if not isinstance(source_workload, Mapping):
            raise ManagementProtocolError("source ComputeRequest workload is malformed")
        expected = {
            "projectId": request.get("projectId"),
            "computePoolId": request.get("computePoolId"),
            "workloadId": source_workload.get("id"),
            "correlation": request.get("correlation"),
        }
        for field, expected_value in expected.items():
            if release_request.get(field) != expected_value:
                raise ManagementProtocolError(f"release intent {field} does not match source ComputeRequest")
        generation = release_request.get("expectedGeneration")
        if type(generation) is not int or generation < 1:
            raise ManagementProtocolError("release expectedGeneration must be a positive server-issued Generation")
        digest = release_request.get("intentDigest")
        if not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise ManagementProtocolError("release intentDigest is malformed")
        if digest != _release_intent_digest(release_request):
            raise ManagementProtocolError("release intentDigest does not match persisted release intent")

    def _validate_release_receipt(
        self,
        receipt: Mapping[str, Any],
        *,
        release_request: Mapping[str, Any],
        cluster_id: str,
    ) -> int:
        expected_generation = release_request["expectedGeneration"]
        required = {
            "requestId": release_request["requestId"],
            "status": "desired-committed",
            "clusterId": cluster_id,
            "projectId": release_request["projectId"],
            "kind": "Workload",
            "resourceId": release_request["workloadId"],
            "action": "delete-request",
            "expectedGeneration": expected_generation,
            "resultingGeneration": expected_generation,
        }
        for field, expected in required.items():
            if receipt.get(field) != expected:
                raise ManagementProtocolError(f"Management release receipt mismatch for {field}")
        digest = receipt.get("requestDigest")
        if not isinstance(digest, str) or not _SHA256_HEX.fullmatch(digest):
            raise ManagementProtocolError("Management release receipt has an invalid requestDigest")
        expected_digest = _management_release_digest(release_request, cluster_id=cluster_id)
        if digest != expected_digest:
            raise ManagementProtocolError(
                "Management release receipt requestDigest does not match this DELETE"
            )
        return expected_generation

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

    async def resolve_binding(
        self,
        *,
        project_id: str,
        compute_pool_id: str,
    ) -> dict[str, Any]:
        """Resolve a trusted Pool route once, before the Run intent is persisted."""
        self._safe_path_segment(project_id, "projectId")
        self._safe_path_segment(compute_pool_id, "computePoolId")
        try:
            resolved = await self.binding_resolver.resolve(
                project_id=project_id,
                compute_pool_id=compute_pool_id,
            )
        except Exception as exc:
            # No request has been sent yet, so this is a setup failure, not
            # an unknown side-effect outcome.
            raise ManagementProtocolError(
                f"Trusted ComputePool binding resolver failed ({type(exc).__name__})"
            ) from None
        if not isinstance(resolved, Mapping):
            raise ManagementProtocolError("ComputePool binding resolver returned a non-object")
        cluster_id = resolved.get("clusterId")
        provider = resolved.get("provider")
        binding_generation = resolved.get("providerBindingGeneration")
        if not isinstance(cluster_id, str) or not cluster_id:
            raise ManagementProtocolError("Trusted ComputePool binding is missing clusterId")
        if not isinstance(provider, str) or not provider:
            raise ManagementProtocolError("Trusted ComputePool binding is missing provider")
        if type(binding_generation) is not int or binding_generation < 1:
            raise ManagementProtocolError("Trusted ComputePool binding has invalid providerBindingGeneration")
        self._safe_path_segment(cluster_id, "clusterId")
        return {
            "projectId": project_id,
            "computePoolId": compute_pool_id,
            "clusterId": cluster_id,
            "provider": provider,
            "providerBindingGeneration": binding_generation,
        }

    async def verify_release_target(
        self,
        *,
        request: Mapping[str, Any],
        binding: Mapping[str, Any],
        expected_generation: int,
    ) -> None:
        """Read-only authorization/Generation check before freezing a new release intent."""
        self._validate_request(request)
        if type(expected_generation) is not int or expected_generation < 1:
            raise ManagementProtocolError("release preflight Generation must be positive")
        project_id = request["projectId"]
        pool_id = request["computePoolId"]
        workload_id = request["workload"]["id"]
        if not isinstance(binding, Mapping):
            raise ManagementProtocolError("Persisted ComputePool route binding is missing")
        if binding.get("projectId") != project_id or binding.get("computePoolId") != pool_id:
            raise ManagementProtocolError("Persisted release preflight route does not match Project/Pool")
        cluster_id = binding.get("clusterId")
        if not isinstance(cluster_id, str) or not cluster_id:
            raise ManagementProtocolError("Persisted release preflight route is missing clusterId")
        if not isinstance(binding.get("provider"), str) or not binding.get("provider"):
            raise ManagementProtocolError("Persisted release preflight route is missing provider")
        if type(binding.get("providerBindingGeneration")) is not int or binding["providerBindingGeneration"] < 1:
            raise ManagementProtocolError("Persisted release preflight route has invalid binding generation")
        self._safe_path_segment(cluster_id, "clusterId")

        token = await self.token_provider.access_token_for_project(project_id)
        if not isinstance(token, str) or not token or any(c in token for c in (chr(10), chr(13))):
            raise ManagementRequestRejected(
                status=401,
                request_id=request["requestId"],
                message="No valid short-lived project access token was supplied",
            )
        resource_url, _ = self._paths(
            cluster_id=cluster_id,
            project_id=project_id,
            workload_id=workload_id,
            request_id=request["requestId"],
        )
        desired = await self._json_read(
            url=resource_url,
            token=token,
            request_id=request["requestId"],
            missing_ok=True,
        )
        if desired is None:
            raise ManagementRequestRejected(
                status=404,
                request_id=request["requestId"],
                message="Authorized Desired Workload is absent before a release operation was persisted",
            )
        generation = desired.get("generation")
        if type(generation) is not int or generation != expected_generation:
            raise ManagementRequestRejected(
                status=412,
                request_id=request["requestId"],
                message="Authorized Desired Workload Generation differs from the release precondition",
            )
        self._validate_desired(
            desired,
            request=request,
            cluster_id=cluster_id,
            generation=expected_generation,
        )
        if desired.get("deleting") is True:
            raise ManagementRequestOutcomeUnknown(
                request["requestId"],
                "Desired Workload is already deleting under an uncorrelated operation; release intent was not persisted",
            )

    async def release_or_reconcile(
        self,
        release_request: dict[str, Any],
        *,
        request: Mapping[str, Any],
        binding: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Issue/reconcile DELETE under a distinct release key; never infer RELEASED from ACK."""
        self._validate_request(request)
        request = copy_json(request)
        release_request = copy_json(release_request)
        self._validate_release_request(release_request, request=request)

        request_id = release_request["requestId"]
        project_id = release_request["projectId"]
        pool_id = release_request["computePoolId"]
        workload_id = release_request["workloadId"]
        expected_generation = release_request["expectedGeneration"]

        if not isinstance(binding, Mapping):
            raise ManagementProtocolError("Persisted ComputePool route binding is missing")
        if binding.get("projectId") != project_id or binding.get("computePoolId") != pool_id:
            raise ManagementProtocolError("Persisted release route does not match ComputeReleaseRequest Project/Pool")
        cluster_id = binding.get("clusterId")
        provider = binding.get("provider")
        binding_generation = binding.get("providerBindingGeneration")
        if not isinstance(cluster_id, str) or not cluster_id:
            raise ManagementProtocolError("Persisted release route binding is missing clusterId")
        if not isinstance(provider, str) or not provider:
            raise ManagementProtocolError("Persisted release route binding is missing provider")
        if type(binding_generation) is not int or binding_generation < 1:
            raise ManagementProtocolError("Persisted release route has invalid providerBindingGeneration")
        self._safe_path_segment(cluster_id, "clusterId")

        token = await self.token_provider.access_token_for_project(project_id)
        if not isinstance(token, str) or not token or any(c in token for c in (chr(10), chr(13))):
            raise ManagementRequestRejected(
                status=401,
                request_id=request_id,
                message="No valid short-lived project access token was supplied",
            )

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
            desired_before = await self._json_read(
                url=resource_url,
                token=token,
                request_id=request_id,
                missing_ok=True,
            )
            if desired_before is None:
                raise ManagementRequestOutcomeUnknown(
                    request_id,
                    "Workload is absent but no owner-scoped release receipt exists; RELEASED is unproven",
                )
            actual_generation = desired_before.get("generation")
            if type(actual_generation) is not int or actual_generation != expected_generation:
                raise ManagementRequestRejected(
                    status=412,
                    request_id=request_id,
                    message="Desired Workload Generation differs from the persisted release precondition",
                )
            self._validate_desired(
                desired_before,
                request=request,
                cluster_id=cluster_id,
                generation=expected_generation,
            )
            if desired_before.get("deleting") is True:
                raise ManagementRequestOutcomeUnknown(
                    request_id,
                    "Workload deletion is already in progress without this release receipt; refusing to adopt an unknown operation",
                )
            try:
                status, _ = await self._request(
                    method="DELETE",
                    url=resource_url,
                    token=token,
                    request_id=request_id,
                    if_match=f'"{expected_generation}"',
                )
            except _ManagementTransportError as exc:
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
                        f"Management DELETE transport failed ({exc.kind}); receipt recovery is inconclusive ({type(read_error).__name__})",
                    ) from None
                if receipt is None:
                    raise ManagementRequestOutcomeUnknown(
                        request_id,
                        f"Management DELETE transport failed ({exc.kind}); no release receipt is visible yet",
                    ) from None
            else:
                if status not in (202, 204):
                    try:
                        receipt = await self._json_read(
                            url=receipt_url,
                            token=token,
                            request_id=request_id,
                            missing_ok=True,
                        )
                    except Exception as read_error:
                        if status in (400, 401, 403, 404, 409, 412, 428, 422):
                            raise ManagementRequestRejected(
                                status=status,
                                request_id=request_id,
                                message=f"Management rejected the release with HTTP {status}",
                            ) from None
                        raise ManagementRequestOutcomeUnknown(
                            request_id,
                            f"Management DELETE returned HTTP {status}; receipt recovery is inconclusive ({type(read_error).__name__})",
                        ) from None
                    if receipt is None:
                        if status in (400, 401, 403, 404, 409, 412, 428, 422):
                            raise ManagementRequestRejected(
                                status=status,
                                request_id=request_id,
                                message=f"Management rejected the release with HTTP {status}",
                            )
                        raise ManagementRequestOutcomeUnknown(
                            request_id,
                            f"Management DELETE returned HTTP {status} without an authoritative receipt",
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
                            "Management DELETE returned accepted but its owner-scoped receipt is not visible yet",
                        )

        assert receipt is not None
        generation = self._validate_release_receipt(
            receipt,
            release_request=release_request,
            cluster_id=cluster_id,
        )
        desired = await self._json_read(
            url=resource_url,
            token=token,
            request_id=request_id,
            missing_ok=True,
        )
        if desired is not None:
            if desired.get("generation") != generation:
                raise ManagementProtocolError("Desired Generation changed after the release receipt committed")
            self._validate_desired(
                desired,
                request=request,
                cluster_id=cluster_id,
                generation=generation,
            )

        # ComputeAllocation v0.1 retains the original CREATE requestId. The
        # separate release key remains on the Run and in the Management receipt.
        allocation: dict[str, Any] = {
            "apiVersion": "compute.allocation/v0.1",
            "requestId": request["requestId"],
            "projectId": project_id,
            "computePoolId": pool_id,
            "workloadId": workload_id,
            "generation": generation,
            "clusterId": cluster_id,
            "provider": provider,
            "providerBindingGeneration": binding_generation,
            "state": "RELEASING",
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
                desired=copy_json(desired) if desired is not None else None,
            )
            if observation is not None:
                if not isinstance(observation, Mapping):
                    raise ManagementProtocolError("Compute release observer returned a non-object")
                identity = {
                    "requestId": request["requestId"],
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
                        raise ManagementProtocolError(f"Independent release observation identity mismatch for {key}")
                observed_state = observation.get("state")
                if observed_state == "RELEASED" and desired is not None:
                    raise ManagementProtocolError(
                        "release observer claimed RELEASED while the Desired Workload still exists"
                    )
                for key in ("state", "observedGeneration", "providerRefs", "conditions", "evidenceRefs"):
                    if key in observation:
                        allocation[key] = copy_json(observation[key])
        return allocation


    async def submit_or_reconcile(
        self,
        request: dict[str, Any],
        *,
        binding: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Commit/reconcile using the route frozen with the request before its first side effect."""
        self._validate_request(request)
        request = copy_json(request)
        request_id = request["requestId"]
        project_id = request["projectId"]
        pool_id = request["computePoolId"]
        workload_id = request["workload"]["id"]

        if not isinstance(binding, Mapping):
            raise ManagementProtocolError("Persisted ComputePool route binding is missing")
        if binding.get("projectId") != project_id or binding.get("computePoolId") != pool_id:
            raise ManagementProtocolError("Persisted route binding does not match ComputeRequest Project/Pool")
        cluster_id = binding.get("clusterId")
        provider = binding.get("provider")
        binding_generation = binding.get("providerBindingGeneration")
        if not isinstance(cluster_id, str) or not cluster_id:
            raise ManagementProtocolError("Persisted route binding is missing clusterId")
        if not isinstance(provider, str) or not provider:
            raise ManagementProtocolError("Persisted route binding is missing provider")
        if type(binding_generation) is not int or binding_generation < 1:
            raise ManagementProtocolError("Persisted route binding has invalid providerBindingGeneration")
        self._safe_path_segment(cluster_id, "clusterId")

        token = await self.token_provider.access_token_for_project(project_id)
        if not isinstance(token, str) or not token or any(c in token for c in (chr(10), chr(13))):
            raise ManagementRequestRejected(
                status=401,
                request_id=request_id,
                message="No valid short-lived project access token was supplied",
            )

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
