from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol
from urllib.parse import quote, unquote

from .models import RunStatus, SessionStatus
from .store import RuntimeStore


CANONICAL_RUN_CONTRACT_VERSION = "runtime.canonical-run.v1"
CANONICAL_RUN_ROUTE_PREFIX = "/v1/runtime/canonical-runs/"


class RuntimeIdentityNotFoundError(LookupError):
    """The requested canonical Run or its owning Session does not exist."""


class RuntimeIdentityUnavailableError(RuntimeError):
    """Canonical Runtime state could not be read safely."""


class RuntimeIdentityProtocolError(RuntimeError):
    """Canonical Runtime state or a remote response violates the contract."""


@dataclass(frozen=True)
class CanonicalRuntimeRunBinding:
    """Canonical Run + Session + Sandbox binding, structurally compatible with ACP.

    This record contains only identity/lifecycle facts from RuntimeStore. It
    deliberately contains no request or workload-assertion claims.
    """

    tenant_id: str
    agent_id: str
    release_id: str
    session_id: str
    run_id: str
    sandbox_id: str
    sandbox_revision: int
    workspace_ref: str
    status: str
    session_status: str

    def to_payload(self) -> dict[str, Any]:
        return {
            "contract_version": CANONICAL_RUN_CONTRACT_VERSION,
            "tenant_id": self.tenant_id,
            "agent_id": self.agent_id,
            "release_id": self.release_id,
            "session_id": self.session_id,
            "run_id": self.run_id,
            "sandbox_id": self.sandbox_id,
            "sandbox_revision": self.sandbox_revision,
            "workspace_ref": self.workspace_ref,
            "status": self.status,
            "session_status": self.session_status,
        }


class RunAuthorityReader(Protocol):
    """Narrow read-only interface consumed by a credential authority."""

    def get_run_binding(self, run_id: str) -> CanonicalRuntimeRunBinding:
        ...


def _non_empty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeIdentityProtocolError(f"canonical field {field} is missing or invalid")
    return value


def _status_value(value: Any, field: str, allowed: set[str]) -> str:
    raw = getattr(value, "value", value)
    if not isinstance(raw, str):
        raise RuntimeIdentityProtocolError(f"canonical field {field} is invalid")
    normalized = raw.lower()
    if normalized not in allowed:
        raise RuntimeIdentityProtocolError(f"canonical field {field} is unknown")
    return normalized


class RuntimeStoreRunAuthorityReader:
    """Read the canonical Run, owning Session and persisted SandboxBinding.

    The injected store must be the same authoritative RuntimeStore used by
    AgentRuntime. No run/tenant/Agent/sandbox fields are accepted from the
    caller. Missing SandboxBinding fails closed rather than synthesizing a
    revision from the convenience `run.sandbox_ref` field.
    """

    _RUN_STATES = {item.value for item in RunStatus}
    _SESSION_STATES = {item.value for item in SessionStatus}

    def __init__(self, store: RuntimeStore) -> None:
        self._store = store

    def get_run_binding(self, run_id: str) -> CanonicalRuntimeRunBinding:
        if not isinstance(run_id, str) or not run_id.strip():
            raise RuntimeIdentityProtocolError("run_id must be a non-empty string")
        try:
            run = self._store.get_run(run_id)
        except (KeyError, LookupError):
            raise RuntimeIdentityNotFoundError("canonical Run does not exist") from None
        except Exception:
            raise RuntimeIdentityUnavailableError("canonical Run store is unavailable") from None

        try:
            stored_run_id = _non_empty_string(getattr(run, "run_id", None), "run_id")
            session_id = _non_empty_string(getattr(run, "session_id", None), "session_id")
            if stored_run_id != run_id:
                raise RuntimeIdentityProtocolError("RuntimeStore returned a different Run ID")
            session = self._store.get_session(session_id)
        except RuntimeIdentityProtocolError:
            raise
        except (KeyError, LookupError):
            raise RuntimeIdentityNotFoundError("canonical Session does not exist") from None
        except Exception:
            raise RuntimeIdentityUnavailableError("canonical Session store is unavailable") from None

        try:
            if _non_empty_string(getattr(session, "session_id", None), "session_id") != session_id:
                raise RuntimeIdentityProtocolError("Run and Session identity do not match")
            tenant_id = _non_empty_string(getattr(session, "tenant_id", None), "tenant_id")
            agent_id = _non_empty_string(getattr(session, "agent_id", None), "agent_id")
            release_id = _non_empty_string(getattr(session, "release_id", None), "release_id")
            run_status = _status_value(
                getattr(run, "status", None), "status", self._RUN_STATES
            )
            session_status = _status_value(
                getattr(session, "status", None), "session_status", self._SESSION_STATES
            )
            binding = getattr(run, "sandbox_binding", None)
            if binding is None:
                raise RuntimeIdentityProtocolError("Run has no persisted SandboxBinding")
            provider = _non_empty_string(getattr(binding, "provider", None), "sandbox.provider")
            sandbox_id = _non_empty_string(getattr(binding, "sandbox_id", None), "sandbox_id")
            workspace_ref = _non_empty_string(
                getattr(binding, "sandbox_ref", None), "workspace_ref"
            )
            revision = getattr(binding, "revision", None)
            if type(revision) is not int or revision <= 0:
                raise RuntimeIdentityProtocolError("sandbox revision must be a positive integer")
            expected_ref = f"sandbox://{provider}/{sandbox_id}"
            if workspace_ref != expected_ref:
                raise RuntimeIdentityProtocolError("SandboxBinding reference does not match its provider and ID")
            if getattr(run, "sandbox_ref", None) != workspace_ref:
                raise RuntimeIdentityProtocolError("Run and SandboxBinding references diverge")
        except RuntimeIdentityProtocolError:
            raise
        except Exception:
            raise RuntimeIdentityProtocolError("canonical Run or Session record is malformed") from None

        return CanonicalRuntimeRunBinding(
            tenant_id=tenant_id,
            agent_id=agent_id,
            release_id=release_id,
            session_id=session_id,
            run_id=stored_run_id,
            sandbox_id=sandbox_id,
            sandbox_revision=revision,
            workspace_ref=workspace_ref,
            status=run_status,
            session_status=session_status,
        )


class RuntimeIdentityServiceAuthenticator(Protocol):
    """Authenticates a service and authorizes a narrow runtime identity role.

    Production implementations must bind this decision to a real service
    identity (for example ingress mTLS identity plus explicit role mapping);
    the WSGI adapter never treats an arbitrary request field as a principal.
    """

    def authorize(self, environ: Mapping[str, Any], *, required_role: str) -> bool:
        ...


class RuntimeCanonicalIdentityWSGI:
    """Mountable, read-only WSGI endpoint; not a TLS listener itself.

    Route: GET /v1/runtime/canonical-runs/{run_id}
    Role: runtime.identity.reader
    """

    ROLE = "runtime.identity.reader"

    def __init__(
        self,
        *,
        reader: RunAuthorityReader,
        authenticator: RuntimeIdentityServiceAuthenticator,
    ) -> None:
        self._reader = reader
        self._authenticator = authenticator

    @staticmethod
    def _response(
        start_response: Callable[..., Any],
        status: str,
        payload: Mapping[str, Any],
    ) -> list[bytes]:
        body = json.dumps(
            payload, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
        start_response(status, [
            ("Content-Type", "application/json; charset=utf-8"),
            ("Content-Length", str(len(body))),
            ("Cache-Control", "no-store"),
            ("X-Content-Type-Options", "nosniff"),
        ])
        return [body]

    def __call__(self, environ: Mapping[str, Any], start_response: Callable[..., Any]) -> list[bytes]:
        method = environ.get("REQUEST_METHOD", "")
        path = environ.get("PATH_INFO", "")
        if not isinstance(path, str) or not path.startswith(CANONICAL_RUN_ROUTE_PREFIX):
            return self._response(start_response, "404 Not Found", {
                "error": {"code": "not_found", "message": "resource not found"}
            })
        encoded_id = path[len(CANONICAL_RUN_ROUTE_PREFIX):]
        if not encoded_id or "/" in encoded_id:
            return self._response(start_response, "404 Not Found", {
                "error": {"code": "not_found", "message": "resource not found"}
            })
        run_id = unquote(encoded_id)
        if not run_id.strip() or "/" in run_id or "\\" in run_id:
            return self._response(start_response, "400 Bad Request", {
                "error": {"code": "invalid_request", "message": "run ID is invalid"}
            })
        if method != "GET":
            return self._response(start_response, "405 Method Not Allowed", {
                "error": {"code": "method_not_allowed", "message": "method not allowed"}
            })
        # A deployment may only set wsgi.url_scheme from a trusted TLS listener
        # or trusted reverse proxy. This is a defense-in-depth check, not TLS.
        if environ.get("wsgi.url_scheme") != "https":
            return self._response(start_response, "403 Forbidden", {
                "error": {"code": "secure_transport_required", "message": "secure transport required"}
            })
        try:
            authorized = self._authenticator.authorize(
                environ, required_role=self.ROLE
            )
        except Exception:
            return self._response(start_response, "503 Service Unavailable", {
                "error": {"code": "identity_auth_unavailable", "message": "service identity unavailable"}
            })
        if authorized is not True:
            return self._response(start_response, "403 Forbidden", {
                "error": {"code": "service_role_denied", "message": "service role authorization denied"}
            })
        try:
            binding = self._reader.get_run_binding(run_id)
        except RuntimeIdentityNotFoundError:
            return self._response(start_response, "404 Not Found", {
                "error": {"code": "not_found", "message": "resource not found"}
            })
        except (RuntimeIdentityUnavailableError, RuntimeIdentityProtocolError):
            return self._response(start_response, "503 Service Unavailable", {
                "error": {"code": "runtime_identity_unavailable", "message": "canonical identity unavailable"}
            })
        except Exception:
            return self._response(start_response, "503 Service Unavailable", {
                "error": {"code": "runtime_identity_unavailable", "message": "canonical identity unavailable"}
            })
        return self._response(start_response, "200 OK", binding.to_payload())


class RuntimeIdentityJsonTransport(Protocol):
    """Read-only JSON transport. Use HttpsJsonAuthorityTransport in production."""

    def request(
        self,
        method: str,
        path: str,
        body: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        ...


class HttpsRuntimeStoreRunAuthorityReader:
    """Remote read-only reader for the canonical Runtime identity endpoint.

    Pass the existing verified-HTTPS transport and a least-privilege service
    token provider. The endpoint must authenticate the caller as
    `runtime.identity.reader`; this class never retries or writes state.
    """

    def __init__(self, transport: RuntimeIdentityJsonTransport) -> None:
        self._transport = transport

    def get_run_binding(self, run_id: str) -> CanonicalRuntimeRunBinding:
        if not isinstance(run_id, str) or not run_id.strip():
            raise RuntimeIdentityProtocolError("run_id must be a non-empty string")
        try:
            payload = self._transport.request(
                "GET", CANONICAL_RUN_ROUTE_PREFIX + quote(run_id, safe="")
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                raise RuntimeIdentityNotFoundError("canonical Run does not exist") from None
            if isinstance(exc, RuntimeIdentityNotFoundError):
                raise
            raise RuntimeIdentityUnavailableError("canonical Runtime identity service is unavailable") from None
        if payload.get("contract_version") != CANONICAL_RUN_CONTRACT_VERSION:
            raise RuntimeIdentityProtocolError("canonical Runtime identity contract version is unsupported")
        try:
            values = {
                field: _non_empty_string(payload.get(field), field)
                for field in (
                    "tenant_id", "agent_id", "release_id", "session_id", "run_id",
                    "sandbox_id", "workspace_ref",
                )
            }
            revision = payload.get("sandbox_revision")
            if type(revision) is not int or revision <= 0:
                raise RuntimeIdentityProtocolError("sandbox revision must be a positive integer")
            if values["run_id"] != run_id:
                raise RuntimeIdentityProtocolError("canonical response Run ID mismatch")
            if values["workspace_ref"] != f"sandbox://{payload.get('provider', '')}/{values['sandbox_id']}":
                # Older deployments may not project provider as a separate
                # top-level field. Validate against the canonical URI instead.
                if not values["workspace_ref"].startswith("sandbox://") or values["workspace_ref"].endswith("/"):
                    raise RuntimeIdentityProtocolError("canonical workspace reference is invalid")
            run_status = _status_value(payload.get("status"), "status", self._RUN_STATES)
            session_status = _status_value(
                payload.get("session_status"), "session_status", self._SESSION_STATES
            )
        except RuntimeIdentityProtocolError:
            raise
        return CanonicalRuntimeRunBinding(
            **values,
            sandbox_revision=revision,
            status=run_status,
            session_status=session_status,
        )

    _RUN_STATES = {item.value for item in RunStatus}
    _SESSION_STATES = {item.value for item in SessionStatus}
