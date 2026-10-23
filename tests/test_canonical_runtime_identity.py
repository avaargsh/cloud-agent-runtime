from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import quote

import pytest

from cloud_agent_runtime.canonical_runtime_identity import (
    CANONICAL_RUN_CONTRACT_VERSION,
    CANONICAL_RUN_ROUTE_PREFIX,
    CanonicalRuntimeRunBinding,
    HttpsRuntimeStoreRunAuthorityReader,
    RuntimeCanonicalIdentityWSGI,
    RuntimeIdentityNotFoundError,
    RuntimeIdentityProtocolError,
    RuntimeIdentityUnavailableError,
    RuntimeStoreRunAuthorityReader,
)
from cloud_agent_runtime.models import (
    Run,
    RunStatus,
    SandboxBinding,
    Session,
    SessionStatus,
)
from cloud_agent_runtime.store import InMemoryStore, SQLiteStore


RUN_ID = "run-a"
SESSION_ID = "session-a"
WORKSPACE = "sandbox://kubernetes/sandbox-a"


def seed_store(store, *, run_status=RunStatus.RUNNING,
               session_status=SessionStatus.ACTIVE, revision=3,
               workspace_ref=WORKSPACE, include_binding=True):
    session = Session(
        session_id=SESSION_ID,
        agent_id="agent-a",
        release_id="release-a",
        tenant_id="tenant-a",
        status=session_status,
    )
    run = Run(
        run_id=RUN_ID,
        session_id=SESSION_ID,
        status=run_status,
        sandbox_ref=workspace_ref,
        sandbox_binding=(
            SandboxBinding(
                provider="kubernetes",
                sandbox_id="sandbox-a",
                sandbox_ref=workspace_ref,
                revision=revision,
            )
            if include_binding else None
        ),
    )
    store.save_session(session)
    store.save_run(run)
    return run, session


def environ(path, method="GET", scheme="https"):
    return {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "QUERY_STRING": "",
        "wsgi.url_scheme": scheme,
    }


def invoke(app, env):
    output = {}
    def start_response(status, headers):
        output["status"] = status
        output["headers"] = dict(headers)
    body = b"".join(app(env, start_response))
    output["body"] = json.loads(body)
    return output


class AllowRole:
    def __init__(self, allowed=True):
        self.allowed = allowed
        self.calls = []

    def authorize(self, environ, *, required_role):
        self.calls.append(required_role)
        return self.allowed


class MemoryTransport:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.calls = []

    def request(self, method, path, body=None):
        self.calls.append((method, path, body))
        if self.error:
            raise self.error
        return self.response or {}


def test_store_reader_projects_canonical_run_session_and_persisted_sandbox():
    store = InMemoryStore()
    seed_store(store)
    binding = RuntimeStoreRunAuthorityReader(store).get_run_binding(RUN_ID)

    assert isinstance(binding, CanonicalRuntimeRunBinding)
    assert binding.tenant_id == "tenant-a"
    assert binding.agent_id == "agent-a"
    assert binding.release_id == "release-a"
    assert binding.session_id == SESSION_ID
    assert binding.run_id == RUN_ID
    assert binding.sandbox_id == "sandbox-a"
    assert binding.sandbox_revision == 3
    assert binding.workspace_ref == WORKSPACE
    assert binding.status == "running"
    assert binding.session_status == "active"
    assert binding.to_payload()["contract_version"] == CANONICAL_RUN_CONTRACT_VERSION


def test_store_reader_survives_store_reconstruction_and_tracks_new_sandbox_revision(tmp_path: Path):
    db = tmp_path / "runtime.sqlite"
    store = SQLiteStore(db)
    seed_store(store)
    store.close()

    recovered = SQLiteStore(db)
    reader = RuntimeStoreRunAuthorityReader(recovered)
    assert reader.get_run_binding(RUN_ID).sandbox_revision == 3

    current = recovered.get_run(RUN_ID)
    current.sandbox_binding = SandboxBinding(
        provider="kubernetes",
        sandbox_id="sandbox-b",
        sandbox_ref="sandbox://kubernetes/sandbox-b",
        revision=4,
    )
    current.sandbox_ref = "sandbox://kubernetes/sandbox-b"
    recovered.save_run(current)

    rebound = reader.get_run_binding(RUN_ID)
    assert rebound.sandbox_id == "sandbox-b"
    assert rebound.sandbox_revision == 4
    assert rebound.workspace_ref == "sandbox://kubernetes/sandbox-b"
    recovered.close()


def test_store_reader_rejects_unknown_run_or_missing_session():
    store = InMemoryStore()
    seed_store(store)
    reader = RuntimeStoreRunAuthorityReader(store)
    with pytest.raises(RuntimeIdentityNotFoundError):
        reader.get_run_binding("forged-run")
    del store.sessions[SESSION_ID]
    with pytest.raises(RuntimeIdentityNotFoundError):
        reader.get_run_binding(RUN_ID)


@pytest.mark.parametrize("run_status", [
    RunStatus.CREATED,
    RunStatus.PAUSED,
    RunStatus.WAITING_APPROVAL,
    RunStatus.SUCCEEDED,
    RunStatus.FAILED,
    RunStatus.CANCELLED,
])
def test_store_reader_returns_non_running_run_state_without_inventing_a_permit(run_status):
    store = InMemoryStore()
    seed_store(store, run_status=run_status)
    binding = RuntimeStoreRunAuthorityReader(store).get_run_binding(RUN_ID)
    assert binding.status == run_status.value
    # The credential authority consumes this state and denies/inactivates it.


@pytest.mark.parametrize("session_status", [SessionStatus.PAUSED, SessionStatus.CLOSED])
def test_store_reader_returns_inactive_session_state(session_status):
    store = InMemoryStore()
    seed_store(store, session_status=session_status)
    assert RuntimeStoreRunAuthorityReader(store).get_run_binding(RUN_ID).session_status == session_status.value


@pytest.mark.parametrize("revision", [0, -1, True, 1.5])
def test_store_reader_rejects_invalid_sandbox_revision(revision):
    store = InMemoryStore()
    seed_store(store, revision=revision)
    with pytest.raises(RuntimeIdentityProtocolError):
        RuntimeStoreRunAuthorityReader(store).get_run_binding(RUN_ID)


def test_store_reader_rejects_missing_or_divergent_persisted_sandbox_binding():
    store = InMemoryStore()
    seed_store(store, include_binding=False)
    with pytest.raises(RuntimeIdentityProtocolError, match="persisted SandboxBinding"):
        RuntimeStoreRunAuthorityReader(store).get_run_binding(RUN_ID)

    store = InMemoryStore()
    _, _ = seed_store(store)
    changed = store.get_run(RUN_ID)
    changed.sandbox_ref = "sandbox://kubernetes/forged-ref"
    store.save_run(changed)
    with pytest.raises(RuntimeIdentityProtocolError, match="references diverge"):
        RuntimeStoreRunAuthorityReader(store).get_run_binding(RUN_ID)


def test_store_reader_fails_closed_when_authoritative_store_is_unavailable():
    class BrokenStore:
        def get_run(self, run_id):
            raise OSError("secret backing-store details must not escape")

        def get_session(self, session_id):
            raise AssertionError("must not read Session after Run read failed")

    with pytest.raises(RuntimeIdentityUnavailableError, match="Run store is unavailable") as exc:
        RuntimeStoreRunAuthorityReader(BrokenStore()).get_run_binding(RUN_ID)
    assert "secret backing-store details" not in str(exc.value)


def test_read_only_wsgi_requires_https_service_role_and_get():
    store = InMemoryStore()
    seed_store(store)
    authenticator = AllowRole()
    app = RuntimeCanonicalIdentityWSGI(
        reader=RuntimeStoreRunAuthorityReader(store),
        authenticator=authenticator,
    )
    path = CANONICAL_RUN_ROUTE_PREFIX + quote(RUN_ID, safe="")

    insecure = invoke(app, environ(path, scheme="http"))
    assert insecure["status"].startswith("403")
    assert authenticator.calls == []

    denied = invoke(app, environ(path))
    assert denied["status"].startswith("200")
    assert denied["body"]["contract_version"] == CANONICAL_RUN_CONTRACT_VERSION
    assert denied["body"]["tenant_id"] == "tenant-a"
    assert denied["body"]["sandbox_revision"] == 3
    assert denied["headers"]["Cache-Control"] == "no-store"
    assert authenticator.calls == ["runtime.identity.reader"]

    post = invoke(app, environ(path, method="POST"))
    assert post["status"].startswith("405")
    assert post["body"]["error"]["code"] == "method_not_allowed"


def test_read_only_wsgi_rejects_unauthorized_unknown_and_unavailable_reads():
    store = InMemoryStore()
    seed_store(store)
    denied_app = RuntimeCanonicalIdentityWSGI(
        reader=RuntimeStoreRunAuthorityReader(store),
        authenticator=AllowRole(False),
    )
    denied = invoke(denied_app, environ(CANONICAL_RUN_ROUTE_PREFIX + RUN_ID))
    assert denied["status"].startswith("403")

    allowed = AllowRole()
    app = RuntimeCanonicalIdentityWSGI(
        reader=RuntimeStoreRunAuthorityReader(store),
        authenticator=allowed,
    )
    missing = invoke(app, environ(CANONICAL_RUN_ROUTE_PREFIX + "no-such-run"))
    assert missing["status"].startswith("404")
    assert missing["body"]["error"]["code"] == "not_found"

    outage = RuntimeCanonicalIdentityWSGI(
        reader=RuntimeStoreRunAuthorityReader(type("Broken", (), {
            "get_run": lambda self, run_id: (_ for _ in ()).throw(OSError("db down")),
            "get_session": lambda self, session_id: None,
        })()),
        authenticator=AllowRole(),
    )
    unavailable = invoke(outage, environ(CANONICAL_RUN_ROUTE_PREFIX + RUN_ID))
    assert unavailable["status"].startswith("503")
    assert "db down" not in json.dumps(unavailable["body"])


def test_remote_reader_validates_version_identity_revision_and_state():
    store = InMemoryStore()
    seed_store(store)
    payload = RuntimeStoreRunAuthorityReader(store).get_run_binding(RUN_ID).to_payload()
    transport = MemoryTransport(payload)
    reader = HttpsRuntimeStoreRunAuthorityReader(transport)
    binding = reader.get_run_binding(RUN_ID)
    assert binding.tenant_id == "tenant-a"
    assert binding.sandbox_revision == 3
    assert transport.calls == [("GET", CANONICAL_RUN_ROUTE_PREFIX + RUN_ID, None)]

    for override in (
        {"contract_version": "runtime.canonical-run.v999"},
        {"run_id": "other-run"},
        {"sandbox_revision": True},
        {"sandbox_revision": 0},
        {"workspace_ref": "sandbox://kubernetes/other-sandbox"},
        {"status": "unknown"},
    ):
        changed = dict(payload)
        changed.update(override)
        with pytest.raises(RuntimeIdentityProtocolError):
            HttpsRuntimeStoreRunAuthorityReader(MemoryTransport(changed)).get_run_binding(RUN_ID)


def test_remote_reader_maps_not_found_and_outage_to_fail_closed_errors():
    class Http404(Exception):
        status = 404

    with pytest.raises(RuntimeIdentityNotFoundError):
        HttpsRuntimeStoreRunAuthorityReader(MemoryTransport(error=Http404())).get_run_binding(RUN_ID)
    with pytest.raises(RuntimeIdentityUnavailableError):
        HttpsRuntimeStoreRunAuthorityReader(MemoryTransport(error=TimeoutError())).get_run_binding(RUN_ID)
