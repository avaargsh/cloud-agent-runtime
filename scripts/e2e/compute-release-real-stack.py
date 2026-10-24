from __future__ import annotations

import asyncio
import json
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import http.client
import ssl
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

from cloud_agent_runtime import AgentRuntime, ExecutionProfileRouter, SQLiteStore
from cloud_agent_runtime.management_compute_client import ManagementComputeClient


POLL_SECONDS = 2
_IMAGE_DIGEST = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"required environment variable is missing: {name}")
    return value


def loopback_url(name: str, default: str) -> str:
    value = os.environ.get(name, default).strip().rstrip("/")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "http"
        or parsed.hostname != "127.0.0.1"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise RuntimeError(f"{name} must be an http://127.0.0.1[:port] URL without a path")
    if parsed.port is not None and not 1 <= parsed.port <= 65535:
        raise RuntimeError(f"{name} has an invalid port")
    return value


def http_json(url: str, *, token: str | None = None, timeout: int = 5) -> tuple[int, dict[str, Any] | None]:
    headers = {"Accept": "application/json", "Cache-Control": "no-store"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            if not body:
                return response.status, None
            value = json.loads(body)
            return response.status, value if isinstance(value, dict) else None
    except urllib.error.HTTPError as response:
        return response.code, None
    except (urllib.error.URLError, TimeoutError, OSError):
        return 0, None


def kubectl_json(namespace: str, kind: str, selector: str, context: str | None) -> dict[str, Any]:
    command = ["kubectl"]
    if context:
        command.extend(["--context", context])
    command.extend(["-n", namespace, "get", kind, "-l", selector, "-o", "json"])
    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=25,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"kubectl provider absence check failed for {kind}: exit={completed.returncode}")
    try:
        document = json.loads(completed.stdout)
    except ValueError as exc:
        raise RuntimeError(f"kubectl returned invalid JSON for {kind}") from exc
    if not isinstance(document, dict) or not isinstance(document.get("items"), list):
        raise RuntimeError(f"kubectl returned an invalid list for {kind}")
    return document


def kubectl_object(namespace: str, kind: str, name: str, context: str | None) -> dict[str, Any] | None:
    command = ["kubectl"]
    if context:
        command.extend(["--context", context])
    command.extend(["-n", namespace, "get", kind, name, "-o", "json"])
    completed = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=25,
    )
    if completed.returncode != 0:
        if "NotFound" in completed.stderr or "not found" in completed.stderr.lower():
            return None
        raise RuntimeError(f"kubectl GET {kind}/{name} failed: exit={completed.returncode}")
    try:
        document = json.loads(completed.stdout)
    except ValueError as exc:
        raise RuntimeError(f"kubectl returned invalid JSON for {kind}/{name}") from exc
    return document if isinstance(document, dict) else None


def true_condition_types(conditions: Any) -> set[str]:
    if not isinstance(conditions, list):
        return set()
    return {
        str(item.get("type"))
        for item in conditions
        if isinstance(item, Mapping) and item.get("status") in (True, "True", "TRUE")
    }


class DisposableKeycloakTokenProvider:
    """Test-only direct-grant token provider for the ephemeral Keycloak realm."""

    def __init__(self) -> None:
        self.issuer = required_env("ACK_OIDC_ISSUER").rstrip("/")
        self.ca_cert = required_env("ACK_OIDC_CA_CERT")
        self._cached_token = ""
        self._expires_at = 0.0

    async def access_token_for_project(self, project_id: str) -> str:
        # Keep direct-grant credentials restricted to this disposable integration
        # realm. The test never emits the response body or the bearer token.
        if self._cached_token and time.monotonic() < self._expires_at:
            return self._cached_token
        form = urllib.parse.urlencode({
            "grant_type": "password",
            "client_id": "gpu-e2e-direct",
            "client_secret": "e2e-direct-secret-not-for-production",
            "username": "ci-operator",
            "password": "ci-disposable-password-not-for-production",
        }).encode("utf-8")
        request = urllib.request.Request(
            f"{self.issuer}/protocol/openid-connect/token",
            data=form,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        context = ssl.create_default_context(cafile=self.ca_cert)
        try:
            with urllib.request.urlopen(request, context=context, timeout=15) as response:
                value = json.loads(response.read())
        except Exception as exc:
            raise RuntimeError(f"disposable OIDC token mint failed ({type(exc).__name__})") from None
        token = value.get("access_token") if isinstance(value, dict) else None
        if not isinstance(token, str) or not token:
            raise RuntimeError("disposable OIDC token response omitted access_token")
        ttl = value.get("expires_in", 60)
        self._cached_token = token
        self._expires_at = time.monotonic() + max(1, int(ttl) - 15)
        return token


class TestPoolBindingResolver:
    """Trusted fixture resolver; the public Management API still authorizes the route."""

    async def resolve(self, *, project_id: str, compute_pool_id: str) -> Mapping[str, Any]:
        return {
            "clusterId": required_env("CLUSTER_ID"),
            "provider": "kueue",
            "providerBindingGeneration": int(os.environ.get("PROVIDER_BINDING_GENERATION", "1")),
        }


class LiveProviderAbsenceObserver:
    """Observe the Control Plane and Kubernetes API independently of DELETE ACK."""

    def __init__(self, control_plane_url: str, namespace: str, context: str | None) -> None:
        self.control_plane_url = control_plane_url.rstrip("/")
        self.namespace = namespace
        self.context = context

    async def observe(
        self,
        *,
        request: dict[str, Any],
        binding: Mapping[str, Any],
        receipt: Mapping[str, Any],
        desired: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        workload_id = str(request["workload"]["id"])
        generation = int(
            desired.get("generation")
            if desired is not None
            else receipt.get("resultingGeneration", 0)
        )
        deletion = receipt.get("action") == "delete-request"
        state = "RELEASING" if deletion else "REQUESTED"
        common: dict[str, Any] = {
            "requestId": request["requestId"],
            "projectId": request["projectId"],
            "computePoolId": request["computePoolId"],
            "workloadId": workload_id,
            "generation": generation,
            "clusterId": binding["clusterId"],
            "provider": binding["provider"],
            "providerBindingGeneration": binding["providerBindingGeneration"],
            "observedGeneration": 0,
            "state": state,
            "providerRefs": [],
            "conditions": [],
            "evidenceRefs": [],
        }
        if desired is not None:
            # A deletion receipt/deleting bit is not enough for RELEASED.
            return common

        control_status, _ = http_json(
            f"{self.control_plane_url}/api/v1/workloads/{workload_id}",
            timeout=5,
        )
        selector = f"ai.compute/workload=job-{workload_id}"
        jobs = kubectl_json(self.namespace, "jobs", selector, self.context)
        pods = kubectl_json(self.namespace, "pods", selector, self.context)
        kueue_workloads = kubectl_json(self.namespace, "workloads.kueue.x-k8s.io", selector, self.context)
        exact_job = kubectl_object(self.namespace, "job", f"job-{workload_id}", self.context)
        if (
            control_status != 404
            or jobs["items"]
            or pods["items"]
            or kueue_workloads["items"]
            or exact_job is not None
        ):
            return common

        # This test uses Kueue's Job-backed accelerator path (not DRA). RELEASED
        # requires independent Control Plane, Job, Pod, and Kueue Workload absence.
        common.update({
            "state": "RELEASED",
            "observedGeneration": generation,
            "conditions": [{
                "type": "CleanupComplete",
                "status": "TRUE",
                "reason": "IndependentControlPlaneAndProviderAbsence",
                "observedAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            }],
            "evidenceRefs": [
                f"management-operation://{receipt.get('requestId')}",
                f"control-plane://{binding['clusterId']}/workloads/{workload_id}/404",
                f"kubernetes://{binding['clusterId']}/namespaces/{self.namespace}/jobs/job-{workload_id}/absent",
                f"kubernetes://{binding['clusterId']}/namespaces/{self.namespace}/pods?selector={urllib.parse.quote(selector)}/absent",
                f"kueue://{binding['clusterId']}/namespaces/{self.namespace}/workloads?selector={urllib.parse.quote(selector)}/absent",
            ],
        })
        return common


def build_client(base_url: str, observer: LiveProviderAbsenceObserver | None) -> ManagementComputeClient:
    return ManagementComputeClient(
        base_url=base_url,
        token_provider=DisposableKeycloakTokenProvider(),
        binding_resolver=TestPoolBindingResolver(),
        observer=observer,
        allow_loopback_http=True,
        timeout_seconds=20,
    )


def profile(project_id: str, accelerator_class: str) -> dict[str, Any]:
    return {
        "apiVersion": "agent.execution-profile/v0.1",
        "profileId": "runtime-gpu-ack-e2e",
        "revision": 1,
        "projectId": project_id,
        "environment": "gpu-workload",
        "resources": {
            "cpuMilli": 100,
            "memoryMiB": 128,
            "ephemeralStorageMiB": 256,
            "timeoutSeconds": 240,
            "maxConcurrency": 1,
        },
        "accelerator": {"mode": "required", "class": accelerator_class, "quota": 1},
        "persistence": "ephemeral",
        "networkPolicyRef": "default-deny",
        "credentialBindingRefs": [],
    }


def wait_for_provider(
    control_plane_url: str,
    namespace: str,
    workload_id: str,
    generation: int,
    image: str,
    context: str | None,
    timeout: int,
) -> None:
    deadline = time.monotonic() + timeout
    selector = f"ai.compute/workload=job-{workload_id}"
    expected_job = f"job-{workload_id}"
    while time.monotonic() < deadline:
        code, data = http_json(f"{control_plane_url.rstrip('/')}/api/v1/workloads/{workload_id}")
        if code == 200 and data is not None:
            metadata = data.get("metadata") or {}
            status = data.get("status") or {}
            conditions = true_condition_types(status.get("conditions"))
            if (
                metadata.get("generation") == generation
                and status.get("syncState") == "Synced"
                and "QuotaReserved" in conditions
                and "Admitted" in conditions
                and bool({"Ready", "Succeeded"} & conditions)
            ):
                job = kubectl_object(namespace, "job", expected_job, context)
                pods = kubectl_json(namespace, "pods", selector, context)
                if job is not None and pods["items"]:
                    job_meta = job.get("metadata") or {}
                    labels = job_meta.get("labels") or {}
                    annotations = job_meta.get("annotations") or {}
                    template = (job.get("spec") or {}).get("template") or {}
                    pod_spec = template.get("spec") or {}
                    containers = pod_spec.get("containers") or []
                    requested_image = containers[0].get("image") if containers else None
                    requested_resources = (containers[0].get("resources") or {}).get("requests", {}) if containers else {}
                    if (
                        job_meta.get("name") == expected_job
                        and labels.get("ai.compute/workload") == expected_job
                        and str(annotations.get("ai.compute/generation", "")) == str(generation)
                        and requested_image == image
                        and str(requested_resources.get("nvidia.com/gpu", "")) == "1"
                    ):
                        for pod in pods["items"]:
                            pod_meta = pod.get("metadata") or {}
                            pod_status = pod.get("status") or {}
                            pod_spec_live = pod.get("spec") or {}
                            pod_ready = "Ready" in true_condition_types(pod_status.get("conditions"))
                            owner = any(
                                isinstance(item, Mapping)
                                and item.get("kind") == "Job"
                                and item.get("name") == expected_job
                                and item.get("uid") == job_meta.get("uid")
                                for item in (pod_meta.get("ownerReferences") or [])
                            )
                            pod_containers = pod_spec_live.get("containers") or []
                            pod_image = pod_containers[0].get("image") if pod_containers else None
                            if pod_status.get("phase") == "Running" and pod_ready and owner and pod_image == image:
                                return
        time.sleep(POLL_SECONDS)
    raise TimeoutError(
        f"timed out waiting for current-generation Kueue admission and independently bound Job/Pod: {workload_id}"
    )


@dataclass
class DeleteAckDropState:
    committed: threading.Event
    allow_drop: threading.Event
    backend_status: int | None = None
    request_id: str | None = None
    error: str | None = None
    intercepted: bool = False


def start_delete_ack_drop_proxy(upstream_url: str) -> tuple[ThreadingHTTPServer, DeleteAckDropState]:
    upstream = urlsplit(upstream_url)
    state = DeleteAckDropState(threading.Event(), threading.Event())

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, _format: str, *_args: Any) -> None:
            return

        def do_GET(self) -> None:  # noqa: N802
            self._forward()

        def do_PUT(self) -> None:  # noqa: N802
            self._forward()

        def do_DELETE(self) -> None:  # noqa: N802
            self._forward()

        def _forward(self) -> None:
            if self.path.startswith("http://") or self.path.startswith("https://"):
                self.send_error(400)
                return
            length = int(self.headers.get("Content-Length", "0"))
            if length < 0 or length > 2 * 1024 * 1024:
                self.send_error(413)
                return
            body = self.rfile.read(length) if length else None
            headers = {}
            for name in ("Authorization", "Content-Type", "Idempotency-Key", "If-Match", "Accept", "Cache-Control", "Origin"):
                value = self.headers.get(name)
                if value is not None:
                    headers[name] = value
            connection = http.client.HTTPConnection(upstream.hostname, upstream.port or 80, timeout=45)
            try:
                connection.request(self.command, self.path, body=body, headers=headers)
                response = connection.getresponse()
                response_body = response.read()
                should_drop = (
                    self.command == "DELETE"
                    and response.status in (202, 204)
                    and not state.intercepted
                )
                if should_drop:
                    state.intercepted = True
                    state.backend_status = response.status
                    state.request_id = self.headers.get("Idempotency-Key")
                    state.committed.set()
                    if not state.allow_drop.wait(timeout=90):
                        state.error = "timed out waiting for parent to SIGKILL caller"
                    # Return without sending any HTTP status/headers. The
                    # caller must die while the committed response is withheld.
                    self.close_connection = True
                    try:
                        self.connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    self.connection.close()
                    return
                self.send_response(response.status)
                for key, value in response.getheaders():
                    if key.lower() in {
                        "connection", "keep-alive", "proxy-authenticate",
                        "proxy-authorization", "te", "trailers",
                        "transfer-encoding", "upgrade", "content-length",
                    }:
                        continue
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(response_body)))
                self.end_headers()
                if response_body and response.status not in (204, 304):
                    self.wfile.write(response_body)
            except Exception as exc:
                state.error = f"fault proxy failure ({type(exc).__name__})"
                state.committed.set()
                try:
                    self.send_error(502)
                except OSError:
                    pass
            finally:
                connection.close()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    return server, state


async def release_child(db_path: str, run_id: str, allocation_path: str, proxy_url: str) -> int:
    store = SQLiteStore(db_path)
    allocation = json.loads(Path(allocation_path).read_text(encoding="utf-8"))
    observer = LiveProviderAbsenceObserver(
        loopback_url("CONTROL_PLANE_BASE_URL", "http://127.0.0.1:8080"),
        required_env("NAMESPACE"),
        os.environ.get("KUBECTL_CONTEXT", "").strip() or None,
    )
    client = build_client(proxy_url, observer)
    router = ExecutionProfileRouter(store=store, compute_client=client)
    result = await router.release(run_id, allocation=allocation)
    print(
        f"UNEXPECTED: release caller received {result.get('state')} before parent killed it",
        file=sys.stderr,
    )
    store.close()
    return 2


def main() -> int:
    management_url = loopback_url("MANAGEMENT_BASE_URL", "http://127.0.0.1:8081")
    control_plane_url = loopback_url("CONTROL_PLANE_BASE_URL", "http://127.0.0.1:8080")
    cluster_id = required_env("CLUSTER_ID")
    project_id = required_env("PROJECT_ID")
    pool_id = required_env("POOL_ID")
    namespace = required_env("NAMESPACE")
    accelerator_class = required_env("ACCELERATOR_CLASS")
    image = required_env("IMAGE")
    if not _IMAGE_DIGEST.fullmatch(image):
        raise RuntimeError("IMAGE must be pinned to @sha256:<64 lowercase hex>")
    timeout = int(os.environ.get("ACCEPTANCE_TIMEOUT_SECONDS", "240"))
    if timeout < 30 or timeout > 1800:
        raise RuntimeError("ACCEPTANCE_TIMEOUT_SECONDS must be between 30 and 1800")
    context = os.environ.get("KUBECTL_CONTEXT", "").strip() or None

    with tempfile.TemporaryDirectory(prefix="runtime-compute-release-") as temp_dir:
        root = Path(temp_dir)
        db_path = str(root / "runtime.sqlite")
        allocation_path = root / "allocation.json"
        child_log_path = root / "release-child.log"

        store = SQLiteStore(db_path)
        runtime = AgentRuntime(store=store)
        session = runtime.create_session(
            agent_id="runtime-compute-release-acceptance",
            release_id="runtime-compute-release-v0.1",
            tenant_id=project_id,
        )
        run = runtime.start_run(session_id=session.session_id)
        create_request_id_before = store.get_run(run.run_id).compute_request
        client = build_client(
            management_url,
            LiveProviderAbsenceObserver(control_plane_url, namespace, context),
        )
        router = ExecutionProfileRouter(store=store, compute_client=client)
        allocation = asyncio.run(router.dispatch(
            run.run_id,
            profile=profile(project_id, accelerator_class),
            compute_pool_id=pool_id,
            image=image,
            command=["/bin/sh", "-c", "sleep 3600"],
        ))
        if not isinstance(allocation, Mapping):
            raise RuntimeError("runtime dispatcher returned no GPU allocation projection")
        durable = store.get_run(run.run_id)
        create_request = durable.compute_request
        binding = durable.compute_binding
        if create_request is None or binding is None:
            raise RuntimeError("Runtime did not persist ComputeRequest + route before Management create")
        if allocation.get("state") not in {"REQUESTED", "ALLOCATING", "ALLOCATED"}:
            raise RuntimeError(f"unexpected create allocation state: {allocation.get('state')}")
        generation = allocation.get("generation")
        if type(generation) is not int or generation < 1:
            raise RuntimeError("Management API did not return a server-issued Workload Generation")
        workload_id = create_request["workload"]["id"]
        if create_request_id_before is not None and create_request_id_before.get("requestId") != create_request["requestId"]:
            raise RuntimeError("Compute create request ID changed after dispatch")
        wait_for_provider(
            control_plane_url,
            namespace,
            workload_id,
            generation,
            image,
            context,
            timeout,
        )
        print(
            f"PASS: Runtime client created live Kueue Workload; runId={run.run_id}; "
            f"sessionId={session.session_id}; createRequestId={create_request['requestId']}; "
            f"workloadId={workload_id}; generation={generation}"
        )
        allocation_path.write_text(json.dumps(allocation, sort_keys=True), encoding="utf-8")
        store.close()

        proxy, state = start_delete_ack_drop_proxy(management_url)
        proxy_thread = threading.Thread(target=proxy.serve_forever, daemon=True)
        proxy_thread.start()
        proxy_url = f"http://127.0.0.1:{proxy.server_port}"
        env = os.environ.copy()
        env["MANAGEMENT_BASE_URL"] = proxy_url
        with child_log_path.open("w", encoding="utf-8") as log:
            child = subprocess.Popen(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--release-child",
                    db_path,
                    run.run_id,
                    str(allocation_path),
                    proxy_url,
                ],
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=log,
                env=env,
            )
            try:
                if not state.committed.wait(timeout=timeout):
                    child.kill()
                    child.wait(timeout=10)
                    raise RuntimeError(state.error or "Management DELETE did not commit before timeout")
                if state.error:
                    raise RuntimeError(state.error)
                if state.backend_status not in (202, 204):
                    raise RuntimeError(f"expected committed DELETE 202/204, got {state.backend_status}")
                if child.poll() is not None:
                    raise RuntimeError("release caller exited before the post-commit ACK-drop barrier")
                child.kill()
                exit_code = child.wait(timeout=10)
                if exit_code != -signal.SIGKILL:
                    raise RuntimeError(f"release caller was not SIGKILLed (exit={exit_code})")
                state.allow_drop.set()
                print(
                    f"PASS: Runtime DELETE commit ACK withheld and caller SIGKILLed; "
                    f"releaseRequestId={state.request_id}; expectedGeneration={generation}"
                )
            finally:
                state.allow_drop.set()
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=10)
                proxy.shutdown()
                proxy.server_close()
                proxy_thread.join(timeout=5)

        store = SQLiteStore(db_path)
        restarted = store.get_run(run.run_id)
        if restarted.compute_request is None or restarted.compute_request["requestId"] != create_request["requestId"]:
            raise RuntimeError("create request identity changed across SQLite restart")
        if restarted.compute_release_request is None:
            raise RuntimeError("release intent was not persisted before DELETE side effect")
        release_request_id = restarted.compute_release_request["requestId"]
        if (
            release_request_id == create_request["requestId"]
            or restarted.compute_release_request["sourceRequestId"] != create_request["requestId"]
            or restarted.compute_release_request["expectedGeneration"] != generation
        ):
            raise RuntimeError("durable release request identity/source/generation is invalid")
        if state.request_id != release_request_id:
            raise RuntimeError("DELETE Idempotency-Key differs from persisted release request ID")
        print(
            f"PASS: SQLite restart retained createRequestId={create_request['requestId']} "
            f"and separate releaseRequestId={release_request_id}"
        )

        client = build_client(management_url, LiveProviderAbsenceObserver(control_plane_url, namespace, context))
        router = ExecutionProfileRouter(store=store, compute_client=client)
        result = asyncio.run(router.release(run.run_id, allocation=allocation))
        if result.get("state") not in {"RELEASING", "RELEASED"}:
            raise RuntimeError(f"release receipt recovery returned unexpected state: {result.get('state')}")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and result.get("state") != "RELEASED":
            time.sleep(POLL_SECONDS)
            result = asyncio.run(router.release(run.run_id, allocation=result))
        if result.get("state") != "RELEASED":
            raise RuntimeError("Runtime never observed independent Provider absence/finalization; RELEASED is unproven")
        final_run = store.get_run(run.run_id)
        if final_run.compute_request["requestId"] != create_request["requestId"]:
            raise RuntimeError("create request identity changed during release recovery")
        if final_run.compute_release_request["requestId"] != release_request_id:
            raise RuntimeError("release request ID changed during repeated release reconciliation")
        if final_run.run_id != run.run_id or final_run.session_id != session.session_id:
            raise RuntimeError("Agent/Session/Run identity changed during provider release recovery")
        print(
            f"PASS: Runtime reconciled same-key DELETE receipt after restart; RELEASED independently proved; "
            f"createRequestId={create_request['requestId']}; releaseRequestId={release_request_id}; "
            f"generation={generation}; Job/Pod/ResourceClaim absent"
        )
        store.close()
        return 0


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--release-child":
        if len(sys.argv) != 6:
            raise SystemExit(64)
        try:
            sys.exit(asyncio.run(release_child(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])))
        except Exception:
            # Do not log tokens or full request/receipt payloads in the worker.
            raise SystemExit(1)
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"FAIL: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)
