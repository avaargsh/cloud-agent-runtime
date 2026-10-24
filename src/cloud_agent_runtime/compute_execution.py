from __future__ import annotations

import copy
import hashlib
import json
import re
import secrets
from typing import Any, Mapping, Protocol

from .models import Run
from .store import RuntimeStore


_IMAGE_DIGEST = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
_REQUEST_ID = re.compile(r"^[0-9a-f]{32}$")
_PROFILE_ID = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_ENVIRONMENTS_WITHOUT_COMPUTE = frozenset({"worker", "sandbox", "workspace"})
_COMPUTE_STATES = frozenset(
    {"REQUESTED", "ALLOCATING", "ALLOCATED", "RELEASING", "RELEASED", "UNKNOWN"}
)


class ComputeProfileConflict(ValueError):
    """A Run was replayed with a different execution-resource intent."""


class ComputeResourceClient(Protocol):
    """Client seam for the authoritative Management/Compute control plane.

    Implementations must submit through the authenticated public Management API,
    recover an ambiguous outcome by the same requestId/Idempotency-Key, and
    return the server-authoritative allocation projection. They must not infer
    ALLOCATED or RELEASED from an HTTP ACK alone.
    """

    async def submit_or_reconcile(
        self,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        ...


def _identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 128 or value.strip() != value:
        raise ValueError(f"{name} must be a non-empty canonical identifier")
    if any(character.isspace() for character in value):
        raise ValueError(f"{name} must not contain whitespace")
    return value


def _build_request(
    *,
    run: Run,
    agent_id: str,
    profile: Mapping[str, Any],
    compute_pool_id: str,
    request_id: str,
    image: str,
    command: list[str] | None,
) -> dict[str, Any]:
    profile_id = profile.get("profileId")
    if not isinstance(profile_id, str) or not _PROFILE_ID.fullmatch(profile_id):
        raise ValueError("profile.profileId must be a DNS-safe identifier")
    revision = profile.get("revision")
    if type(revision) is not int or revision < 1:
        raise ValueError("profile.revision must be a positive integer")
    project_id = _identifier(profile.get("projectId"), "profile.projectId")
    compute_pool_id = _identifier(compute_pool_id, "computePoolId")
    if not _REQUEST_ID.fullmatch(request_id):
        raise ValueError("requestId must be exactly 32 lowercase hexadecimal characters")
    if not isinstance(image, str) or not _IMAGE_DIGEST.fullmatch(image):
        raise ValueError("workload image must be pinned to @sha256:<64 lowercase hex>")
    if command is not None:
        if (
            not isinstance(command, list)
            or len(command) > 128
            or not all(isinstance(item, str) and len(item) <= 4096 for item in command)
        ):
            raise ValueError("command must be a list of at most 128 strings (4096 chars each)")

    accelerator = profile.get("accelerator")
    if not isinstance(accelerator, Mapping) or accelerator.get("mode") != "required":
        raise ValueError("gpu-workload profiles must declare accelerator.mode=required")
    accelerator_class = accelerator.get("class")
    quota = accelerator.get("quota")
    if (
        not isinstance(accelerator_class, str)
        or not accelerator_class.strip()
        or len(accelerator_class) > 128
        or any(character.isspace() for character in accelerator_class)
    ):
        raise ValueError("GPU accelerator class is invalid")
    if type(quota) is not int or quota < 1:
        raise ValueError("GPU accelerator quota must be a positive integer")

    # Run IDs may be caller-supplied and need not be Kubernetes-safe. Derive a
    # stable provider Workload ID without replacing the canonical Run/Session IDs.
    workload_suffix = hashlib.sha256(
        f"{run.session_id}\\0{run.run_id}".encode("utf-8")
    ).hexdigest()[:24]
    workload: dict[str, Any] = {
        "id": f"run-{workload_suffix}",
        "expectedGeneration": 0,
        "image": image,
        "accelerator": {"class": accelerator_class, "quota": quota},
    }
    if command is not None:
        workload["command"] = list(command)

    request: dict[str, Any] = {
        "apiVersion": "compute.request/v0.1",
        "requestId": request_id,
        "projectId": project_id,
        "computePoolId": compute_pool_id,
        "profileRef": {"profileId": profile_id, "revision": revision},
        "correlation": {
            "agentId": agent_id,
            "sessionId": run.session_id,
            "runId": run.run_id,
        },
        "workload": workload,
    }
    canonical = json.dumps(request, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    request["intentDigest"] = "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return request


def _validate_allocation(
    allocation: Mapping[str, Any],
    request: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {
        "requestId": request["requestId"],
        "projectId": request["projectId"],
        "computePoolId": request["computePoolId"],
        "workloadId": request["workload"]["id"],
    }
    for field, value in expected.items():
        if allocation.get(field) != value:
            raise RuntimeError(
                f"Control Plane allocation identity mismatch for {field}; fail closed"
            )

    generation = allocation.get("generation")
    observed_generation = allocation.get("observedGeneration")
    if type(generation) is not int or generation < 1:
        raise RuntimeError("Control Plane returned an invalid desired generation")
    if type(observed_generation) is not int or observed_generation < 0:
        raise RuntimeError("Control Plane returned an invalid observed generation")

    cluster_id = allocation.get("clusterId")
    provider = allocation.get("provider")
    binding_generation = allocation.get("providerBindingGeneration")
    if not isinstance(cluster_id, str) or not cluster_id:
        raise RuntimeError("Control Plane allocation is missing clusterId")
    if not isinstance(provider, str) or not provider:
        raise RuntimeError("Control Plane allocation is missing provider identity")
    if type(binding_generation) is not int or binding_generation < 1:
        raise RuntimeError("Control Plane allocation has an invalid provider binding generation")

    state = allocation.get("state")
    if state not in _COMPUTE_STATES:
        raise RuntimeError("Control Plane returned an unknown ComputeAllocation state")
    provider_refs = allocation.get("providerRefs", [])
    conditions = allocation.get("conditions", [])
    evidence_refs = allocation.get("evidenceRefs", [])
    if not isinstance(provider_refs, list) or not isinstance(conditions, list):
        raise RuntimeError("Control Plane allocation has malformed provider refs or conditions")
    if not isinstance(evidence_refs, list):
        raise RuntimeError("Control Plane allocation has malformed evidence refs")

    if state in {"ALLOCATED", "RELEASED"} and observed_generation != generation:
        raise RuntimeError(
            f"Control Plane claimed {state} with stale observedGeneration "
            f"{observed_generation} for desired generation {generation}"
        )

    if state == "ALLOCATED":
        owns = any(
            isinstance(condition, Mapping)
            and condition.get("type") == "OwnershipProven"
            and condition.get("status") == "TRUE"
            for condition in conditions
        )
        if not provider_refs or not owns or not evidence_refs:
            raise RuntimeError(
                "Control Plane claimed ALLOCATED without current ownership evidence"
            )
        image_digest = request["workload"]["image"].rsplit("@", 1)[1]
        for reference in provider_refs:
            if not isinstance(reference, Mapping):
                raise RuntimeError("Control Plane returned a malformed provider reference")
            for field in ("kind", "namespace", "name", "uid", "resourceVersion", "generation", "ownershipProofRef"):
                if field not in reference or reference[field] in (None, ""):
                    raise RuntimeError(f"Provider reference is missing {field}")
            if type(reference["generation"]) is not int or reference["generation"] < 1:
                raise RuntimeError("Provider reference has an invalid object generation")
            if reference["kind"] in {"Job", "Pod"}:
                if reference.get("imageDigest") != image_digest:
                    raise RuntimeError("Provider reference image digest does not match requested workload")
    if state == "RELEASED":
        cleaned = any(
            isinstance(condition, Mapping)
            and condition.get("type") == "CleanupComplete"
            and condition.get("status") == "TRUE"
            for condition in conditions
        )
        if provider_refs or not cleaned or not evidence_refs:
            raise RuntimeError(
                "Control Plane claimed RELEASED while cleanup/absence evidence is incomplete"
            )
    return copy.deepcopy(dict(allocation))


class ExecutionProfileRouter:
    """Route a canonical Run through the v0.1 execution-resource contract.

    The profile must be a trusted, schema-validated projection obtained from the
    authoritative profile resolver; never pass a profile document directly
    supplied by a browser or Agent. Ordinary Worker/Sandbox/Workspace profiles
    stay on the existing runtime path and never call the Compute client. GPU
    Workloads use the same persisted Session/Run identity and one immutable
    ComputeRequest intent, stored before the external side effect.

    This is a client adapter seam, not a production HTTP client. The injected
    ComputeResourceClient owns OIDC/authz, Management receipts, Desired-state
    reads, ambiguity recovery and independent allocation observations.
    """

    def __init__(self, *, store: RuntimeStore, compute_client: ComputeResourceClient) -> None:
        if not callable(getattr(store, "persist_compute_request", None)):
            raise TypeError(
                "RuntimeStore must implement atomic persist_compute_request for GPU retries"
            )
        self.store = store
        self.compute_client = compute_client

    async def dispatch(
        self,
        run_id: str,
        *,
        profile: Mapping[str, Any],
        compute_pool_id: str | None = None,
        image: str | None = None,
        command: list[str] | None = None,
    ) -> dict[str, Any] | None:
        run = self.store.get_run(run_id)
        session = self.store.get_session(run.session_id)
        if profile.get("apiVersion") != "agent.execution-profile/v0.1":
            raise ValueError("unsupported execution profile apiVersion")
        environment = profile.get("environment")
        accelerator = profile.get("accelerator")
        if not isinstance(accelerator, Mapping):
            raise ValueError("profile.accelerator must be an object")
        _identifier(profile.get("projectId"), "profile.projectId")

        if environment in _ENVIRONMENTS_WITHOUT_COMPUTE:
            if accelerator.get("mode") != "none":
                raise ValueError("non-GPU profiles must declare accelerator.mode=none")
            if run.compute_request is not None:
                raise ComputeProfileConflict(
                    "a Run with an immutable GPU ComputeRequest cannot switch to a non-GPU profile"
                )
            return None

        if environment != "gpu-workload":
            raise ValueError("unsupported execution profile environment")
        if compute_pool_id is None:
            raise ValueError("gpu-workload requires an authorized computePoolId")
        if image is None:
            raise ValueError("gpu-workload requires an immutable workload image")
        if run.compute_request is not None:
            stored_id = run.compute_request.get("requestId")
            if not isinstance(stored_id, str) or not _REQUEST_ID.fullmatch(stored_id):
                raise ComputeProfileConflict("persisted ComputeRequest has an invalid requestId")
            request_id = stored_id
        else:
            # This random key is persisted atomically before submit_or_reconcile.
            request_id = secrets.token_hex(16)

        candidate = _build_request(
            run=run,
            agent_id=session.agent_id,
            profile=profile,
            compute_pool_id=compute_pool_id,
            request_id=request_id,
            image=image,
            command=command,
        )
        try:
            request = self.store.persist_compute_request(run_id, candidate)
        except ValueError as exc:
            raise ComputeProfileConflict(str(exc)) from exc

        # A restarted/replayed caller submits the same stored request ID. The
        # adapter must reconcile the original receipt before any new side effect.
        allocation = await self.compute_client.submit_or_reconcile(copy.deepcopy(request))
        if not isinstance(allocation, Mapping):
            raise RuntimeError("Compute client returned a non-object allocation")
        return _validate_allocation(allocation, request)
