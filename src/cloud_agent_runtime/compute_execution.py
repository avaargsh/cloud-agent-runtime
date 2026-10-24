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

    async def resolve_binding(
        self,
        *,
        project_id: str,
        compute_pool_id: str,
    ) -> dict[str, Any]:
        """Resolve the authoritative Pool -> Cluster route before first intent commit."""
        ...

    async def submit_or_reconcile(
        self,
        request: dict[str, Any],
        *,
        binding: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Reconcile only through the route frozen in the persisted Run."""
        ...


    async def verify_release_target(
        self,
        *,
        request: Mapping[str, Any],
        binding: Mapping[str, Any],
        expected_generation: int,
    ) -> None:
        """Verify the authorized current Desired Generation before release intent is persisted."""
        ...

    async def release_or_reconcile(
        self,
        release_request: dict[str, Any],
        *,
        request: Mapping[str, Any],
        binding: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Release through the public Management API and prove cleanup independently."""
        ...


def _validate_binding(
    binding: Mapping[str, Any],
    *,
    project_id: str,
    compute_pool_id: str,
) -> dict[str, Any]:
    if binding.get("projectId") != project_id:
        raise ValueError("trusted ComputePool binding projectId does not match ComputeRequest")
    if binding.get("computePoolId") != compute_pool_id:
        raise ValueError("trusted ComputePool binding poolId does not match ComputeRequest")
    cluster_id = binding.get("clusterId")
    provider = binding.get("provider")
    generation = binding.get("providerBindingGeneration")
    if not isinstance(cluster_id, str) or not cluster_id:
        raise ValueError("trusted ComputePool binding is missing clusterId")
    if not isinstance(provider, str) or not provider:
        raise ValueError("trusted ComputePool binding is missing provider")
    if type(generation) is not int or generation < 1:
        raise ValueError("trusted ComputePool binding has an invalid providerBindingGeneration")
    # Persist only the contract's stable route identity, not credentials or
    # transient resolver data.
    return {
        "projectId": project_id,
        "computePoolId": compute_pool_id,
        "clusterId": cluster_id,
        "provider": provider,
        "providerBindingGeneration": generation,
    }


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


def _build_release_request(
    *,
    run: Run,
    request: Mapping[str, Any],
    request_id: str,
    expected_generation: int,
) -> dict[str, Any]:
    if not _REQUEST_ID.fullmatch(request_id):
        raise ValueError("release requestId must be exactly 32 lowercase hexadecimal characters")
    source_id = request.get("requestId")
    if not isinstance(source_id, str) or not _REQUEST_ID.fullmatch(source_id):
        raise ValueError("source ComputeRequest requestId is invalid")
    if request_id == source_id:
        raise ValueError("release requestId must be distinct from the create requestId")
    if type(expected_generation) is not int or expected_generation < 1:
        raise ValueError("release expectedGeneration must be a positive server-issued Generation")
    correlation = request.get("correlation")
    if not isinstance(correlation, Mapping):
        raise ValueError("source ComputeRequest correlation is malformed")
    release_request: dict[str, Any] = {
        "apiVersion": "compute.release-request/v0.1",
        "requestId": request_id,
        "sourceRequestId": source_id,
        "projectId": request["projectId"],
        "computePoolId": request["computePoolId"],
        "workloadId": request["workload"]["id"],
        "expectedGeneration": expected_generation,
        "correlation": {
            "agentId": correlation["agentId"],
            "sessionId": run.session_id,
            "runId": run.run_id,
        },
    }
    canonical = json.dumps(
        release_request,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    release_request["intentDigest"] = "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return release_request


def _validate_allocation(
    allocation: Mapping[str, Any],
    request: Mapping[str, Any],
    binding: Mapping[str, Any],
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
    for field in ("clusterId", "provider", "providerBindingGeneration"):
        if allocation.get(field) != binding.get(field):
            raise RuntimeError(
                f"Control Plane allocation route binding mismatch for {field}; fail closed"
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

    async def release(
        self,
        run_id: str,
        *,
        allocation: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist one release operation and reconcile it without changing create identity.

        A DELETE receipt only proves that deletion was accepted by Management.
        The client/observer may return RELEASED only after independently proving
        Provider resource absence and durable cleanup/finalization evidence.
        """
        run = self.store.get_run(run_id)
        request = run.compute_request
        binding = run.compute_binding
        if request is None or binding is None:
            raise ComputeProfileConflict("Run has no persisted ComputeRequest/route to release")
        if not isinstance(allocation, Mapping):
            raise ValueError("allocation must be an observed ComputeAllocation object")
        validated = _validate_allocation(allocation, request, binding)
        state = validated["state"]
        if state == "RELEASED" and run.compute_release_request is None:
            raise ComputeProfileConflict(
                "cannot accept RELEASED without a durable release request owned by this Run"
            )
        expected_generation = validated["generation"]
        existing_release = run.compute_release_request
        if existing_release is None:
            verifier = getattr(self.compute_client, "verify_release_target", None)
            if not callable(verifier):
                raise TypeError(
                    "Compute client must verify authoritative Desired Generation before persisting a new release intent"
                )
            # This is a read-only authority check. It prevents a stale or
            # caller-forged Allocation generation from permanently pinning the
            # Run to an invalid immutable release intent.
            preflight_error: Exception | None = None
            try:
                await verifier(
                    request=copy.deepcopy(request),
                    binding=copy.deepcopy(binding),
                    expected_generation=expected_generation,
                )
            except Exception as exc:
                preflight_error = exc

            # Another caller can persist the release key while this caller is
            # doing the read-only check. If that happened, recover its exact key
            # rather than replacing it; still reject a changed Generation.
            refreshed = self.store.get_run(run_id)
            existing_release = refreshed.compute_release_request
            if existing_release is None and preflight_error is not None:
                raise preflight_error
            if existing_release is not None:
                if existing_release.get("expectedGeneration") != expected_generation:
                    raise ComputeProfileConflict(
                        "persisted release intent Generation differs from the observed allocation"
                    )
        release_id = (
            existing_release.get("requestId")
            if isinstance(existing_release, Mapping)
            else secrets.token_hex(16)
        )
        candidate = _build_release_request(
            run=run,
            request=request,
            request_id=str(release_id),
            expected_generation=expected_generation,
        )
        try:
            release_request = self.store.persist_compute_release_request(run_id, candidate)
        except ValueError as exc:
            raise ComputeProfileConflict(str(exc)) from exc
        if not callable(getattr(self.compute_client, "release_or_reconcile", None)):
            raise TypeError("Compute client must implement release_or_reconcile for P0 lifecycle recovery")
        result = await self.compute_client.release_or_reconcile(
            copy.deepcopy(release_request),
            request=copy.deepcopy(request),
            binding=copy.deepcopy(binding),
        )
        if not isinstance(result, Mapping):
            raise RuntimeError("Compute client returned a non-object release allocation")
        return _validate_allocation(result, request, binding)


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
            if run.compute_binding is None:
                raise ComputeProfileConflict(
                    "persisted ComputeRequest has no frozen Pool-to-Cluster route binding"
                )
            request_id = stored_id
            # Validate the stored binding against the stored request, not the
            # caller's new candidate. A changed caller Pool must reach the
            # atomic intent comparison and fail as immutable intent drift.
            candidate_binding = _validate_binding(
                run.compute_binding,
                project_id=str(run.compute_request["projectId"]),
                compute_pool_id=str(run.compute_request["computePoolId"]),
            )
        else:
            # Resolve the trusted route before side effects. Its first value is
            # committed atomically with the request ID and must be reused after
            # restart/timeout, even if the pool is later rebound elsewhere.
            candidate_binding = _validate_binding(
                await self.compute_client.resolve_binding(
                    project_id=str(profile["projectId"]),
                    compute_pool_id=compute_pool_id,
                ),
                project_id=str(profile["projectId"]),
                compute_pool_id=compute_pool_id,
            )
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
            request, binding = self.store.persist_compute_request(
                run_id,
                candidate,
                candidate_binding,
            )
        except ValueError as exc:
            raise ComputeProfileConflict(str(exc)) from exc

        # Concurrent first dispatches can resolve different routes; the
        # atomic winner's binding must be used by every loser/retry.
        binding = _validate_binding(
            binding,
            project_id=str(profile["projectId"]),
            compute_pool_id=compute_pool_id,
        )
        allocation = await self.compute_client.submit_or_reconcile(
            copy.deepcopy(request),
            binding=copy.deepcopy(binding),
        )
        if not isinstance(allocation, Mapping):
            raise RuntimeError("Compute client returned a non-object allocation")
        return _validate_allocation(allocation, request, binding)
