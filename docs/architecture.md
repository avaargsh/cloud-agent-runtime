# Architecture

## Runtime planes

```text
Client Plane
  Web / Mobile / Desktop
          |
          v
Application API
  Auth / Tenant / Billing / Quota / RBAC
          |
          v
Agent Control Plane
  Profile / Session / Run / Binding / Policy / Approval
          |
          v
Harness Adapter
  Codex / Claude / OpenCode / custom
          |
          v
Sandbox Plane
  allocate / warm / snapshot / resume / terminate
          |
          v
Capability Plane
  MCP / REST / browser / code / data / A2A
          |
          v
State & Evidence
  PostgreSQL / object storage / OpenTelemetry
          |
          v
Model / Compute
  hosted APIs or vLLM / SGLang
```

## Canonical identities

The runtime owns stable IDs for:

- Agent
- Release
- Session
- Run
- Sandbox
- Artifact
- Evidence

Provider-specific IDs are attached as references.

## Session is not chat history

A session is a durable identity and state boundary.

It can reference:

- provider threads,
- durable workflow state,
- sandbox snapshots,
- artifacts,
- memory/context state,
- approvals and budgets.

## Run

A Run is one bounded execution attempt inside a Session.

It has:

- lifecycle status,
- sandbox binding,
- artifact references,
- evidence references,
- provider execution references.

## Sandbox

Sandbox state is explicitly lifecycle-managed:

```text
Allocate -> Warm -> Bind -> Execute -> Snapshot/Pause -> Resume -> Terminate
```

The runtime should be able to resume a user/session without assuming the original process is still alive.

## Policy boundary

The harness can propose tool calls and actions.

The application/control/runtime layers still own:

- tenant authorization,
- budget,
- approval,
- capability binding,
- irreversible-action checks,
- audit.
