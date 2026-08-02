"""REST control plane for durable executions.

All button-relevant state is server-authoritative: every response includes
``allowedCommands`` computed from the state machine, so the frontend can enable
exactly the controls the server would accept and nothing else.
"""

import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from engine.durable.service import get_engine
from engine.durable.state_machine import allowed_commands
from engine.durable.versioning import MissingVersionError, VersionInUseError

router = APIRouter(prefix="/api/durable", tags=["durable"])


class CreateExecutionRequest(BaseModel):
    executionId: Optional[str] = None
    flow: Optional[Dict[str, Any]] = None
    flowId: Optional[str] = None
    flowVersion: Optional[int] = None
    variables: Optional[Dict[str, Any]] = None
    autoStart: bool = True


class CommandRequest(BaseModel):
    command: str


class ApprovalRequest(BaseModel):
    token: str
    decision: str
    approver: Optional[str] = None


class FlowSpecRequest(BaseModel):
    flow: Dict[str, Any]


class ImportRequest(BaseModel):
    bundle: Dict[str, Any]
    overwrite: bool = False


def _new_id() -> str:
    return f"dexec_{int(time.time() * 1000)}"


@router.post("")
async def create_execution(req: CreateExecutionRequest):
    engine = get_engine()
    execution_id = req.executionId or _new_id()
    try:
        execution = await engine.create(
            execution_id,
            flow_spec=req.flow,
            variables=req.variables or {},
            flow_id=req.flowId,
            flow_version=req.flowVersion,
        )
    except MissingVersionError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    if req.autoStart:
        await engine.command(execution_id, "start")

    snap = engine.snapshot(execution_id)
    return snap


@router.get("/{execution_id}")
async def get_execution(execution_id: str, afterSeq: int = 0):
    engine = get_engine()
    try:
        return engine.snapshot(execution_id, after_seq=afterSeq)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Execution not found")


@router.post("/{execution_id}/command")
async def send_command(execution_id: str, req: CommandRequest):
    engine = get_engine()
    if req.command not in {"start", "pause", "resume", "cancel"}:
        raise HTTPException(status_code=400, detail=f"Unknown command: {req.command}")
    try:
        result = await engine.command(execution_id, req.command)
    except KeyError:
        raise HTTPException(status_code=404, detail="Execution not found")
    except MissingVersionError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return result


@router.get("/{execution_id}/allowed")
async def get_allowed(execution_id: str):
    engine = get_engine()
    snap = engine.snapshot(execution_id)
    return {
        "executionId": execution_id,
        "state": snap["state"],
        "allowedCommands": sorted(allowed_commands(snap["state"])),
    }


# ---------------------------------------------------------------------------
# human approvals
# ---------------------------------------------------------------------------

@router.get("/{execution_id}/approvals")
async def list_pending_approvals(execution_id: str):
    """List the execution's pending approvals, each with a fresh recovery token."""
    engine = get_engine()
    snap = engine.snapshot(execution_id)
    return {
        "executionId": execution_id,
        "state": snap["state"],
        "pendingApprovals": snap.get("pendingApprovals", []),
    }


@router.post("/{execution_id}/approvals")
async def respond_to_approval(execution_id: str, req: ApprovalRequest):
    """Approve or reject via a recovery token.

    Stale, expired, duplicate and forged tokens are all rejected (``accepted``
    false) rather than illegally applied; the response echoes the server's view
    of state and allowed commands so the UI stays server-driven.
    """
    engine = get_engine()
    result = await engine.respond_approval(req.token, req.decision, req.approver)
    return result


@router.post("/{execution_id}/approvals/expire")
async def expire_approvals(execution_id: str):
    """Force-timeout any pending approvals past their deadline (sweeper hook)."""
    engine = get_engine()
    expired = await engine.expire_due_approvals(execution_id)
    snap = engine.snapshot(execution_id)
    return {"executionId": execution_id, "expired": expired, "state": snap["state"]}


# ---------------------------------------------------------------------------
# flow definition versioning
# ---------------------------------------------------------------------------

@router.post("/flows/version")
async def create_flow_version(req: FlowSpecRequest):
    """Register a flow definition, creating a new immutable version if changed."""
    engine = get_engine()
    try:
        record = engine.register_flow(req.flow)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return record


@router.put("/flows/{flow_id}/version")
async def edit_flow_version(flow_id: str, req: FlowSpecRequest):
    """Edit a flow. Always produces a new version; never mutates existing ones."""
    engine = get_engine()
    spec = dict(req.flow)
    spec["id"] = flow_id
    try:
        record = engine.edit_flow(spec)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return record


@router.get("/flows/{flow_id}/versions")
async def list_flow_versions(flow_id: str):
    engine = get_engine()
    return {
        "flowId": flow_id,
        "latest": engine.versions.latest_version(flow_id),
        "versions": engine.list_versions(flow_id),
    }


@router.get("/flows/{flow_id}/versions/{version}")
async def get_flow_version(flow_id: str, version: int):
    engine = get_engine()
    record = engine.get_version(flow_id, version)
    if record is None:
        raise HTTPException(status_code=404, detail="Flow version not found")
    return record


@router.get("/flows/{flow_id}/diff")
async def diff_flow_versions(flow_id: str, fromVersion: int, toVersion: int):
    engine = get_engine()
    try:
        return engine.diff_versions(flow_id, fromVersion, toVersion)
    except MissingVersionError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/flows/{flow_id}/versions/{version}")
async def delete_flow_version(flow_id: str, version: int, force: bool = False):
    engine = get_engine()
    try:
        return engine.delete_version(flow_id, version, force=force)
    except VersionInUseError as e:
        raise HTTPException(
            status_code=409,
            detail={"message": str(e), "executionIds": e.execution_ids},
        )


@router.get("/flows/{flow_id}/export")
async def export_flow(flow_id: str):
    engine = get_engine()
    return engine.export_flow(flow_id)


@router.post("/flows/import")
async def import_flow(req: ImportRequest):
    engine = get_engine()
    try:
        return engine.import_flow(req.bundle, overwrite=req.overwrite)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
