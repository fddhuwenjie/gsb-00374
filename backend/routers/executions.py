import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from engine import state_machine as sm
from models.flow import FlowDefinition


router = APIRouter(prefix="/api/executions", tags=["executions"])

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _rm(request: Request):
    return request.app.state.runtime_manager


class StartExecutionRequest(BaseModel):
    flowId: str
    variables: Optional[Dict[str, Any]] = None
    flow: Optional[FlowDefinition] = None


class CommandRequest(BaseModel):
    command: str
    commandId: Optional[str] = None
    token: Optional[str] = None
    comment: Optional[str] = None
    approver: Optional[str] = None


class ApprovalRequest(BaseModel):
    token: str
    approver: Optional[str] = None
    comment: Optional[str] = None


@router.get("")
async def list_executions(request: Request,
                          flowId: Optional[str] = Query(None),
                          limit: int = Query(100)):
    rm = _rm(request)
    rows = rm.store.list_executions(flow_id=flowId, limit=limit)
    return rows


@router.post("")
async def start_execution(request: Request, payload: StartExecutionRequest):
    rm = _rm(request)
    flow = payload.flow
    if flow is None:
        flow = rm.flow_store.get_flow(payload.flowId)
        if not flow:
            raise HTTPException(status_code=404, detail="Flow not found")
    else:
        try:
            existing = rm.flow_store.get_flow(flow.id)
            if not existing:
                rm.flow_store.create_flow(flow)
        except Exception:
            pass

    eid = await rm.start_execution(flow.id, variables=payload.variables, flow=flow)
    return {"executionId": eid, "snapshot": rm.store.snapshot(eid)}


@router.get("/{execution_id}")
async def get_execution(request: Request, execution_id: str):
    rm = _rm(request)
    row = rm.store.get_execution_row(execution_id)
    if not row:
        raise HTTPException(status_code=404, detail="Execution not found")
    return rm.store.snapshot(execution_id)


@router.get("/{execution_id}/snapshot")
async def get_snapshot(request: Request, execution_id: str):
    rm = _rm(request)
    try:
        return rm.store.snapshot(execution_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Execution not found")


@router.get("/{execution_id}/events")
async def get_events(request: Request, execution_id: str, sinceSeq: int = Query(0)):
    rm = _rm(request)
    events = rm.store.get_events_since(execution_id, sinceSeq)
    return {"events": events, "latestSeq": rm.store.get_latest_seq(execution_id)}


@router.post("/{execution_id}/command")
async def send_command(request: Request, execution_id: str, payload: CommandRequest):
    rm = _rm(request)
    try:
        extra = {}
        if payload.command in ("approve", "reject"):
            extra["token"] = payload.token
            extra["comment"] = payload.comment
            extra["approver"] = payload.approver
        accepted = await rm.send_command(
            execution_id, payload.command, payload.commandId, **extra
        )
    except KeyError:
        if payload.commandId:
            rm.store.complete_command(payload.commandId, False)
        raise HTTPException(status_code=404, detail="Execution not found")
    except sm.IllegalTransitionError as e:
        if payload.commandId:
            rm.store.complete_command(payload.commandId, False)
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        if payload.commandId:
            rm.store.complete_command(payload.commandId, False)
        raise HTTPException(status_code=400, detail=str(e))
    return {"accepted": accepted}


@router.post("/{execution_id}/approve")
async def approve_execution(request: Request, execution_id: str, payload: ApprovalRequest):
    rm = _rm(request)
    try:
        accepted = await rm.send_command(
            execution_id, "approve",
            token=payload.token, approver=payload.approver, comment=payload.comment,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Execution not found")
    return {"accepted": accepted}


@router.post("/{execution_id}/reject")
async def reject_execution(request: Request, execution_id: str, payload: ApprovalRequest):
    rm = _rm(request)
    try:
        accepted = await rm.send_command(
            execution_id, "reject",
            token=payload.token, approver=payload.approver, comment=payload.comment,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Execution not found")
    return {"accepted": accepted}


@router.get("/{execution_id}/approvals")
async def list_approvals(request: Request, execution_id: str):
    rm = _rm(request)
    return {"approvals": rm.store.list_pending_approvals(execution_id)}


@router.post("/{execution_id}/pause")
async def pause_execution(request: Request, execution_id: str, payload: CommandRequest = CommandRequest(command="pause")):
    return await send_command(request, execution_id, payload)


@router.post("/{execution_id}/resume")
async def resume_execution(request: Request, execution_id: str, payload: CommandRequest = CommandRequest(command="resume")):
    return await send_command(request, execution_id, payload)


@router.post("/{execution_id}/cancel")
async def cancel_execution(request: Request, execution_id: str, payload: CommandRequest = CommandRequest(command="cancel")):
    return await send_command(request, execution_id, payload)


@router.post("/{execution_id}/retry")
async def retry_execution(request: Request, execution_id: str, payload: CommandRequest = CommandRequest(command="retry")):
    return await send_command(request, execution_id, payload)


@router.delete("/{execution_id}")
async def delete_execution(request: Request, execution_id: str):
    rm = _rm(request)
    executor = rm.get_executor(execution_id)
    if executor:
        await executor.request_cancel()
    deleted = rm.store.delete_execution(execution_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Execution not found")
    return {"success": True}
