import time
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from engine.execution_manager import CommandRejected
from models.flow import ApprovalResponse, FlowDefinition
from runtime import get_manager

router = APIRouter(prefix="/api/executions", tags=["executions"])


class StartExecutionRequest(BaseModel):
    flow: FlowDefinition
    variables: Optional[Dict[str, Any]] = None
    requestId: Optional[str] = None


class CommandRequest(BaseModel):
    requestId: Optional[str] = None


@router.get("")
async def list_executions(flow_id: Optional[str] = Query(None, alias="flowId")):
    manager = get_manager()
    items = manager.list_executions()
    if flow_id:
        items = [i for i in items if i['flowId'] == flow_id]
    return items


@router.post("")
async def start_execution(request: StartExecutionRequest):
    manager = get_manager()
    request_id = request.requestId or str(uuid.uuid4())
    try:
        execution_id = await manager.start_execution(
            request.flow, variables=request.variables, request_id=request_id,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    snap = manager.get_snapshot(execution_id)
    return {
        "executionId": execution_id,
        "flowId": request.flow.id,
        "status": snap.status,
        "seq": snap.seq,
        "requestId": request_id,
    }


@router.get("/{execution_id}")
async def get_execution(execution_id: str):
    manager = get_manager()
    try:
        return manager.get_snapshot(execution_id).model_dump()
    except CommandRejected:
        raise HTTPException(status_code=404, detail="Execution not found")


@router.get("/{execution_id}/events")
async def get_events(execution_id: str, after_seq: int = Query(0, alias="afterSeq")):
    manager = get_manager()
    try:
        manager.get_snapshot(execution_id)
    except CommandRejected:
        raise HTTPException(status_code=404, detail="Execution not found")
    events = manager.get_events(execution_id, after_seq=after_seq)
    return {
        "executionId": execution_id,
        "events": [e.model_dump() for e in events],
    }


@router.post("/{execution_id}/pause")
async def pause_execution(execution_id: str, request: Optional[CommandRequest] = None):
    return await _command(execution_id, 'pause', request)


@router.post("/{execution_id}/resume")
async def resume_execution(execution_id: str, request: Optional[CommandRequest] = None):
    return await _command(execution_id, 'resume', request)


@router.post("/{execution_id}/cancel")
async def cancel_execution(execution_id: str, request: Optional[CommandRequest] = None):
    return await _command(execution_id, 'cancel', request)


@router.post("/{execution_id}/step")
async def step_execution(execution_id: str, request: Optional[CommandRequest] = None):
    return await _command(execution_id, 'step', request)


@router.post("/{execution_id}/approvals/{token}/respond")
async def respond_to_approval(execution_id: str, token: str, response: dict):
    manager = get_manager()
    try:
        approval_response = ApprovalResponse(
            token=token,
            executionId=execution_id,
            decision=response.get('decision', 'approved'),
            responder=response.get('responder'),
            comment=response.get('comment'),
            requestId=response.get('requestId'),
        )
        snap = await manager.respond_to_approval(approval_response)
    except CommandRejected as e:
        raise HTTPException(status_code=409, detail=e.reason)
    return {
        "executionId": execution_id,
        "token": token,
        "decision": approval_response.decision,
        "accepted": True,
        "status": snap.status,
        "allowedActions": snap.allowedActions,
        "snapshot": snap.model_dump(),
    }


async def _command(execution_id: str, action: str, request: Optional[CommandRequest]):
    manager = get_manager()
    request_id = (request.requestId if request else None) or str(uuid.uuid4())
    try:
        snap = await manager.control(execution_id, action, request_id)
    except CommandRejected as e:
        raise HTTPException(status_code=409, detail=e.reason)
    return {
        "executionId": execution_id,
        "command": action,
        "accepted": True,
        "requestId": request_id,
        "status": snap.status,
        "allowedActions": snap.allowedActions,
        "snapshot": snap.model_dump(),
    }


@router.delete("/{execution_id}")
async def delete_execution(execution_id: str):
    manager = get_manager()
    ok = manager.event_store.delete_execution(execution_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Execution not found")
    return {"success": True}
