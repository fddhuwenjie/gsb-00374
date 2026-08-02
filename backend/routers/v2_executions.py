from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from models.flow import FlowDefinition
from runtime import get_manager

router = APIRouter(prefix="/api/v2/executions", tags=["executions-v2"])


class StartExecutionV2Request(BaseModel):
    flow: FlowDefinition
    variables: Optional[Dict[str, Any]] = None
    executionId: Optional[str] = None


class CommandRequest(BaseModel):
    commandId: str
    command: str  # pause | resume | cancel | retry | approve | reject
    expectedStatus: Optional[str] = None
    token: Optional[str] = None
    approver: Optional[str] = None
    nodeId: Optional[str] = None
    attempt: Optional[Any] = None
    flowVersion: Optional[int] = None


@router.post("")
async def start_execution(request: StartExecutionV2Request):
    manager = get_manager()
    snapshot = await manager.start_execution(
        request.flow, request.variables, request.executionId
    )
    return snapshot


@router.get("/{execution_id}")
async def get_execution_snapshot(execution_id: str):
    manager = get_manager()
    try:
        return manager.snapshot(execution_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Execution not found")


@router.get("/{execution_id}/events")
async def get_execution_events(execution_id: str,
                               since: int = Query(0, ge=0)):
    manager = get_manager()
    try:
        events = manager.events_since(execution_id, since)
    except KeyError:
        raise HTTPException(status_code=404, detail="Execution not found")
    snapshot = manager.snapshot(execution_id)
    return {'events': events, 'latestSeq': snapshot['seq']}


@router.post("/{execution_id}/commands")
async def send_command(execution_id: str, request: CommandRequest):
    manager = get_manager()
    result = await manager.command(
        execution_id, request.commandId, request.command, request.expectedStatus,
        token=request.token, approver=request.approver, node_id=request.nodeId,
        attempt=request.attempt, flow_version=request.flowVersion,
    )
    if result.get('detail') == 'execution not found':
        raise HTTPException(status_code=404, detail="Execution not found")
    return result
