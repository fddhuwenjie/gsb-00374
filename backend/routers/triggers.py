import os
import uuid
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Request, Query
from pydantic import BaseModel
from models.flow import Trigger
from storage.trigger_store import TriggerStore
from storage.flow_store import FlowStore
from engine.cron_parser import CronExpression, CronParseError

router = APIRouter(prefix="/api/triggers", tags=["triggers"])

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
trigger_store = TriggerStore(os.path.join(BASE_DIR, "flows"))
flow_store = FlowStore(os.path.join(BASE_DIR, "flows"))


class CreateTriggerRequest(BaseModel):
    flowId: str
    type: str
    cronExpression: Optional[str] = None
    webhookPath: Optional[str] = None
    sourceFlowId: Optional[str] = None
    enabled: bool = True


class UpdateTriggerRequest(BaseModel):
    flowId: Optional[str] = None
    type: Optional[str] = None
    cronExpression: Optional[str] = None
    webhookPath: Optional[str] = None
    sourceFlowId: Optional[str] = None
    enabled: Optional[bool] = None


class ValidateCronRequest(BaseModel):
    expression: str


@router.get("", response_model=List[Trigger])
async def list_triggers(flow_id: Optional[str] = Query(None, alias="flowId")):
    return trigger_store.list_triggers(flow_id)


@router.get("/{trigger_id}", response_model=Trigger)
async def get_trigger(trigger_id: str):
    trigger = trigger_store.get_trigger(trigger_id)
    if not trigger:
        raise HTTPException(status_code=404, detail="Trigger not found")
    return trigger


@router.get("/{trigger_id}/next-runs")
async def get_trigger_next_runs(trigger_id: str, count: int = 5):
    trigger = trigger_store.get_trigger(trigger_id)
    if not trigger:
        raise HTTPException(status_code=404, detail="Trigger not found")

    if trigger.type != 'cron':
        raise HTTPException(status_code=400, detail="Only cron triggers have next runs")

    try:
        cron = CronExpression(trigger.cronExpression)
        runs = cron.next_runs(min(count, 20))
        return {"nextRuns": [dt.isoformat() for dt in runs]}
    except CronParseError as e:
        raise HTTPException(status_code=400, detail=f"Invalid cron expression: {e}")


@router.post("", response_model=Trigger)
async def create_trigger(request: CreateTriggerRequest):
    flow = flow_store.get_flow(request.flowId)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    if request.type not in ['cron', 'webhook', 'flow_completed']:
        raise HTTPException(status_code=400, detail="Invalid trigger type")

    if request.type == 'cron':
        if not request.cronExpression:
            raise HTTPException(status_code=400, detail="cronExpression is required for cron triggers")
        try:
            CronExpression(request.cronExpression)
        except CronParseError as e:
            raise HTTPException(status_code=400, detail=f"Invalid cron expression: {e}")

    if request.type == 'webhook':
        if not request.webhookPath:
            raise HTTPException(status_code=400, detail="webhookPath is required for webhook triggers")
        if not request.webhookPath.startswith('/'):
            request.webhookPath = '/' + request.webhookPath
        existing = trigger_store.get_trigger_by_webhook_path(request.webhookPath)
        if existing:
            raise HTTPException(status_code=400, detail="Webhook path already in use")

    if request.type == 'flow_completed':
        if not request.sourceFlowId:
            raise HTTPException(status_code=400, detail="sourceFlowId is required for flow_completed triggers")
        source_flow = flow_store.get_flow(request.sourceFlowId)
        if not source_flow:
            raise HTTPException(status_code=404, detail="Source flow not found")

    trigger = Trigger(
        id=f"trigger_{uuid.uuid4().hex[:12]}",
        flowId=request.flowId,
        type=request.type,
        cronExpression=request.cronExpression,
        webhookPath=request.webhookPath,
        sourceFlowId=request.sourceFlowId,
        enabled=request.enabled,
        createdAt=0
    )

    return trigger_store.create_trigger(trigger)


@router.put("/{trigger_id}", response_model=Trigger)
async def update_trigger(trigger_id: str, request: UpdateTriggerRequest):
    existing = trigger_store.get_trigger(trigger_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Trigger not found")

    updated = Trigger(
        id=existing.id,
        flowId=request.flowId or existing.flowId,
        type=request.type or existing.type,
        cronExpression=request.cronExpression if request.cronExpression is not None else existing.cronExpression,
        webhookPath=request.webhookPath if request.webhookPath is not None else existing.webhookPath,
        sourceFlowId=request.sourceFlowId if request.sourceFlowId is not None else existing.sourceFlowId,
        enabled=request.enabled if request.enabled is not None else existing.enabled,
        createdAt=existing.createdAt
    )

    if updated.type == 'cron' and updated.cronExpression:
        try:
            CronExpression(updated.cronExpression)
        except CronParseError as e:
            raise HTTPException(status_code=400, detail=f"Invalid cron expression: {e}")

    if updated.type == 'webhook' and updated.webhookPath:
        if not updated.webhookPath.startswith('/'):
            updated.webhookPath = '/' + updated.webhookPath
        other = trigger_store.get_trigger_by_webhook_path(updated.webhookPath)
        if other and other.id != trigger_id:
            raise HTTPException(status_code=400, detail="Webhook path already in use")

    result = trigger_store.update_trigger(trigger_id, updated)
    if not result:
        raise HTTPException(status_code=500, detail="Failed to update trigger")
    return result


@router.delete("/{trigger_id}")
async def delete_trigger(trigger_id: str):
    success = trigger_store.delete_trigger(trigger_id)
    if not success:
        raise HTTPException(status_code=404, detail="Trigger not found")
    return {"success": True}


@router.post("/{trigger_id}/toggle")
async def toggle_trigger(trigger_id: str, request: dict):
    enabled = request.get('enabled', True)
    result = trigger_store.toggle_trigger(trigger_id, enabled)
    if not result:
        raise HTTPException(status_code=404, detail="Trigger not found")
    return result


@router.post("/validate-cron")
async def validate_cron(request: ValidateCronRequest):
    try:
        cron = CronExpression(request.expression)
        next_runs = cron.next_runs(5)
        return {
            "valid": True,
            "expression": request.expression,
            "nextRuns": [dt.isoformat() for dt in next_runs]
        }
    except CronParseError as e:
        return {
            "valid": False,
            "expression": request.expression,
            "error": str(e)
        }


@router.post("/webhook/{path:path}")
async def handle_webhook(path: str, request: Request):
    webhook_path = '/' + path
    trigger = trigger_store.get_trigger_by_webhook_path(webhook_path)
    if not trigger or not trigger.enabled:
        raise HTTPException(status_code=404, detail="Webhook not found")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    from main import scheduler
    if scheduler:
        await scheduler.trigger_webhook(webhook_path, payload)

    return {"success": True, "triggered": trigger.flowId}
