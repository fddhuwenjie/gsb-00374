import json
import os
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from engine.runtime_manager import FlowVersionMissingError
from models.flow import FlowDefinition

router = APIRouter(prefix="/api/flows", tags=["flows"])


def _flow_store(request: Request):
    return request.app.state.flow_store


def _rm(request: Request):
    return request.app.state.runtime_manager


@router.get("", response_model=List[FlowDefinition])
async def list_flows(request: Request):
    return _flow_store(request).list_flows()


@router.get("/{flow_id}", response_model=FlowDefinition)
async def get_flow(request: Request, flow_id: str):
    flow = _flow_store(request).get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    return flow


@router.post("", response_model=FlowDefinition)
async def create_flow(request: Request, flow: FlowDefinition):
    store = _flow_store(request)
    rm = _rm(request)
    created = store.create_flow(flow)
    try:
        rm._ensure_version(created)
    except Exception:
        pass
    return created


@router.put("/{flow_id}", response_model=FlowDefinition)
async def update_flow(request: Request, flow_id: str, flow: FlowDefinition):
    store = _flow_store(request)
    rm = _rm(request)
    updated = store.update_flow(flow_id, flow)
    if not updated:
        raise HTTPException(status_code=404, detail="Flow not found")
    try:
        rm._ensure_version(updated)
    except Exception:
        pass
    return updated


@router.delete("/{flow_id}")
async def delete_flow(request: Request, flow_id: str):
    rm = _rm(request)
    if not rm.delete_flow_safely(flow_id):
        raise HTTPException(
            status_code=409,
            detail="Flow has active executions; cannot delete until they finish",
        )
    return {"success": True}


@router.get("/{flow_id}/versions")
async def list_versions(request: Request, flow_id: str):
    rm = _rm(request)
    return {"versions": rm.list_versions(flow_id)}


@router.get("/{flow_id}/versions/{version}")
async def get_version(request: Request, flow_id: str, version: int):
    rm = _rm(request)
    try:
        flow = rm._get_version_flow(flow_id, version)
    except FlowVersionMissingError:
        raise HTTPException(status_code=404, detail="Version not found")
    return flow.model_dump()


@router.get("/{flow_id}/versions/{from_version}/diff/{to_version}")
async def diff_versions(request: Request, flow_id: str, from_version: int, to_version: int):
    rm = _rm(request)
    try:
        return rm.get_version_diff(flow_id, from_version, to_version)
    except FlowVersionMissingError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/{flow_id}/versions/{version}")
async def delete_version(request: Request, flow_id: str, version: int):
    rm = _rm(request)
    if not rm.delete_flow_version_safely(flow_id, version):
        raise HTTPException(
            status_code=409,
            detail="Version is referenced by active executions; cannot delete",
        )
    return {"success": True}


class ImportFlowRequest(BaseModel):
    flow: FlowDefinition
    version: Optional[int] = None
    nodeConfigHash: Optional[str] = None


@router.post("/import")
async def import_flow(request: Request, payload: ImportFlowRequest):
    rm = _rm(request)
    store = _flow_store(request)
    flow = payload.flow
    existing = store.get_flow(flow.id)
    if existing:
        store.update_flow(flow.id, flow)
    else:
        store.create_flow(flow)
    version = rm.save_flow_as_new_version(flow)
    return {"flowId": flow.id, "version": version}


@router.get("/{flow_id}/export")
async def export_flow(request: Request, flow_id: str):
    rm = _rm(request)
    store = _flow_store(request)
    flow = store.get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    latest = rm.store.get_latest_flow_version(flow_id)
    version_row = rm.store.get_flow_version(flow_id, latest) if latest else None
    return {
        "flow": flow.model_dump(),
        "version": latest,
        "nodeConfigHash": version_row["node_config_hash"] if version_row else None,
        "versions": rm.list_versions(flow_id),
    }
