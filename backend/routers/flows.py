import os
from typing import List
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, FileResponse
from models.flow import FlowDefinition
from storage.flow_store import FlowStore
from storage.trace_store import TraceStore

router = APIRouter(prefix="/api/flows", tags=["flows"])

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
flow_store = FlowStore(os.path.join(BASE_DIR, "flows"))
trace_store = TraceStore(os.path.join(BASE_DIR, "flows", "traces"))


@router.get("", response_model=List[FlowDefinition])
async def list_flows():
    return flow_store.list_flows()


@router.get("/{flow_id}", response_model=FlowDefinition)
async def get_flow(flow_id: str):
    flow = flow_store.get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    return flow


@router.post("", response_model=FlowDefinition)
async def create_flow(flow: FlowDefinition):
    return flow_store.create_flow(flow)


@router.put("/{flow_id}", response_model=FlowDefinition)
async def update_flow(flow_id: str, flow: FlowDefinition):
    updated = flow_store.update_flow(flow_id, flow)
    if not updated:
        raise HTTPException(status_code=404, detail="Flow not found")
    return updated


@router.delete("/{flow_id}")
async def delete_flow(flow_id: str):
    success = flow_store.delete_flow(flow_id)
    if not success:
        raise HTTPException(status_code=404, detail="Flow not found")
    return {"success": True}


@router.get("/{flow_id}/export")
async def export_flow(flow_id: str):
    flow = flow_store.get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    
    filename = f"{flow.name}_{flow_id}.json"
    filepath = os.path.join("/tmp", filename)
    
    import json
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(flow.model_dump(), f, indent=2, ensure_ascii=False)
    
    return FileResponse(
        filepath,
        media_type="application/json",
        filename=filename
    )


@router.get("/{flow_id}/traces")
async def list_flow_traces(flow_id: str):
    traces = trace_store.list_traces(flow_id)
    return traces


@router.get("/traces/{trace_id}")
async def get_trace(trace_id: str):
    trace = trace_store.get_trace(trace_id)
    if not trace:
        raise HTTPException(status_code=404, detail="Trace not found")
    return trace
