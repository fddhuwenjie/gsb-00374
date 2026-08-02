import os
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel

from models.flow import FlowDefinition, FlowVersionMeta
from storage.versioned_flow_store import VersionedFlowStore, VersionInUse
from storage.trace_store import TraceStore
from runtime import get_runtime

router = APIRouter(prefix="/api/flows", tags=["flows"])


def _flow_store() -> VersionedFlowStore:
    return get_runtime().flow_store


class UpdateFlowRequest(BaseModel):
    flow: FlowDefinition
    comment: Optional[str] = None
    force: bool = False


@router.get("", response_model=List[FlowDefinition])
async def list_flows():
    return _flow_store().list_flows()


@router.get("/{flow_id}", response_model=FlowDefinition)
async def get_flow(flow_id: str):
    flow = _flow_store().get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    return flow


@router.post("", response_model=FlowDefinition)
async def create_flow(flow: FlowDefinition):
    saved, _ = _flow_store().create_flow(flow)
    return saved


@router.put("/{flow_id}", response_model=FlowDefinition)
async def update_flow(flow_id: str, request: UpdateFlowRequest):
    updated, fv = _flow_store().update_flow(
        flow_id, request.flow,
        comment=request.comment, force=request.force,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Flow not found")
    return updated


@router.delete("/{flow_id}")
async def delete_flow(flow_id: str):
    store = _flow_store()
    runtime = get_runtime()

    def _check(fid: str, ver: int) -> int:
        return runtime.manager.count_active_executions_for_version(fid, ver)

    try:
        success = store.delete_flow(flow_id, active_version_check=_check)
    except VersionInUse as e:
        raise HTTPException(status_code=409, detail=str(e))
    if not success:
        raise HTTPException(status_code=404, detail="Flow not found")
    return {"success": True}


# ----- version endpoints -----

@router.get("/{flow_id}/versions", response_model=List[FlowVersionMeta])
async def list_versions(flow_id: str):
    store = _flow_store()
    if store.get_flow(flow_id) is None:
        raise HTTPException(status_code=404, detail="Flow not found")
    return store.list_versions(flow_id)


@router.get("/{flow_id}/versions/{version}")
async def get_version(flow_id: str, version: int):
    fv = _flow_store().get_version(flow_id, version)
    if fv is None:
        raise HTTPException(status_code=404, detail="Version not found")
    return fv.model_dump()


@router.get("/{flow_id}/versions/{version}/diff")
async def diff_version(flow_id: str, version: int,
                       against: int = Query(..., alias="against")):
    diff = _flow_store().diff_versions(flow_id, against, version)
    if diff is None:
        raise HTTPException(status_code=404, detail="One or both versions not found")
    return diff


@router.delete("/{flow_id}/versions/{version}")
async def delete_version(flow_id: str, version: int):
    runtime = get_runtime()

    def _check(fid: str, ver: int) -> int:
        return runtime.manager.count_active_executions_for_version(fid, ver)

    try:
        ok = _flow_store().delete_version(
            flow_id, version, active_version_check=_check
        )
    except VersionInUse as e:
        raise HTTPException(status_code=409, detail=str(e))
    if not ok:
        raise HTTPException(status_code=404, detail="Version not found")
    return {"success": True}


# ----- export / import -----

@router.get("/{flow_id}/export")
async def export_flow(flow_id: str):
    flow = _flow_store().get_flow(flow_id)
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    versions = _flow_store().list_versions(flow_id)
    payload = {
        "flow": flow.model_dump(),
        "versions": [v.model_dump() for v in versions],
        "versionDefinitions": {},
    }
    for v in versions:
        fv = _flow_store().get_version(flow_id, v.version)
        if fv is not None:
            payload["versionDefinitions"][str(v.version)] = fv.definition.model_dump()

    import tempfile
    filename = f"{flow.name}_{flow_id}.json"
    filepath = os.path.join(tempfile.gettempdir(), filename)
    import json
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    return FileResponse(
        filepath,
        media_type="application/json",
        filename=filename,
    )


class ImportFlowRequest(BaseModel):
    flow: FlowDefinition
    versions: Optional[List[FlowVersionMeta]] = None
    versionDefinitions: Optional[dict] = None


@router.post("/import")
async def import_flow(request: ImportFlowRequest):
    store = _flow_store()
    flow = request.flow

    if store.get_flow(flow.id) is None:
        saved, fv = store.create_flow(flow)
    else:
        saved, fv = store.update_flow(flow.id, flow, force=True)

    return {
        "success": True,
        "flowId": saved.id,
        "latestVersion": saved.version,
    }


# ----- traces (legacy) -----

@router.get("/{flow_id}/traces")
async def list_flow_traces(flow_id: str):
    traces = TraceStore(os.path.join(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))), "flows", "traces"
    )).list_traces(flow_id)
    return traces


@router.get("/traces/{trace_id}")
async def get_trace(trace_id: str):
    traces = TraceStore(os.path.join(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))), "flows", "traces"
    ))
    trace = traces.get_trace(trace_id)
    if not trace:
        raise HTTPException(status_code=404, detail="Trace not found")
    return trace
