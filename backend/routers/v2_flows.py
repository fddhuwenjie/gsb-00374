from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from models.flow import FlowDefinition
from runtime import get_manager
from storage.flow_version_store import (
    VersionError, VersionInUseError, VersionNotFoundError,
)

router = APIRouter(prefix="/api/v2/flows", tags=["flow-versions"])


class SaveFlowRequest(BaseModel):
    flow: FlowDefinition


class ImportBundleRequest(BaseModel):
    bundle: Dict[str, Any]


def _version_store():
    manager = get_manager()
    if manager.version_store is None:
        raise HTTPException(status_code=500, detail="Version store not configured")
    return manager.version_store


@router.post("")
async def save_flow_version(request: SaveFlowRequest):
    """Save a flow definition. Identical content binds to the existing
    version; changed content creates a new immutable version. Editing a
    running flow therefore never mutates what live executions see."""
    store = _version_store()
    record = store.save_version(request.flow)
    return record


@router.get("/{flow_id}/versions")
async def list_versions(flow_id: str):
    store = _version_store()
    return store.list_versions(flow_id)


@router.get("/{flow_id}/versions/{version}")
async def get_version(flow_id: str, version: int):
    store = _version_store()
    record = store.get(flow_id, version)
    if record is None:
        raise HTTPException(status_code=404, detail="Version not found")
    return record


@router.get("/{flow_id}/diff")
async def diff_versions(flow_id: str,
                        from_version: int = Query(..., alias="from"),
                        to_version: int = Query(..., alias="to")):
    store = _version_store()
    try:
        return store.diff(flow_id, from_version, to_version)
    except VersionNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.delete("/{flow_id}/versions/{version}")
async def delete_version(flow_id: str, version: int):
    store = _version_store()
    manager = get_manager()
    try:
        store.delete(flow_id, version, referenced=manager.referenced_versions())
    except VersionInUseError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except VersionNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return {'success': True}


@router.get("/{flow_id}/versions/{version}/export")
async def export_version(flow_id: str, version: int):
    store = _version_store()
    try:
        return store.export_version(flow_id, version)
    except VersionNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/import")
async def import_version(request: ImportBundleRequest):
    store = _version_store()
    try:
        return store.import_bundle(request.bundle)
    except VersionError as e:
        raise HTTPException(status_code=400, detail=str(e))
