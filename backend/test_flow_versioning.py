import asyncio
import json
import os
import tempfile
import time
from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

import main as app_main
from engine import state_machine as sm
from engine.flow_versioning import (
    diff_versions,
    flow_config_hash,
    node_config_hash,
)
from engine.runtime_manager import FlowVersionMissingError, RuntimeManager
from models.flow import FlowDefinition, FlowEdge, FlowNode, NodeData, Position
from storage.event_store import EventStore
from storage.flow_store import FlowStore


@pytest.fixture()
def data_dir():
    with tempfile.TemporaryDirectory() as tmp:
        yield tmp


@pytest.fixture()
def components(data_dir):
    db = os.path.join(data_dir, "events.db")
    flow_store = FlowStore(data_dir)
    event_store = EventStore(db)
    from engine.event_bus import EventBus
    manager = RuntimeManager(event_store, flow_store, EventBus())
    try:
        yield flow_store, event_store, manager, data_dir
    finally:
        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(manager.shutdown())
            loop.close()
        except Exception:
            pass
        try:
            event_store.close()
        except Exception:
            pass


@pytest.fixture()
def app_client(data_dir):
    app = app_main.create_app(data_dir)
    client = TestClient(app)
    with client:
        yield client
    app.state.event_store.close()


def n(node_id: str, ntype: str = "task", code: Optional[str] = None,
      label: Optional[str] = None) -> FlowNode:
    data_kwargs: Dict[str, Any] = {"label": label or node_id}
    if code is not None:
        data_kwargs["code"] = code
    return FlowNode(
        id=node_id, type=ntype,
        position=Position(x=0, y=0),
        data=NodeData(**data_kwargs),
    )


def e(eid: str, src: str, tgt: str) -> FlowEdge:
    return FlowEdge(id=eid, source=src, target=tgt)


def make_flow(flow_id: str, nodes: List[FlowNode], edges: List[FlowEdge]) -> FlowDefinition:
    return FlowDefinition(
        id=flow_id, name=flow_id, nodes=nodes, edges=edges,
        createdAt=1, updatedAt=1,
    )


def linear_flow(flow_id: str, code: str = "ctx['x']=1") -> FlowDefinition:
    return make_flow(flow_id, [
        n("start", "start"),
        n("task", "task", code=code),
        n("end", "end"),
    ], [e("e1", "start", "task"), e("e2", "task", "end")])


async def wait_status(store: EventStore, eid: str, statuses, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = store.get_execution_row(eid)
        if row and row["status"] in statuses:
            return row
        await asyncio.sleep(0.02)
    raise AssertionError(f"timeout waiting for {statuses}, last={store.snapshot(eid)['status']}")


# ---- Unit: config hash stability ----

class TestConfigHash:
    def test_hash_stable_for_same_config(self):
        node1 = n("a", "task", code="ctx['x']=1")
        node2 = n("a", "task", code="ctx['x']=1")
        assert node_config_hash(node1) == node_config_hash(node2)

    def test_hash_differs_for_code_change(self):
        node1 = n("a", "task", code="ctx['x']=1")
        node2 = n("a", "task", code="ctx['x']=2")
        assert node_config_hash(node1) != node_config_hash(node2)

    def test_position_does_not_affect_hash(self):
        node1 = FlowNode(id="a", type="task", position=Position(x=0, y=0),
                         data=NodeData(label="a", code="x=1"))
        node2 = FlowNode(id="a", type="task", position=Position(x=100, y=200),
                         data=NodeData(label="a", code="x=1"))
        assert node_config_hash(node1) == node_config_hash(node2)

    def test_label_does_not_affect_hash(self):
        node1 = n("a", "task", code="x=1", label="First")
        node2 = n("a", "task", code="x=1", label="Second")
        assert node_config_hash(node1) == node_config_hash(node2)

    def test_flow_hash_includes_edges(self):
        f1 = make_flow("f", [n("start", "start"), n("end", "end")], [e("e1", "start", "end")])
        f2 = make_flow("f", [n("start", "start"), n("end", "end")], [e("e1", "start", "end")])
        assert flow_config_hash(f1) == flow_config_hash(f2)


# ---- Unit: version diff ----

class TestVersionDiff:
    def test_detect_added_node(self):
        old = make_flow("f", [n("start", "start"), n("end", "end")], [e("e1", "start", "end")])
        new = make_flow("f", [n("start", "start"), n("mid", "task", code="1"), n("end", "end")],
                        [e("e1", "start", "mid"), e("e2", "mid", "end")])
        d = diff_versions(old, new)
        assert "mid" in d["addedNodes"]
        assert d["hasStructuralChange"]

    def test_detect_removed_node(self):
        old = make_flow("f", [n("start", "start"), n("mid", "task"), n("end", "end")], [])
        new = make_flow("f", [n("start", "start"), n("end", "end")], [])
        d = diff_versions(old, new)
        assert "mid" in d["removedNodes"]

    def test_detect_config_change(self):
        old = linear_flow("f", code="ctx['x']=1")
        new = linear_flow("f", code="ctx['x']=2")
        d = diff_versions(old, new)
        changed_ids = [c["nodeId"] for c in d["configChangedNodes"]]
        assert "task" in changed_ids

    def test_detect_edge_change(self):
        old = make_flow("f", [n("a", "task"), n("b", "task")], [e("e1", "a", "b")])
        new = make_flow("f", [n("a", "task"), n("b", "task")], [e("e1", "b", "a")])
        d = diff_versions(old, new)
        assert len(d["changedEdges"]) == 1
        assert d["changedEdges"][0]["edgeId"] == "e1"

    def test_position_only_change_not_structural(self):
        old = make_flow("f", [n("start", "start"), n("end", "end")], [e("e1", "start", "end")])
        moved = FlowNode(id="start", type="start", position=Position(x=99, y=99),
                         data=NodeData(label="start"))
        new = make_flow("f", [moved, n("end", "end")], [e("e1", "start", "end")])
        d = diff_versions(old, new)
        assert "start" in d["positionChangedNodes"]
        assert not d["hasStructuralChange"]


# ---- Version creation and binding ----

class TestVersionBinding:
    @pytest.mark.asyncio
    async def test_start_creates_version_1(self, components):
        _, store, manager, _ = components
        flow = linear_flow("v1-flow")
        eid = await manager.start_execution(flow.id, flow=flow)
        row = store.get_execution_row(eid)
        assert row["flow_version"] == 1
        snap = store.snapshot(eid)
        assert snap["flowVersion"] == 1

    @pytest.mark.asyncio
    async def test_identical_flow_reuses_version(self, components):
        _, store, manager, _ = components
        flow = linear_flow("reuse-flow")
        await manager.start_execution(flow.id, flow=flow)
        await manager.start_execution(flow.id, flow=flow)
        versions = store.list_flow_versions("reuse-flow")
        assert len(versions) == 1

    @pytest.mark.asyncio
    async def test_edited_flow_creates_new_version(self, components):
        _, store, manager, _ = components
        flow = linear_flow("edit-flow", code="ctx['x']=1")
        eid1 = await manager.start_execution(flow.id, flow=flow)
        row1 = store.get_execution_row(eid1)
        assert row1["flow_version"] == 1

        edited = linear_flow("edit-flow", code="ctx['x']=2")
        manager.flow_store.update_flow("edit-flow", edited)
        eid2 = await manager.start_execution(edited.id, flow=edited)
        row2 = store.get_execution_row(eid2)
        assert row2["flow_version"] == 2
        assert row1["node_config_hash"] != row2["node_config_hash"]

    @pytest.mark.asyncio
    async def test_version_snapshot_immutable(self, components):
        _, store, manager, _ = components
        flow = linear_flow("immut-flow", code="ctx['x']=1")
        eid = await manager.start_execution(flow.id, flow=flow)
        row = store.get_execution_row(eid)
        v1 = row["flow_version"]

        v1_row = store.get_flow_version("immut-flow", v1)
        assert v1_row is not None

        edited = linear_flow("immut-flow", code="ctx['x']=999")
        manager.flow_store.update_flow("immut-flow", edited)
        manager._ensure_version(edited)

        v1_after = store.get_flow_version("immut-flow", v1)
        v1_def = json.loads(v1_after["definition"])
        task_node = next(n for n in v1_def["nodes"] if n["id"] == "task")
        assert task_node["data"]["code"] == "ctx['x']=1"


# ---- Edit-run concurrency: old execution not polluted ----

class TestEditRunConcurrency:
    @pytest.mark.asyncio
    async def test_running_execution_uses_original_version_after_edit(self, components):
        _, store, manager, _ = components
        wait_node = n("wait", "wait")
        wait_node.data.seconds = 1.0
        flow = make_flow("conc-flow", [
            n("start", "start"), wait_node, n("after", "task", code="ctx['v']=1"), n("end", "end"),
        ], [e("e1", "start", "wait"), e("e2", "wait", "after"), e("e3", "after", "end")])

        eid = await manager.start_execution(flow.id, flow=flow)
        await asyncio.sleep(0.1)
        row = store.get_execution_row(eid)
        original_version = row["flow_version"]

        edited = make_flow("conc-flow", [
            n("start", "start"),
            n("wait", "wait"),
            n("after", "task", code="ctx['v']=999"),
            n("end", "end"),
        ], [e("e1", "start", "wait"), e("e2", "wait", "after"), e("e3", "after", "end")])
        for node in edited.nodes:
            if node.id == "wait":
                node.data.seconds = 1.0
        manager.flow_store.update_flow("conc-flow", edited)
        new_version = manager.save_flow_as_new_version(edited)
        assert new_version > original_version

        await wait_status(store, eid, sm.TERMINAL_STATES, timeout=5)
        snap = store.snapshot(eid)
        assert snap["flowVersion"] == original_version
        assert snap["variables"]["v"] == 1

    @pytest.mark.asyncio
    async def test_new_execution_after_edit_uses_new_version(self, components):
        _, store, manager, _ = components
        flow = linear_flow("newver-flow", code="ctx['x']=1")
        eid1 = await manager.start_execution(flow.id, flow=flow)
        await wait_status(store, eid1, sm.TERMINAL_STATES)

        edited = linear_flow("newver-flow", code="ctx['x']=2")
        manager.flow_store.update_flow("newver-flow", edited)
        eid2 = await manager.start_execution(edited.id, flow=edited)
        await wait_status(store, eid2, sm.TERMINAL_STATES)

        assert store.get_execution_row(eid1)["flow_version"] == 1
        assert store.get_execution_row(eid2)["flow_version"] == 2
        assert store.snapshot(eid2)["variables"]["x"] == 2


# ---- Old version recovery ----

class TestOldVersionRecovery:
    @pytest.mark.asyncio
    async def test_recovery_uses_bound_version_not_current(self, components):
        flow_store, store, manager, _ = components
        wait_node = n("wait", "wait")
        wait_node.data.seconds = 10.0
        flow = make_flow("rec-flow", [
            n("start", "start"), wait_node, n("end", "end"),
        ], [e("e1", "start", "wait"), e("e2", "wait", "end")])
        eid = "exec-rec"
        await manager.start_execution(flow.id, flow=flow, execution_id=eid)
        await wait_status(store, eid, {sm.RUNNING}, 3)

        edited = make_flow("rec-flow", [
            n("start", "start"),
            n("newnode", "task", code="ctx['new']=True"),
            n("end", "end"),
        ], [e("e1", "start", "newnode"), e("e2", "newnode", "end")])
        manager.flow_store.update_flow("rec-flow", edited)
        manager.save_flow_as_new_version(edited)

        await manager.shutdown()
        manager2 = RuntimeManager(store, flow_store, manager.bus)
        try:
            recovered = await manager2.recover_all()
            assert recovered == 1
            executor = manager2.get_executor(eid)
            assert executor is not None
            node_ids = {node.id for node in executor.flow.nodes}
            assert "wait" in node_ids
            assert "newnode" not in node_ids
            await manager2.send_command(eid, "cancel", "cancel-rec")
            await wait_status(store, eid, {sm.CANCELLED}, 3)
        finally:
            await manager2.shutdown()

    @pytest.mark.asyncio
    async def test_recovery_rejected_when_version_missing(self, components):
        flow_store, store, manager, _ = components
        flow = linear_flow("missing-ver-flow", code="ctx['x']=1")
        eid = "exec-missing"
        await manager.start_execution(flow.id, flow=flow, execution_id=eid)
        await wait_status(store, eid, sm.TERMINAL_STATES)
        bound_version = store.get_execution_row(eid)["flow_version"]

        store.delete_flow_version("missing-ver-flow", bound_version)
        await manager.shutdown()

        manager2 = RuntimeManager(store, flow_store, manager.bus)
        try:
            with pytest.raises(FlowVersionMissingError):
                manager2._load_flow_for_execution(store.get_execution_row(eid))
        finally:
            await manager2.shutdown()


# ---- Version deletion protection ----

class TestVersionDeletionProtection:
    @pytest.mark.asyncio
    async def test_cannot_delete_version_with_active_execution(self, components):
        _, store, manager, _ = components
        wait_node = n("wait", "wait")
        wait_node.data.seconds = 5.0
        flow = make_flow("prot-flow", [
            n("start", "start"), wait_node, n("end", "end"),
        ], [e("e1", "start", "wait"), e("e2", "wait", "end")])
        eid = await manager.start_execution(flow.id, flow=flow)
        await asyncio.sleep(0.1)
        version = store.get_execution_row(eid)["flow_version"]

        assert manager.delete_flow_version_safely("prot-flow", version) is False
        assert store.get_flow_version("prot-flow", version) is not None

        await manager.send_command(eid, "cancel", "c1")
        await wait_status(store, eid, {sm.CANCELLED}, 3)
        assert manager.delete_flow_version_safely("prot-flow", version) is True

    @pytest.mark.asyncio
    async def test_cannot_delete_flow_with_active_execution(self, components):
        _, store, manager, _ = components
        wait_node = n("wait", "wait")
        wait_node.data.seconds = 5.0
        flow = make_flow("del-flow", [
            n("start", "start"), wait_node, n("end", "end"),
        ], [e("e1", "start", "wait"), e("e2", "wait", "end")])
        eid = await manager.start_execution(flow.id, flow=flow)
        await asyncio.sleep(0.1)

        assert manager.delete_flow_safely("del-flow") is False
        assert manager.flow_store.get_flow("del-flow") is not None

        await manager.send_command(eid, "cancel", "c2")
        await wait_status(store, eid, {sm.CANCELLED}, 3)
        assert manager.delete_flow_safely("del-flow") is True
        assert manager.flow_store.get_flow("del-flow") is None


# ---- Retry uses original version ----

class TestRetryUsesOriginalVersion:
    @pytest.mark.asyncio
    async def test_retry_after_edit_uses_original_version(self, components):
        _, store, manager, _ = components
        flow = linear_flow("retry-ver-flow", code="ctx['x']=1\nraise ValueError('fail')")
        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_status(store, eid, {sm.FAILED}, 5)
        original_version = store.get_execution_row(eid)["flow_version"]

        edited = linear_flow("retry-ver-flow", code="ctx['x']=999")
        manager.flow_store.update_flow("retry-ver-flow", edited)
        manager.save_flow_as_new_version(edited)

        fixed = linear_flow("retry-ver-flow", code="ctx['x']=42")
        manager.flow_store.update_flow("retry-ver-flow", fixed)
        manager.save_flow_as_new_version(fixed)

        store.delete_flow_version("retry-ver-flow", original_version)
        with pytest.raises(FlowVersionMissingError):
            await manager.send_command(eid, "retry", "retry-cmd")


# ---- Export / import ----

class TestExportImport:
    def test_export_includes_version_info(self, app_client):
        flow = linear_flow("exp-flow").model_dump()
        app_client.post("/api/flows", json=flow)
        resp = app_client.get("/api/flows/exp-flow/export")
        assert resp.status_code == 200
        data = resp.json()
        assert data["version"] == 1
        assert data["nodeConfigHash"] is not None
        assert len(data["versions"]) >= 1

    def test_export_import_roundtrip_preserves_definition(self, app_client):
        flow = linear_flow("imp-flow").model_dump()
        app_client.post("/api/flows", json=flow)
        export = app_client.get("/api/flows/imp-flow/export").json()

        resp = app_client.post("/api/flows/import", json={
            "flow": export["flow"],
        })
        assert resp.status_code == 200
        assert resp.json()["version"] >= 1

        versions = app_client.get("/api/flows/imp-flow/versions").json()
        assert len(versions["versions"]) >= 1

    def test_import_creates_version_with_same_hash(self, app_client):
        flow = linear_flow("hash-flow").model_dump()
        app_client.post("/api/flows", json=flow)
        export = app_client.get("/api/flows/hash-flow/export").json()
        original_hash = export["nodeConfigHash"]

        app_client.post("/api/flows/import", json={"flow": export["flow"]})
        export2 = app_client.get("/api/flows/hash-flow/export").json()
        assert export2["nodeConfigHash"] == original_hash

    def test_versions_listing_endpoint(self, app_client):
        flow = linear_flow("lst-flow").model_dump()
        app_client.post("/api/flows", json=flow)
        resp = app_client.get("/api/flows/lst-flow/versions")
        assert resp.status_code == 200
        versions = resp.json()["versions"]
        assert len(versions) == 1
        assert versions[0]["version"] == 1

    def test_diff_endpoint(self, app_client):
        flow = linear_flow("diff-flow", code="ctx['x']=1").model_dump()
        app_client.post("/api/flows", json=flow)
        edited = linear_flow("diff-flow", code="ctx['x']=2").model_dump()
        app_client.put("/api/flows/diff-flow", json=edited)
        resp = app_client.get("/api/flows/diff-flow/versions/1/diff/2")
        assert resp.status_code == 200
        d = resp.json()
        assert d["hasStructuralChange"] is True
        changed = [c["nodeId"] for c in d["configChangedNodes"]]
        assert "task" in changed

    def test_get_specific_version(self, app_client):
        flow = linear_flow("getver-flow", code="ctx['x']=1").model_dump()
        app_client.post("/api/flows", json=flow)
        edited = linear_flow("getver-flow", code="ctx['x']=2").model_dump()
        app_client.put("/api/flows/getver-flow", json=edited)
        resp = app_client.get("/api/flows/getver-flow/versions/1")
        assert resp.status_code == 200
        v1 = resp.json()
        task = next(n for n in v1["nodes"] if n["id"] == "task")
        assert task["data"]["code"] == "ctx['x']=1"


# ---- Snapshot includes version info ----

class TestSnapshotVersion:
    @pytest.mark.asyncio
    async def test_snapshot_contains_flow_version(self, components):
        _, store, manager, _ = components
        flow = linear_flow("snap-flow")
        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_status(store, eid, sm.TERMINAL_STATES)
        snap = store.snapshot(eid)
        assert "flowVersion" in snap
        assert snap["flowVersion"] == 1
        assert "nodeConfigHash" in snap
        assert snap["nodeConfigHash"] is not None
