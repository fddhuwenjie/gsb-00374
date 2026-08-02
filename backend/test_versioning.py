"""Tests for workflow definition versioning, immutable version binding,
edit/run concurrency, version-diff, deletion protection and recovery
when the bound version is missing."""
import asyncio
import json
import os
import sys
import tempfile
import time
import uuid

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from engine import state_machine as sm
from engine.execution_manager import ExecutionManager
from engine.flow_version import (
    compute_node_config_hash,
    diff_flows,
    is_structurally_identical,
)
from models.flow import (
    FlowDefinition, FlowNode, FlowEdge, Position, NodeData,
    FileWriteConfig,
)
from storage.event_store import EventStore
from storage.versioned_flow_store import (
    VersionedFlowStore,
    VersionInUse,
    VersionNotFound,
)


def _node(nid, ntype='task', data=None):
    if data is None:
        data = NodeData(label=nid, code=f"ctx['{nid}']=True")
    return FlowNode(id=nid, type=ntype, position=Position(x=0, y=0), data=data)


def _edge(eid, src, tgt):
    return FlowEdge(id=eid, source=src, target=tgt)


def _simple_flow(flow_id='vf', nodes=None, edges=None):
    if nodes is None:
        nodes = [_node('start', 'start', NodeData(label='s')),
                 _node('t1'),
                 _node('end', 'end', NodeData(label='e'))]
    if edges is None:
        edges = [_edge('e1', 'start', 't1'), _edge('e2', 't1', 'end')]
    return FlowDefinition(
        id=flow_id, name='VFlow', nodes=nodes, edges=edges,
        createdAt=0, updatedAt=0,
    )


@pytest.fixture
def tmp_dir(tmp_path):
    return str(tmp_path / 'vf')


@pytest.fixture
def flow_store(tmp_dir):
    return VersionedFlowStore(tmp_dir)


@pytest.fixture
def manager(tmp_dir, flow_store):
    events = EventStore(os.path.join(tmp_dir, 'event_store'))
    return ExecutionManager(events, flow_store)


# ---------------------------------------------------------------------------
# config hash / diff
# ---------------------------------------------------------------------------

class TestFlowDiff:
    def test_hash_deterministic(self):
        f = _simple_flow()
        h1 = compute_node_config_hash(f)
        h2 = compute_node_config_hash(f)
        assert h1 == h2
        assert len(h1) == 16

    def test_hash_changes_on_node_config(self):
        f1 = _simple_flow()
        f2 = _simple_flow()
        f2.nodes[1].data.code = "ctx['changed']=True"
        assert compute_node_config_hash(f1) != compute_node_config_hash(f2)

    def test_hash_changes_on_edge_change(self):
        f1 = _simple_flow()
        f2 = _simple_flow()
        f2.edges[0].target = 'end'
        assert compute_node_config_hash(f1) != compute_node_config_hash(f2)

    def test_diff_node_added(self):
        f1 = _simple_flow()
        nodes2 = list(f1.nodes) + [_node('t2')]
        edges2 = list(f1.edges) + [_edge('e3', 't1', 't2'),
                                    _edge('e4', 't2', 'end')]
        f2 = f1.model_copy(update={'nodes': nodes2, 'edges': edges2})
        d = diff_flows(f1, f2)
        assert 't2' in d.addedNodes
        assert 'e3' in d.addedEdges

    def test_diff_node_removed(self):
        f1 = _simple_flow()
        f2 = _simple_flow(nodes=[_node('start', 'start', NodeData(label='s')),
                                  _node('end', 'end', NodeData(label='e'))],
                          edges=[_edge('e0', 'start', 'end')])
        d = diff_flows(f1, f2)
        assert 't1' in d.removedNodes

    def test_diff_config_changed(self):
        f1 = _simple_flow()
        f2 = f1.model_copy(deep=True)
        f2.nodes[1].data.label = 'Renamed'
        d = diff_flows(f1, f2)
        assert 't1' in d.changedNodes

    def test_diff_edge_endpoint_changed(self):
        f1 = _simple_flow()
        f2 = f1.model_copy(deep=True)
        f2.edges[1].target = 'start'
        d = diff_flows(f1, f2)
        assert 'e2' in d.changedEdges

    def test_identical_after_name_change(self):
        f1 = _simple_flow()
        f2 = f1.model_copy(update={'name': 'New Name'})
        assert is_structurally_identical(f1, f2)
        d = diff_flows(f1, f2)
        assert d.renamed is True
        assert d.nameAfter == 'New Name'


# ---------------------------------------------------------------------------
# versioned store
# ---------------------------------------------------------------------------

class TestVersionedStore:
    def test_create_assigns_version_1(self, flow_store):
        f = _simple_flow()
        saved, fv = flow_store.create_flow(f)
        assert saved.version == 1
        assert fv.meta.version == 1
        assert fv.meta.nodeConfigHash

    def test_update_creates_new_version(self, flow_store):
        f = _simple_flow()
        flow_store.create_flow(f)
        f2 = f.model_copy(deep=True)
        f2.nodes[1].data.code = "ctx['x']=2"
        updated, fv = flow_store.update_flow(f.id, f2)
        assert updated.version == 2
        assert fv.meta.version == 2
        versions = flow_store.list_versions(f.id)
        assert [v.version for v in versions] == [1, 2]

    def test_identical_update_no_new_version(self, flow_store):
        f = _simple_flow()
        flow_store.create_flow(f)
        f2 = f.model_copy(deep=True, update={'name': 'renamed'})
        updated, fv = flow_store.update_flow(f.id, f2)
        assert updated.version == 1
        assert len(flow_store.list_versions(f.id)) == 1

    def test_get_version_returns_immutable(self, flow_store):
        f = _simple_flow()
        flow_store.create_flow(f)
        f2 = f.model_copy(deep=True)
        f2.nodes[1].data.code = "ctx['x']=2"
        flow_store.update_flow(f.id, f2)

        v1 = flow_store.get_version_definition(f.id, 1)
        assert v1 is not None
        assert "ctx['t1']=True" in v1.nodes[1].data.code

        latest = flow_store.get_flow(f.id)
        assert latest.version == 2

    def test_get_missing_version_returns_none(self, flow_store):
        f = _simple_flow()
        flow_store.create_flow(f)
        assert flow_store.get_version(f.id, 99) is None

    def test_delete_latest_refused(self, flow_store):
        f = _simple_flow()
        flow_store.create_flow(f)
        with pytest.raises(VersionInUse):
            flow_store.delete_version(f.id, 1)

    def test_delete_old_version(self, flow_store):
        f = _simple_flow()
        flow_store.create_flow(f)
        f2 = f.model_copy(deep=True)
        f2.nodes[1].data.code = "ctx['x']=2"
        flow_store.update_flow(f.id, f2)
        assert flow_store.delete_version(f.id, 1)
        assert flow_store.get_version(f.id, 1) is None
        assert flow_store.get_version(f.id, 2) is not None


# ---------------------------------------------------------------------------
# execution binds immutable version
# ---------------------------------------------------------------------------

class TestExecutionVersionBinding:
    @pytest.mark.asyncio
    async def test_execution_binds_flow_version(self, manager, flow_store):
        f = _simple_flow()
        flow_store.create_flow(f)
        eid = await manager.start_execution(f)
        await asyncio.sleep(0.5)

        snap = manager.get_snapshot(eid)
        assert snap.flowVersion == 1
        assert snap.nodeConfigHash
        assert snap.status == sm.SUCCEEDED

        record = manager.event_store.load_record(eid)
        assert record.flowVersion == 1
        assert record.nodeConfigHash == snap.nodeConfigHash

    @pytest.mark.asyncio
    async def test_editing_flow_creates_new_version_not_affecting_running(
        self, manager, flow_store
    ):
        f = _simple_flow(flow_id='conc')
        f.nodes[1].data.code = (
            "x=0\nfor i in range(20000000):\n    x+=i\n"
            "ctx['old']=True"
        )
        flow_store.create_flow(f)

        eid = await manager.start_execution(f, flow_version=1)
        await asyncio.sleep(0.3)
        snap = manager.get_snapshot(eid)
        assert snap.status == sm.RUNNING

        edited = f.model_copy(deep=True)
        edited.nodes[1].data.code = "ctx['new']=999"
        updated, fv = flow_store.update_flow(f.id, edited)
        assert updated.version == 2

        await asyncio.sleep(8.0)
        snap = manager.get_snapshot(eid)
        assert snap.flowVersion == 1
        assert snap.variables.get('old') is True
        assert 'new' not in snap.variables
        assert snap.nodeConfigHash != fv.meta.nodeConfigHash

    @pytest.mark.asyncio
    async def test_new_execution_uses_latest_version(self, manager, flow_store):
        f = _simple_flow(flow_id='latest')
        flow_store.create_flow(f)

        f2 = f.model_copy(deep=True)
        f2.nodes[1].data.code = "ctx['v2']=True"
        flow_store.update_flow(f.id, f2)

        eid = await manager.start_execution(f2)
        await asyncio.sleep(0.5)
        snap = manager.get_snapshot(eid)
        assert snap.flowVersion == 2
        assert snap.variables.get('v2') is True


# ---------------------------------------------------------------------------
# recovery with missing version
# ---------------------------------------------------------------------------

class TestRecoveryVersionGuard:
    @pytest.mark.asyncio
    async def test_recovery_uses_bound_version_not_latest(
        self, tmp_dir, flow_store
    ):
        events1 = EventStore(os.path.join(tmp_dir, 'event_store'))
        mgr1 = ExecutionManager(events1, flow_store)

        f = _simple_flow(flow_id='recov')
        f.nodes[1].data.code = (
            "x=0\nfor i in range(20000000):\n    x+=i\nctx['v1_slow']=True"
        )
        flow_store.create_flow(f)
        f2 = f.model_copy(deep=True)
        f2.nodes[1].data.code = "ctx['v2_fast']=True"
        flow_store.update_flow(f.id, f2)

        eid = await mgr1.start_execution(f, flow_version=1)
        await asyncio.sleep(0.5)
        snap = mgr1.get_snapshot(eid)
        assert snap.flowVersion == 1
        assert snap.status == sm.RUNNING, f"Expected running, got {snap.status}"

        await mgr1.shutdown()
        await asyncio.sleep(0.3)

        events2 = EventStore(os.path.join(tmp_dir, 'event_store'))
        mgr2 = ExecutionManager(events2, flow_store)
        await mgr2.recover_all()
        await asyncio.sleep(0.5)

        snap2 = mgr2.get_snapshot(eid)
        assert snap2.flowVersion == 1
        rec = events2.load_record(eid)
        assert 'v2_fast' not in str(rec.flow.nodes[1].data.code)
        assert 'v1_slow' in str(rec.flow.nodes[1].data.code)

        await mgr2.control(eid, 'cancel', request_id='c')
        await asyncio.sleep(8.0)

    @pytest.mark.asyncio
    async def test_recovery_fails_when_version_missing(self, tmp_dir):
        store_dir = os.path.join(tmp_dir, 'vfs')
        flow_store = VersionedFlowStore(store_dir)
        events1 = EventStore(os.path.join(tmp_dir, 'event_store'))
        mgr1 = ExecutionManager(events1, flow_store)

        f = _simple_flow(flow_id='missing')
        f.nodes[1].data.code = (
            "x=0\nfor i in range(20000000):\n    x+=i\nctx['slow_v1']=True"
        )
        flow_store.create_flow(f)
        f2 = f.model_copy(deep=True)
        f2.nodes[1].data.code = "ctx['v2']=True"
        flow_store.update_flow(f.id, f2)

        eid = await mgr1.start_execution(f, flow_version=1)
        await asyncio.sleep(0.3)
        snap = mgr1.get_snapshot(eid)
        assert snap.status == sm.RUNNING, f"Expected running, got {snap.status}"

        await mgr1.shutdown()
        await asyncio.sleep(0.2)

        import shutil
        v1_path = os.path.join(store_dir, 'versions', 'missing', 'v1.json')
        if os.path.exists(v1_path):
            os.remove(v1_path)
        idx_path = os.path.join(store_dir, 'missing.versions.json')
        with open(idx_path) as fh:
            idx = json.load(fh)
        idx['versions'] = [v for v in idx['versions'] if v['version'] != 1]
        with open(idx_path, 'w') as fh:
            json.dump(idx, fh)

        events2 = EventStore(os.path.join(tmp_dir, 'event_store'))
        mgr2 = ExecutionManager(events2, flow_store)
        await mgr2.recover_all()
        await asyncio.sleep(0.3)

        snap = mgr2.get_snapshot(eid)
        assert snap.status == sm.FAILED
        assert 'version' in (snap.lastError or '').lower()


# ---------------------------------------------------------------------------
# deletion protection
# ---------------------------------------------------------------------------

class TestDeletionProtection:
    @pytest.mark.asyncio
    async def test_cannot_delete_version_with_active_execution(
        self, manager, flow_store
    ):
        f = _simple_flow(flow_id='delprot')
        flow_store.create_flow(f)
        f2 = f.model_copy(deep=True)
        f2.nodes[1].data.code = (
            "x=0\nfor i in range(10000000):\n    x+=i\nctx['slow']=True"
        )
        flow_store.update_flow(f.id, f2)

        eid = await manager.start_execution(f, flow_version=1)
        await asyncio.sleep(0.1)

        def check(fid, ver):
            return manager.count_active_executions_for_version(fid, ver)

        with pytest.raises(VersionInUse):
            flow_store.delete_version(f.id, 1, active_version_check=check)

        await manager.control(eid, 'cancel', request_id='c')
        await asyncio.sleep(5.0)

        assert flow_store.delete_version(f.id, 1, active_version_check=check)

    @pytest.mark.asyncio
    async def test_cannot_delete_flow_with_active_execution(
        self, manager, flow_store
    ):
        f = _simple_flow(flow_id='delflow')
        flow_store.create_flow(f)
        slow = f.model_copy(deep=True)
        slow.nodes[1].data.code = (
            "x=0\nfor i in range(10000000):\n    x+=i"
        )
        eid = await manager.start_execution(slow)
        await asyncio.sleep(0.1)

        with pytest.raises(VersionInUse):
            flow_store.delete_flow(
                f.id,
                active_version_check=manager.count_active_executions_for_version,
            )


# ---------------------------------------------------------------------------
# export / import
# ---------------------------------------------------------------------------

class TestExportImport:
    def test_export_contains_all_versions(self, flow_store):
        f = _simple_flow(flow_id='exp')
        flow_store.create_flow(f)
        f2 = f.model_copy(deep=True)
        f2.nodes[1].data.code = "ctx['v2']=1"
        flow_store.update_flow(f.id, f2)

        versions = flow_store.list_versions(f.id)
        assert len(versions) == 2

        bundle = {
            'flow': flow_store.get_flow(f.id).model_dump(),
            'versions': [v.model_dump() for v in versions],
            'versionDefinitions': {
                str(v.version): flow_store.get_version_definition(
                    f.id, v.version
                ).model_dump()
                for v in versions
            },
        }
        data = json.dumps(bundle)
        parsed = json.loads(data)
        assert parsed['flow']['version'] == 2
        assert '1' in parsed['versionDefinitions']
        assert '2' in parsed['versionDefinitions']

    def test_import_creates_or_updates(self, tmp_dir):
        store1 = VersionedFlowStore(os.path.join(tmp_dir, 's1'))
        f = _simple_flow(flow_id='imp')
        store1.create_flow(f)
        f2 = f.model_copy(deep=True)
        f2.nodes[1].data.code = "ctx['v2']=1"
        store1.update_flow(f.id, f2)

        bundle = {
            'flow': store1.get_flow(f.id).model_dump(),
            'versions': [v.model_dump() for v in store1.list_versions(f.id)],
        }
        store2_dir = os.path.join(tmp_dir, 's2')
        store2 = VersionedFlowStore(store2_dir)
        imported = FlowDefinition(**bundle['flow'])
        saved, fv = store2.create_flow(imported)
        assert saved.version == 1
        assert len(store2.list_versions(f.id)) == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
