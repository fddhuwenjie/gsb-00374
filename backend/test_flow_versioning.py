"""Integration tests for flow definition versioning on top of the persistent
execution state machine.

Proves that editing a flow creates new immutable versions while existing
executions keep running, recovering, retrying and replaying against the
original version bound at start time.
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.flow import FlowDefinition
from engine.execution_manager import ExecutionManager
from storage.flow_version_store import (
    FlowVersionStore, VersionInUseError, compute_config_hash, diff_flows,
)
from test_resumable_engine import (
    CountingGateway, make_node, make_edge, make_flow, linear_flow,
    status_sequence, wait_until,
)


def edited(flow: FlowDefinition, mutate) -> FlowDefinition:
    data = flow.model_dump()
    mutate(data)
    return FlowDefinition(**data)


def set_file_content(flow: FlowDefinition, node_id: str, content: str) -> FlowDefinition:
    def mutate(data):
        for n in data['nodes']:
            if n['id'] == node_id:
                n['data']['fileConfig']['content'] = content
    return edited(flow, mutate)


# ----------------------------------------------------------------------
# 1. edit-vs-run concurrency: running execution is bound to v1 forever
# ----------------------------------------------------------------------
def test_edit_run_concurrency(tmp_path):
    async def main():
        version_store = FlowVersionStore(str(tmp_path / 'versions'))
        gateway = CountingGateway()
        manager = ExecutionManager(str(tmp_path / 'exec'), gateway, version_store)

        flow_v1 = linear_flow('f_conc', [
            make_node('w', 'wait', seconds=0.4),
            make_node('f', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'f.txt'), 'content': 'v1'}),
        ])
        snap = await manager.start_execution(flow_v1)
        exec_id = snap['executionId']
        journal = manager._get_journal(exec_id)
        assert snap['flowVersion'] == 1
        assert snap['configHash'] == compute_config_hash(flow_v1)

        # edit the flow WHILE the execution is running: must create v2,
        # never mutate v1
        flow_v2 = set_file_content(flow_v1, 'f', 'v2')
        record_v2 = version_store.save_version(flow_v2)
        assert record_v2['version'] == 2
        assert record_v2['configHash'] != snap['configHash']

        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'succeeded',
                         desc='succeeded')

        # the execution ran with the ORIGINAL definition embedded in its journal
        journaled_flow = journal.flow()
        f_node = next(n for n in journaled_flow.nodes if n.id == 'f')
        assert f_node.data.fileConfig.content == 'v1'
        assert manager.snapshot(exec_id)['flowVersion'] == 1
        # idempotency key semantics unchanged: executionId:nodeId:attempt
        assert gateway.total('f') == 1
        assert any(k.endswith(':f:1') for k in gateway.counts)

        # a new execution of the edited flow binds v2
        snap2 = await manager.start_execution(flow_v2)
        assert snap2['flowVersion'] == 2

    asyncio.run(main())


# ----------------------------------------------------------------------
# 2. old-version recovery and retry after the definition moved on
# ----------------------------------------------------------------------
def test_old_version_recovery_and_retry(tmp_path):
    async def main():
        version_store = FlowVersionStore(str(tmp_path / 'versions'))

        # --- crash recovery still uses v1 after v2 exists ---
        gw1 = CountingGateway()
        gw1.delays['g'] = 0.5
        m1 = ExecutionManager(str(tmp_path / 'exec'), gw1, version_store)
        flow_v1 = linear_flow('f_oldver', [
            make_node('f', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'f.txt'), 'content': 'v1'}),
            make_node('g', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'g.txt'), 'content': 'g'}),
        ])
        snap = await m1.start_execution(flow_v1)
        exec_id = snap['executionId']
        journal = m1._get_journal(exec_id)
        await wait_until(
            lambda: any(e['type'] == 'node_started' and e['nodeId'] == 'g'
                        for e in journal.all_events()),
            desc='g started')
        m1._runners[exec_id].cancel()
        try:
            await m1._runners[exec_id]
        except (asyncio.CancelledError, Exception):
            pass

        # definition moves to v2 before the process restarts
        version_store.save_version(set_file_content(flow_v1, 'f', 'v2'))

        gw2 = CountingGateway()
        m2 = ExecutionManager(str(tmp_path / 'exec'), gw2, version_store)
        recovered = await m2.recover_all()
        assert exec_id in recovered
        await wait_until(lambda: m2.snapshot(exec_id)['status'] == 'succeeded',
                         desc='succeeded after recovery')
        # recovery used the bound v1: f not re-executed, journal flow intact
        assert gw2.total('f') == 0
        assert m2.snapshot(exec_id)['flowVersion'] == 1
        f_node = next(n for n in journal.flow().nodes if n.id == 'f')
        assert f_node.data.fileConfig.content == 'v1'

        # --- retry of a failed execution still uses v1 ---
        gw3 = CountingGateway()
        gw3.fail_remaining['h'] = 99
        m3 = ExecutionManager(str(tmp_path / 'exec2'), gw3, version_store)
        flow_r = linear_flow('f_oldver_retry', [
            make_node('h', 'http',
                      httpConfig={'url': 'http://example.invalid/z', 'method': 'GET'}),
        ])
        snap_r = await m3.start_execution(flow_r)
        exec_r = snap_r['executionId']
        await wait_until(lambda: m3.snapshot(exec_r)['status'] == 'failed',
                         desc='failed')
        # edit AFTER failure; retry must still run the bound v1
        version_store.save_version(
            edited(flow_r, lambda d: d['nodes'][1]['data']['httpConfig']
                   .update({'url': 'http://example.invalid/v2'}))
        )
        gw3.fail_remaining['h'] = 0
        r = await m3.command(exec_r, 'cmd-retry-1', 'retry')
        assert r['accepted']
        await wait_until(lambda: m3.snapshot(exec_r)['status'] == 'succeeded',
                         desc='succeeded after retry')
        assert m3.snapshot(exec_r)['flowVersion'] == 1
        h_node = next(n for n in m3._get_journal(exec_r).flow().nodes if n.id == 'h')
        assert h_node.data.httpConfig.url == 'http://example.invalid/z'

    asyncio.run(main())


# ----------------------------------------------------------------------
# 3. version diff: node add/remove/config-change vs edge changes
# ----------------------------------------------------------------------
def test_version_diff(tmp_path):
    store = FlowVersionStore(str(tmp_path / 'versions'))

    flow_v1 = make_flow('f_diff', [
        make_node('start', 'start'),
        make_node('a', 'task', code='ctx["x"] = 1'),
        make_node('b', 'wait', seconds=1),
        make_node('end', 'end'),
    ], [
        make_edge('start', 'a'),
        make_edge('a', 'b'),
        make_edge('b', 'end'),
    ])
    flow_v2 = make_flow('f_diff', [
        make_node('start', 'start'),
        make_node('a', 'task', code='ctx["x"] = 2'),   # config changed
        # b removed
        make_node('c', 'filewrite',
                  fileConfig={'path': '/tmp/c.txt', 'content': 'c'}),  # added
        make_node('end', 'end'),
    ], [
        make_edge('start', 'a'),
        make_edge('a', 'c'),                            # edge added
        make_edge('c', 'end'),                          # edge added
        # a->b and b->end removed
    ])

    r1 = store.save_version(flow_v1)
    r2 = store.save_version(flow_v2)
    assert (r1['version'], r2['version']) == (1, 2)

    d = store.diff('f_diff', 1, 2)
    assert d['nodesAdded'] == ['c']
    assert d['nodesRemoved'] == ['b']
    assert d['nodesConfigChanged'] == ['a']
    assert set(d['edgesAdded']) == {'a->c:', 'c->end:'}
    assert set(d['edgesRemoved']) == {'a->b:', 'b->end:'}

    # saving identical content does not create a new version
    r1_again = store.save_version(flow_v1)
    assert r1_again['version'] == 1


# ----------------------------------------------------------------------
# 4. version deletion protection + recovery refusal for missing version
# ----------------------------------------------------------------------
def test_version_delete_protection_and_recovery_refusal(tmp_path):
    async def main():
        version_store = FlowVersionStore(str(tmp_path / 'versions'))

        # --- deletion protection: referenced version cannot be deleted ---
        gateway = CountingGateway()
        m1 = ExecutionManager(str(tmp_path / 'exec'), gateway, version_store)
        flow = linear_flow('f_protect', [])
        snap = await m1.start_execution(flow)
        await wait_until(lambda: m1.snapshot(snap['executionId'])['status'] == 'succeeded',
                         desc='succeeded')
        with pytest.raises(VersionInUseError):
            version_store.delete('f_protect', 1,
                                 referenced=m1.referenced_versions())

        # --- recovery refusal: version file removed out-of-band ---
        gw2 = CountingGateway()
        gw2.delays['g'] = 0.5
        m2 = ExecutionManager(str(tmp_path / 'exec2'), gw2, version_store)
        flow2 = linear_flow('f_gone', [
            make_node('g', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'g.txt'), 'content': 'g'}),
        ])
        snap2 = await m2.start_execution(flow2)
        exec_id2 = snap2['executionId']
        await wait_until(lambda: m2.snapshot(exec_id2)['status'] == 'running',
                         desc='running')
        m2._runners[exec_id2].cancel()
        try:
            await m2._runners[exec_id2]
        except (asyncio.CancelledError, Exception):
            pass
        os.remove(version_store._path('f_gone', 1))

        m3 = ExecutionManager(str(tmp_path / 'exec2'), CountingGateway(), version_store)
        recovered = await m3.recover_all()
        assert exec_id2 not in recovered
        assert exec_id2 in m3.recovery_refused
        assert m3.snapshot(exec_id2)['recoveryRefused'] is True

        # --- retry refusal: failed execution whose version vanished ---
        gw4 = CountingGateway()
        gw4.fail_remaining['h'] = 99
        m4 = ExecutionManager(str(tmp_path / 'exec3'), gw4, version_store)
        flow3 = linear_flow('f_gone_retry', [
            make_node('h', 'http',
                      httpConfig={'url': 'http://example.invalid/q', 'method': 'GET'}),
        ])
        snap3 = await m4.start_execution(flow3)
        exec_id3 = snap3['executionId']
        await wait_until(lambda: m4.snapshot(exec_id3)['status'] == 'failed',
                         desc='failed')
        os.remove(version_store._path('f_gone_retry', 1))
        r = await m4.command(exec_id3, 'cmd-retry-x', 'retry')
        assert r['accepted'] is False
        assert r['result'] == 'rejected'
        assert 'version' in (r['detail'] or '')

    asyncio.run(main())


# ----------------------------------------------------------------------
# 5. export / import preserves version identity and content
# ----------------------------------------------------------------------
def test_export_import(tmp_path):
    async def main():
        store_a = FlowVersionStore(str(tmp_path / 'versions_a'))
        gateway = CountingGateway()
        manager = ExecutionManager(str(tmp_path / 'exec'), gateway, store_a)

        flow = linear_flow('f_export', [
            make_node('f', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'f.txt'), 'content': 'x'}),
        ])
        snap = await manager.start_execution(flow)
        await wait_until(lambda: manager.snapshot(snap['executionId'])['status'] == 'succeeded',
                         desc='succeeded')
        original_hash = snap['configHash']

        bundle = store_a.export_version('f_export', 1)
        assert bundle['format'] == 'flow-version-bundle/v1'
        assert bundle['configHash'] == original_hash

        # import into an independent store: same content, same hash
        store_b = FlowVersionStore(str(tmp_path / 'versions_b'))
        record_b = store_b.import_bundle(bundle)
        assert record_b['version'] == 1
        assert record_b['configHash'] == original_hash
        assert record_b['flow'] == bundle['flow']

        # executions started from the imported definition bind the same hash
        manager_b = ExecutionManager(str(tmp_path / 'exec_b'), CountingGateway(), store_b)
        snap_b = await manager_b.start_execution(FlowDefinition(**record_b['flow']))
        assert snap_b['configHash'] == original_hash
        await wait_until(lambda: manager_b.snapshot(snap_b['executionId'])['status'] == 'succeeded',
                         desc='succeeded (imported)')

        # re-import is idempotent: no duplicate version created
        record_b2 = store_b.import_bundle(bundle)
        assert record_b2['version'] == 1
        assert len(store_b.list_versions('f_export')) == 1

    asyncio.run(main())
