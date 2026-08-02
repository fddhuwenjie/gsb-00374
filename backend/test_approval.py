"""Integration tests for human approval nodes and expirable recovery tokens
on the persistent execution state machine.

Assertions are made directly on persisted event sequences and on how many
times downstream nodes physically executed.
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from engine.execution_manager import ExecutionManager
from storage.flow_version_store import FlowVersionStore
from test_resumable_engine import (
    CountingGateway, make_node, make_edge, make_flow, linear_flow,
    status_sequence, wait_until, FakeWebSocket,
)
from ws.monitor import monitor_websocket_endpoint


def approval_node(node_id, approvers, timeout):
    return make_node(node_id, 'approval', approvalConfig={
        'approvers': approvers, 'timeoutSeconds': timeout,
    })


def requested_token(journal, node_id):
    events = [e for e in journal.all_events()
              if e['type'] == 'approval_requested' and e['nodeId'] == node_id]
    assert events, f'no approval requested for {node_id}'
    return events[-1]['token']


# ----------------------------------------------------------------------
# 1. multi-approver race: exactly one response wins; duplicates are no-ops
# ----------------------------------------------------------------------
def test_multi_approver_race(tmp_path):
    async def main():
        gateway = CountingGateway()
        manager = ExecutionManager(str(tmp_path), gateway)
        flow = linear_flow('f_ap_race', [
            approval_node('ap', ['alice', 'bob'], 5.0),
            make_node('f', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'f.txt'), 'content': 'x'}),
        ])
        snap = await manager.start_execution(flow)
        exec_id = snap['executionId']
        journal = manager._get_journal(exec_id)

        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'awaiting_approval',
                         desc='awaiting_approval')
        token = requested_token(journal, 'ap')
        assert manager.snapshot(exec_id)['pendingApprovals'][0]['token'] == token

        # alice and bob respond concurrently with full binding fields
        r_alice, r_bob = await asyncio.gather(
            manager.command(exec_id, 'cmd-a', 'approve', token=token,
                            approver='alice', node_id='ap', attempt=1,
                            flow_version=None),
            manager.command(exec_id, 'cmd-b', 'approve', token=token,
                            approver='bob', node_id='ap', attempt=1),
        )
        results = sorted([r_alice['result'], r_bob['result']])
        assert results == ['accepted', 'rejected']

        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'succeeded',
                         desc='succeeded')
        assert status_sequence(journal) == [
            'running', 'awaiting_approval', 'running', 'succeeded'
        ]
        # downstream node executed exactly once
        assert gateway.total('f') == 1
        # exactly one resolution was persisted
        resolved = [e for e in journal.all_events() if e['type'] == 'approval_resolved']
        assert len(resolved) == 1
        assert resolved[0]['decision'] == 'approved'

        # duplicate delivery of the winning command is acknowledged idempotently
        winner = 'cmd-a' if r_alice['result'] == 'accepted' else 'cmd-b'
        dup = await manager.command(exec_id, winner, 'approve', token=token,
                                    approver='alice', node_id='ap', attempt=1)
        assert dup['result'] == 'duplicate'
        resolved = [e for e in journal.all_events() if e['type'] == 'approval_resolved']
        assert len(resolved) == 1

    asyncio.run(main())


# ----------------------------------------------------------------------
# 2. approval timeout -> failed; late response rejected
# ----------------------------------------------------------------------
def test_approval_timeout(tmp_path):
    async def main():
        gateway = CountingGateway()
        manager = ExecutionManager(str(tmp_path), gateway)
        flow = linear_flow('f_ap_timeout', [
            approval_node('ap', ['alice'], 0.2),
            make_node('f', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'f.txt'), 'content': 'x'}),
        ])
        snap = await manager.start_execution(flow)
        exec_id = snap['executionId']
        journal = manager._get_journal(exec_id)

        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'failed',
                         desc='failed after timeout')
        assert status_sequence(journal) == ['running', 'awaiting_approval', 'failed']
        assert gateway.total('f') == 0
        resolved = [e for e in journal.all_events() if e['type'] == 'approval_resolved']
        assert resolved[0]['decision'] == 'expired'

        # a late response against the expired/closed request is rejected
        token = requested_token(journal, 'ap')
        late = await manager.command(exec_id, 'cmd-late', 'approve',
                                     token=token, approver='alice')
        assert late['accepted'] is False

    asyncio.run(main())


# ----------------------------------------------------------------------
# 3. reject and cancel while pending
# ----------------------------------------------------------------------
def test_approval_reject_and_cancel(tmp_path):
    async def main():
        gateway = CountingGateway()
        manager = ExecutionManager(str(tmp_path), gateway)

        # reject -> failed, downstream never runs
        flow = linear_flow('f_ap_reject', [
            approval_node('ap', ['alice'], 5.0),
            make_node('f', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'f1.txt'), 'content': 'x'}),
        ])
        snap = await manager.start_execution(flow)
        exec_id = snap['executionId']
        journal = manager._get_journal(exec_id)
        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'awaiting_approval',
                         desc='awaiting')
        token = requested_token(journal, 'ap')
        r = await manager.command(exec_id, 'cmd-rej', 'reject',
                                  token=token, approver='alice')
        assert r['accepted']
        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'failed',
                         desc='failed')
        assert status_sequence(journal) == ['running', 'awaiting_approval', 'failed']
        assert gateway.total('f') == 0

        # cancel while pending -> cancelled; response after cancel rejected
        flow2 = linear_flow('f_ap_cancel', [
            approval_node('ap', ['alice'], 5.0),
            make_node('g', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'g.txt'), 'content': 'x'}),
        ])
        snap2 = await manager.start_execution(flow2)
        exec_id2 = snap2['executionId']
        journal2 = manager._get_journal(exec_id2)
        await wait_until(lambda: manager.snapshot(exec_id2)['status'] == 'awaiting_approval',
                         desc='awaiting 2')
        token2 = requested_token(journal2, 'ap')
        rc = await manager.command(exec_id2, 'cmd-cancel', 'cancel')
        assert rc['accepted']
        await wait_until(lambda: not manager.has_live_runner(exec_id2),
                         desc='runner exited')
        assert status_sequence(journal2) == ['running', 'awaiting_approval', 'cancelled']
        ra = await manager.command(exec_id2, 'cmd-ap-after-cancel', 'approve',
                                   token=token2, approver='alice')
        assert ra['accepted'] is False
        assert gateway.total('g') == 0

    asyncio.run(main())


# ----------------------------------------------------------------------
# 4. restart recovers pending approval with original token and deadline
# ----------------------------------------------------------------------
def test_restart_recovers_pending_approval(tmp_path):
    async def main():
        gateway1 = CountingGateway()
        m1 = ExecutionManager(str(tmp_path), gateway1)
        flow = linear_flow('f_ap_restart', [
            approval_node('ap', ['alice'], 5.0),
            make_node('f', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'f.txt'), 'content': 'x'}),
        ])
        snap = await m1.start_execution(flow)
        exec_id = snap['executionId']
        journal = m1._get_journal(exec_id)
        await wait_until(lambda: m1.snapshot(exec_id)['status'] == 'awaiting_approval',
                         desc='awaiting')
        token_before = requested_token(journal, 'ap')
        deadline_before = next(
            e for e in journal.all_events()
            if e['type'] == 'approval_requested'
        )['deadline']

        # crash while waiting for a human
        m1._runners[exec_id].cancel()
        try:
            await m1._runners[exec_id]
        except (asyncio.CancelledError, Exception):
            pass

        # new process: pending approval and its deadline are recovered
        gateway2 = CountingGateway()
        m2 = ExecutionManager(str(tmp_path), gateway2)
        recovered = await m2.recover_all()
        assert exec_id in recovered
        snap2 = m2.snapshot(exec_id)
        assert snap2['status'] == 'awaiting_approval'
        assert snap2['pendingApprovals'][0]['token'] == token_before
        assert snap2['pendingApprovals'][0]['deadline'] == deadline_before

        # stale token rejected, original token still works after restart
        bogus = await m2.command(exec_id, 'cmd-bogus', 'approve',
                                 token='deadbeef', approver='alice')
        assert bogus['accepted'] is False
        r = await m2.command(exec_id, 'cmd-ok', 'approve',
                             token=token_before, approver='alice')
        assert r['accepted']
        await wait_until(lambda: m2.snapshot(exec_id)['status'] == 'succeeded',
                         desc='succeeded')
        assert status_sequence(m2._get_journal(exec_id)) == [
            'running', 'awaiting_approval', 'running', 'succeeded'
        ]
        assert gateway1.total('f') + gateway2.total('f') == 1

    asyncio.run(main())


# ----------------------------------------------------------------------
# 5. parallel branch approval: only the matching generation is released
# ----------------------------------------------------------------------
def test_parallel_approval_generation_isolation(tmp_path):
    async def main():
        gateway = CountingGateway()
        gateway.fail_remaining['hB'] = 1  # branch B fails generation 1.0
        manager = ExecutionManager(str(tmp_path), gateway)

        from models.flow import RetryConfig
        nodes = [
            make_node('start', 'start'),
            make_node('par', 'parallel',
                      parallelConfig={'branchNodeIds': ['apA', 'hB']},
                      retry=RetryConfig(maxAttempts=2, delaySeconds=0.05,
                                        backoff='fixed', maxDelaySeconds=1)),
            approval_node('apA', ['alice'], 10.0),
            make_node('hB', 'http',
                      httpConfig={'url': 'http://example.invalid/b', 'method': 'GET'}),
            make_node('end', 'end'),
        ]
        edges = [make_edge('start', 'par'), make_edge('par', 'end')]
        flow = make_flow('f_ap_par', nodes, edges)

        snap = await manager.start_execution(flow)
        exec_id = snap['executionId']
        journal = manager._get_journal(exec_id)

        # generation 1.0: approval requested, then branch B fails -> retry.
        # generation 1.1: a NEW approval request supersedes the old one.
        await wait_until(
            lambda: len([e for e in journal.all_events()
                         if e['type'] == 'approval_requested']) >= 2,
            desc='second generation approval requested')
        tokens = [e['token'] for e in journal.all_events()
                  if e['type'] == 'approval_requested']
        t_old, t_new = tokens[0], tokens[-1]
        assert t_old != t_new

        # the old generation's token must not release anything
        stale = await manager.command(exec_id, 'cmd-stale', 'approve',
                                      token=t_old, approver='alice')
        assert stale['accepted'] is False
        assert manager.snapshot(exec_id)['status'] == 'awaiting_approval'

        # the current generation's token releases only its own request
        ok = await manager.command(exec_id, 'cmd-new', 'approve',
                                   token=t_new, approver='alice')
        assert ok['accepted']
        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'succeeded',
                         desc='succeeded')

        resolved = [e for e in journal.all_events()
                    if e['type'] == 'approval_resolved']
        assert len(resolved) == 1
        assert resolved[0]['token'] == t_new
        # join completed only with generation-1.1 results
        merged = manager.snapshot(exec_id)['variables']['par_result']
        assert merged['_generation'] == '1.1'
        assert status_sequence(journal) == [
            'running', 'awaiting_approval', 'retry_wait',
            'running', 'awaiting_approval', 'running', 'succeeded',
        ]

    asyncio.run(main())


# ----------------------------------------------------------------------
# 6. stale WebSocket messages: replay never resends applied events
# ----------------------------------------------------------------------
def test_websocket_no_stale_replay(tmp_path):
    async def main():
        gateway = CountingGateway()
        manager = ExecutionManager(str(tmp_path), gateway)
        flow = linear_flow('f_ap_ws', [
            approval_node('ap', ['alice'], 5.0),
        ])
        snap = await manager.start_execution(flow)
        exec_id = snap['executionId']
        journal = manager._get_journal(exec_id)
        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'awaiting_approval',
                         desc='awaiting')
        token = requested_token(journal, 'ap')
        await manager.command(exec_id, 'cmd-ok', 'approve', token=token, approver='alice')
        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'succeeded',
                         desc='succeeded')
        last = journal.last_seq

        # reconnect claiming everything applied: snapshot only, zero events
        ws = FakeWebSocket()
        task = asyncio.create_task(monitor_websocket_endpoint(ws, exec_id, last, manager))
        await asyncio.sleep(0.2)
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass
        assert len(ws.sent) == 1 and ws.sent[0]['type'] == 'snapshot'
        assert ws.sent[0]['seq'] == last

        # reconnect from an older cursor: strictly increasing, no duplicates
        ws2 = FakeWebSocket()
        task2 = asyncio.create_task(
            monitor_websocket_endpoint(ws2, exec_id, last - 2, manager))
        await asyncio.sleep(0.2)
        task2.cancel()
        try:
            await task2
        except (asyncio.CancelledError, Exception):
            pass
        seqs = [m['event']['seq'] for m in ws2.sent[1:]]
        assert seqs == [last - 1, last]
        assert ws2.sent[0]['pendingApprovals'] == []

    asyncio.run(main())
