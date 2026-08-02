"""Self-starting integration tests for the persistent, resumable, idempotent
execution state machine.

Each test builds its own ExecutionManager against a temporary journal
directory (tmp_path) — no external server or pre-existing data required.
State event sequences and physical side-effect counts are asserted directly
from the persisted journal and a counting side-effect gateway.
"""
import asyncio
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models.flow import FlowDefinition, FlowNode, FlowEdge, NodeData, Position, RetryConfig
from engine.execution_manager import ExecutionManager
from engine.resumable_executor import SideEffectGateway
from engine.executor import ExecutionError
from engine.state_machine import ExecutionStateMachine, IllegalTransitionError
from storage.execution_journal import ExecutionJournal
from ws.monitor import monitor_websocket_endpoint


# ----------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------
def make_node(node_id, node_type, **data):
    return FlowNode(id=node_id, type=node_type,
                    position=Position(x=0, y=0),
                    data=NodeData(label=node_id, **data))


def make_edge(src, tgt, handle=None):
    return FlowEdge(id=f"{src}->{tgt}:{handle}", source=src, target=tgt,
                    sourceHandle=handle)


def make_flow(flow_id, nodes, edges):
    return FlowDefinition(id=flow_id, name=flow_id, nodes=nodes, edges=edges,
                          createdAt=time.time(), updatedAt=time.time())


class CountingGateway(SideEffectGateway):
    """Counts every physical side effect and can inject failures/delays."""

    def __init__(self):
        self.counts = {}
        self.fail_remaining = {}
        self.delays = {}

    async def perform(self, node, idempotency_key, variables):
        self.counts[idempotency_key] = self.counts.get(idempotency_key, 0) + 1
        if self.delays.get(node.id):
            await asyncio.sleep(self.delays[node.id])
        if self.fail_remaining.get(node.id, 0) > 0:
            self.fail_remaining[node.id] -= 1
            raise ExecutionError(f"injected failure for {node.id}")
        return {'nodeId': node.id, 'key': idempotency_key}

    def total(self, node_id):
        return sum(v for k, v in self.counts.items() if f':{node_id}:' in k)


def status_sequence(journal):
    return [e['toStatus'] for e in journal.all_events() if e['type'] == 'status']


async def wait_until(pred, timeout=5.0, interval=0.01, desc='condition'):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"timeout waiting for {desc}")


def linear_flow(flow_id, middle_nodes):
    nodes = [make_node('start', 'start')] + middle_nodes + [make_node('end', 'end')]
    edges = []
    chain = ['start'] + [n.id for n in middle_nodes] + ['end']
    for a, b in zip(chain, chain[1:]):
        edges.append(make_edge(a, b))
    return make_flow(flow_id, nodes, edges)


# ----------------------------------------------------------------------
# 1. state machine rejects illegal transitions
# ----------------------------------------------------------------------
def test_illegal_transitions_rejected(tmp_path):
    flow = linear_flow('f_illegal', [])
    journal = ExecutionJournal.create(str(tmp_path), flow)
    sm = ExecutionStateMachine(journal)
    assert sm.status == 'queued'

    with pytest.raises(IllegalTransitionError):
        sm.transition('succeeded')
    with pytest.raises(IllegalTransitionError):
        sm.transition('paused')

    sm.transition('running')
    with pytest.raises(IllegalTransitionError):
        sm.transition('paused')  # must pass through pausing first

    # every persisted transition carries monotonic seq and previous status
    events = [e for e in journal.all_events() if e['type'] == 'status']
    assert [e['seq'] for e in events] == sorted(e['seq'] for e in events)
    assert events[0]['fromStatus'] == 'queued' and events[0]['toStatus'] == 'running'

    # journal seq is strictly monotonic across all event types
    seqs = [e['seq'] for e in journal.all_events()]
    assert seqs == list(range(1, len(seqs) + 1))


# ----------------------------------------------------------------------
# 2. pause race: pausing -> persisted boundary -> paused; duplicate commands
# ----------------------------------------------------------------------
def test_pause_race_and_duplicate_commands(tmp_path):
    async def main():
        gateway = CountingGateway()
        manager = ExecutionManager(str(tmp_path), gateway)
        flow = linear_flow('f_pause', [
            make_node('w', 'wait', seconds=0.5),
            make_node('f', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'out.txt'), 'content': 'x'}),
        ])
        snap = await manager.start_execution(flow)
        exec_id = snap['executionId']
        runner_task = manager._runners[exec_id]

        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'running',
                         desc='running')

        # pause + duplicate delivery + a second, now-illegal pause
        r1 = await manager.command(exec_id, 'cmd-pause-1', 'pause')
        r_dup = await manager.command(exec_id, 'cmd-pause-1', 'pause')
        r2 = await manager.command(exec_id, 'cmd-pause-2', 'pause')
        assert r1['accepted'] and r1['result'] == 'accepted'
        assert r_dup['result'] == 'duplicate'
        assert r2['accepted'] is False and r2['result'] == 'rejected'

        # node finishes and is persisted before paused is reached
        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'paused',
                         desc='paused')
        journal = manager._get_journal(exec_id)
        events = journal.all_events()
        nc = next(e for e in events
                  if e['type'] == 'node_completed' and e['nodeId'] == 'w')
        paused_ev = next(e for e in events
                         if e['type'] == 'status' and e['toStatus'] == 'paused')
        assert nc['seq'] < paused_ev['seq']

        # same runner is still the only executor; resume must not spawn another
        assert manager._runners[exec_id] is runner_task
        stale = await manager.command(exec_id, 'cmd-resume-stale', 'resume',
                                      expected_status='running')
        assert stale['result'] == 'stale'
        rr = await manager.command(exec_id, 'cmd-resume-1', 'resume')
        assert rr['accepted']
        assert manager._runners[exec_id] is runner_task

        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'succeeded',
                         desc='succeeded')
        assert status_sequence(journal) == [
            'running', 'pausing', 'paused', 'running', 'succeeded'
        ]
        assert gateway.total('f') == 1
        assert gateway.total('w') == 0  # wait has no side effect

    asyncio.run(main())


# ----------------------------------------------------------------------
# 3. process restart: resume from the last complete node boundary only
# ----------------------------------------------------------------------
def test_process_restart_resumes_from_boundary(tmp_path):
    async def main():
        gw1 = CountingGateway()
        gw1.delays['g'] = 0.5
        m1 = ExecutionManager(str(tmp_path), gw1)
        flow = linear_flow('f_restart', [
            make_node('t', 'task', code="ctx['x'] = 41"),
            make_node('g', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'g.txt'), 'content': 'g'}),
        ])
        snap = await m1.start_execution(flow)
        exec_id = snap['executionId']
        journal = m1._get_journal(exec_id)

        await wait_until(
            lambda: any(e['type'] == 'node_started' and e['nodeId'] == 'g'
                        for e in journal.all_events()),
            desc='g started')
        # simulate power loss: drop the in-memory runner without any cleanup
        m1._runners[exec_id].cancel()
        try:
            await m1._runners[exec_id]
        except (asyncio.CancelledError, Exception):
            pass

        # new "process": brand new manager over the same journal directory
        gw2 = CountingGateway()
        m2 = ExecutionManager(str(tmp_path), gw2)
        recovered = await m2.recover_all()
        assert exec_id in recovered

        await wait_until(lambda: m2.snapshot(exec_id)['status'] == 'succeeded',
                         desc='succeeded after recovery')
        snap2 = m2.snapshot(exec_id)
        # resumed from persisted boundary variables, not from memory
        assert snap2['variables'].get('x') == 41
        assert status_sequence(m2._get_journal(exec_id)) == [
            'running', 'queued', 'running', 'succeeded'
        ]
        # interrupted node g re-executed exactly once in the new process
        assert gw2.total('g') == 1

    asyncio.run(main())


# ----------------------------------------------------------------------
# 4. parallel join waits for same-generation branches; retry re-generates
# ----------------------------------------------------------------------
def test_parallel_join_generation(tmp_path):
    async def main():
        gateway = CountingGateway()
        gateway.fail_remaining['hB'] = 1  # first generation fails on branch B
        manager = ExecutionManager(str(tmp_path), gateway)

        nodes = [
            make_node('start', 'start'),
            make_node('par', 'parallel',
                      parallelConfig={'branchNodeIds': ['fA', 'hB']},
                      retry=RetryConfig(maxAttempts=2, delaySeconds=0.05,
                                        backoff='fixed', maxDelaySeconds=1)),
            make_node('fA', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'a.txt'), 'content': 'a'}),
            make_node('hB', 'http',
                      httpConfig={'url': 'http://example.invalid/x', 'method': 'GET'}),
            make_node('end', 'end'),
        ]
        edges = [make_edge('start', 'par'), make_edge('par', 'end')]
        flow = make_flow('f_parallel', nodes, edges)

        snap = await manager.start_execution(flow)
        exec_id = snap['executionId']
        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'succeeded',
                         desc='succeeded')

        # both branches physically ran in both generations: the retried join
        # did not consume the previous generation's results
        assert gateway.total('fA') == 2
        assert gateway.total('hB') == 2

        variables = manager.snapshot(exec_id)['variables']
        merged = variables['par_result']
        assert merged['_generation'] == '1.1'
        assert merged['hB_result']['key'].endswith(':hB:1.1')
        assert merged['fA_result']['key'].endswith(':fA:1.1')

        journal = manager._get_journal(exec_id)
        assert status_sequence(journal) == ['running', 'retry_wait', 'running', 'succeeded']

    asyncio.run(main())


# ----------------------------------------------------------------------
# 5. failure -> retry_wait sequence -> failed; retry command resumes to success
# ----------------------------------------------------------------------
def test_failure_retry_sequence(tmp_path):
    async def main():
        gateway = CountingGateway()
        gateway.fail_remaining['h'] = 99  # always fail at first
        manager = ExecutionManager(str(tmp_path), gateway)
        flow = linear_flow('f_retry', [
            make_node('h', 'http',
                      httpConfig={'url': 'http://example.invalid/y', 'method': 'GET'},
                      retry=RetryConfig(maxAttempts=2, delaySeconds=0.05,
                                        backoff='fixed', maxDelaySeconds=1)),
            make_node('f', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'f.txt'), 'content': 'f'}),
        ])
        snap = await manager.start_execution(flow)
        exec_id = snap['executionId']
        journal = manager._get_journal(exec_id)

        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'failed',
                         desc='failed')
        assert status_sequence(journal) == ['running', 'retry_wait', 'running', 'failed']
        assert manager.has_live_runner(exec_id) is False
        assert gateway.total('f') == 0

        # fix the world, then retry the failed execution
        gateway.fail_remaining['h'] = 0
        r = await manager.command(exec_id, 'cmd-retry-1', 'retry')
        assert r['accepted']
        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'succeeded',
                         desc='succeeded after retry')

        assert status_sequence(journal) == [
            'running', 'retry_wait', 'running', 'failed', 'queued', 'running', 'succeeded'
        ]
        # retried node ran 3 physical attempts total; downstream ran once
        assert gateway.total('h') == 3
        assert gateway.total('f') == 1

        # duplicate retry command must not spawn anything
        dup = await manager.command(exec_id, 'cmd-retry-1', 'retry')
        assert dup['result'] == 'duplicate'
        assert manager.has_live_runner(exec_id) is False

    asyncio.run(main())


# ----------------------------------------------------------------------
# 6. duplicate / stale / illegal commands and single-runner guarantee
# ----------------------------------------------------------------------
def test_duplicate_stale_and_illegal_commands(tmp_path):
    async def main():
        gateway = CountingGateway()
        manager = ExecutionManager(str(tmp_path), gateway)
        flow = linear_flow('f_cmds', [])
        snap = await manager.start_execution(flow)
        exec_id = snap['executionId']
        journal = manager._get_journal(exec_id)

        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'succeeded',
                         desc='succeeded')

        # terminal state: everything rejected
        r = await manager.command(exec_id, 'c-pause', 'pause')
        assert r['result'] == 'rejected'
        r = await manager.command(exec_id, 'c-cancel', 'cancel')
        assert r['result'] == 'rejected'
        r = await manager.command(exec_id, 'c-retry', 'retry', expected_status='failed')
        assert r['result'] == 'stale'

        # duplicate of a rejected command is acknowledged without journaling
        seq_before = journal.last_seq
        r = await manager.command(exec_id, 'c-pause', 'pause')
        assert r['result'] == 'duplicate'
        assert journal.last_seq == seq_before

        # a second start with the same execution id cannot create a second executor
        with pytest.raises(Exception):
            await manager.start_execution(flow, execution_id=exec_id)

    asyncio.run(main())


# ----------------------------------------------------------------------
# 7. side-effect idempotency across restart: successful effect never repeats
# ----------------------------------------------------------------------
def test_side_effect_idempotency_across_restart(tmp_path):
    async def main():
        gw1 = CountingGateway()
        gw1.delays['g'] = 0.5
        m1 = ExecutionManager(str(tmp_path), gw1)
        flow = linear_flow('f_idem', [
            make_node('f', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'f.txt'), 'content': 'f'}),
            make_node('g', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'g.txt'), 'content': 'g'}),
        ])
        snap = await m1.start_execution(flow)
        exec_id = snap['executionId']
        journal = m1._get_journal(exec_id)

        # crash while g is in flight; f's effect AND boundary are durable
        await wait_until(
            lambda: any(e['type'] == 'node_started' and e['nodeId'] == 'g'
                        for e in journal.all_events()),
            desc='g started')
        m1._runners[exec_id].cancel()
        try:
            await m1._runners[exec_id]
        except (asyncio.CancelledError, Exception):
            pass
        assert gw1.total('f') == 1

        gw2 = CountingGateway()
        m2 = ExecutionManager(str(tmp_path), gw2)
        await m2.recover_all()
        await wait_until(lambda: m2.snapshot(exec_id)['status'] == 'succeeded',
                         desc='succeeded after recovery')

        # f's successful side effect was not repeated in the new process;
        # g (no recorded success) was performed exactly once more
        assert gw2.total('f') == 0
        assert gw2.total('g') == 1
        assert gw1.total('f') + gw2.total('f') == 1

    asyncio.run(main())


# ----------------------------------------------------------------------
# 8. WebSocket replay: snapshot first, then ordered gap-free increments
# ----------------------------------------------------------------------
class FakeWebSocket:
    def __init__(self):
        self.sent = []
        self.accepted = False

    async def accept(self):
        self.accepted = True

    async def send_json(self, message):
        self.sent.append(message)

    async def close(self):
        pass


def test_websocket_replay(tmp_path):
    async def main():
        gateway = CountingGateway()
        manager = ExecutionManager(str(tmp_path), gateway)
        flow = linear_flow('f_ws', [
            make_node('w', 'wait', seconds=0.1),
            make_node('f', 'filewrite',
                      fileConfig={'path': str(tmp_path / 'f.txt'), 'content': 'f'}),
        ])
        snap = await manager.start_execution(flow)
        exec_id = snap['executionId']
        journal = manager._get_journal(exec_id)

        # late joiner connecting while the execution is live (since=0)
        ws1 = FakeWebSocket()
        task1 = asyncio.create_task(monitor_websocket_endpoint(ws1, exec_id, 0, manager))
        await wait_until(lambda: manager.snapshot(exec_id)['status'] == 'succeeded',
                         desc='succeeded')
        await asyncio.sleep(0.3)  # let live events flush
        task1.cancel()
        try:
            await task1
        except (asyncio.CancelledError, Exception):
            pass

        assert ws1.sent[0]['type'] == 'snapshot'
        # snapshot reflects connect-time state; increments carry it to succeeded
        assert ws1.sent[0]['status'] in ('running', 'succeeded')
        assert 'allowedCommands' in ws1.sent[0]
        assert 'allowedTransitions' in ws1.sent[0]
        seqs = [m['event']['seq'] for m in ws1.sent[1:]]
        last = journal.last_seq
        # gap-free, duplicate-free, in order, complete from seq 1
        assert seqs == list(range(1, last + 1))

        # reconnecting client: snapshot + only events newer than `since`
        ws2 = FakeWebSocket()
        task2 = asyncio.create_task(monitor_websocket_endpoint(ws2, exec_id, 2, manager))
        await asyncio.sleep(0.3)
        task2.cancel()
        try:
            await task2
        except (asyncio.CancelledError, Exception):
            pass

        assert ws2.sent[0]['type'] == 'snapshot'
        assert ws2.sent[0]['seq'] == last
        seqs2 = [m['event']['seq'] for m in ws2.sent[1:]]
        assert seqs2 == list(range(3, last + 1))

    asyncio.run(main())
