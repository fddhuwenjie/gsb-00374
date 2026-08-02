import asyncio
import json
import os
import sys
import tempfile
import threading
import time
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler

import pytest
import websockets

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from engine import state_machine as sm
from engine.execution_manager import ExecutionManager
from models.flow import (
    FlowDefinition, FlowNode, FlowEdge, Position, NodeData,
    FileWriteConfig, RetryConfig, ParallelConfig, HttpConfig,
)
from storage.event_store import EventStore


def _node(node_id, node_type, data=None):
    if data is None:
        data = NodeData(label=node_id)
    return FlowNode(id=node_id, type=node_type, position=Position(x=0, y=0), data=data)


def _edge(edge_id, source, target, handle=None):
    return FlowEdge(id=edge_id, source=source, target=target, sourceHandle=handle)


def _linear_flow(nodes_spec, flow_id='test_flow'):
    nodes = [_node('start', 'start')]
    edges = []
    prev = 'start'
    for i, spec in enumerate(nodes_spec):
        nid = spec.get('id', f'n{i}')
        ntype = spec['type']
        data = spec.get('data', NodeData(label=nid))
        nodes.append(_node(nid, ntype, data))
        edges.append(_edge(f'e{prev}_{nid}', prev, nid))
        prev = nid
    nodes.append(_node('end', 'end'))
    edges.append(_edge(f'e{prev}_end', prev, 'end'))
    return FlowDefinition(
        id=flow_id, name='Test', nodes=nodes, edges=edges,
        createdAt=0, updatedAt=0,
    )


def _busy_wait_code(iterations=2000000):
    return f"x = 0\nfor i in range({iterations}):\n    x += i\nctx['busy_done'] = True"


class _RetryHandler(BaseHTTPRequestHandler):
    attempt_counter = 0
    fail_until = 2

    def do_GET(self):
        type(self).attempt_counter += 1
        if type(self).attempt_counter <= type(self).fail_until:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b'error')
        else:
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"ok": true}')

    def log_message(self, format, *args):
        pass


@pytest.fixture
def retry_http_server():
    _RetryHandler.attempt_counter = 0
    _RetryHandler.fail_until = 2
    server = HTTPServer(('127.0.0.1', 0), _RetryHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f'http://127.0.0.1:{port}/'
    server.shutdown()


@pytest.fixture
def tmp_flow_dir(tmp_path):
    return str(tmp_path / 'flows')


@pytest.fixture
def event_store(tmp_flow_dir):
    return EventStore(os.path.join(tmp_flow_dir, 'event_store'))


@pytest.fixture
def manager(event_store):
    return ExecutionManager(event_store)


@pytest.mark.asyncio
async def test_illegal_transitions_rejected():
    with pytest.raises(sm.IllegalTransitionError):
        sm.assert_transition(sm.SUCCEEDED, sm.RUNNING)
    with pytest.raises(sm.IllegalTransitionError):
        sm.assert_transition(sm.FAILED, sm.RUNNING)
    with pytest.raises(sm.IllegalTransitionError):
        sm.assert_transition(sm.CANCELLED, sm.RUNNING)
    with pytest.raises(sm.IllegalTransitionError):
        sm.assert_transition(sm.QUEUED, sm.PAUSED)
    assert sm.can_transition(sm.QUEUED, sm.RUNNING)
    assert sm.can_transition(sm.RUNNING, sm.PAUSING)
    assert sm.can_transition(sm.PAUSING, sm.PAUSED)
    assert sm.can_transition(sm.PAUSED, sm.RUNNING)


@pytest.mark.asyncio
async def test_state_sequence_happy_path(manager):
    flow = _linear_flow([
        {'id': 'task1', 'type': 'task', 'data': NodeData(label='t1', code="ctx['x']=1")},
    ])
    eid = await manager.start_execution(flow)
    await asyncio.sleep(0.5)

    snap = manager.get_snapshot(eid)
    assert snap.status == sm.SUCCEEDED
    assert snap.variables.get('x') == 1

    events = manager.get_events(eid)
    transitions = [(e.fromState, e.toState) for e in events if e.eventType == 'transition']
    assert (sm.QUEUED, sm.RUNNING) in transitions
    assert transitions[-1] == (sm.RUNNING, sm.SUCCEEDED)

    seqs = [e.seq for e in events]
    assert seqs == sorted(seqs)
    assert len(seqs) == len(set(seqs))


@pytest.mark.asyncio
async def test_pause_race_non_interruptible(manager):
    flow = _linear_flow([
        {'id': 'slow_task', 'type': 'task',
         'data': NodeData(label='slow', code=_busy_wait_code(5000000))},
    ])
    eid = await manager.start_execution(flow)
    await asyncio.sleep(0.05)

    snap = manager.get_snapshot(eid)
    assert snap.status == sm.RUNNING

    await manager.control(eid, 'pause', request_id='pause-1')

    await asyncio.sleep(0.1)
    snap = manager.get_snapshot(eid)
    assert snap.status in (sm.PAUSING, sm.PAUSED), \
        f"Expected pausing or paused, got {snap.status}"

    await asyncio.sleep(2.0)
    snap = manager.get_snapshot(eid)
    assert snap.status == sm.PAUSED
    assert snap.variables.get('busy_done') is True, \
        "Node must complete before paused"

    events = manager.get_events(eid)
    statuses_seen = [e.toState for e in events if e.eventType == 'transition']
    assert sm.PAUSING in statuses_seen
    assert sm.PAUSED in statuses_seen
    pausing_idx = statuses_seen.index(sm.PAUSING)
    paused_idx = statuses_seen.index(sm.PAUSED)
    assert paused_idx > pausing_idx

    await manager.control(eid, 'resume', request_id='resume-1')
    await asyncio.sleep(0.5)
    snap = manager.get_snapshot(eid)
    assert snap.status == sm.SUCCEEDED


@pytest.mark.asyncio
async def test_duplicate_pause_request(manager):
    flow = _linear_flow([
        {'id': 't1', 'type': 'task',
         'data': NodeData(label='t1', code=_busy_wait_code(3000000))},
    ])
    eid = await manager.start_execution(flow)
    await asyncio.sleep(0.05)

    req_id = 'dup-pause-' + str(uuid.uuid4())
    await manager.control(eid, 'pause', request_id=req_id)
    snap2 = await manager.control(eid, 'pause', request_id=req_id)
    assert snap2.status in (sm.PAUSING, sm.PAUSED)

    await asyncio.sleep(2.0)
    snap = manager.get_snapshot(eid)
    assert snap.status == sm.PAUSED

    events = manager.get_events(eid)
    pause_transitions = [
        e for e in events
        if e.eventType == 'transition' and e.toState == sm.PAUSING
    ]
    assert len(pause_transitions) == 1


@pytest.mark.asyncio
async def test_duplicate_cancel_terminal(manager):
    flow = _linear_flow([
        {'id': 't1', 'type': 'task',
         'data': NodeData(label='t1', code="ctx['x']=1")},
    ])
    eid = await manager.start_execution(flow)
    await asyncio.sleep(0.5)
    snap = manager.get_snapshot(eid)
    assert snap.status == sm.SUCCEEDED

    req_id = 'cancel-1'
    await manager.control(eid, 'cancel', request_id=req_id)
    snap2 = await manager.control(eid, 'cancel', request_id=req_id)
    assert snap2.status == sm.SUCCEEDED

    events = manager.get_events(eid)
    cancelled = [e for e in events if e.toState == sm.CANCELLED]
    assert len(cancelled) == 0


@pytest.mark.asyncio
async def test_single_runner_guard(manager):
    flow = _linear_flow([
        {'id': 't1', 'type': 'task',
         'data': NodeData(label='t1', code=_busy_wait_code(10000000))},
    ])
    eid = await manager.start_execution(flow)
    await asyncio.sleep(0.05)

    await manager.control(eid, 'resume', request_id='resume-running')
    await asyncio.sleep(0.1)

    snap = manager.get_snapshot(eid)
    assert snap.status == sm.RUNNING
    await asyncio.sleep(8.0)
    snap = manager.get_snapshot(eid)
    assert snap.status == sm.SUCCEEDED


@pytest.mark.asyncio
async def test_failure_retry_http(manager, retry_http_server):
    flow = _linear_flow([
        {'id': 'retry_http', 'type': 'http',
         'data': NodeData(label='retry_http',
                          httpConfig=HttpConfig(
                              url=retry_http_server, method='GET',
                              headers={}, body='', timeout=5.0),
                          retry=RetryConfig(maxAttempts=3, delaySeconds=0.1,
                                            backoff='fixed', maxDelaySeconds=1.0))},
    ])
    eid = await manager.start_execution(flow)
    await asyncio.sleep(3.0)

    snap = manager.get_snapshot(eid)
    assert snap.status == sm.SUCCEEDED, f"Expected succeeded, got {snap.status}, err={snap.lastError}"
    assert _RetryHandler.attempt_counter == 3

    events = manager.get_events(eid)
    retry_waits = [e for e in events if e.toState == sm.RETRY_WAIT]
    assert len(retry_waits) == 2


@pytest.mark.asyncio
async def test_failure_retry_exhausted(manager):
    flow = _linear_flow([
        {'id': 'fail_node', 'type': 'task',
         'data': NodeData(label='fail', code="raise ValueError('always fail')",
                          retry=RetryConfig(maxAttempts=2, delaySeconds=0.05,
                                            backoff='fixed', maxDelaySeconds=1.0))},
    ])
    eid = await manager.start_execution(flow)
    await asyncio.sleep(1.5)

    snap = manager.get_snapshot(eid)
    assert snap.status == sm.FAILED
    assert snap.lastError is not None


@pytest.mark.asyncio
async def test_side_effect_idempotency_on_recovery(tmp_flow_dir):
    target_file = os.path.join(tempfile.gettempdir(),
                               f'idem_{uuid.uuid4().hex}.txt')

    store1 = EventStore(os.path.join(tmp_flow_dir, 'event_store'))
    mgr1 = ExecutionManager(store1)

    flow = _linear_flow([
        {'id': 'write1', 'type': 'file',
         'data': NodeData(label='write',
                          fileConfig=FileWriteConfig(path=target_file,
                                                     content='hello',
                                                     mode='write'))},
        {'id': 'slow1', 'type': 'task',
         'data': NodeData(label='slow', code=_busy_wait_code(20000000))},
    ])

    eid = await mgr1.start_execution(flow, execution_id='idem-test-exec')
    await asyncio.sleep(0.5)

    snap = mgr1.get_snapshot(eid)
    assert snap.status == sm.RUNNING
    assert os.path.exists(target_file)

    record = store1.load_record(eid)
    side_effect_keys = list(record.sideEffects.keys())
    assert len(side_effect_keys) == 1
    assert 'idem-test-exec:write1:1' in side_effect_keys[0]

    await mgr1.shutdown()
    await asyncio.sleep(0.3)

    store2 = EventStore(os.path.join(tmp_flow_dir, 'event_store'))
    mgr2 = ExecutionManager(store2)
    recovered = await mgr2.recover_all()
    assert recovered >= 1

    await asyncio.sleep(2.0)
    await mgr2.control(eid, 'cancel', request_id='cancel-after-recovery')
    await asyncio.sleep(0.5)

    with open(target_file, 'r') as f:
        content = f.read()
    assert content == 'hello'
    assert content.count('hello') == 1

    record2 = store2.load_record(eid)
    write_effects = [k for k in record2.sideEffects if 'write1' in k]
    assert len(write_effects) == 1

    try:
        os.unlink(target_file)
    except OSError:
        pass


@pytest.mark.asyncio
async def test_process_restart_resumes_from_checkpoint(tmp_flow_dir):
    store1 = EventStore(os.path.join(tmp_flow_dir, 'event_store'))
    mgr1 = ExecutionManager(store1)

    flow = _linear_flow([
        {'id': 't1', 'type': 'task',
         'data': NodeData(label='t1', code="ctx['a']=1")},
        {'id': 't2', 'type': 'task',
         'data': NodeData(label='t2', code="ctx['b']=2")},
        {'id': 't3', 'type': 'task',
         'data': NodeData(label='t3', code=_busy_wait_code(5000000))},
    ])

    eid = await mgr1.start_execution(flow, execution_id='restart-exec')
    await asyncio.sleep(0.5)

    snap1 = mgr1.get_snapshot(eid)
    assert snap1.status == sm.RUNNING
    assert 't1' in snap1.completedNodes
    assert 't2' in snap1.completedNodes
    assert snap1.variables.get('a') == 1
    assert snap1.variables.get('b') == 2
    assert snap1.resumeFromNodeId == 't3'

    await mgr1.shutdown()
    await asyncio.sleep(0.3)

    store2 = EventStore(os.path.join(tmp_flow_dir, 'event_store'))
    mgr2 = ExecutionManager(store2)
    await mgr2.recover_all()

    await asyncio.sleep(0.3)
    snap2 = mgr2.get_snapshot(eid)
    assert snap2.status == sm.RUNNING
    assert snap2.resumeFromNodeId == 't3'
    assert snap2.variables.get('a') == 1
    assert snap2.variables.get('b') == 2

    await mgr2.control(eid, 'cancel', request_id='cancel')
    await asyncio.sleep(5.0)
    snap3 = mgr2.get_snapshot(eid)
    assert snap3.status == sm.CANCELLED, f"Expected cancelled, got {snap3.status}"


@pytest.mark.asyncio
async def test_preserves_pause_after_restart(tmp_flow_dir):
    store1 = EventStore(os.path.join(tmp_flow_dir, 'event_store'))
    mgr1 = ExecutionManager(store1)

    flow = _linear_flow([
        {'id': 'slow_task', 'type': 'task',
         'data': NodeData(label='slow', code=_busy_wait_code(20000000))},
    ])

    eid = await mgr1.start_execution(flow, execution_id='pause-restart')
    await asyncio.sleep(0.05)
    await mgr1.control(eid, 'pause', request_id='p1')
    await asyncio.sleep(0.05)

    snap = mgr1.get_snapshot(eid)
    assert snap.status == sm.PAUSING, f"Expected pausing, got {snap.status}"

    await mgr1.shutdown()
    await asyncio.sleep(0.3)

    store2 = EventStore(os.path.join(tmp_flow_dir, 'event_store'))
    mgr2 = ExecutionManager(store2)
    await mgr2.recover_all()
    await asyncio.sleep(8.0)

    snap2 = mgr2.get_snapshot(eid)
    assert snap2.status == sm.PAUSED, f"Expected paused, got {snap2.status}"

    await mgr2.control(eid, 'cancel', request_id='c1')
    await asyncio.sleep(0.3)


@pytest.mark.asyncio
async def test_parallel_join_generation(manager):
    flow = FlowDefinition(
        id='par_flow', name='Parallel',
        nodes=[
            _node('start', 'start'),
            _node('parallel1', 'parallel', NodeData(
                label='par', parallelConfig=ParallelConfig(
                    branchNodeIds=['b1', 'b2']))),
            _node('b1', 'task', NodeData(label='b1',
                                         code="ctx['br1']='yes'",
                                         anchorId='anchor1')),
            _node('b2', 'task', NodeData(label='b2',
                                         code="ctx['br2']='yes'",
                                         anchorId='anchor1')),
            _node('end', 'end'),
        ],
        edges=[
            _edge('e1', 'start', 'parallel1'),
            _edge('e2', 'parallel1', 'end'),
        ],
        createdAt=0, updatedAt=0,
    )
    eid = await manager.start_execution(flow)
    await asyncio.sleep(3.0)

    snap = manager.get_snapshot(eid)
    assert snap.status == sm.SUCCEEDED
    assert snap.variables.get('parallel1_result') is not None
    branch_b1 = snap.variables.get('branch_b1', {})
    branch_b2 = snap.variables.get('branch_b2', {})
    assert branch_b1.get('br1') == 'yes', f"branch_b1={branch_b1}"
    assert branch_b2.get('br2') == 'yes', f"branch_b2={branch_b2}"

    events = manager.get_events(eid)
    join_events = [e for e in events if e.eventType == 'parallelJoin']
    assert len(join_events) >= 2
    generations = {e.generation for e in join_events if e.generation}
    assert len(generations) == 1
    gen = generations.pop()
    assert gen >= 1
    for e in join_events:
        assert e.generation == gen


@pytest.mark.asyncio
async def test_event_seq_monotonic_and_dedup(manager):
    flow = _linear_flow([
        {'id': 't1', 'type': 'task',
         'data': NodeData(label='t1', code="ctx['x']=1")},
    ])
    eid = await manager.start_execution(flow)
    await asyncio.sleep(0.5)

    events = manager.get_events(eid)
    seqs = [e.seq for e in events]
    assert seqs == list(range(1, len(seqs) + 1))

    after = manager.get_events(eid, after_seq=5)
    for e in after:
        assert e.seq > 5

    all_events = manager.get_events(eid, after_seq=0)
    assert len(all_events) == len(events)


@pytest.mark.asyncio
async def test_cancel_during_retry_wait(manager):
    flow = _linear_flow([
        {'id': 'fail', 'type': 'task',
         'data': NodeData(label='fail', code="raise ValueError('x')",
                          retry=RetryConfig(maxAttempts=5, delaySeconds=10,
                                            backoff='fixed', maxDelaySeconds=10))},
    ])
    eid = await manager.start_execution(flow)
    await asyncio.sleep(0.5)

    snap = manager.get_snapshot(eid)
    assert snap.status == sm.RETRY_WAIT

    await manager.control(eid, 'cancel', request_id='cancel-retry')
    await asyncio.sleep(0.5)

    snap = manager.get_snapshot(eid)
    assert snap.status == sm.CANCELLED


@pytest.mark.asyncio
async def test_allowed_actions_from_snapshot(manager):
    flow = _linear_flow([
        {'id': 't1', 'type': 'task',
         'data': NodeData(label='t1', code=_busy_wait_code(3000000))},
    ])
    eid = await manager.start_execution(flow)
    await asyncio.sleep(0.05)
    snap = manager.get_snapshot(eid)
    assert 'pause' in snap.allowedActions
    assert 'cancel' in snap.allowedActions
    assert 'resume' not in snap.allowedActions

    await manager.control(eid, 'pause', request_id='p1')
    await asyncio.sleep(2.0)
    snap = manager.get_snapshot(eid)
    assert snap.status == sm.PAUSED
    assert 'resume' in snap.allowedActions
    assert 'cancel' in snap.allowedActions
    assert 'step' in snap.allowedActions


@pytest.mark.asyncio
async def test_side_effect_count_in_events(manager):
    target = os.path.join(tempfile.gettempdir(),
                          f'sidefx_{uuid.uuid4().hex}.txt')
    flow = _linear_flow([
        {'id': 'fw', 'type': 'file',
         'data': NodeData(label='fw',
                          fileConfig=FileWriteConfig(path=target,
                                                     content='data',
                                                     mode='write'))},
    ])
    eid = await manager.start_execution(flow)
    await asyncio.sleep(0.5)

    snap = manager.get_snapshot(eid)
    assert snap.status == sm.SUCCEEDED

    events = manager.get_events(eid)
    side_effect_events = [e for e in events if e.eventType == 'sideEffect']
    assert len(side_effect_events) == 1
    assert side_effect_events[0].payload.get('reused') is False
    assert side_effect_events[0].idempotencyKey is not None

    try:
        os.unlink(target)
    except OSError:
        pass


@pytest.mark.asyncio
async def test_terminal_states_reject_commands(manager):
    flow = _linear_flow([
        {'id': 't1', 'type': 'task',
         'data': NodeData(label='t1', code="ctx['x']=1")},
    ])
    eid = await manager.start_execution(flow)
    await asyncio.sleep(0.5)
    snap = manager.get_snapshot(eid)
    assert snap.status == sm.SUCCEEDED

    result = await manager.control(eid, 'pause', request_id='p-terminal')
    assert result.status == sm.SUCCEEDED

    result2 = await manager.control(eid, 'cancel', request_id='c-terminal')
    assert result2.status == sm.SUCCEEDED

    events = manager.get_events(eid)
    extra_transitions = [
        e for e in events
        if e.eventType == 'transition' and e.fromState == sm.SUCCEEDED
    ]
    assert len(extra_transitions) == 0


class TestWebSocket:
    @pytest.fixture
    def server(self, tmp_flow_dir):
        import uvicorn
        from runtime import Runtime as RuntimeCls, set_runtime as sr

        runtime = RuntimeCls(flows_dir=tmp_flow_dir)
        sr(runtime)

        import main as main_module
        config = uvicorn.Config(main_module.app, host='127.0.0.1', port=0,
                                log_level='error')
        server = uvicorn.Server(config)

        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()

        while not server.started:
            time.sleep(0.05)

        port = None
        for srv in server.servers:
            for sock in srv.sockets:
                port = sock.getsockname()[1]
                break
            if port:
                break

        yield f'ws://127.0.0.1:{port}/ws/execute', runtime

        server.should_exit = True
        thread.join(timeout=5)
        sr(None)

    @pytest.mark.asyncio
    async def test_snapshot_then_incremental(self, server):
        uri, runtime = server
        mgr = runtime.manager

        flow = _linear_flow([
            {'id': 't1', 'type': 'task',
             'data': NodeData(label='t1', code="ctx['x']=42")},
        ])
        eid = await mgr.start_execution(flow)
        await asyncio.sleep(0.5)
        assert mgr.get_snapshot(eid).status == sm.SUCCEEDED

        async with websockets.connect(uri) as ws:
            await ws.send(json.dumps({
                'type': 'subscribe',
                'executionId': eid,
                'afterSeq': 0,
            }))
            raw = await asyncio.wait_for(ws.recv(), timeout=5)
            msg = json.loads(raw)
            assert msg['type'] == 'subscribed'
            assert msg['snapshot']['status'] == sm.SUCCEEDED
            assert msg['snapshot']['seq'] > 0
            assert msg['snapshot']['variables'].get('x') == 42

            events_after = []
            try:
                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                    m = json.loads(raw)
                    if m['type'] == 'event':
                        events_after.append(m['event'])
            except asyncio.TimeoutError:
                pass

            all_seqs = [msg['snapshot']['seq']] + [e['seq'] for e in events_after]
            assert all_seqs == sorted(all_seqs)

    @pytest.mark.asyncio
    async def test_late_joiner_gets_snapshot(self, server):
        uri, runtime = server
        mgr = runtime.manager

        flow = _linear_flow([
            {'id': 't1', 'type': 'task',
             'data': NodeData(label='t1', code="ctx['val']=99")},
        ])
        eid = await mgr.start_execution(flow)
        await asyncio.sleep(0.5)

        async with websockets.connect(uri) as ws:
            await ws.send(json.dumps({
                'type': 'subscribe', 'executionId': eid, 'afterSeq': 0,
            }))
            raw = await asyncio.wait_for(ws.recv(), timeout=5)
            msg = json.loads(raw)
            assert msg['type'] == 'subscribed'
            assert msg['snapshot']['variables'].get('val') == 99

    @pytest.mark.asyncio
    async def test_command_via_websocket(self, server):
        uri, runtime = server

        flow = _linear_flow([
            {'id': 'slow', 'type': 'task',
             'data': NodeData(label='slow', code=_busy_wait_code(10000000))},
        ])
        flow_dict = flow.model_dump()

        async with websockets.connect(uri) as ws:
            await ws.send(json.dumps({'type': 'execute', 'flow': flow_dict}))
            raw = await asyncio.wait_for(ws.recv(), timeout=5)
            sub = json.loads(raw)
            assert sub['type'] == 'subscribed'
            eid = sub['executionId']

            raw = await asyncio.wait_for(ws.recv(), timeout=2)
            cr = json.loads(raw)
            assert cr['type'] == 'commandResult'
            assert cr.get('accepted') is True

            await asyncio.sleep(0.3)
            req_id = 'ws-cancel-' + str(uuid.uuid4())
            await ws.send(json.dumps({
                'type': 'cancel', 'executionId': eid, 'requestId': req_id,
            }))

            try:
                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=3)
                    m = json.loads(raw)
                    if m['type'] == 'commandResult' and m.get('command') == 'cancel':
                        assert m.get('accepted') is True
                        break
            except asyncio.TimeoutError:
                pass

            await asyncio.sleep(0.5)
            snap = runtime.manager.get_snapshot(eid)
            assert snap.status == sm.CANCELLED


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
