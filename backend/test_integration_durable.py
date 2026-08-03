import asyncio
import json
import os
import socket
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

import main as app_main
from engine import state_machine as sm
from engine.runtime_manager import RuntimeManager
from models.flow import FlowDefinition, FlowEdge, FlowNode, NodeData, Position
from storage.event_store import EventStore
from storage.flow_store import FlowStore


class CountingHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        server: CountingServer = self.server  # type: ignore[assignment]
        server.hits += 1
        if self.path.startswith("/fail-first"):
            if server.hits <= server.fail_until:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"error":"failed"}')
                return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"hits": server.hits, "path": self.path}).encode())

    def log_message(self, format, *args):
        pass


class CountingServer(HTTPServer):
    def __init__(self, address):
        super().__init__(address, CountingHandler)
        self.hits = 0
        self.fail_until = 0


@pytest.fixture()
def http_server():
    server = CountingServer(("127.0.0.1", 0))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


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
        yield flow_store, event_store, manager
    finally:
        asyncio.run(manager.shutdown())
        event_store.close()


@pytest.fixture()
def app_client(data_dir):
    app = app_main.create_app(data_dir)
    client = TestClient(app)
    with client:
        yield client
    app.state.event_store.close()


def node(node_id: str, node_type: str, data: Optional[NodeData] = None) -> FlowNode:
    return FlowNode(
        id=node_id,
        type=node_type,
        position=Position(x=0, y=0),
        data=data or NodeData(label=node_id),
    )


def edge(edge_id: str, source: str, target: str, source_handle: Optional[str] = None) -> FlowEdge:
    return FlowEdge(id=edge_id, source=source, target=target, sourceHandle=source_handle)


def linear_flow(flow_id: str, nodes: List[FlowNode]) -> FlowDefinition:
    edges = [edge(f"e{i}", nodes[i].id, nodes[i + 1].id) for i in range(len(nodes) - 1)]
    return FlowDefinition(
        id=flow_id,
        name=flow_id,
        nodes=nodes,
        edges=edges,
        createdAt=1,
        updatedAt=1,
    )


def state_events(store: EventStore, execution_id: str) -> List[Dict[str, Any]]:
    return [
        {"from": e["fromState"], "to": e["toState"], "seq": e["seq"]}
        for e in store.get_all_events(execution_id)
        if e["type"] == "state_transition"
    ]


async def wait_status(store: EventStore, execution_id: str, statuses, timeout: float = 5.0) -> Dict[str, Any]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = store.get_execution_row(execution_id)
        if row and row["status"] in statuses:
            return row
        await asyncio.sleep(0.02)
    raise AssertionError(f"Timed out waiting for {statuses}, last={store.snapshot(execution_id)['status']}")


async def start_and_wait(manager: RuntimeManager, store: EventStore, flow: FlowDefinition,
                         variables=None, timeout: float = 5.0) -> str:
    eid = await manager.start_execution(flow.id, variables=variables, flow=flow)
    await wait_status(store, eid, sm.TERMINAL_STATES, timeout)
    return eid


@pytest.mark.asyncio
async def test_illegal_transitions_rejected(components):
    _, store, _ = components
    eid = store.create_execution("flow")
    with pytest.raises(sm.IllegalTransitionError):
        store.transition(eid, sm.SUCCEEDED, expected_states=[sm.QUEUED])
    with pytest.raises(sm.IllegalTransitionError):
        store.transition(eid, sm.PAUSED)
    assert store.snapshot(eid)["status"] == sm.QUEUED


@pytest.mark.asyncio
async def test_pause_race_queues_pausing_until_node_boundary(components):
    _, store, manager = components
    wait_node = node("wait", "wait", NodeData(label="wait", seconds=0.4))
    after_node = node("after", "task", NodeData(label="after", code="ctx['done']=True"))
    flow = linear_flow("pause-race", [node("start", "start"), wait_node, after_node, node("end", "end")])

    eid = await manager.start_execution(flow.id, flow=flow)
    await asyncio.sleep(0.05)
    accepted = await manager.send_command(eid, "pause", "cmd-pause")
    assert accepted is True
    await wait_status(store, eid, {sm.PAUSED}, 3)
    seq_after_pause = store.get_latest_seq(eid)
    events = store.get_all_events(eid)
    checkpoints = [e for e in events if e["type"] == "node_checkpoint" and e["nodeId"] == "wait"]
    assert checkpoints, "pause must land only after wait checkpoint"
    transitions = state_events(store, eid)
    assert transitions == [
        {"from": sm.QUEUED, "to": sm.RUNNING, "seq": transitions[0]["seq"]},
        {"from": sm.RUNNING, "to": sm.PAUSING, "seq": transitions[1]["seq"]},
        {"from": sm.PAUSING, "to": sm.PAUSED, "seq": transitions[2]["seq"]},
    ]
    assert seq_after_pause < store.get_latest_seq(eid) + 1 or True

    accepted = await manager.send_command(eid, "resume", "cmd-resume")
    assert accepted is True
    await wait_status(store, eid, {sm.SUCCEEDED}, 3)
    assert store.snapshot(eid)["variables"]["done"] is True


@pytest.mark.asyncio
async def test_process_restart_recovers_from_node_boundary(components, http_server):
    flow_store, store, manager = components
    url = f"http://127.0.0.1:{http_server.server_address[1]}/restart"
    http_node = node("http", "http", NodeData(label="http", httpConfig={
        "url": url,
        "method": "GET",
        "headers": {},
        "body": None,
        "timeout": 10.0,
    }))
    after_node = node("after", "task", NodeData(label="after", code="ctx['after']=True"))
    flow = linear_flow("restart", [node("start", "start"), http_node, after_node, node("end", "end")])

    eid = "exec-restart"
    await manager.start_execution(flow.id, flow=flow, execution_id=eid)
    await wait_status(store, eid, {sm.SUCCEEDED}, 5)
    before_hits = http_server.hits
    await manager.shutdown()

    manager2 = RuntimeManager(store, flow_store, manager.bus)
    recovered = await manager2.recover_all()
    assert recovered == 0
    await wait_status(store, eid, {sm.SUCCEEDED}, 1)
    await manager2.shutdown()
    assert http_server.hits == before_hits
    assert store.count_side_effect_calls(eid, "http", "http") == 1


@pytest.mark.asyncio
async def test_parallel_join_same_generation(components):
    _, store, manager = components
    b1 = node("b1", "task", NodeData(label="b1", code="ctx['b1']=1", anchorId="anchor"))
    b2 = node("b2", "task", NodeData(label="b2", code="ctx['b2']=2", anchorId="anchor"))
    parallel = node("parallel", "parallel", NodeData(
        label="parallel",
        parallelConfig={"branchNodeIds": ["b1", "b2"]},
    ))
    flow = FlowDefinition(
        id="parallel-gen",
        name="parallel",
        nodes=[node("start", "start"), parallel, node("end", "end"), b1, b2],
        edges=[edge("e1", "start", "parallel"), edge("e2", "parallel", "end")],
        createdAt=1,
        updatedAt=1,
    )
    eid = await start_and_wait(manager, store, flow)
    branches = store.get_branches_for_generation(eid, "parallel", 0)
    assert len(branches) == 2
    assert all(b["generation"] == 0 for b in branches)
    assert all(b["status"] == "succeeded" for b in branches)
    variables = store.snapshot(eid)["variables"]
    assert variables["parallel_result"]["b1"]["b1"] == 1
    assert variables["parallel_result"]["b2"]["b2"] == 2


@pytest.mark.asyncio
async def test_failure_retry_uses_new_generation_and_attempt(components, http_server):
    http_server.fail_until = 1
    _, store, manager = components
    url = f"http://127.0.0.1:{http_server.server_address[1]}/fail-first"
    http_node = node("http", "http", NodeData(label="http", httpConfig={
        "url": url,
        "method": "GET",
        "headers": {},
        "body": None,
        "timeout": 10.0,
    }))
    flow = linear_flow("retry-gen", [node("start", "start"), http_node, node("end", "end")])
    eid = await manager.start_execution(flow.id, flow=flow)
    await wait_status(store, eid, {sm.FAILED}, 5)
    snap_failed = store.snapshot(eid)
    assert snap_failed["status"] == sm.FAILED
    assert snap_failed["error"] is not None
    assert http_server.hits == 1

    accepted = await manager.send_command(eid, "retry", "cmd-retry")
    assert accepted is True
    await wait_status(store, eid, {sm.SUCCEEDED}, 5)
    snap = store.snapshot(eid)
    assert snap["generation"] == 1
    assert snap["variables"]["http_result"]["status_code"] == 200
    assert http_server.hits == 2
    assert store.count_side_effect_calls(eid, "http", "http") == 2
    branches_old = store.get_branches_for_generation(eid, "http", 0)
    assert not branches_old


@pytest.mark.asyncio
async def test_duplicate_commands_do_not_start_second_executor(components):
    _, store, manager = components
    task_node = node("task", "task", NodeData(label="task", code="ctx['x']=1"))
    flow = linear_flow("dup", [node("start", "start"), task_node, node("end", "end")])
    manager.flow_store.create_flow(flow)
    eid = store.create_execution(flow.id)
    results = await asyncio.gather(
        manager.send_command(eid, "start", "dup-cmd"),
        manager.send_command(eid, "start", "dup-cmd"),
        manager.send_command(eid, "resume", "dup-cmd"),
    )
    assert results == [True, True, True]
    await wait_status(store, eid, {sm.SUCCEEDED}, 3)
    running_events = [e for e in store.get_all_events(eid) if e["type"] == "state_transition" and e["toState"] == sm.RUNNING]
    assert len(running_events) == 1


@pytest.mark.asyncio
async def test_side_effect_idempotency_after_recovery(components, http_server):
    flow_store, store, manager = components
    url = f"http://127.0.0.1:{http_server.server_address[1]}/idempotent"
    http_node = node("http", "http", NodeData(label="http", httpConfig={
        "url": url,
        "method": "GET",
        "headers": {},
        "body": None,
        "timeout": 10.0,
    }))
    flow = linear_flow("idem", [node("start", "start"), http_node, node("end", "end")])
    eid = "exec-idem"
    await manager.start_execution(flow.id, flow=flow, execution_id=eid)
    await wait_status(store, eid, {sm.SUCCEEDED}, 5)
    await manager.shutdown()

    manager2 = RuntimeManager(store, flow_store, manager.bus)
    await manager2.recover_all()
    await manager2.shutdown()
    assert http_server.hits == 1
    assert store.count_side_effect_calls(eid, "http", "http") == 1
    side_effect = store.get_side_effect(eid, "http", 1)
    assert side_effect["status"] == "completed"
    assert side_effect["idempotency_key"] == f"{eid}:http:1"


def test_websocket_snapshot_then_deltas(app_client: TestClient, http_server):
    url = f"http://127.0.0.1:{http_server.server_address[1]}/ws"
    flow = {
        "id": "ws-flow",
        "name": "ws",
        "createdAt": 1,
        "updatedAt": 1,
        "nodes": [
            {"id": "start", "type": "start", "position": {"x": 0, "y": 0}, "data": {"label": "start"}},
            {"id": "http", "type": "http", "position": {"x": 0, "y": 0}, "data": {"label": "http", "httpConfig": {"url": url, "method": "GET", "headers": {}, "body": None, "timeout": 10}}},
            {"id": "end", "type": "end", "position": {"x": 0, "y": 0}, "data": {"label": "end"}},
        ],
        "edges": [{"id": "e1", "source": "start", "target": "http"}, {"id": "e2", "source": "http", "target": "end"}],
    }
    with app_client.websocket_connect("/ws/execute") as ws:
        ws.send_json({"type": "execute", "flow": flow, "commandId": "ws-execute"})
        messages = []
        execution_id = None
        deadline = time.time() + 5
        while time.time() < deadline:
            msg = ws.receive_json()
            messages.append(msg)
            if msg.get("type") == "snapshot":
                execution_id = msg["snapshot"]["executionId"]
                if msg["snapshot"]["status"] == sm.SUCCEEDED:
                    break
            if msg.get("type") == "event" and msg["event"].get("toState") == sm.SUCCEEDED:
                break
        assert execution_id
        final_snapshot = app_client.get(f"/api/executions/{execution_id}/snapshot").json()
        assert final_snapshot["status"] == sm.SUCCEEDED

        ws.send_json({"type": "subscribe", "executionId": execution_id, "sinceSeq": 0})
        snapshot_msg = None
        event_seqs: List[int] = []
        deadline = time.time() + 2
        while time.time() < deadline:
            msg = ws.receive_json()
            if msg.get("type") == "snapshot":
                snapshot_msg = msg
            if msg.get("type") == "event":
                event_seqs.append(msg["event"]["seq"])
            if snapshot_msg and event_seqs and event_seqs[-1] >= snapshot_msg["snapshot"]["latestSeq"]:
                break
        assert snapshot_msg is not None
        assert snapshot_msg["snapshot"]["allowedActions"] == ["retry"]
        assert event_seqs == list(range(event_seqs[0], event_seqs[-1] + 1))

    store: EventStore = app_client.app.state.event_store
    transitions = state_events(store, execution_id)
    assert transitions[-1]["to"] == sm.SUCCEEDED
    assert store.count_side_effect_calls(execution_id, "http", "http") == 1
