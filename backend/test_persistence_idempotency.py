import asyncio
import json
import os
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
        if self.path.startswith("/slow"):
            time.sleep(server.slow_delay)
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
        self.slow_delay = 0.0


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
    closed = False
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


# ---- State machine transition tests ----

class TestStateMachine:
    def test_all_states_defined(self):
        assert sm.QUEUED == "queued"
        assert sm.RUNNING == "running"
        assert sm.PAUSING == "pausing"
        assert sm.PAUSED == "paused"
        assert sm.RETRY_WAIT == "retry_wait"
        assert sm.SUCCEEDED == "succeeded"
        assert sm.FAILED == "failed"
        assert sm.CANCELLED == "cancelled"

    def test_illegal_transitions_rejected(self, components):
        _, store, _, _ = components
        eid = store.create_execution("flow")
        with pytest.raises(sm.IllegalTransitionError):
            store.transition(eid, sm.SUCCEEDED, expected_states=[sm.QUEUED])
        with pytest.raises(sm.IllegalTransitionError):
            store.transition(eid, sm.PAUSED)
        assert store.snapshot(eid)["status"] == sm.QUEUED

    def test_legal_transition_sequence(self, components):
        _, store, _, _ = components
        eid = store.create_execution("flow")
        store.transition(eid, sm.RUNNING)
        store.transition(eid, sm.PAUSING)
        store.transition(eid, sm.PAUSED)
        store.transition(eid, sm.RUNNING)
        store.transition(eid, sm.SUCCEEDED)
        snap = store.snapshot(eid)
        assert snap["status"] == sm.SUCCEEDED
        transitions = state_events(store, eid)
        assert [t["to"] for t in transitions] == [sm.RUNNING, sm.PAUSING, sm.PAUSED, sm.RUNNING, sm.SUCCEEDED]
        assert [t["from"] for t in transitions] == [sm.QUEUED, sm.RUNNING, sm.PAUSING, sm.PAUSED, sm.RUNNING]
        for i in range(1, len(transitions)):
            assert transitions[i]["seq"] > transitions[i-1]["seq"]

    def test_terminal_states_have_no_outgoing(self, components):
        _, store, _, _ = components
        for terminal in sm.TERMINAL_STATES:
            eid = store.create_execution(f"flow-{terminal}")
            store.transition(eid, sm.RUNNING)
            if terminal == sm.SUCCEEDED:
                store.transition(eid, sm.SUCCEEDED)
            elif terminal == sm.FAILED:
                store.transition(eid, sm.FAILED)
            elif terminal == sm.CANCELLED:
                store.transition(eid, sm.CANCELLED)
            with pytest.raises(sm.IllegalTransitionError):
                store.transition(eid, sm.RUNNING)

    def test_event_seq_is_monotonic(self, components):
        _, store, _, _ = components
        eid = store.create_execution("flow")
        seqs = []
        _, seq, _ = store.transition(eid, sm.RUNNING)
        seqs.append(seq)
        _, seq, _ = store.transition(eid, sm.PAUSING)
        seqs.append(seq)
        _, seq, _ = store.transition(eid, sm.PAUSED)
        seqs.append(seq)
        _, seq, _ = store.transition(eid, sm.RUNNING)
        seqs.append(seq)
        _, seq, _ = store.transition(eid, sm.SUCCEEDED)
        seqs.append(seq)
        assert seqs == sorted(seqs)
        assert len(set(seqs)) == len(seqs)
        assert seqs[0] == 2
        assert seqs[-1] == 6

    def test_allowed_actions_drive_buttons(self, components):
        _, store, _, _ = components
        eid = store.create_execution("flow")
        snap = store.snapshot(eid)
        assert "start" in snap["allowedActions"]
        assert "cancel" in snap["allowedActions"]
        assert "pause" not in snap["allowedActions"]
        store.transition(eid, sm.RUNNING)
        snap = store.snapshot(eid)
        assert "pause" in snap["allowedActions"]
        assert "cancel" in snap["allowedActions"]
        assert "start" not in snap["allowedActions"]


# ---- Pause race tests ----

class TestPauseRace:
    @pytest.mark.asyncio
    async def test_pause_waits_for_node_boundary(self, components):
        _, store, manager, _ = components
        wait_node = node("wait", "wait", NodeData(label="wait", seconds=0.4))
        after_node = node("after", "task", NodeData(label="after", code="ctx['done']=True"))
        flow = linear_flow("pause-race", [node("start", "start"), wait_node, after_node, node("end", "end")])

        eid = await manager.start_execution(flow.id, flow=flow)
        await asyncio.sleep(0.05)
        accepted = await manager.send_command(eid, "pause", "cmd-pause")
        assert accepted is True
        await wait_status(store, eid, {sm.PAUSED}, 3)
        events = store.get_all_events(eid)
        checkpoints = [e for e in events if e["type"] == "node_checkpoint" and e["nodeId"] == "wait"]
        assert checkpoints, "pause must land only after wait checkpoint"
        transitions = state_events(store, eid)
        to_states = [t["to"] for t in transitions]
        assert to_states == [sm.RUNNING, sm.PAUSING, sm.PAUSED]
        assert transitions[0]["from"] == sm.QUEUED

        accepted = await manager.send_command(eid, "resume", "cmd-resume")
        assert accepted is True
        await wait_status(store, eid, {sm.SUCCEEDED}, 3)
        assert store.snapshot(eid)["variables"]["done"] is True

    @pytest.mark.asyncio
    async def test_duplicate_pause_does_not_start_second_executor(self, components):
        _, store, manager, _ = components
        wait_node = node("wait", "wait", NodeData(label="wait", seconds=0.3))
        flow = linear_flow("dup-pause", [node("start", "start"), wait_node, node("end", "end")])
        eid = await manager.start_execution(flow.id, flow=flow)
        await asyncio.sleep(0.05)
        results = await asyncio.gather(
            manager.send_command(eid, "pause", "pause-1"),
            manager.send_command(eid, "pause", "pause-2"),
            manager.send_command(eid, "pause", "pause-1"),
        )
        assert all(results)
        await wait_status(store, eid, {sm.PAUSED}, 3)
        running_events = [
            e for e in store.get_all_events(eid)
            if e["type"] == "state_transition" and e["toState"] == sm.RUNNING
        ]
        assert len(running_events) == 1


# ---- Process restart / recovery tests ----

class TestProcessRestart:
    @pytest.mark.asyncio
    async def test_recover_from_last_node_boundary(self, components, http_server):
        flow_store, store, manager, _ = components
        url = f"http://127.0.0.1:{http_server.server_address[1]}/restart"
        http_node = node("http", "http", NodeData(label="http", httpConfig={
            "url": url, "method": "GET", "headers": {}, "body": None, "timeout": 10.0,
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
    async def test_recover_running_execution_after_crash(self, components):
        flow_store, store, manager, data_dir = components
        wait_node = node("wait", "wait", NodeData(label="wait", seconds=10.0))
        flow = linear_flow("crash", [node("start", "start"), wait_node, node("end", "end")])
        eid = "exec-crash"
        await manager.start_execution(flow.id, flow=flow, execution_id=eid)
        await wait_status(store, eid, {sm.RUNNING}, 3)
        await manager.shutdown()

        row = store.get_execution_row(eid)
        assert row["status"] == sm.RUNNING

        manager2 = RuntimeManager(store, flow_store, manager.bus)
        try:
            recovered = await manager2.recover_all()
            assert recovered == 1
            await asyncio.sleep(0.2)
            row = store.get_execution_row(eid)
            assert row["status"] == sm.RUNNING
            await manager2.send_command(eid, "cancel", "cancel-crash")
            await wait_status(store, eid, {sm.CANCELLED}, 3)
        finally:
            await manager2.shutdown()
            store.close()


# ---- Parallel join tests ----

class TestParallelJoin:
    @pytest.mark.asyncio
    async def test_parallel_branches_same_generation(self, components):
        _, store, manager, _ = components
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
    async def test_branch_retry_does_not_consume_prior_generation(self, components):
        _, store, manager, _ = components
        b1 = node("b1", "task", NodeData(label="b1", code="ctx['b1']=1", anchorId="anchor"))
        b2 = node("b2", "http", NodeData(label="b2", httpConfig={
            "url": "http://127.0.0.1:9/fail", "method": "GET", "headers": {}, "body": None, "timeout": 2.0,
        }, anchorId="anchor"))
        parallel = node("parallel", "parallel", NodeData(
            label="parallel",
            parallelConfig={"branchNodeIds": ["b1", "b2"]},
        ))
        flow = FlowDefinition(
            id="parallel-retry-gen",
            name="parallel",
            nodes=[node("start", "start"), parallel, node("end", "end"), b1, b2],
            edges=[edge("e1", "start", "parallel"), edge("e2", "parallel", "end")],
            createdAt=1,
            updatedAt=1,
        )
        eid = await start_and_wait(manager, store, flow, timeout=10.0)
        snap = store.snapshot(eid)
        assert snap["status"] == sm.FAILED
        old_branches = store.get_branches_for_generation(eid, "parallel", 0)
        assert any(b["status"] == "failed" for b in old_branches)


# ---- Failure retry tests ----

class TestFailureRetry:
    @pytest.mark.asyncio
    async def test_retry_uses_new_generation(self, components, http_server):
        http_server.fail_until = 1
        _, store, manager, _ = components
        url = f"http://127.0.0.1:{http_server.server_address[1]}/fail-first"
        http_node = node("http", "http", NodeData(label="http", httpConfig={
            "url": url, "method": "GET", "headers": {}, "body": None, "timeout": 10.0,
        }))
        flow = linear_flow("retry-gen", [node("start", "start"), http_node, node("end", "end")])
        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_status(store, eid, {sm.FAILED}, 5)
        assert http_server.hits == 1

        accepted = await manager.send_command(eid, "retry", "cmd-retry")
        assert accepted is True
        await wait_status(store, eid, {sm.SUCCEEDED}, 5)
        snap = store.snapshot(eid)
        assert snap["generation"] == 1
        assert snap["variables"]["http_result"]["status_code"] == 200
        assert http_server.hits == 2
        assert store.count_side_effect_calls(eid, "http", "http") == 2

    @pytest.mark.asyncio
    async def test_node_level_retry_transitions_retry_wait(self, components):
        _, store, manager, _ = components
        task_node = node("task", "task", NodeData(
            label="flaky",
            code="ctx['n']=ctx.get('n',0)+1\nraise ValueError('fail')",
            retry={"maxAttempts": 2, "delaySeconds": 0.1, "backoff": "fixed", "maxDelaySeconds": 1.0},
        ))
        flow = linear_flow("retry-wait", [node("start", "start"), task_node, node("end", "end")])
        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_status(store, eid, {sm.FAILED}, 5)
        events = store.get_all_events(eid)
        retry_wait_events = [e for e in events if e.get("toState") == sm.RETRY_WAIT]
        assert len(retry_wait_events) >= 1


# ---- Duplicate command tests ----

class TestDuplicateCommands:
    @pytest.mark.asyncio
    async def test_duplicate_start_commands_no_second_executor(self, components):
        _, store, manager, _ = components
        task_node = node("task", "task", NodeData(label="task", code="ctx['x']=1"))
        flow = linear_flow("dup", [node("start", "start"), task_node, node("end", "end")])
        flow_store = manager.flow_store
        flow_store.create_flow(flow)
        eid = store.create_execution(flow.id)
        results = await asyncio.gather(
            manager.send_command(eid, "start", "dup-cmd-1"),
            manager.send_command(eid, "start", "dup-cmd-1"),
            manager.send_command(eid, "resume", "dup-cmd-1"),
        )
        assert all(r is True for r in results)
        await wait_status(store, eid, {sm.SUCCEEDED}, 3)
        running_events = [
            e for e in store.get_all_events(eid)
            if e["type"] == "state_transition" and e["toState"] == sm.RUNNING
        ]
        assert len(running_events) == 1

    @pytest.mark.asyncio
    async def test_old_cancel_request_does_not_stop_new_executor(self, components):
        _, store, manager, _ = components
        task_node = node("task", "task", NodeData(label="task", code="ctx['x']=1"))
        flow = linear_flow("old-cancel", [node("start", "start"), task_node, node("end", "end")])
        eid = await start_and_wait(manager, store, flow)
        assert store.snapshot(eid)["status"] == sm.SUCCEEDED

    @pytest.mark.asyncio
    async def test_cancel_prevents_resume(self, components):
        _, store, manager, _ = components
        wait_node = node("wait", "wait", NodeData(label="wait", seconds=0.3))
        flow = linear_flow("cancel", [node("start", "start"), wait_node, node("end", "end")])
        eid = await manager.start_execution(flow.id, flow=flow)
        await asyncio.sleep(0.05)
        accepted = await manager.send_command(eid, "cancel", "cmd-cancel")
        assert accepted is True
        await wait_status(store, eid, {sm.CANCELLED}, 3)
        accepted = await manager.send_command(eid, "resume", "cmd-resume")
        assert accepted is False


# ---- Side effect idempotency tests ----

class TestSideEffectIdempotency:
    @pytest.mark.asyncio
    async def test_http_idempotent_after_recovery(self, components, http_server):
        flow_store, store, manager, _ = components
        url = f"http://127.0.0.1:{http_server.server_address[1]}/idempotent"
        http_node = node("http", "http", NodeData(label="http", httpConfig={
            "url": url, "method": "GET", "headers": {}, "body": None, "timeout": 10.0,
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
        assert side_effect is not None
        assert side_effect["status"] == "completed"
        assert side_effect["idempotency_key"] == f"{eid}:http:1"

    @pytest.mark.asyncio
    async def test_file_write_idempotent_after_recovery(self, components):
        flow_store, store, manager, data_dir = components
        file_path = os.path.join(data_dir, "output.txt")
        fw_node = node("fw", "file_write", NodeData(label="write", fileWriteConfig={
            "path": file_path, "content": "hello-idempotent", "mode": "w",
        }))
        flow = linear_flow("file-idem", [node("start", "start"), fw_node, node("end", "end")])
        eid = "exec-file-idem"
        await manager.start_execution(flow.id, flow=flow, execution_id=eid)
        await wait_status(store, eid, {sm.SUCCEEDED}, 5)
        await manager.shutdown()

        assert os.path.exists(file_path)
        with open(file_path, "r") as f:
            assert f.read() == "hello-idempotent"

        manager2 = RuntimeManager(store, flow_store, manager.bus)
        await manager2.recover_all()
        await manager2.shutdown()
        assert store.count_side_effect_calls(eid, "fw", "file_write") == 1
        side_effect = store.get_side_effect(eid, "fw", 1)
        assert side_effect is not None
        assert side_effect["status"] == "completed"

    @pytest.mark.asyncio
    async def test_idempotency_key_format(self, components):
        _, store, _, _ = components
        key = store.idempotency_key("exec-1", "node-1", 3)
        assert key == "exec-1:node-1:3"


# ---- WebSocket replay tests ----

class TestWebSocketReplay:
    def test_snapshot_then_incremental_deltas(self, app_client: TestClient, http_server):
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
            execution_id = None
            deadline = time.time() + 5
            while time.time() < deadline:
                msg = ws.receive_json()
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
            assert "retry" in snapshot_msg["snapshot"]["allowedActions"]
            assert event_seqs == list(range(event_seqs[0], event_seqs[-1] + 1))

    def test_late_joiner_gets_snapshot_then_deltas(self, app_client: TestClient):
        flow = {
            "id": "late-flow",
            "name": "late",
            "createdAt": 1,
            "updatedAt": 1,
            "nodes": [
                {"id": "start", "type": "start", "position": {"x": 0, "y": 0}, "data": {"label": "start"}},
                {"id": "task1", "type": "task", "position": {"x": 0, "y": 0}, "data": {"label": "task", "code": "ctx['x']=42"}},
                {"id": "end", "type": "end", "position": {"x": 0, "y": 0}, "data": {"label": "end"}},
            ],
            "edges": [{"id": "e1", "source": "start", "target": "task1"}, {"id": "e2", "source": "task1", "target": "end"}],
        }
        with app_client.websocket_connect("/ws/execute") as ws:
            ws.send_json({"type": "execute", "flow": flow, "commandId": "late-exec"})
            execution_id = None
            deadline = time.time() + 5
            while time.time() < deadline:
                msg = ws.receive_json()
                if msg.get("type") == "snapshot":
                    execution_id = msg["snapshot"]["executionId"]
                    if msg["snapshot"]["status"] == sm.SUCCEEDED:
                        break
            assert execution_id

        with app_client.websocket_connect("/ws/execute") as ws2:
            ws2.send_json({"type": "subscribe", "executionId": execution_id, "sinceSeq": 0})
            got_snapshot = False
            event_seqs: List[int] = []
            latest_seq = 0
            deadline = time.time() + 2
            while time.time() < deadline:
                msg = ws2.receive_json()
                if msg.get("type") == "snapshot":
                    got_snapshot = True
                    latest_seq = msg["snapshot"]["latestSeq"]
                    assert msg["snapshot"]["status"] == sm.SUCCEEDED
                    assert msg["snapshot"]["variables"]["x"] == 42
                if msg.get("type") == "event":
                    event_seqs.append(msg["event"]["seq"])
                if got_snapshot and event_seqs and event_seqs[-1] >= latest_seq:
                    break
            assert got_snapshot
            assert event_seqs == list(range(event_seqs[0], event_seqs[-1] + 1))


# ---- Full state event sequence assertions ----

class TestStateEventSequences:
    @pytest.mark.asyncio
    async def test_successful_execution_event_sequence(self, components):
        _, store, manager, _ = components
        task_node = node("task", "task", NodeData(label="task", code="ctx['x']=1"))
        flow = linear_flow("seq", [node("start", "start"), task_node, node("end", "end")])
        eid = await start_and_wait(manager, store, flow)
        transitions = state_events(store, eid)
        to_states = [t["to"] for t in transitions]
        assert to_states[0] == sm.RUNNING
        assert transitions[0]["from"] == sm.QUEUED
        assert to_states[-1] == sm.SUCCEEDED
        for i in range(1, len(transitions)):
            assert transitions[i]["seq"] > transitions[i-1]["seq"]

    @pytest.mark.asyncio
    async def test_cancelled_execution_event_sequence(self, components):
        _, store, manager, _ = components
        wait_node = node("wait", "wait", NodeData(label="wait", seconds=1.0))
        flow = linear_flow("cancel-seq", [node("start", "start"), wait_node, node("end", "end")])
        eid = await manager.start_execution(flow.id, flow=flow)
        await asyncio.sleep(0.05)
        await manager.send_command(eid, "cancel", "cancel-seq-cmd")
        await wait_status(store, eid, {sm.CANCELLED}, 3)
        transitions = state_events(store, eid)
        to_states = [t["to"] for t in transitions]
        assert to_states[-1] == sm.CANCELLED
        assert sm.RUNNING in to_states
