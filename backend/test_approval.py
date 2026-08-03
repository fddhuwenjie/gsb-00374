import asyncio
import json
import os
import tempfile
import time
from typing import Any, Dict, List, Optional

import pytest

from engine import state_machine as sm
from engine.runtime_manager import FlowVersionMissingError, RuntimeManager
from models.flow import (
    ApprovalConfig,
    FlowDefinition,
    FlowEdge,
    FlowNode,
    NodeData,
    Position,
)
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


def n(node_id: str, ntype: str = "task", **kwargs) -> FlowNode:
    data_kwargs: Dict[str, Any] = {"label": kwargs.pop("label", node_id)}
    data_kwargs.update(kwargs)
    return FlowNode(
        id=node_id, type=ntype, position=Position(x=0, y=0),
        data=NodeData(**data_kwargs),
    )


def approval_node(node_id: str, prompt: str = "Approve?",
                  timeout_seconds: float = 3600,
                  label: Optional[str] = None) -> FlowNode:
    return FlowNode(
        id=node_id, type="approval", position=Position(x=0, y=0),
        data=NodeData(
            label=label or node_id,
            approvalConfig=ApprovalConfig(
                prompt=prompt, timeoutSeconds=timeout_seconds,
            ),
        ),
    )


def e(eid: str, src: str, tgt: str) -> FlowEdge:
    return FlowEdge(id=eid, source=src, target=tgt)


def make_flow(flow_id: str, nodes: List[FlowNode], edges: List[FlowEdge]) -> FlowDefinition:
    return FlowDefinition(
        id=flow_id, name=flow_id, nodes=nodes, edges=edges,
        createdAt=1, updatedAt=1,
    )


async def wait_for_status(store: EventStore, eid: str, statuses, timeout: float = 5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = store.get_execution_row(eid)
        if row and row["status"] in statuses:
            return row
        await asyncio.sleep(0.02)
    raise AssertionError(f"timeout waiting for {statuses}, last={store.snapshot(eid)['status']}")


def state_transitions(store: EventStore, eid: str) -> List[Dict[str, Any]]:
    return [
        {"from": ev["fromState"], "to": ev["toState"], "seq": ev["seq"]}
        for ev in store.get_all_events(eid)
        if ev["type"] == "state_transition"
    ]


# ---- State machine ----

class TestApprovalStateMachine:
    def test_awaiting_approval_defined(self):
        assert sm.AWAITING_APPROVAL == "awaiting_approval"

    def test_running_can_transition_to_awaiting(self, components):
        _, store, _, _ = components
        eid = store.create_execution("f")
        store.transition(eid, sm.RUNNING)
        _, seq, _ = store.transition(eid, sm.AWAITING_APPROVAL)
        assert store.snapshot(eid)["status"] == sm.AWAITING_APPROVAL

    def test_awaiting_can_approve_to_running(self, components):
        _, store, _, _ = components
        eid = store.create_execution("f")
        store.transition(eid, sm.RUNNING)
        store.transition(eid, sm.AWAITING_APPROVAL)
        store.transition(eid, sm.RUNNING, expected_states=[sm.AWAITING_APPROVAL])
        assert store.snapshot(eid)["status"] == sm.RUNNING

    def test_awaiting_can_reject_to_failed(self, components):
        _, store, _, _ = components
        eid = store.create_execution("f")
        store.transition(eid, sm.RUNNING)
        store.transition(eid, sm.AWAITING_APPROVAL)
        store.transition(eid, sm.FAILED, expected_states=[sm.AWAITING_APPROVAL])
        assert store.snapshot(eid)["status"] == sm.FAILED

    def test_awaiting_can_cancel(self, components):
        _, store, _, _ = components
        eid = store.create_execution("f")
        store.transition(eid, sm.RUNNING)
        store.transition(eid, sm.AWAITING_APPROVAL)
        store.transition(eid, sm.CANCELLED, expected_states=[sm.AWAITING_APPROVAL])
        assert store.snapshot(eid)["status"] == sm.CANCELLED

    def test_illegal_transition_from_awaiting(self, components):
        _, store, _, _ = components
        eid = store.create_execution("f")
        store.transition(eid, sm.RUNNING)
        store.transition(eid, sm.AWAITING_APPROVAL)
        with pytest.raises(sm.IllegalTransitionError):
            store.transition(eid, sm.QUEUED)

    def test_allowed_actions_include_approve_reject(self, components):
        _, store, _, _ = components
        eid = store.create_execution("f")
        store.transition(eid, sm.RUNNING)
        store.transition(eid, sm.AWAITING_APPROVAL)
        actions = store.snapshot(eid)["allowedActions"]
        assert "approve" in actions
        assert "reject" in actions
        assert "cancel" in actions


# ---- Basic approval flow ----

class TestBasicApproval:
    @pytest.mark.asyncio
    async def test_approve_continues_execution(self, components):
        _, store, manager, _ = components
        flow = make_flow("appr1", [
            n("start", "start"),
            approval_node("ap1", prompt="Continue?"),
            n("after", "task", code="ctx['approved']=True"),
            n("end", "end"),
        ], [e("e1", "start", "ap1"), e("e2", "ap1", "after"), e("e3", "after", "end")])

        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_for_status(store, eid, {sm.AWAITING_APPROVAL})
        snap = store.snapshot(eid)
        assert snap["pendingApproval"] is not None
        assert snap["pendingApproval"]["prompt"] == "Continue?"
        token = snap["pendingApproval"]["token"]

        accepted = await manager.send_command(eid, "approve", token=token, approver="alice")
        assert accepted is True

        await wait_for_status(store, eid, sm.TERMINAL_STATES, timeout=5)
        snap = store.snapshot(eid)
        assert snap["status"] == sm.SUCCEEDED
        assert snap["variables"]["approved"] is True
        assert snap["variables"]["ap1_result"]["approved"] is True
        assert snap["variables"]["ap1_result"]["approver"] == "alice"

    @pytest.mark.asyncio
    async def test_reject_fails_execution(self, components):
        _, store, manager, _ = components
        flow = make_flow("appr2", [
            n("start", "start"),
            approval_node("ap1", prompt="Continue?"),
            n("after", "task", code="ctx['should_not_run']=True"),
            n("end", "end"),
        ], [e("e1", "start", "ap1"), e("e2", "ap1", "after"), e("e3", "after", "end")])

        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_for_status(store, eid, {sm.AWAITING_APPROVAL})
        token = store.snapshot(eid)["pendingApproval"]["token"]

        accepted = await manager.send_command(eid, "reject", token=token, approver="bob", comment="No")
        assert accepted is True

        await wait_for_status(store, eid, sm.TERMINAL_STATES, timeout=5)
        snap = store.snapshot(eid)
        assert snap["status"] == sm.FAILED
        assert "should_not_run" not in snap["variables"]
        assert snap["variables"]["ap1_result"]["approved"] is False

    @pytest.mark.asyncio
    async def test_approval_state_sequence(self, components):
        _, store, manager, _ = components
        flow = make_flow("appr-seq", [
            n("start", "start"),
            approval_node("ap1"),
            n("end", "end"),
        ], [e("e1", "start", "ap1"), e("e2", "ap1", "end")])

        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_for_status(store, eid, {sm.AWAITING_APPROVAL})
        token = store.snapshot(eid)["pendingApproval"]["token"]
        await manager.send_command(eid, "approve", token=token)
        await wait_for_status(store, eid, sm.TERMINAL_STATES)

        transitions = state_transitions(store, eid)
        to_states = [t["to"] for t in transitions]
        assert sm.RUNNING in to_states
        assert sm.AWAITING_APPROVAL in to_states
        assert to_states[-1] == sm.SUCCEEDED
        await_idx = to_states.index(sm.AWAITING_APPROVAL)
        assert to_states[await_idx - 1] == sm.RUNNING
        assert to_states[await_idx + 1] == sm.RUNNING


# ---- Multi-approver race ----

class TestMultiApproverRace:
    @pytest.mark.asyncio
    async def test_duplicate_approve_second_is_ignored(self, components):
        _, store, manager, _ = components
        flow = make_flow("race1", [
            n("start", "start"),
            approval_node("ap1"),
            n("end", "end"),
        ], [e("e1", "start", "ap1"), e("e2", "ap1", "end")])

        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_for_status(store, eid, {sm.AWAITING_APPROVAL})
        token = store.snapshot(eid)["pendingApproval"]["token"]

        results = await asyncio.gather(
            manager.send_command(eid, "approve", token=token, approver="alice"),
            manager.send_command(eid, "approve", token=token, approver="bob"),
            manager.send_command(eid, "approve", token=token, approver="charlie"),
        )
        assert results[0] is True

        await wait_for_status(store, eid, sm.TERMINAL_STATES)
        snap = store.snapshot(eid)
        assert snap["status"] == sm.SUCCEEDED
        approvals = store._conn.execute(
            "SELECT * FROM approvals WHERE execution_id = ?", (eid,)
        ).fetchall()
        assert len(approvals) == 1
        assert approvals[0]["status"] == "approved"
        assert approvals[0]["responded_by"] == "alice"

    @pytest.mark.asyncio
    async def test_approve_then_reject_reject_ignored(self, components):
        _, store, manager, _ = components
        flow = make_flow("race2", [
            n("start", "start"),
            approval_node("ap1"),
            n("end", "end"),
        ], [e("e1", "start", "ap1"), e("e2", "ap1", "end")])

        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_for_status(store, eid, {sm.AWAITING_APPROVAL})
        token = store.snapshot(eid)["pendingApproval"]["token"]

        r1 = await manager.send_command(eid, "approve", token=token, approver="alice")
        r2 = await manager.send_command(eid, "reject", token=token, approver="bob")
        assert r1 is True
        assert r2 is False

        await wait_for_status(store, eid, sm.TERMINAL_STATES)
        assert store.snapshot(eid)["status"] == sm.SUCCEEDED

    @pytest.mark.asyncio
    async def test_wrong_token_rejected(self, components):
        _, store, manager, _ = components
        flow = make_flow("race3", [
            n("start", "start"),
            approval_node("ap1"),
            n("end", "end"),
        ], [e("e1", "start", "ap1"), e("e2", "ap1", "end")])

        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_for_status(store, eid, {sm.AWAITING_APPROVAL})

        accepted = await manager.send_command(eid, "approve", token="wrong-token", approver="eve")
        assert accepted is False
        assert store.snapshot(eid)["status"] == sm.AWAITING_APPROVAL


# ---- Expiration ----

class TestApprovalExpiration:
    @pytest.mark.asyncio
    async def test_approval_times_out(self, components):
        _, store, manager, _ = components
        flow = make_flow("exp1", [
            n("start", "start"),
            approval_node("ap1", prompt="Hurry", timeout_seconds=0.5),
            n("after", "task", code="ctx['ran']=True"),
            n("end", "end"),
        ], [e("e1", "start", "ap1"), e("e2", "ap1", "after"), e("e3", "after", "end")])

        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_for_status(store, eid, {sm.AWAITING_APPROVAL})
        await wait_for_status(store, eid, sm.TERMINAL_STATES, timeout=5)
        snap = store.snapshot(eid)
        assert snap["status"] == sm.FAILED
        assert "ran" not in snap["variables"]
        approvals = store._conn.execute(
            "SELECT * FROM approvals WHERE execution_id = ?", (eid,)
        ).fetchall()
        assert approvals[0]["status"] == "expired"

    @pytest.mark.asyncio
    async def test_response_before_deadline_succeeds(self, components):
        _, store, manager, _ = components
        flow = make_flow("exp2", [
            n("start", "start"),
            approval_node("ap1", timeout_seconds=2.0),
            n("end", "end"),
        ], [e("e1", "start", "ap1"), e("e2", "ap1", "end")])

        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_for_status(store, eid, {sm.AWAITING_APPROVAL})
        token = store.snapshot(eid)["pendingApproval"]["token"]
        await asyncio.sleep(0.3)
        accepted = await manager.send_command(eid, "approve", token=token)
        assert accepted is True
        await wait_for_status(store, eid, sm.TERMINAL_STATES)
        assert store.snapshot(eid)["status"] == sm.SUCCEEDED


# ---- Cancel ----

class TestApprovalCancel:
    @pytest.mark.asyncio
    async def test_cancel_during_approval(self, components):
        _, store, manager, _ = components
        flow = make_flow("cancel1", [
            n("start", "start"),
            approval_node("ap1"),
            n("end", "end"),
        ], [e("e1", "start", "ap1"), e("e2", "ap1", "end")])

        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_for_status(store, eid, {sm.AWAITING_APPROVAL})
        accepted = await manager.send_command(eid, "cancel", "cancel-cmd")
        assert accepted is True
        await wait_for_status(store, eid, {sm.CANCELLED})
        approvals = store._conn.execute(
            "SELECT * FROM approvals WHERE execution_id = ?", (eid,)
        ).fetchall()
        assert approvals[0]["status"] == "cancelled"


# ---- Restart recovery ----

class TestApprovalRecovery:
    @pytest.mark.asyncio
    async def test_pending_approval_survives_restart(self, components):
        flow_store, store, manager, _ = components
        flow = make_flow("rec1", [
            n("start", "start"),
            approval_node("ap1", prompt="Persist me"),
            n("after", "task", code="ctx['ran']=True"),
            n("end", "end"),
        ], [e("e1", "start", "ap1"), e("e2", "ap1", "after"), e("e3", "after", "end")])

        eid = "exec-rec"
        await manager.start_execution(flow.id, flow=flow, execution_id=eid)
        await wait_for_status(store, eid, {sm.AWAITING_APPROVAL})
        token_before = store.snapshot(eid)["pendingApproval"]["token"]
        deadline_before = store.snapshot(eid)["pendingApproval"]["deadline"]

        await manager.shutdown()
        manager2 = RuntimeManager(store, flow_store, manager.bus)
        try:
            recovered = await manager2.recover_all()
            assert recovered == 1
            row = store.get_execution_row(eid)
            assert row["status"] == sm.AWAITING_APPROVAL
            snap = store.snapshot(eid)
            assert snap["pendingApproval"]["token"] == token_before
            assert snap["pendingApproval"]["deadline"] == deadline_before

            accepted = await manager2.send_command(eid, "approve", token=token_before, approver="recovery")
            assert accepted is True
            await wait_for_status(store, eid, sm.TERMINAL_STATES)
            assert store.snapshot(eid)["status"] == sm.SUCCEEDED
            assert store.snapshot(eid)["variables"]["ran"] is True
        finally:
            await manager2.shutdown()


# ---- Parallel branch isolation ----

class TestParallelApprovalIsolation:
    @pytest.mark.asyncio
    async def test_approval_in_one_branch_does_not_release_other(self, components):
        _, store, manager, _ = components
        b1_approval = approval_node("b1_ap", prompt="Branch 1")
        b1_after = n("b1_done", "task", code="ctx['b1']=True", anchorId="anchor")
        b2_slow = n("b2_slow", "wait", seconds=2.0)
        b2_after = n("b2_done", "task", code="ctx['b2']=True", anchorId="anchor")
        parallel = FlowNode(
            id="par", type="parallel", position=Position(x=0, y=0),
            data=NodeData(label="par", parallelConfig={"branchNodeIds": ["b1_ap", "b2_slow"]}),
        )
        flow = make_flow("par-appr", [
            n("start", "start"),
            parallel,
            n("end", "end"),
            b1_approval, b1_after, b2_slow, b2_after,
        ], [
            e("e1", "start", "par"), e("e2", "par", "end"),
            e("b1_e1", "b1_ap", "b1_done"),
            e("b2_e1", "b2_slow", "b2_done"),
        ])

        eid = await manager.start_execution(flow.id, flow=flow)
        await asyncio.sleep(0.5)

        pending = store.list_pending_approvals(eid)
        assert len(pending) == 1
        assert pending[0]["node_id"] == "b1_ap"
        assert pending[0]["branch_id"] is not None
        b1_branch = pending[0]["branch_id"]
        token = pending[0]["token"]

        accepted = await manager.send_command(eid, "approve", token=token, approver="mgr")
        assert accepted is True
        await asyncio.sleep(1.0)

        branches = store._conn.execute(
            "SELECT branch_id, status FROM branch_state WHERE execution_id = ?", (eid,)
        ).fetchall()
        b1_status = next(b["status"] for b in branches if b["branch_id"] == b1_branch)
        assert b1_status == "succeeded"

        await wait_for_status(store, eid, sm.TERMINAL_STATES, timeout=15)
        snap = store.snapshot(eid)
        assert snap["status"] == sm.SUCCEEDED
        assert snap["variables"].get("b1") is True
        assert snap["variables"].get("b2") is True

    @pytest.mark.asyncio
    async def test_reject_in_branch_fails_execution(self, components):
        _, store, manager, _ = components
        b1_approval = approval_node("b1_ap", prompt="Branch 1")
        b1_after = n("b1_done", "task", code="ctx['b1']=True", anchorId="anchor")
        b2_slow = n("b2_slow", "wait", seconds=5.0)
        b2_after = n("b2_done", "task", code="ctx['b2']=True", anchorId="anchor")
        parallel = FlowNode(
            id="par", type="parallel", position=Position(x=0, y=0),
            data=NodeData(label="par", parallelConfig={"branchNodeIds": ["b1_ap", "b2_slow"]}),
        )
        flow = make_flow("par-rej", [
            n("start", "start"),
            parallel,
            n("end", "end"),
            b1_approval, b1_after, b2_slow, b2_after,
        ], [
            e("e1", "start", "par"), e("e2", "par", "end"),
            e("b1_e1", "b1_ap", "b1_done"),
            e("b2_e1", "b2_slow", "b2_done"),
        ])

        eid = await manager.start_execution(flow.id, flow=flow)
        await asyncio.sleep(0.5)
        pending = store.list_pending_approvals(eid)
        token = pending[0]["token"]

        await manager.send_command(eid, "reject", token=token, comment="No")
        await wait_for_status(store, eid, sm.TERMINAL_STATES, timeout=5)
        assert store.snapshot(eid)["status"] == sm.FAILED


# ---- Token binding to executionId/nodeId/attempt/flowVersion ----

class TestTokenBinding:
    @pytest.mark.asyncio
    async def test_approval_record_binds_all_fields(self, components):
        _, store, manager, _ = components
        flow = make_flow("bind1", [
            n("start", "start"),
            approval_node("ap1", prompt="Bind"),
            n("end", "end"),
        ], [e("e1", "start", "ap1"), e("e2", "ap1", "end")])

        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_for_status(store, eid, {sm.AWAITING_APPROVAL})
        pending = store.list_pending_approvals(eid)[0]

        assert pending["execution_id"] == eid
        assert pending["node_id"] == "ap1"
        assert pending["attempt"] >= 1
        assert pending["flow_version"] == 1
        assert pending["token"]
        assert pending["deadline"] > time.time()
        assert pending["status"] == "pending"


# ---- Subsequent node execution count ----

class TestSubsequentNodeExecution:
    @pytest.mark.asyncio
    async def test_after_node_runs_exactly_once_after_approval(self, components):
        _, store, manager, _ = components
        counter_node = n("counter", "task", code="ctx['count']=ctx.get('count',0)+1")
        flow = make_flow("count1", [
            n("start", "start"),
            approval_node("ap1"),
            counter_node,
            n("end", "end"),
        ], [e("e1", "start", "ap1"), e("e2", "ap1", "counter"), e("e3", "counter", "end")])

        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_for_status(store, eid, {sm.AWAITING_APPROVAL})
        token = store.snapshot(eid)["pendingApproval"]["token"]
        await manager.send_command(eid, "approve", token=token)
        await wait_for_status(store, eid, sm.TERMINAL_STATES)

        attempts = store._conn.execute(
            "SELECT attempt, status FROM node_attempts WHERE execution_id = ? AND node_id = 'counter'",
            (eid,),
        ).fetchall()
        succeeded = [a for a in attempts if a["status"] == "succeeded"]
        assert len(succeeded) == 1
        assert store.snapshot(eid)["variables"]["count"] == 1

    @pytest.mark.asyncio
    async def test_after_node_never_runs_after_rejection(self, components):
        _, store, manager, _ = components
        flow = make_flow("count2", [
            n("start", "start"),
            approval_node("ap1"),
            n("after", "task", code="ctx['ran']=True"),
            n("end", "end"),
        ], [e("e1", "start", "ap1"), e("e2", "ap1", "after"), e("e3", "after", "end")])

        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_for_status(store, eid, {sm.AWAITING_APPROVAL})
        token = store.snapshot(eid)["pendingApproval"]["token"]
        await manager.send_command(eid, "reject", token=token)
        await wait_for_status(store, eid, sm.TERMINAL_STATES)

        attempts = store._conn.execute(
            "SELECT * FROM node_attempts WHERE execution_id = ? AND node_id = 'after'",
            (eid,),
        ).fetchall()
        assert len(attempts) == 0


# ---- Snapshot exposes approval info for frontend rendering ----

class TestSnapshotApprovalInfo:
    @pytest.mark.asyncio
    async def test_snapshot_contains_pending_approval(self, components):
        _, store, manager, _ = components
        flow = make_flow("snap1", [
            n("start", "start"),
            approval_node("ap1", prompt="Render me", timeout_seconds=100),
            n("end", "end"),
        ], [e("e1", "start", "ap1"), e("e2", "ap1", "end")])

        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_for_status(store, eid, {sm.AWAITING_APPROVAL})
        snap = store.snapshot(eid)
        assert snap["pendingApproval"] is not None
        pa = snap["pendingApproval"]
        assert pa["prompt"] == "Render me"
        assert pa["nodeId"] == "ap1"
        assert pa["token"]
        assert pa["deadline"] > time.time()
        assert "approve" in snap["allowedActions"]
        assert "reject" in snap["allowedActions"]

    @pytest.mark.asyncio
    async def test_snapshot_approval_null_when_not_waiting(self, components):
        _, store, manager, _ = components
        flow = make_flow("snap2", [
            n("start", "start"),
            n("task", "task", code="ctx['x']=1"),
            n("end", "end"),
        ], [e("e1", "start", "task"), e("e2", "task", "end")])

        eid = await manager.start_execution(flow.id, flow=flow)
        await wait_for_status(store, eid, sm.TERMINAL_STATES)
        snap = store.snapshot(eid)
        assert snap["pendingApproval"] is None
