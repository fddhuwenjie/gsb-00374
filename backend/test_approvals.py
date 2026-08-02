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
from engine.execution_manager import ExecutionManager, CommandRejected
from models.flow import (
    ApprovalConfig,
    ApprovalResponse,
    FlowDefinition,
    FlowNode,
    FlowEdge,
    Position,
    NodeData,
    ParallelConfig,
)
from storage.event_store import EventStore


def _node(node_id, node_type, data=None):
    if data is None:
        data = NodeData(label=node_id)
    return FlowNode(id=node_id, type=node_type, position=Position(x=0, y=0), data=data)


def _edge(edge_id, source, target, handle=None):
    return FlowEdge(id=edge_id, source=source, target=target, sourceHandle=handle)


def _approval_flow(timeout_seconds=600, approvers=None, flow_id='test_approval'):
    nodes = [
        _node('start', 'start'),
        _node('approval1', 'approval', NodeData(
            label='Approval',
            approvalConfig=ApprovalConfig(
                approvers=approvers or ['alice', 'bob'],
                timeoutSeconds=timeout_seconds,
                description='Test approval',
            ),
        )),
        _node('after_approval', 'task', NodeData(
            label='After', code="ctx['approved']=True",
        )),
        _node('end', 'end'),
    ]
    edges = [
        _edge('e_start_app', 'start', 'approval1'),
        _edge('e_app_after', 'approval1', 'after_approval'),
        _edge('e_after_end', 'after_approval', 'end'),
    ]
    return FlowDefinition(
        id=flow_id, name='Approval Test', nodes=nodes, edges=edges,
        createdAt=0, updatedAt=0,
    )


def _parallel_approval_flow(timeout_seconds=600):
    nodes = [
        _node('start', 'start'),
        _node('parallel1', 'parallel', NodeData(
            label='Parallel',
            parallelConfig=ParallelConfig(branchNodeIds=['branch_a', 'branch_b']),
        )),
        _node('branch_a', 'approval', NodeData(
            label='Approve A',
            anchorId='parallel1',
            approvalConfig=ApprovalConfig(
                approvers=['alice'], timeoutSeconds=timeout_seconds,
                description='Branch A approval',
            ),
        )),
        _node('branch_b', 'approval', NodeData(
            label='Approve B',
            anchorId='parallel1',
            approvalConfig=ApprovalConfig(
                approvers=['bob'], timeoutSeconds=timeout_seconds,
                description='Branch B approval',
            ),
        )),
        _node('after_join', 'task', NodeData(
            label='After Join', code="ctx['joined']=True",
        )),
        _node('end', 'end'),
    ]
    edges = [
        _edge('e_start_par', 'start', 'parallel1'),
        _edge('e_par_after', 'parallel1', 'after_join'),
        _edge('e_after_end', 'after_join', 'end'),
    ]
    return FlowDefinition(
        id='test_par_approval', name='Parallel Approval',
        nodes=nodes, edges=edges, createdAt=0, updatedAt=0,
    )


@pytest.fixture
def tmp_dir(tmp_path):
    return str(tmp_path / 'flows')


@pytest.fixture
def event_store(tmp_dir):
    return EventStore(os.path.join(tmp_dir, 'event_store'))


@pytest.fixture
def manager(event_store):
    return ExecutionManager(event_store)


async def _wait_for_status(manager, eid, status, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        snap = manager.get_snapshot(eid)
        if snap.status == status:
            return snap
        await asyncio.sleep(0.05)
    snap = manager.get_snapshot(eid)
    raise AssertionError(
        f"Timed out waiting for {status}, last status: {snap.status}"
    )


async def _wait_for_pending_approval(manager, eid, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        snap = manager.get_snapshot(eid)
        if snap.pendingApproval is not None and snap.pendingApproval.status == 'pending':
            return snap.pendingApproval
        if sm.is_terminal(snap.status):
            raise AssertionError(
                f"Execution reached terminal state {snap.status} before approval"
            )
        await asyncio.sleep(0.05)
    snap = manager.get_snapshot(eid)
    raise AssertionError(
        f"Timed out waiting for pending approval, status: {snap.status}"
    )


def _get_latest_pending_approval(manager, eid):
    record = manager.event_store.load_record(eid)
    for a in reversed(record.approvals):
        if a.status == 'pending':
            return a
    return None


class TestApprovalStateMachine:
    def test_legal_transitions(self):
        assert sm.can_transition(sm.RUNNING, sm.AWAITING_APPROVAL)
        assert sm.can_transition(sm.AWAITING_APPROVAL, sm.RUNNING)
        assert sm.can_transition(sm.AWAITING_APPROVAL, sm.FAILED)
        assert sm.can_transition(sm.AWAITING_APPROVAL, sm.CANCELLED)
        assert not sm.can_transition(sm.AWAITING_APPROVAL, sm.PAUSED)
        assert not sm.can_transition(sm.AWAITING_APPROVAL, sm.PAUSING)
        assert not sm.can_transition(sm.SUCCEEDED, sm.AWAITING_APPROVAL)

    def test_allowed_actions(self):
        actions = sm.allowed_actions(sm.AWAITING_APPROVAL)
        assert 'cancel' in actions
        assert 'pause' not in actions


class TestApprovalHappyPath:
    @pytest.mark.asyncio
    async def test_approve_continues_execution(self, manager):
        flow = _approval_flow(timeout_seconds=60)
        eid = await manager.start_execution(flow)

        approval = await _wait_for_pending_approval(manager, eid)
        assert approval is not None
        assert approval.flowVersion == 1
        assert approval.nodeId == 'approval1'
        assert approval.attempt >= 1
        assert approval.status == 'pending'
        assert approval.deadline > time.time()

        snap = manager.get_snapshot(eid)
        assert snap.status == sm.AWAITING_APPROVAL
        assert 'cancel' in snap.allowedActions

        response = ApprovalResponse(
            token=approval.token,
            executionId=eid,
            decision='approved',
            responder='alice',
            comment='Looks good',
        )
        result = await manager.respond_to_approval(response)
        assert result.status == sm.RUNNING

        await _wait_for_status(manager, eid, sm.SUCCEEDED)
        snap = manager.get_snapshot(eid)
        assert snap.variables.get('approved') is True

        events = manager.get_events(eid)
        types = [e.eventType for e in events]
        assert 'approvalRequested' in types
        assert 'approvalResponded' in types

        transitions = [(e.fromState, e.toState) for e in events if e.eventType == 'transition']
        assert (sm.RUNNING, sm.AWAITING_APPROVAL) in transitions
        assert (sm.AWAITING_APPROVAL, sm.RUNNING) in transitions

    @pytest.mark.asyncio
    async def test_reject_fails_execution(self, manager):
        flow = _approval_flow(timeout_seconds=60)
        eid = await manager.start_execution(flow)

        approval = await _wait_for_pending_approval(manager, eid)

        response = ApprovalResponse(
            token=approval.token,
            executionId=eid,
            decision='rejected',
            responder='bob',
            comment='Not good',
        )
        result = await manager.respond_to_approval(response)
        assert result.status == sm.FAILED

        await asyncio.sleep(0.3)
        snap = manager.get_snapshot(eid)
        assert snap.status == sm.FAILED
        assert 'rejected' in (snap.lastError or '')
        assert snap.variables.get('approved') is None

        events = manager.get_events(eid)
        transitions = [(e.fromState, e.toState) for e in events if e.eventType == 'transition']
        assert (sm.AWAITING_APPROVAL, sm.FAILED) in transitions

    @pytest.mark.asyncio
    async def test_approval_timeout(self, manager):
        flow = _approval_flow(timeout_seconds=0.5)
        eid = await manager.start_execution(flow)

        approval = await _wait_for_pending_approval(manager, eid)
        assert approval is not None

        await _wait_for_status(manager, eid, sm.FAILED, timeout=5.0)
        snap = manager.get_snapshot(eid)
        assert 'expired' in (snap.lastError or '').lower()

        events = manager.get_events(eid)
        types = [e.eventType for e in events]
        assert 'approvalExpired' in types

    @pytest.mark.asyncio
    async def test_cancel_during_approval(self, manager):
        flow = _approval_flow(timeout_seconds=60)
        eid = await manager.start_execution(flow)

        await _wait_for_pending_approval(manager, eid)

        await manager.control(eid, 'cancel', request_id='cancel-1')
        await asyncio.sleep(0.3)

        snap = manager.get_snapshot(eid)
        assert snap.status == sm.CANCELLED

        record = manager.event_store.load_record(eid)
        cancelled_approvals = [a for a in record.approvals if a.status == 'cancelled']
        assert len(cancelled_approvals) >= 1


class TestApprovalIdempotencyAndStaleTokens:
    @pytest.mark.asyncio
    async def test_duplicate_approval_response_is_idempotent(self, manager):
        flow = _approval_flow(timeout_seconds=60)
        eid = await manager.start_execution(flow)
        approval = await _wait_for_pending_approval(manager, eid)

        req_id = 'approval-req-' + str(uuid.uuid4())
        response = ApprovalResponse(
            token=approval.token, executionId=eid,
            decision='approved', responder='alice',
            requestId=req_id,
        )
        r1 = await manager.respond_to_approval(response)
        r2 = await manager.respond_to_approval(response)

        assert r1.status == sm.RUNNING
        assert r2.status == r1.status

        await _wait_for_status(manager, eid, sm.SUCCEEDED)

    @pytest.mark.asyncio
    async def test_stale_token_rejected(self, manager):
        flow = _approval_flow(timeout_seconds=60)
        eid = await manager.start_execution(flow)
        await _wait_for_pending_approval(manager, eid)

        fake_token = str(uuid.uuid4())
        response = ApprovalResponse(
            token=fake_token, executionId=eid,
            decision='approved', responder='eve',
        )
        with pytest.raises(CommandRejected) as exc_info:
            await manager.respond_to_approval(response)
        assert 'token' in str(exc_info.value).lower() or 'invalid' in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_wrong_execution_token_rejected(self, manager):
        flow1 = _approval_flow(timeout_seconds=60, flow_id='flow1')
        flow2 = _approval_flow(timeout_seconds=60, flow_id='flow2')
        eid1 = await manager.start_execution(flow1)
        eid2 = await manager.start_execution(flow2)

        a1 = await _wait_for_pending_approval(manager, eid1)
        await _wait_for_pending_approval(manager, eid2)

        response = ApprovalResponse(
            token=a1.token, executionId=eid2,
            decision='approved', responder='alice',
        )
        with pytest.raises(CommandRejected):
            await manager.respond_to_approval(response)

    @pytest.mark.asyncio
    async def test_response_after_terminal_state_rejected(self, manager):
        flow = _approval_flow(timeout_seconds=60)
        eid = await manager.start_execution(flow)
        approval = await _wait_for_pending_approval(manager, eid)

        await manager.control(eid, 'cancel', request_id='cancel-1')
        await asyncio.sleep(0.3)

        response = ApprovalResponse(
            token=approval.token, executionId=eid,
            decision='approved', responder='alice',
        )
        with pytest.raises(CommandRejected):
            await manager.respond_to_approval(response)


class TestApprovalRecovery:
    @pytest.mark.asyncio
    async def test_pending_approval_survives_restart(self, tmp_dir, event_store):
        manager1 = ExecutionManager(event_store)
        flow = _approval_flow(timeout_seconds=60)
        eid = await manager1.start_execution(flow)
        approval = await _wait_for_pending_approval(manager1, eid)
        token = approval.token
        deadline = approval.deadline
        await manager1.shutdown()

        await asyncio.sleep(0.3)

        event_store2 = EventStore(os.path.join(tmp_dir, 'event_store'))
        manager2 = ExecutionManager(event_store2)
        await manager2.recover_all()
        await asyncio.sleep(0.3)

        snap = manager2.get_snapshot(eid)
        assert snap.status == sm.AWAITING_APPROVAL
        assert snap.pendingApproval is not None
        assert snap.pendingApproval.token == token
        assert abs(snap.pendingApproval.deadline - deadline) < 1.0
        assert snap.pendingApproval.status == 'pending'

        response = ApprovalResponse(
            token=token, executionId=eid,
            decision='approved', responder='alice',
        )
        await manager2.respond_to_approval(response)
        await _wait_for_status(manager2, eid, sm.SUCCEEDED, timeout=5.0)
        await manager2.shutdown()

    @pytest.mark.asyncio
    async def test_expired_approval_during_restart(self, tmp_dir, event_store):
        manager1 = ExecutionManager(event_store)
        flow = _approval_flow(timeout_seconds=0.3)
        eid = await manager1.start_execution(flow)
        await _wait_for_pending_approval(manager1, eid)
        await manager1.shutdown()

        await asyncio.sleep(1.0)

        event_store2 = EventStore(os.path.join(tmp_dir, 'event_store'))
        manager2 = ExecutionManager(event_store2)
        await manager2.recover_all()
        await asyncio.sleep(0.5)

        snap = manager2.get_snapshot(eid)
        assert snap.status == sm.FAILED
        assert 'expired' in (snap.lastError or '').lower()
        await manager2.shutdown()

    @pytest.mark.asyncio
    async def test_approval_events_monotonic_after_recovery(self, tmp_dir, event_store):
        manager1 = ExecutionManager(event_store)
        flow = _approval_flow(timeout_seconds=60)
        eid = await manager1.start_execution(flow)
        approval = await _wait_for_pending_approval(manager1, eid)
        seq_before = manager1.get_snapshot(eid).seq
        await manager1.shutdown()

        event_store2 = EventStore(os.path.join(tmp_dir, 'event_store'))
        manager2 = ExecutionManager(event_store2)
        await manager2.recover_all()
        await asyncio.sleep(0.2)

        response = ApprovalResponse(
            token=approval.token, executionId=eid,
            decision='approved', responder='alice',
        )
        await manager2.respond_to_approval(response)
        await _wait_for_status(manager2, eid, sm.SUCCEEDED)

        events = manager2.get_events(eid)
        seqs = [e.seq for e in events]
        assert seqs == sorted(seqs)
        assert len(seqs) == len(set(seqs))
        assert seqs[-1] > seq_before
        await manager2.shutdown()


class TestParallelBranchApprovals:
    @pytest.mark.asyncio
    async def test_parallel_approvals_independent(self, manager):
        flow = _parallel_approval_flow(timeout_seconds=60)
        eid = await manager.start_execution(flow)
        await asyncio.sleep(0.5)

        record = manager.event_store.load_record(eid)
        pending = [a for a in record.approvals if a.status == 'pending']
        assert len(pending) == 2

        tokens = {a.nodeId: a.token for a in pending}
        generations = {a.nodeId: a.generation for a in pending}
        assert generations['branch_a'] == generations['branch_b']
        gen = generations['branch_a']
        assert gen > 0

        response_a = ApprovalResponse(
            token=tokens['branch_a'], executionId=eid,
            decision='approved', responder='alice',
        )
        await manager.respond_to_approval(response_a)
        await asyncio.sleep(0.3)

        record = manager.event_store.load_record(eid)
        pending_after = [a for a in record.approvals if a.status == 'pending']
        assert len(pending_after) == 1
        assert pending_after[0].nodeId == 'branch_b'

        response_b = ApprovalResponse(
            token=tokens['branch_b'], executionId=eid,
            decision='approved', responder='bob',
        )
        await manager.respond_to_approval(response_b)

        await _wait_for_status(manager, eid, sm.SUCCEEDED, timeout=5.0)
        snap = manager.get_snapshot(eid)
        assert snap.variables.get('joined') is True

        events = manager.get_events(eid)
        seqs = [event.seq for event in events]
        assert seqs == sorted(seqs)
        assert len(seqs) == len(set(seqs))
        assert snap.seq == seqs[-1]

    @pytest.mark.asyncio
    async def test_parallel_approval_generation_isolation(self, manager):
        flow = _parallel_approval_flow(timeout_seconds=60)
        eid = await manager.start_execution(flow)
        await asyncio.sleep(0.5)

        record = manager.event_store.load_record(eid)
        pending = [a for a in record.approvals if a.status == 'pending']
        assert len(pending) == 2
        token_a = [a for a in pending if a.nodeId == 'branch_a'][0].token
        token_b = [a for a in pending if a.nodeId == 'branch_b'][0].token

        response = ApprovalResponse(
            token=token_a, executionId=eid,
            decision='rejected', responder='alice',
        )
        await manager.respond_to_approval(response)
        await asyncio.sleep(0.5)

        snap = manager.get_snapshot(eid)
        assert snap.status == sm.FAILED

        record = manager.event_store.load_record(eid)
        approvals_a = [a for a in record.approvals if a.nodeId == 'branch_a']
        approvals_b = [a for a in record.approvals if a.nodeId == 'branch_b']
        assert any(a.status == 'rejected' for a in approvals_a)
        assert any(a.status == 'pending' for a in approvals_b)

    @pytest.mark.asyncio
    async def test_parallel_branch_approval_timeout(self, manager):
        flow = _parallel_approval_flow(timeout_seconds=0.5)
        eid = await manager.start_execution(flow)

        await _wait_for_status(manager, eid, sm.FAILED, timeout=5.0)
        snap = manager.get_snapshot(eid)
        assert snap.status == sm.FAILED

        record = manager.event_store.load_record(eid)
        expired = [a for a in record.approvals if a.status == 'expired']
        assert len(expired) >= 1


class TestMultiApproverRace:
    @pytest.mark.asyncio
    async def test_concurrent_responses_only_one_wins(self, manager):
        flow = _approval_flow(timeout_seconds=60, approvers=['alice', 'bob', 'carol'])
        eid = await manager.start_execution(flow)
        approval = await _wait_for_pending_approval(manager, eid)
        token = approval.token

        async def respond(responder, decision):
            return await manager.respond_to_approval(ApprovalResponse(
                token=token, executionId=eid,
                decision=decision, responder=responder,
                requestId=f'race-{responder}-{uuid.uuid4()}',
            ))

        results = await asyncio.gather(
            respond('alice', 'approved'),
            respond('bob', 'approved'),
            respond('carol', 'rejected'),
            return_exceptions=True,
        )

        successes = [r for r in results if not isinstance(r, Exception)]
        assert len(successes) >= 1

        await asyncio.sleep(0.5)
        snap = manager.get_snapshot(eid)
        assert snap.status in (sm.RUNNING, sm.SUCCEEDED, sm.FAILED)

        record = manager.event_store.load_record(eid)
        target_approval = None
        for a in record.approvals:
            if a.token == token:
                target_approval = a
                break
        assert target_approval is not None
        assert target_approval.status != 'pending'

    @pytest.mark.asyncio
    async def test_approve_and_reject_race_first_wins(self, manager):
        flow = _approval_flow(timeout_seconds=60)
        eid = await manager.start_execution(flow)
        approval = await _wait_for_pending_approval(manager, eid)
        token = approval.token

        async def respond(responder, decision):
            return await manager.respond_to_approval(ApprovalResponse(
                token=token, executionId=eid,
                decision=decision, responder=responder,
                requestId=f'race2-{responder}-{uuid.uuid4()}',
            ))

        results = await asyncio.gather(
            respond('alice', 'approved'),
            respond('bob', 'rejected'),
            return_exceptions=True,
        )

        await asyncio.sleep(0.5)
        snap = manager.get_snapshot(eid)
        record = manager.event_store.load_record(eid)
        target = next((a for a in record.approvals if a.token == token), None)
        assert target is not None
        assert target.status in ('approved', 'rejected')
        if target.status == 'approved':
            assert snap.status in (sm.RUNNING, sm.SUCCEEDED)
        else:
            assert snap.status == sm.FAILED


class TestApprovalEventSequence:
    @pytest.mark.asyncio
    async def test_event_sequence_on_approve(self, manager):
        flow = _approval_flow(timeout_seconds=60)
        eid = await manager.start_execution(flow)
        approval = await _wait_for_pending_approval(manager, eid)

        await manager.respond_to_approval(ApprovalResponse(
            token=approval.token, executionId=eid,
            decision='approved', responder='alice',
        ))
        await _wait_for_status(manager, eid, sm.SUCCEEDED)

        events = manager.get_events(eid)
        seqs = [e.seq for e in events]
        assert seqs == sorted(seqs)
        assert len(seqs) == len(set(seqs))

        approval_events = [e for e in events if e.eventType in (
            'approvalRequested', 'approvalResponded', 'approvalExpired', 'approvalCancelled'
        )]
        assert approval_events[0].eventType == 'approvalRequested'
        assert approval_events[-1].eventType == 'approvalResponded'
        assert approval_events[-1].payload.get('decision') == 'approved'

    @pytest.mark.asyncio
    async def test_approval_binds_flow_version(self, manager):
        flow = _approval_flow(timeout_seconds=60)
        eid = await manager.start_execution(flow)
        approval = await _wait_for_pending_approval(manager, eid)

        assert approval.flowVersion == 1

        events = manager.get_events(eid)
        req_event = next(e for e in events if e.eventType == 'approvalRequested')
        assert req_event.payload.get('flowVersion') == 1

    @pytest.mark.asyncio
    async def test_node_execution_count_after_approval(self, manager):
        flow = _approval_flow(timeout_seconds=60)
        eid = await manager.start_execution(flow)
        approval = await _wait_for_pending_approval(manager, eid)

        await manager.respond_to_approval(ApprovalResponse(
            token=approval.token, executionId=eid,
            decision='approved', responder='alice',
        ))
        await _wait_for_status(manager, eid, sm.SUCCEEDED)

        record = manager.event_store.load_record(eid)
        assert 'after_approval' in record.snapshot.completedNodes

        events = manager.get_events(eid)
        node_enters = [e for e in events if e.eventType == 'nodeEnter']
        after_enters = [e for e in node_enters if e.nodeId == 'after_approval']
        assert len(after_enters) == 1
        node_exits = [e for e in events if e.eventType == 'nodeExit']
        after_exits = [e for e in node_exits if e.nodeId == 'after_approval']
        assert len(after_exits) == 1


class TestApprovalStaleMessages:
    @pytest.mark.asyncio
    async def test_old_events_have_monotonic_seq(self, manager):
        flow = _approval_flow(timeout_seconds=60)
        eid = await manager.start_execution(flow)
        approval = await _wait_for_pending_approval(manager, eid)

        events_before = manager.get_events(eid)
        seq_before = events_before[-1].seq

        await manager.respond_to_approval(ApprovalResponse(
            token=approval.token, executionId=eid,
            decision='approved', responder='alice',
        ))
        await _wait_for_status(manager, eid, sm.SUCCEEDED)

        all_events = manager.get_events(eid)
        seqs = [e.seq for e in all_events]
        assert seqs == sorted(seqs)
        assert len(seqs) == len(set(seqs))
        assert all(s > seq_before for s in seqs if s > seq_before)

        approval_events = [e for e in all_events if e.eventType in (
            'approvalRequested', 'approvalResponded', 'approvalExpired'
        )]
        assert approval_events[0].seq < approval_events[-1].seq

    @pytest.mark.asyncio
    async def test_stale_token_after_approval_resolved(self, manager):
        flow = _approval_flow(timeout_seconds=60)
        eid = await manager.start_execution(flow)
        approval = await _wait_for_pending_approval(manager, eid)
        stale_token = approval.token

        await manager.respond_to_approval(ApprovalResponse(
            token=stale_token, executionId=eid,
            decision='approved', responder='alice',
        ))
        await _wait_for_status(manager, eid, sm.SUCCEEDED)

        response = ApprovalResponse(
            token=stale_token, executionId=eid,
            decision='rejected', responder='bob',
            requestId='late-response-1',
        )
        with pytest.raises(CommandRejected):
            await manager.respond_to_approval(response)

    @pytest.mark.asyncio
    async def test_approval_in_rejected_state_rejects_further_responses(self, manager):
        flow = _approval_flow(timeout_seconds=60)
        eid = await manager.start_execution(flow)
        approval = await _wait_for_pending_approval(manager, eid)

        await manager.respond_to_approval(ApprovalResponse(
            token=approval.token, executionId=eid,
            decision='rejected', responder='alice',
        ))
        await asyncio.sleep(0.3)

        with pytest.raises(CommandRejected):
            await manager.respond_to_approval(ApprovalResponse(
                token=approval.token, executionId=eid,
                decision='approved', responder='bob',
            ))
