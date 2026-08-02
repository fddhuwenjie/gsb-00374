"""Integration tests for human approval nodes and expirable recovery tokens.

Run with:  python -m pytest backend/test_approval.py -v

Builds on the durable engine (same event seq, execution generation, idempotency
keys and immutable flowVersion) and proves the approval subsystem is correct:

  * an approval request binds executionId + nodeId + attempt + flowVersion and a
    signed, expirable recovery token
  * approve / reject / timeout / duplicate-response / stale-token all go through
    *legal* state-machine transitions (illegal ones never happen)
  * multiple approvers racing -> exactly one decision is recorded (first wins)
  * pending approval + deadline survive a process restart and are recoverable
  * an approval inside a parallel branch releases only that branch's generation,
    never a sibling branch or a stale generation
  * old / stale recovery tokens and duplicate WebSocket-style replays are ignored

Every test asserts the final durable event sequence and the number of times the
subsequent node actually executed.
"""

import asyncio
import json
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from engine.durable.engine import DurableEngine
from engine.durable.event_store import EventStore, RecoveredState
from engine.durable.state_machine import ExecState, TransitionError, check_transition, allowed_commands
from engine.durable.approvals import (
    DECISION_APPROVED,
    DECISION_REJECTED,
    DECISION_TIMEOUT,
    approval_id,
    decode_token,
    issue_token,
)


@pytest.fixture
def tmp_root(tmp_path):
    return str(tmp_path / "flows")


def kinds(events):
    return [e["kind"] for e in events]


def states(events):
    return [e["state"] for e in events if e["kind"] == "state"]


def transitions(events):
    return [(e["prevState"], e["state"]) for e in events if e["kind"] == "state"]


def seqs(events):
    return [e["seq"] for e in events]


def count_node_started(events, node_id):
    return sum(
        1 for e in events if e["kind"] == "node_started" and e.get("nodeId") == node_id
    )


def approval_flow(timeout=None):
    node = {"type": "approval", "next": "work", "prompt": "please review"}
    if timeout is not None:
        node["timeoutSeconds"] = timeout
    return {
        "id": "appr",
        "nodes": {
            "s": {"type": "start", "next": "gate"},
            "gate": node,
            "work": {"type": "task", "next": "e", "ops": [{"set": {"shipped": True}}]},
            "e": {"type": "end"},
        },
    }


# ---------------------------------------------------------------------------
# 0. state machine legality for the new state
# ---------------------------------------------------------------------------

def test_approval_transitions_are_legal_and_others_illegal():
    check_transition(ExecState.RUNNING, ExecState.AWAITING_APPROVAL)
    check_transition(ExecState.AWAITING_APPROVAL, ExecState.RUNNING)
    check_transition(ExecState.AWAITING_APPROVAL, ExecState.FAILED)
    check_transition(ExecState.AWAITING_APPROVAL, ExecState.CANCELLED)
    # Illegal: cannot pause or succeed directly out of awaiting_approval.
    with pytest.raises(TransitionError):
        check_transition(ExecState.AWAITING_APPROVAL, ExecState.PAUSED)
    with pytest.raises(TransitionError):
        check_transition(ExecState.AWAITING_APPROVAL, ExecState.SUCCEEDED)
    assert allowed_commands(ExecState.AWAITING_APPROVAL) == {"approve", "reject", "cancel"}


# ---------------------------------------------------------------------------
# 1. approve happy path + binding
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_approve_resumes_and_binds_identity(tmp_root):
    engine = DurableEngine(tmp_root)
    await engine.create("a1", approval_flow(), {})
    await engine.start("a1")
    await asyncio.sleep(0.05)

    snap = engine.snapshot("a1")
    assert snap["state"] == ExecState.AWAITING_APPROVAL
    assert len(snap["pendingApprovals"]) == 1
    pending = snap["pendingApprovals"][0]
    # Bound to executionId + nodeId + attempt + flowVersion.
    assert pending["nodeId"] == "gate"
    assert pending["attempt"] == 1
    assert pending["flowVersion"] == 1
    expected_aid = approval_id("a1", "gate", None, 1, 1, 1)
    assert pending["approvalId"] == expected_aid

    # The token decodes to the same binding and carries no deadline here.
    payload = decode_token(tmp_root, pending["token"])
    assert payload["approvalId"] == expected_aid
    assert payload["executionId"] == "a1"
    assert payload["flowVersion"] == 1

    result = await engine.respond_approval(pending["token"], "approved", "alice")
    assert result["accepted"] is True
    await engine.wait("a1")

    snap2 = engine.snapshot("a1")
    assert snap2["state"] == ExecState.SUCCEEDED
    assert snap2["variables"]["shipped"] is True
    trs = transitions(snap2["events"])
    assert (ExecState.RUNNING, ExecState.AWAITING_APPROVAL) in trs
    assert (ExecState.AWAITING_APPROVAL, ExecState.RUNNING) in trs
    # subsequent node ran exactly once
    assert count_node_started(snap2["events"], "work") == 1
    resolved = [e for e in snap2["events"] if e["kind"] == "approval_resolved"]
    assert len(resolved) == 1 and resolved[0]["decision"] == DECISION_APPROVED


# ---------------------------------------------------------------------------
# 2. reject fails the execution; the next node never runs
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reject_fails_and_skips_next_node(tmp_root):
    engine = DurableEngine(tmp_root)
    await engine.create("r1", approval_flow(), {})
    await engine.start("r1")
    await asyncio.sleep(0.05)
    token = engine.snapshot("r1")["pendingApprovals"][0]["token"]

    result = await engine.respond_approval(token, "rejected", "bob")
    assert result["accepted"] is True
    await engine.wait("r1")

    snap = engine.snapshot("r1")
    assert snap["state"] == ExecState.FAILED
    assert transitions(snap["events"])[-1] == (ExecState.AWAITING_APPROVAL, ExecState.FAILED)
    # The gated node never executed.
    assert count_node_started(snap["events"], "work") == 0
    assert "shipped" not in snap["variables"]


# ---------------------------------------------------------------------------
# 3. multiple approvers race -> exactly one decision recorded
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_multi_approver_race_first_wins(tmp_root):
    engine = DurableEngine(tmp_root)
    await engine.create("m1", approval_flow(), {})
    await engine.start("m1")
    await asyncio.sleep(0.05)
    token = engine.snapshot("m1")["pendingApprovals"][0]["token"]

    # Two approvers hit the same token concurrently (one approve, one reject).
    results = await asyncio.gather(
        engine.respond_approval(token, "approved", "alice"),
        engine.respond_approval(token, "rejected", "bob"),
    )
    accepted = [r for r in results if r.get("accepted")]
    assert len(accepted) == 1  # exactly one winner

    await engine.wait("m1")
    snap = engine.snapshot("m1")
    resolved = [e for e in snap["events"] if e["kind"] == "approval_resolved"]
    assert len(resolved) == 1  # only ONE resolution persisted, no double-apply
    # The subsequent node ran at most once (0 if rejected won, 1 if approved won).
    assert count_node_started(snap["events"], "work") == (
        1 if resolved[0]["decision"] == DECISION_APPROVED else 0
    )


@pytest.mark.asyncio
async def test_duplicate_response_is_noop(tmp_root):
    engine = DurableEngine(tmp_root)
    await engine.create("d1", approval_flow(), {})
    await engine.start("d1")
    await asyncio.sleep(0.05)
    token = engine.snapshot("d1")["pendingApprovals"][0]["token"]

    first = await engine.respond_approval(token, "approved", "alice")
    second = await engine.respond_approval(token, "approved", "alice")  # duplicate
    third = await engine.respond_approval(token, "rejected", "carol")   # stale now
    assert first["accepted"] is True
    assert second["accepted"] is False
    assert third["accepted"] is False

    await engine.wait("d1")
    snap = engine.snapshot("d1")
    resolved = [e for e in snap["events"] if e["kind"] == "approval_resolved"]
    assert len(resolved) == 1


# ---------------------------------------------------------------------------
# 4. timeout / expiry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_approval_times_out_when_deadline_passes(tmp_root):
    engine = DurableEngine(tmp_root)
    await engine.create("t1", approval_flow(timeout=0.1), {})
    await engine.start("t1")
    # Do not respond; the deadline elapses and the live waiter self-times-out.
    await engine.wait("t1")

    snap = engine.snapshot("t1")
    assert snap["state"] == ExecState.FAILED
    resolved = [e for e in snap["events"] if e["kind"] == "approval_resolved"]
    assert len(resolved) == 1 and resolved[0]["decision"] == DECISION_TIMEOUT
    assert count_node_started(snap["events"], "work") == 0


@pytest.mark.asyncio
async def test_expired_token_is_rejected(tmp_root):
    engine = DurableEngine(tmp_root)
    await engine.create("t2", approval_flow(timeout=0.08), {})
    await engine.start("t2")
    await asyncio.sleep(0.02)
    token = engine.snapshot("t2")["pendingApprovals"][0]["token"]

    # Let the token expire, then attempt to approve with it.
    await asyncio.sleep(0.12)
    result = await engine.respond_approval(token, "approved", "late")
    assert result["accepted"] is False
    assert result["reason"] in ("expired", "stale_or_resolved")

    await engine.wait("t2")
    snap = engine.snapshot("t2")
    assert snap["state"] == ExecState.FAILED
    assert count_node_started(snap["events"], "work") == 0


# ---------------------------------------------------------------------------
# 5. cancel while awaiting approval
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cancel_while_awaiting_approval(tmp_root):
    engine = DurableEngine(tmp_root)
    await engine.create("c1", approval_flow(), {})
    await engine.start("c1")
    await asyncio.sleep(0.05)
    assert engine.snapshot("c1")["state"] == ExecState.AWAITING_APPROVAL

    res = await engine.command("c1", "cancel")
    assert res["accepted"] is True
    await engine.wait("c1")

    snap = engine.snapshot("c1")
    assert snap["state"] == ExecState.CANCELLED
    assert transitions(snap["events"])[-1] == (ExecState.AWAITING_APPROVAL, ExecState.CANCELLED)
    assert count_node_started(snap["events"], "work") == 0
    # The approval stays unresolved (cancellation is not a decision).
    assert not [e for e in snap["events"] if e["kind"] == "approval_resolved"]


# ---------------------------------------------------------------------------
# 6. restart recovery of a pending approval + deadline
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pending_approval_and_deadline_recover_after_restart(tmp_root):
    engine1 = DurableEngine(tmp_root)
    await engine1.create("rec1", approval_flow(timeout=50), {})
    await engine1.start("rec1")
    await asyncio.sleep(0.05)
    snap1 = engine1.snapshot("rec1")
    assert snap1["state"] == ExecState.AWAITING_APPROVAL
    orig = snap1["pendingApprovals"][0]
    orig_deadline = orig["deadline"]
    assert orig_deadline is not None

    # Simulate a crash: drop the task/engine and re-read purely from disk.
    task = engine1._tasks.get("rec1")
    if task and not task.done():
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    # The pending approval + its deadline are recoverable from the event log.
    disk = RecoveredState.fold(EventStore(tmp_root, "rec1").read_events())
    assert orig["approvalId"] in disk.pending_approvals
    assert disk.pending_approvals[orig["approvalId"]]["deadline"] == orig_deadline

    engine2 = DurableEngine(tmp_root)
    ex = await engine2.recover("rec1")
    assert ex.state == ExecState.AWAITING_APPROVAL
    snap2 = engine2.snapshot("rec1")
    rec = snap2["pendingApprovals"][0]
    assert rec["approvalId"] == orig["approvalId"]
    assert rec["deadline"] == orig_deadline  # deadline preserved across restart

    # A token issued BEFORE the crash (bound to the same approvalId + deadline)
    # still resolves after recovery, because the binding still matches.
    old_token = orig["token"]
    await engine2.start("rec1")
    await asyncio.sleep(0.02)
    result = await engine2.respond_approval(old_token, "approved", "alice")
    assert result["accepted"] is True
    await engine2.wait("rec1")

    snap3 = engine2.snapshot("rec1")
    assert snap3["state"] == ExecState.SUCCEEDED
    assert count_node_started(snap3["events"], "work") == 1
    # Exactly one request and one resolution across both lifetimes.
    assert len([e for e in snap3["events"] if e["kind"] == "approval_requested"]) == 1
    assert len([e for e in snap3["events"] if e["kind"] == "approval_resolved"]) == 1


# ---------------------------------------------------------------------------
# 7. parallel branch approval releases only its own branch/generation
# ---------------------------------------------------------------------------

def parallel_approval_flow():
    return {
        "id": "par",
        "nodes": {
            "s": {"type": "start", "next": "p"},
            "p": {
                "type": "parallel",
                "next": "e",
                "required": ["x", "y"],
                "branches": {
                    # x needs approval; y runs freely.
                    "x": {"value": 1, "requiresApproval": True},
                    "y": {"value": 2, "work": 0.01},
                },
            },
            "e": {"type": "end"},
        },
    }


@pytest.mark.asyncio
async def test_parallel_branch_approval_releases_only_its_branch(tmp_root):
    engine = DurableEngine(tmp_root)
    await engine.create("pb1", parallel_approval_flow(), {})
    await engine.start("pb1")
    await asyncio.sleep(0.05)

    # Wait until the free branch y has emitted its boundary and x is pending
    # (poll rather than assume a fixed delay, so the test is not timing-fragile).
    for _ in range(50):
        snap = engine.snapshot("pb1")
        branch_ids = {
            e["branchId"] for e in snap["events"] if e["kind"] == "branch_boundary"
        }
        if snap["state"] == ExecState.AWAITING_APPROVAL and branch_ids == {"y"}:
            break
        await asyncio.sleep(0.01)

    assert snap["state"] == ExecState.AWAITING_APPROVAL
    pend = snap["pendingApprovals"]
    # Exactly one pending approval, bound to branch x at generation 1.
    assert len(pend) == 1
    assert pend[0]["branchId"] == "x"
    assert pend[0]["generation"] == 1
    # y already produced its branch boundary; x has not (still gated).
    branch_evs = [e for e in snap["events"] if e["kind"] == "branch_boundary"]
    assert {e["branchId"] for e in branch_evs} == {"y"}

    # Approve branch x -> only x is released; join completes at generation 1.
    result = await engine.respond_approval(pend[0]["token"], "approved", "alice")
    assert result["accepted"] is True
    await engine.wait("pb1")

    snap2 = engine.snapshot("pb1")
    assert snap2["state"] == ExecState.SUCCEEDED
    merged = snap2["variables"]["p_result"]
    assert merged["generation"] == 1
    assert merged["branches"] == {"x": 1, "y": 2}
    # Both branch boundaries are generation 1; the approval only released x.
    x_boundaries = [
        e for e in snap2["events"]
        if e["kind"] == "branch_boundary" and e["branchId"] == "x"
    ]
    assert len(x_boundaries) == 1 and x_boundaries[0]["generation"] == 1
    resolved = [e for e in snap2["events"] if e["kind"] == "approval_resolved"]
    assert len(resolved) == 1 and resolved[0]["branchId"] == "x"


@pytest.mark.asyncio
async def test_branch_approval_token_does_not_release_sibling(tmp_root):
    # Two branches both need approval; approving one must not release the other.
    flow = {
        "id": "par2",
        "nodes": {
            "s": {"type": "start", "next": "p"},
            "p": {
                "type": "parallel",
                "next": "e",
                "required": ["x", "y"],
                "branches": {
                    "x": {"value": 1, "requiresApproval": True},
                    "y": {"value": 2, "requiresApproval": True},
                },
            },
            "e": {"type": "end"},
        },
    }
    engine = DurableEngine(tmp_root)
    await engine.create("pb2", flow, {})
    await engine.start("pb2")
    await asyncio.sleep(0.05)

    pend = {p["branchId"]: p for p in engine.snapshot("pb2")["pendingApprovals"]}
    assert set(pend) == {"x", "y"}
    assert pend["x"]["approvalId"] != pend["y"]["approvalId"]

    # Approve only x. y must remain pending; execution still awaiting_approval.
    await engine.respond_approval(pend["x"]["token"], "approved", "alice")
    await asyncio.sleep(0.03)
    mid = engine.snapshot("pb2")
    assert mid["state"] == ExecState.AWAITING_APPROVAL
    still_pending = {p["branchId"] for p in mid["pendingApprovals"]}
    assert still_pending == {"y"}
    x_bounds = [e for e in mid["events"] if e["kind"] == "branch_boundary" and e["branchId"] == "x"]
    y_bounds = [e for e in mid["events"] if e["kind"] == "branch_boundary" and e["branchId"] == "y"]
    assert len(x_bounds) == 1 and len(y_bounds) == 0  # only x released

    # Now approve y -> join completes.
    await engine.respond_approval(pend["y"]["token"], "approved", "bob")
    await engine.wait("pb2")
    snap = engine.snapshot("pb2")
    assert snap["state"] == ExecState.SUCCEEDED
    assert snap["variables"]["p_result"]["branches"] == {"x": 1, "y": 2}


# ---------------------------------------------------------------------------
# 8. stale / old-generation token & old WebSocket replay are ignored
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stale_token_from_wrong_generation_is_ignored(tmp_root):
    engine = DurableEngine(tmp_root)
    await engine.create("s1", approval_flow(), {})
    await engine.start("s1")
    await asyncio.sleep(0.05)

    # Forge a token for a DIFFERENT generation/attempt (never issued by server).
    fake_binding = {
        "approvalId": approval_id("s1", "gate", None, 99, 99, 1),
        "executionId": "s1",
        "nodeId": "gate",
        "branchId": None,
        "generation": 99,
        "attempt": 99,
        "flowVersion": 1,
    }
    fake_token = issue_token(tmp_root, fake_binding, None)
    result = await engine.respond_approval(fake_token, "approved", "attacker")
    assert result["accepted"] is False
    assert result["reason"] == "stale_or_resolved"

    # The real approval is still pending and untouched.
    snap = engine.snapshot("s1")
    assert snap["state"] == ExecState.AWAITING_APPROVAL
    assert len(snap["pendingApprovals"]) == 1

    # Now resolve legitimately and prove the forged one left no trace.
    await engine.respond_approval(snap["pendingApprovals"][0]["token"], "approved", "alice")
    await engine.wait("s1")
    snap2 = engine.snapshot("s1")
    resolved = [e for e in snap2["events"] if e["kind"] == "approval_resolved"]
    assert len(resolved) == 1 and resolved[0]["approver"] == "alice"


@pytest.mark.asyncio
async def test_forged_token_bad_signature_is_rejected(tmp_root):
    engine = DurableEngine(tmp_root)
    await engine.create("f1", approval_flow(), {})
    await engine.start("f1")
    await asyncio.sleep(0.05)
    good = engine.snapshot("f1")["pendingApprovals"][0]["token"]

    # Tamper with the signature.
    body, _sig = good.rsplit(".", 1)
    forged = body + ".deadbeefdeadbeefdeadbeefdeadbeef"
    result = await engine.respond_approval(forged, "approved", "attacker")
    assert result["accepted"] is False
    assert result["reason"].startswith("bad_token")

    # Cleanly approve afterwards for a clean shutdown.
    await engine.respond_approval(good, "approved", "alice")
    await engine.wait("f1")
    assert engine.snapshot("f1")["state"] == ExecState.SUCCEEDED


@pytest.mark.asyncio
async def test_old_websocket_replay_does_not_double_apply(tmp_root):
    """An old approve message replayed after the approval already resolved (e.g.
    a duplicated WebSocket/HTTP delivery) must be a no-op -- the event log keeps
    a single resolution and the next node runs exactly once."""
    engine = DurableEngine(tmp_root)
    await engine.create("w1", approval_flow(), {})
    await engine.start("w1")
    await asyncio.sleep(0.05)
    token = engine.snapshot("w1")["pendingApprovals"][0]["token"]

    await engine.respond_approval(token, "approved", "alice")
    await engine.wait("w1")

    # Replay the very same (now stale) message several times.
    for _ in range(3):
        again = await engine.respond_approval(token, "approved", "alice")
        assert again["accepted"] is False

    snap = engine.snapshot("w1")
    assert snap["state"] == ExecState.SUCCEEDED
    assert len([e for e in snap["events"] if e["kind"] == "approval_resolved"]) == 1
    assert count_node_started(snap["events"], "work") == 1
    # Event sequence is still gapless and monotonic.
    assert seqs(snap["events"]) == list(range(1, len(snap["events"]) + 1))


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
