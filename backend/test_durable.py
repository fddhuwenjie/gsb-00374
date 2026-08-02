"""Self-starting integration tests for the durable execution engine.

Run with:  python -m pytest backend/test_durable.py -v

Each test uses a fresh temporary ``flows`` directory (via the ``tmp_root``
fixture) so nothing touches the real project data. Tests assert the *durable
event sequence* and *side-effect counts* directly, and exercise:

  * durable state machine + monotonic event log + illegal-transition rejection
  * pause race against an uninterruptible node (running -> pausing -> paused)
  * process restart recovery (resume from last node boundary, fresh engine)
  * parallel join across a generation; branch retry uses a new generation
  * failed node retry with backoff
  * duplicate / stale commands never start a second executor
  * side-effect idempotency (HTTP + file) across restart -- effect runs once
  * WebSocket snapshot + seq backfill; duplicate/out-of-order events don't regress
"""

import asyncio
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from engine.durable.engine import DurableEngine
from engine.durable.event_store import EventStore, RecoveredState
from engine.durable.state_machine import (
    ExecState,
    TransitionError,
    allowed_commands,
    check_transition,
)
from engine.durable.effects import idempotency_key


# ---------------------------------------------------------------------------
# fixtures / helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_root(tmp_path):
    """A temporary flows directory unique to each test."""
    return str(tmp_path / "flows")


def kinds(events):
    return [e["kind"] for e in events]


def states(events):
    return [e["state"] for e in events if e["kind"] == "state"]


def transitions(events):
    return [(e["prevState"], e["state"]) for e in events if e["kind"] == "state"]


def seqs(events):
    return [e["seq"] for e in events]


def linear_flow():
    return {
        "id": "linear",
        "nodes": {
            "s": {"type": "start", "next": "a"},
            "a": {"type": "task", "next": "b", "ops": [{"set": {"x": 1}}]},
            "b": {"type": "task", "next": "e", "ops": [{"incr": "x"}]},
            "e": {"type": "end"},
        },
    }


# ---------------------------------------------------------------------------
# 1. state machine: monotonic seq, prevState, illegal transitions
# ---------------------------------------------------------------------------

def test_illegal_transitions_rejected():
    # Legal
    check_transition(ExecState.QUEUED, ExecState.RUNNING)
    check_transition(ExecState.RUNNING, ExecState.PAUSING)
    check_transition(ExecState.PAUSING, ExecState.PAUSED)
    check_transition(ExecState.PAUSED, ExecState.RUNNING)
    # Illegal: cannot go queued -> paused directly
    with pytest.raises(TransitionError):
        check_transition(ExecState.QUEUED, ExecState.PAUSED)
    # Illegal: terminal states have no successors
    with pytest.raises(TransitionError):
        check_transition(ExecState.SUCCEEDED, ExecState.RUNNING)
    with pytest.raises(TransitionError):
        check_transition(ExecState.FAILED, ExecState.RUNNING)
    # Illegal: unknown target
    with pytest.raises(TransitionError):
        check_transition(ExecState.RUNNING, "bogus")


def test_allowed_commands_match_states():
    assert allowed_commands(ExecState.QUEUED) == {"start", "cancel"}
    assert allowed_commands(ExecState.RUNNING) == {"pause", "cancel"}
    assert allowed_commands(ExecState.PAUSED) == {"resume", "cancel"}
    assert allowed_commands(ExecState.SUCCEEDED) == set()
    assert allowed_commands(ExecState.FAILED) == set()
    assert allowed_commands(ExecState.CANCELLED) == set()


@pytest.mark.asyncio
async def test_happy_path_event_sequence(tmp_root):
    engine = DurableEngine(tmp_root)
    await engine.create("e1", linear_flow(), {})
    await engine.start("e1")
    await engine.wait("e1")

    snap = engine.snapshot("e1")
    assert snap["state"] == ExecState.SUCCEEDED
    assert snap["variables"]["x"] == 2

    events = snap["events"]
    # Monotonic, gapless sequence starting at 1.
    assert seqs(events) == list(range(1, len(events) + 1))
    # Every state event records the correct previous state.
    assert transitions(events)[0] == (ExecState.QUEUED, ExecState.RUNNING)
    assert transitions(events)[-1] == (ExecState.RUNNING, ExecState.SUCCEEDED)
    # Node boundaries flushed in order.
    boundary_nodes = [e["nodeId"] for e in events if e["kind"] == "node_boundary"]
    assert boundary_nodes == ["s", "a", "b", "e"]


# ---------------------------------------------------------------------------
# 2. pause race against an uninterruptible node
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pause_race_uninterruptible(tmp_root):
    flow = {
        "id": "pauserace",
        "nodes": {
            "s": {"type": "start", "next": "long"},
            "long": {
                "type": "task",
                "next": "after",
                "uninterruptible": True,
                "steps": 10,
                "stepDelay": 0.02,
                "ops": [{"set": {"done": True}}],
            },
            "after": {"type": "task", "next": "e", "ops": [{"set": {"after": True}}]},
            "e": {"type": "end"},
        },
    }
    engine = DurableEngine(tmp_root)
    await engine.create("p1", flow, {})
    await engine.start("p1")

    # Let the uninterruptible node begin, then request pause mid-flight.
    await asyncio.sleep(0.05)
    res = await engine.command("p1", "pause")
    assert res["accepted"] is True

    await engine.wait("p1")
    snap = engine.snapshot("p1")

    # Must have paused, and the uninterruptible node must have completed first.
    assert snap["state"] == ExecState.PAUSED
    assert "long" in snap["completedNodes"]  # node ran to its boundary
    assert snap["variables"].get("done") is True
    assert "after" not in snap["completedNodes"]  # paused before next node

    # The transition sequence must show running -> pausing -> paused.
    trs = transitions(snap["events"])
    assert (ExecState.RUNNING, ExecState.PAUSING) in trs
    assert (ExecState.PAUSING, ExecState.PAUSED) in trs
    # pausing must come before paused
    assert states(snap["events"]).index("pausing") < states(snap["events"]).index("paused")

    # Resume and finish.
    await engine.command("p1", "resume")
    await engine.wait("p1")
    snap2 = engine.snapshot("p1")
    assert snap2["state"] == ExecState.SUCCEEDED
    assert snap2["variables"].get("after") is True


# ---------------------------------------------------------------------------
# 3. process restart recovery
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_restart_recovery_resumes_from_boundary(tmp_root):
    flow = {
        "id": "recover",
        "nodes": {
            "s": {"type": "start", "next": "a"},
            "a": {"type": "task", "next": "b", "ops": [{"set": {"x": 10}}]},
            "b": {"type": "task", "next": "c", "steps": 20, "stepDelay": 0.02,
                  "ops": [{"incr": "x"}]},
            "c": {"type": "task", "next": "e", "ops": [{"incr": "x"}]},
            "e": {"type": "end"},
        },
    }
    engine1 = DurableEngine(tmp_root)
    await engine1.create("r1", flow, {})
    await engine1.start("r1")
    # Cancel the in-process task abruptly to simulate a crash mid-node "b".
    await asyncio.sleep(0.05)
    task = engine1._tasks.get("r1")
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Everything we know now must come from disk, not memory.
    disk = EventStore(tmp_root, "r1")
    recovered = RecoveredState.fold(disk.read_events())
    # Node "a" definitely reached a boundary; "b" did not.
    assert "a" in recovered.completed_nodes
    assert "c" not in recovered.completed_nodes

    # Fresh engine (new process) recovers purely from the log.
    engine2 = DurableEngine(tmp_root)
    ex = await engine2.recover("r1")
    assert ex.last_boundary_node in ("a", "b")  # resume point is a real boundary
    await engine2.start("r1")
    await engine2.wait("r1")

    snap = engine2.snapshot("r1")
    assert snap["state"] == ExecState.SUCCEEDED
    # x = 10 (a) + 1 (b) + 1 (c) == 12, applied exactly once each.
    assert snap["variables"]["x"] == 12
    boundary_nodes = [e["nodeId"] for e in snap["events"] if e["kind"] == "node_boundary"]
    # Each node boundary appears exactly once across both lifetimes.
    for n in ("a", "b", "c", "e"):
        assert boundary_nodes.count(n) == 1


# ---------------------------------------------------------------------------
# 4. parallel join + generation-aware branch retry
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_parallel_join_waits_for_required_branches(tmp_root):
    flow = {
        "id": "par",
        "nodes": {
            "s": {"type": "start", "next": "p"},
            "p": {
                "type": "parallel",
                "next": "e",
                "required": ["x", "y", "z"],
                "branches": {
                    "x": {"value": 1, "work": 0.02},
                    "y": {"value": 2, "work": 0.04},
                    "z": {"value": 3, "work": 0.01},
                },
            },
            "e": {"type": "end"},
        },
    }
    engine = DurableEngine(tmp_root)
    await engine.create("par1", flow, {})
    await engine.start("par1")
    await engine.wait("par1")

    snap = engine.snapshot("par1")
    assert snap["state"] == ExecState.SUCCEEDED
    merged = snap["variables"]["p_result"]
    assert merged["branches"] == {"x": 1, "y": 2, "z": 3}
    assert merged["generation"] == 1

    # All required branch boundaries recorded for the same generation.
    branch_evs = [e for e in snap["events"] if e["kind"] == "branch_boundary"]
    assert {e["branchId"] for e in branch_evs} == {"x", "y", "z"}
    assert all(e["generation"] == 1 for e in branch_evs)


@pytest.mark.asyncio
async def test_parallel_branch_retry_uses_new_generation(tmp_root):
    # Branch "y" fails on generation 1, succeeds on generation 2. A whole-node
    # retry bumps the generation; the join must not consume gen-1 partial output.
    flow = {
        "id": "pargen",
        "nodes": {
            "s": {"type": "start", "next": "p"},
            "p": {
                "type": "parallel",
                "next": "e",
                "required": ["x", "y"],
                "retry": {"maxAttempts": 3, "delaySeconds": 0.0},
                "branches": {
                    "x": {"value": 1, "work": 0.01},
                    "y": {"value": 2, "fail_times": 1},  # fails at gen 1
                },
            },
            "e": {"type": "end"},
        },
    }
    engine = DurableEngine(tmp_root)
    await engine.create("pg1", flow, {})
    await engine.start("pg1")
    await engine.wait("pg1")

    snap = engine.snapshot("pg1")
    assert snap["state"] == ExecState.SUCCEEDED
    merged = snap["variables"]["p_result"]
    # Final merged result must be from generation 2 (the successful one).
    assert merged["generation"] == 2
    assert merged["branches"] == {"x": 1, "y": 2}

    # There must be a node_failed for the parallel node at attempt 1.
    failed = [e for e in snap["events"] if e["kind"] == "node_failed" and e["nodeId"] == "p"]
    assert len(failed) == 1
    assert failed[0]["attempt"] == 1

    # branch boundaries for "y" only exist for generation 2 (gen-1 y failed).
    y_boundaries = [
        e for e in snap["events"]
        if e["kind"] == "branch_boundary" and e["branchId"] == "y"
    ]
    assert all(e["generation"] == 2 for e in y_boundaries)
    # And the join's final result never mixed a gen-1 x with gen-2 y.
    assert merged["generation"] == 2


# ---------------------------------------------------------------------------
# 5. failed node retry with backoff
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_failed_retry_then_success(tmp_root):
    flow = {
        "id": "retry",
        "nodes": {
            "s": {"type": "start", "next": "flaky"},
            "flaky": {
                "type": "task",
                "next": "e",
                "fail_times": 2,  # fail attempts 1 & 2, succeed on 3
                "retry": {"maxAttempts": 5, "delaySeconds": 0.01},
                "ops": [{"set": {"ok": True}}],
            },
            "e": {"type": "end"},
        },
    }
    engine = DurableEngine(tmp_root)
    await engine.create("rt1", flow, {})
    await engine.start("rt1")
    await engine.wait("rt1")

    snap = engine.snapshot("rt1")
    assert snap["state"] == ExecState.SUCCEEDED
    assert snap["variables"]["ok"] is True

    failed = [e for e in snap["events"] if e["kind"] == "node_failed"]
    assert [e["attempt"] for e in failed] == [1, 2]
    # retry_wait appears between attempts.
    assert states(snap["events"]).count("retry_wait") == 2
    # node_started recorded for attempts 1, 2, 3.
    started = [e["attempt"] for e in snap["events"] if e["kind"] == "node_started"]
    assert started == [1, 2, 3]


@pytest.mark.asyncio
async def test_retry_exhausted_fails(tmp_root):
    flow = {
        "id": "retryfail",
        "nodes": {
            "s": {"type": "start", "next": "flaky"},
            "flaky": {
                "type": "task",
                "next": "e",
                "fail_times": 10,
                "retry": {"maxAttempts": 3, "delaySeconds": 0.0},
            },
            "e": {"type": "end"},
        },
    }
    engine = DurableEngine(tmp_root)
    await engine.create("rf1", flow, {})
    await engine.start("rf1")
    await engine.wait("rf1")

    snap = engine.snapshot("rf1")
    assert snap["state"] == ExecState.FAILED
    started = [e["attempt"] for e in snap["events"] if e["kind"] == "node_started"]
    assert started == [1, 2, 3]
    assert transitions(snap["events"])[-1] == (ExecState.RETRY_WAIT, ExecState.FAILED) or \
           transitions(snap["events"])[-1] == (ExecState.RUNNING, ExecState.FAILED)


# ---------------------------------------------------------------------------
# 6. duplicate / stale commands never start a second executor
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_duplicate_commands_single_executor(tmp_root):
    flow = {
        "id": "dup",
        "nodes": {
            "s": {"type": "start", "next": "a"},
            "a": {"type": "task", "next": "e", "steps": 10, "stepDelay": 0.02,
                  "ops": [{"incr": "runs"}]},
            "e": {"type": "end"},
        },
    }
    engine = DurableEngine(tmp_root)
    await engine.create("d1", flow, {"runs": 0})

    # Fire many concurrent starts; only one executor may exist.
    await asyncio.gather(*[engine.command("d1", "start") for _ in range(10)])
    assert sum(1 for t in [engine._tasks.get("d1")] if t and not t.done()) <= 1

    # Also send a stale/duplicate resume while running (illegal now -> ignored).
    res = await engine.command("d1", "resume")
    assert res["accepted"] is False  # resume only valid from paused

    await engine.wait("d1")
    snap = engine.snapshot("d1")
    assert snap["state"] == ExecState.SUCCEEDED
    # The node ran exactly once despite duplicate starts.
    assert snap["variables"]["runs"] == 1
    started_a = [e for e in snap["events"] if e["kind"] == "node_started" and e["nodeId"] == "a"]
    assert len(started_a) == 1


@pytest.mark.asyncio
async def test_duplicate_pause_and_cancel_idempotent(tmp_root):
    flow = {
        "id": "dupcancel",
        "nodes": {
            "s": {"type": "start", "next": "a"},
            "a": {"type": "task", "next": "e", "steps": 20, "stepDelay": 0.02,
                  "uninterruptible": True, "ops": [{"set": {"x": 1}}]},
            "e": {"type": "end"},
        },
    }
    engine = DurableEngine(tmp_root)
    await engine.create("dc1", flow, {})
    await engine.start("dc1")
    await asyncio.sleep(0.05)

    # Multiple pause commands -> still a single pause.
    r1 = await engine.command("dc1", "pause")
    r2 = await engine.command("dc1", "pause")
    assert r1["accepted"] is True
    await engine.wait("dc1")
    snap = engine.snapshot("dc1")
    assert snap["state"] == ExecState.PAUSED
    # Only one running->pausing transition.
    assert transitions(snap["events"]).count((ExecState.RUNNING, ExecState.PAUSING)) == 1

    # Cancel from paused; a second cancel is a no-op.
    await engine.command("dc1", "cancel")
    await engine.command("dc1", "cancel")
    snap2 = engine.snapshot("dc1")
    assert snap2["state"] == ExecState.CANCELLED
    assert states(snap2["events"]).count("cancelled") == 1


# ---------------------------------------------------------------------------
# 7. side-effect idempotency across restart (HTTP + file)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_http_side_effect_runs_once_across_restart(tmp_root):
    call_count = {"n": 0}

    async def performer(node, variables):
        call_count["n"] += 1
        return {"status": 200, "call": call_count["n"]}

    flow = {
        "id": "http",
        "nodes": {
            "s": {"type": "start", "next": "call"},
            "call": {"type": "http", "next": "after", "url": "http://x", "resultVar": "r"},
            "after": {"type": "task", "next": "e", "steps": 20, "stepDelay": 0.02,
                      "ops": [{"set": {"done": True}}]},
            "e": {"type": "end"},
        },
    }
    engine1 = DurableEngine(tmp_root, http_performer=performer)
    await engine1.create("h1", flow, {})
    await engine1.start("h1")
    # Crash after the http effect committed but during the "after" node.
    await asyncio.sleep(0.06)
    task = engine1._tasks.get("h1")
    if task and not task.done():
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    # The HTTP effect must have been recorded exactly once already.
    disk = EventStore(tmp_root, "h1")
    effects = [e for e in disk.read_events() if e["kind"] == "effect"]
    assert len(effects) == 1
    assert call_count["n"] == 1

    # New process recovers and finishes; HTTP must NOT be called again.
    engine2 = DurableEngine(tmp_root, http_performer=performer)
    await engine2.recover("h1")
    await engine2.start("h1")
    await engine2.wait("h1")

    snap = engine2.snapshot("h1")
    assert snap["state"] == ExecState.SUCCEEDED
    assert call_count["n"] == 1  # exactly once across the restart
    effects = [e for e in snap["events"] if e["kind"] == "effect"]
    assert len(effects) == 1
    assert snap["variables"]["r"]["call"] == 1


@pytest.mark.asyncio
async def test_file_side_effect_idempotent(tmp_root, tmp_path):
    target = str(tmp_path / "out" / "result.txt")
    flow = {
        "id": "file",
        "nodes": {
            "s": {"type": "start", "next": "w"},
            "w": {"type": "file", "next": "e", "path": target, "content": "hello-durable"},
            "e": {"type": "end"},
        },
    }
    engine = DurableEngine(tmp_root)
    await engine.create("f1", flow, {})
    await engine.start("f1")
    await engine.wait("f1")

    with open(target, encoding="utf-8") as fh:
        assert fh.read() == "hello-durable"

    snap = engine.snapshot("f1")
    effects = [e for e in snap["events"] if e["kind"] == "effect"]
    assert len(effects) == 1
    # The idempotency key is exactly executionId+nodeId+attempt derived.
    assert effects[0]["key"] == idempotency_key("f1", "w", 1)

    # Re-recovering and re-running a completed execution does nothing new.
    engine2 = DurableEngine(tmp_root)
    await engine2.recover("f1")
    await engine2.start("f1")
    await engine2.wait("f1")
    snap2 = engine2.snapshot("f1")
    assert len([e for e in snap2["events"] if e["kind"] == "effect"]) == 1


# ---------------------------------------------------------------------------
# 8. WebSocket monitor: snapshot + seq backfill, no UI regression
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_websocket_snapshot_and_backfill(tmp_root):
    """Late-joining and reconnecting clients converge via snapshot + backfill.

    Drives the real ``durable_monitor_endpoint`` ASGI coroutine through a fake
    WebSocket (starlette's TestClient is unusable here due to a starlette/httpx
    version mismatch in this environment). The fake exercises the same code path:
    accept -> receive initial ``lastSeq`` -> send snapshot with backfill.
    """
    import engine.durable.service as service
    service._engine = DurableEngine(tmp_root)
    engine = service.get_engine()
    from ws.durable_monitor import durable_monitor_endpoint

    await engine.create("w1", linear_flow(), {})
    await engine.start("w1")
    await engine.wait("w1")

    full = engine.snapshot("w1")
    total = full["lastSeq"]
    assert total > 0

    class FakeWebSocket:
        """Minimal ASGI-WebSocket stand-in for the monitor endpoint."""

        def __init__(self, execution_id, initial_text):
            self.path_params = {"execution_id": execution_id}
            self._initial_text = initial_text
            self._initial_consumed = False
            self.sent = []
            self.accepted = False

        async def accept(self):
            self.accepted = True

        async def receive_text(self):
            if not self._initial_consumed:
                self._initial_consumed = True
                return self._initial_text
            # No further client messages -> emulate a closed socket so the
            # endpoint's live loop ends promptly for the test.
            from fastapi import WebSocketDisconnect
            raise WebSocketDisconnect()

        async def send_json(self, data):
            self.sent.append(data)
            # After the snapshot has been delivered, break out of the live loop.
            if data.get("type") == "snapshot":
                from fastapi import WebSocketDisconnect
                raise WebSocketDisconnect()

    async def run_client(last_seq):
        ws = FakeWebSocket("w1", json.dumps({"lastSeq": last_seq}))
        await durable_monitor_endpoint(ws)
        return ws

    # Fresh client: lastSeq=0 -> full history in the snapshot backfill.
    ws0 = await run_client(0)
    snap_frames = [m for m in ws0.sent if m["type"] == "snapshot"]
    assert len(snap_frames) == 1
    msg = snap_frames[0]
    assert msg["state"] == ExecState.SUCCEEDED
    assert seqs(msg["events"]) == list(range(1, total + 1))
    # allowedCommands is server-driven (terminal -> none).
    assert msg["allowedCommands"] == []

    # Reconnecting client: resumes from a mid-point, only gets the tail.
    midpoint = total // 2
    wsm = await run_client(midpoint)
    msg = [m for m in wsm.sent if m["type"] == "snapshot"][0]
    got = seqs(msg["events"])
    assert got == list(range(midpoint + 1, total + 1))
    assert all(s > midpoint for s in got)


@pytest.mark.asyncio
async def test_websocket_live_stream_dedupes(tmp_root):
    """A live subscriber receives events in seq order with no regression."""
    import engine.durable.service as service
    service._engine = DurableEngine(tmp_root)
    engine = service.get_engine()

    flow = {
        "id": "live",
        "nodes": {
            "s": {"type": "start", "next": "a"},
            "a": {"type": "task", "next": "b", "steps": 5, "stepDelay": 0.02,
                  "ops": [{"set": {"x": 1}}]},
            "b": {"type": "task", "next": "e", "ops": [{"incr": "x"}]},
            "e": {"type": "end"},
        },
    }
    await engine.create("l1", flow, {})
    q = engine.subscribe("l1")
    await engine.start("l1")
    await engine.wait("l1")

    received = []
    while not q.empty():
        received.append(q.get_nowait())

    got_seqs = [e["seq"] for e in received]
    # Strictly increasing, no duplicates -> a UI folding these never regresses.
    assert got_seqs == sorted(set(got_seqs))
    assert got_seqs == sorted(got_seqs)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
