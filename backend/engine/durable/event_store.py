"""Append-only, crash-safe event log + node-boundary checkpoints.

Layout (all under ``<root>/durable/<executionId>/``)::

    events.jsonl    one JSON object per line, in strict monotonic ``seq`` order
    header.json     immutable execution header (flowId, flow snapshot, inputs)

Everything about an execution's live state is derivable by folding the event
log. Recovery never trusts in-memory task state -- it re-reads ``events.jsonl``
and resumes only from the last *completed node boundary*.

Event kinds
-----------
``state``          a state-machine transition (carries prevState + new state)
``node_started``   a node began an attempt (nodeId, attempt, generation)
``node_boundary``  a node attempt *completed and its output is durably flushed*;
                   this is the ONLY point recovery is allowed to resume from
``node_failed``    a node attempt failed (may be followed by retry_wait/state)
``effect``         a side effect committed with its idempotency key + result
``branch_boundary``a parallel branch completed for a given generation

Each event has a monotonically increasing integer ``seq`` starting at 1, plus a
wall-clock ``ts``. The ``seq`` is the backbone of WebSocket backfill.
"""

from __future__ import annotations

import json
import os
import threading
from typing import Any, Dict, List, Optional

from engine.durable.state_machine import ExecState


class EventStore:
    """Per-execution append-only event log with fsync durability.

    A module-level lock table guards concurrent appends within one process. The
    file itself is opened in append mode and flushed+fsynced on every write so a
    crash (simulated by dropping the in-memory objects and re-opening) never
    loses a committed event.
    """

    _locks: Dict[str, threading.Lock] = {}
    _locks_guard = threading.Lock()

    def __init__(self, root_dir: str, execution_id: str):
        self.root_dir = root_dir
        self.execution_id = execution_id
        self.exec_dir = os.path.join(root_dir, "durable", execution_id)
        os.makedirs(self.exec_dir, exist_ok=True)
        self.events_path = os.path.join(self.exec_dir, "events.jsonl")
        self.header_path = os.path.join(self.exec_dir, "header.json")
        with EventStore._locks_guard:
            if execution_id not in EventStore._locks:
                EventStore._locks[execution_id] = threading.Lock()
            self._lock = EventStore._locks[execution_id]

    # ---- header -------------------------------------------------------

    def write_header(self, header: Dict[str, Any]) -> None:
        tmp = self.header_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(header, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.header_path)

    def read_header(self) -> Optional[Dict[str, Any]]:
        if not os.path.exists(self.header_path):
            return None
        with open(self.header_path, "r", encoding="utf-8") as f:
            return json.load(f)

    # ---- append -------------------------------------------------------

    def _next_seq(self) -> int:
        # Derive from the last persisted line so the counter survives restarts.
        last = 0
        if os.path.exists(self.events_path):
            with open(self.events_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        last = json.loads(line)["seq"]
                    except (json.JSONDecodeError, KeyError):
                        # Ignore a torn trailing line from a crash mid-write.
                        continue
        return last + 1

    def append(self, kind: str, payload: Dict[str, Any], ts: float) -> Dict[str, Any]:
        with self._lock:
            seq = self._next_seq()
            event = {"seq": seq, "kind": kind, "ts": ts, **payload}
            line = json.dumps(event, ensure_ascii=False)
            with open(self.events_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
                f.flush()
                os.fsync(f.fileno())
            return event

    # ---- read ---------------------------------------------------------

    def read_events(self, after_seq: int = 0) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        if not os.path.exists(self.events_path):
            return events
        with open(self.events_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue  # torn trailing line after a crash
                if ev.get("seq", 0) > after_seq:
                    events.append(ev)
        return events

    def last_seq(self) -> int:
        seq = 0
        for ev in self.read_events():
            seq = max(seq, ev.get("seq", 0))
        return seq


class RecoveredState:
    """Folded view of an execution reconstructed purely from the event log."""

    def __init__(self) -> None:
        self.state: str = ExecState.QUEUED
        self.prev_state: Optional[str] = None
        self.last_seq: int = 0
        # nodeId -> variables snapshot at the last completed boundary
        self.node_boundaries: Dict[str, Dict[str, Any]] = {}
        # ordered list of completed node ids (by boundary)
        self.completed_nodes: List[str] = []
        # committed side-effect idempotency keys -> result
        self.effects: Dict[str, Any] = {}
        # (nodeId) -> highest attempt number seen
        self.attempts: Dict[str, int] = {}
        # (nodeId) -> highest attempt that emitted node_started
        self.started_attempt: Dict[str, int] = {}
        # (nodeId) -> set of attempts that emitted node_failed
        self.failed_attempts: Dict[str, set] = {}
        # nodeIds whose boundary has been flushed
        self.boundary_flushed: set = set()
        # variables at the most recent boundary (the resume context)
        self.variables: Dict[str, Any] = {}
        # last node whose boundary was flushed (resume point)
        self.last_boundary_node: Optional[str] = None
        # parallel: (parallelNodeId, branchId) -> generation completed
        self.branch_boundaries: Dict[str, int] = {}
        self.loop_counts: Dict[str, int] = {}
        # approvalId -> pending approval request payload (unresolved only)
        self.pending_approvals: Dict[str, Dict[str, Any]] = {}
        # approvalId -> resolution payload (decision, approver, ...)
        self.resolved_approvals: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def fold(cls, events: List[Dict[str, Any]]) -> "RecoveredState":
        rs = cls()
        for ev in events:
            rs.last_seq = max(rs.last_seq, ev.get("seq", 0))
            kind = ev.get("kind")
            if kind == "state":
                rs.prev_state = ev.get("prevState")
                rs.state = ev.get("state", rs.state)
            elif kind == "node_started":
                nid = ev.get("nodeId")
                att = ev.get("attempt", 1)
                rs.attempts[nid] = max(rs.attempts.get(nid, 0), att)
                rs.started_attempt[nid] = max(rs.started_attempt.get(nid, 0), att)
            elif kind == "node_boundary":
                nid = ev.get("nodeId")
                rs.node_boundaries[nid] = ev.get("variables", {})
                rs.boundary_flushed.add(nid)
                if nid not in rs.completed_nodes:
                    rs.completed_nodes.append(nid)
                rs.variables = dict(ev.get("variables", {}))
                rs.last_boundary_node = nid
                if "loopCounts" in ev:
                    rs.loop_counts = dict(ev["loopCounts"])
            elif kind == "node_failed":
                nid = ev.get("nodeId")
                att = ev.get("attempt", 1)
                rs.attempts[nid] = max(rs.attempts.get(nid, 0), att)
                rs.failed_attempts.setdefault(nid, set()).add(att)
            elif kind == "effect":
                rs.effects[ev.get("key")] = ev.get("result")
            elif kind == "branch_boundary":
                key = f"{ev.get('parallelNodeId')}::{ev.get('branchId')}"
                rs.branch_boundaries[key] = ev.get("generation", 0)
            elif kind == "approval_requested":
                aid = ev.get("approvalId")
                # A request is pending until a matching resolution arrives.
                rs.pending_approvals[aid] = {
                    "approvalId": aid,
                    "executionId": ev.get("executionId"),
                    "nodeId": ev.get("nodeId"),
                    "branchId": ev.get("branchId"),
                    "generation": ev.get("generation"),
                    "attempt": ev.get("attempt"),
                    "flowVersion": ev.get("flowVersion"),
                    "deadline": ev.get("deadline"),
                    "requestedAt": ev.get("ts"),
                }
            elif kind == "approval_resolved":
                aid = ev.get("approvalId")
                rs.resolved_approvals[aid] = {
                    "approvalId": aid,
                    "decision": ev.get("decision"),
                    "approver": ev.get("approver"),
                    "nodeId": ev.get("nodeId"),
                    "branchId": ev.get("branchId"),
                    "generation": ev.get("generation"),
                    "resolvedAt": ev.get("ts"),
                }
                # Once resolved, it is no longer pending.
                rs.pending_approvals.pop(aid, None)
        return rs
