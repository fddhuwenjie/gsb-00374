"""Process-wide durable execution engine + monitor pub/sub.

The engine is the single entry point used by REST/WebSocket layers and tests. It
guarantees, per ``executionId``:
  * at most one live :class:`DurableExecution` object, and
  * at most one running asyncio task (the *single executor* invariant).

Commands are idempotent and safe to repeat or arrive stale:
  * ``start``  -> only launches a task if none is running and state is queued.
  * ``resume`` -> only relaunches if paused; a duplicate resume is a no-op.
  * ``pause``/``cancel`` -> flip a flag on the existing runner; never spawn work.

Monitors (WebSocket clients) subscribe to per-execution event streams. Because
every event carries a monotonic ``seq`` and is durably logged first, a monitor
can take a snapshot then replay any events it missed by ``seq`` -- late joiners
and reconnecting clients converge without ever regressing.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Callable, Dict, List, Optional

from engine.durable.event_store import EventStore, RecoveredState
from engine.durable.models import DurableFlow
from engine.durable.runner import DurableExecution, HttpPerformer
from engine.durable.state_machine import ExecState, allowed_commands
from engine.durable.versioning import (
    FlowVersionStore,
    MissingVersionError,
    VersionInUseError,
    content_hash,
    diff_specs,
)
from engine.durable.approvals import (
    DECISION_APPROVED,
    DECISION_REJECTED,
    DECISION_TIMEOUT,
    VALID_DECISIONS,
    TokenError,
    decode_token,
    is_expired,
    issue_token,
)


class DurableEngine:
    def __init__(self, root_dir: str, http_performer: Optional[HttpPerformer] = None):
        self.root_dir = root_dir
        self.http_performer = http_performer
        self.versions = FlowVersionStore(root_dir)
        self._executions: Dict[str, DurableExecution] = {}
        self._tasks: Dict[str, asyncio.Task] = {}
        self._guard = asyncio.Lock()
        # executionId -> list of asyncio.Queue for live monitors
        self._subscribers: Dict[str, List[asyncio.Queue]] = {}

    # ------------------------------------------------------------------
    # monitor pub/sub
    # ------------------------------------------------------------------

    def _notify(self, execution_id: str, event: Dict[str, Any]) -> None:
        for q in list(self._subscribers.get(execution_id, [])):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass

    def subscribe(self, execution_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._subscribers.setdefault(execution_id, []).append(q)
        return q

    def unsubscribe(self, execution_id: str, q: asyncio.Queue) -> None:
        subs = self._subscribers.get(execution_id)
        if subs and q in subs:
            subs.remove(q)

    def snapshot(self, execution_id: str, after_seq: int = 0) -> Dict[str, Any]:
        """Return a monitor snapshot: current state + events after ``after_seq``.

        Late-joining and reconnecting clients call this with the last ``seq`` they
        saw (0 for a fresh join) and then continue on the live stream.
        """
        store = EventStore(self.root_dir, execution_id)
        events = store.read_events(after_seq=after_seq)
        all_events = store.read_events()
        recovered = RecoveredState.fold(all_events)
        header = store.read_header() or {}
        # Expose pending approvals with a freshly signed recovery token each, so
        # a late/reconnecting client can act on the current pending state.
        pending = []
        for aid, req in recovered.pending_approvals.items():
            token = issue_token(
                self.root_dir,
                {
                    "approvalId": aid,
                    "executionId": execution_id,
                    "nodeId": req.get("nodeId"),
                    "branchId": req.get("branchId"),
                    "generation": req.get("generation"),
                    "attempt": req.get("attempt"),
                    "flowVersion": req.get("flowVersion"),
                },
                req.get("deadline"),
            )
            pending.append({**req, "token": token})
        return {
            "executionId": execution_id,
            "flowId": header.get("flowId"),
            "flowVersion": header.get("flowVersion"),
            "contentHash": header.get("contentHash"),
            "state": recovered.state,
            "prevState": recovered.prev_state,
            "lastSeq": recovered.last_seq,
            "variables": recovered.variables,
            "completedNodes": recovered.completed_nodes,
            "allowedCommands": sorted(allowed_commands(recovered.state)),
            "pendingApprovals": pending,
            "events": events,
        }

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    async def create(
        self,
        execution_id: str,
        flow_spec: Optional[Dict[str, Any]] = None,
        variables: Optional[Dict[str, Any]] = None,
        flow_id: Optional[str] = None,
        flow_version: Optional[int] = None,
    ) -> DurableExecution:
        """Create + persist a new queued execution bound to an immutable version.

        Either pass ``flow_spec`` (a new/edited definition, which is materialised
        as a version) or pass ``flow_id`` + ``flow_version`` to bind an existing
        version. The execution's header pins ``flowVersion`` and ``contentHash``
        so recovery/retry/backfill always use that exact definition even after the
        flow is later edited into newer versions.
        """
        async with self._guard:
            if execution_id in self._executions:
                return self._executions[execution_id]

            store = EventStore(self.root_dir, execution_id)
            existing = store.read_events()
            if existing:
                # Already exists on disk -> recover instead of clobbering.
                return await self._load_locked(execution_id)

            # Resolve the immutable version this execution binds to.
            if flow_spec is not None:
                record = self.versions.create_version(flow_spec["id"], flow_spec)
            elif flow_id is not None:
                target = flow_version or self.versions.latest_version(flow_id)
                record = self.versions.require_version(flow_id, target)
            else:
                raise ValueError("create requires flow_spec or flow_id")

            bound_spec = record["spec"]
            flow = DurableFlow(bound_spec)
            store.write_header(
                {
                    "executionId": execution_id,
                    "flowId": flow.id,
                    "flowVersion": record["version"],
                    "contentHash": record["contentHash"],
                    "nodeHashes": record["nodeHashes"],
                    "variables": variables or {},
                    "createdAt": time.time(),
                }
            )
            recovered = RecoveredState()
            recovered.variables = dict(variables or {})
            execution = DurableExecution(
                self, execution_id, flow, store, recovered,
                flow_version=record["version"],
            )
            self._executions[execution_id] = execution
            return execution

    async def _load_locked(self, execution_id: str) -> DurableExecution:
        store = EventStore(self.root_dir, execution_id)
        header = store.read_header()
        if header is None:
            raise KeyError(f"No such execution: {execution_id}")

        # Recover strictly against the ORIGINAL bound version. If it is missing
        # from the version store we refuse to recover rather than silently run a
        # different definition.
        flow_id = header.get("flowId")
        version = header.get("flowVersion")
        if version is not None:
            try:
                record = self.versions.require_version(flow_id, version)
            except MissingVersionError as e:
                raise MissingVersionError(
                    f"Cannot recover execution {execution_id}: bound flow "
                    f"{flow_id} v{version} is missing"
                ) from e
            bound_spec = record["spec"]
            # Defensive: the pinned content hash must still match the stored one.
            if header.get("contentHash") and header["contentHash"] != record["contentHash"]:
                raise MissingVersionError(
                    f"Cannot recover execution {execution_id}: bound content hash "
                    f"for {flow_id} v{version} does not match the version store"
                )
        else:
            # Legacy executions without a bound version fall back to the header
            # spec if present.
            bound_spec = header.get("flow")
            if bound_spec is None:
                raise MissingVersionError(
                    f"Cannot recover execution {execution_id}: no bound version "
                    f"and no embedded flow spec"
                )

        flow = DurableFlow(bound_spec)
        events = store.read_events()
        recovered = RecoveredState.fold(events)
        if not recovered.variables and not recovered.completed_nodes:
            recovered.variables = dict(header.get("variables", {}))
        execution = DurableExecution(
            self, execution_id, flow, store, recovered,
            flow_version=header.get("flowVersion"),
        )
        self._executions[execution_id] = execution
        return execution

    async def recover(self, execution_id: str) -> DurableExecution:
        """Reconstruct an execution from disk into a *fresh* engine.

        This simulates a process restart: no in-memory task state is trusted; the
        state is folded from ``events.jsonl`` and resume happens from the last
        node boundary.
        """
        async with self._guard:
            # Drop any stale in-memory handle so we truly re-read from disk.
            self._executions.pop(execution_id, None)
            return await self._load_locked(execution_id)

    async def start(self, execution_id: str) -> DurableExecution:
        """Start (or resume) execution, enforcing the single-executor invariant."""
        async with self._guard:
            execution = self._executions.get(execution_id)
            if execution is None:
                execution = await self._load_locked(execution_id)

            self._launch_if_idle(execution_id, execution)
            return execution

    def _launch_if_idle(self, execution_id: str, execution: DurableExecution) -> None:
        existing = self._tasks.get(execution_id)
        if existing is not None and not existing.done():
            # A runner already owns this execution -> do NOT start a second one.
            return
        if execution.state in ExecState.TERMINAL:
            return
        task = asyncio.create_task(self._run_wrapper(execution_id, execution))
        self._tasks[execution_id] = task
        execution._task = task

    async def _run_wrapper(self, execution_id: str, execution: DurableExecution) -> None:
        try:
            await execution.run()
        finally:
            # Clear the task slot so a later resume can relaunch.
            if self._tasks.get(execution_id) is execution._task:
                self._tasks.pop(execution_id, None)

    async def wait(self, execution_id: str) -> None:
        task = self._tasks.get(execution_id)
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)

    # ------------------------------------------------------------------
    # commands
    # ------------------------------------------------------------------

    async def command(self, execution_id: str, command: str) -> Dict[str, Any]:
        """Dispatch a client command; always safe to repeat / arrive stale."""
        async with self._guard:
            execution = self._executions.get(execution_id)
            if execution is None:
                execution = await self._load_locked(execution_id)

            accepted = False
            if command == "start":
                if execution.state == ExecState.QUEUED:
                    self._launch_if_idle(execution_id, execution)
                    accepted = True
            elif command == "resume":
                if execution.state == ExecState.PAUSED:
                    self._launch_if_idle(execution_id, execution)
                    accepted = True
            elif command == "pause":
                accepted = execution.request_pause()
            elif command == "cancel":
                accepted = execution.request_cancel()

            return {
                "executionId": execution_id,
                "command": command,
                "accepted": accepted,
                "state": execution.state,
                "allowedCommands": sorted(allowed_commands(execution.state)),
            }

    def get(self, execution_id: str) -> Optional[DurableExecution]:
        return self._executions.get(execution_id)

    # ------------------------------------------------------------------
    # human approvals
    # ------------------------------------------------------------------

    async def respond_approval(
        self,
        token: str,
        decision: str,
        approver: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Apply an approve/reject decision presented via a recovery token.

        The token is verified, then checked against the *live* pending state:
          * a forged / malformed token is rejected,
          * an expired token (now > deadline) is rejected as a timeout,
          * a stale token (its approvalId is no longer pending -- e.g. already
            resolved, or the node was retried into a new generation) is rejected,
          * the first valid decision wins; duplicates / races are no-ops.

        Because the decision is keyed by approvalId, a decision for one parallel
        branch/generation can never release another.
        """
        if decision not in (DECISION_APPROVED, DECISION_REJECTED):
            return {"accepted": False, "reason": "invalid_decision"}

        try:
            payload = decode_token(self.root_dir, token)
        except TokenError as e:
            return {"accepted": False, "reason": f"bad_token:{e}"}

        execution_id = payload.get("executionId")
        aid = payload.get("approvalId")
        deadline = payload.get("deadline")

        async with self._guard:
            execution = self._executions.get(execution_id)
            if execution is None:
                try:
                    execution = await self._load_locked(execution_id)
                except (KeyError, MissingVersionError) as e:
                    return {"accepted": False, "reason": f"no_execution:{e}"}

            # Must still be a live pending approval (guards stale / duplicate).
            if aid not in execution.pending_approvals:
                return {
                    "accepted": False,
                    "reason": "stale_or_resolved",
                    "state": execution.state,
                }

            # Expired token -> record a timeout instead of the requested decision.
            if is_expired(deadline):
                execution.resolve_approval(aid, DECISION_TIMEOUT, approver=None)
                return {
                    "accepted": False,
                    "reason": "expired",
                    "state": execution.state,
                }

            accepted = execution.resolve_approval(aid, decision, approver)
            return {
                "executionId": execution_id,
                "approvalId": aid,
                "accepted": accepted,
                "decision": decision if accepted else None,
                "state": execution.state,
                "allowedCommands": sorted(allowed_commands(execution.state)),
            }

    async def expire_due_approvals(self, execution_id: str) -> int:
        """Force-timeout any pending approvals whose deadline has elapsed.

        Returns the number of approvals expired. This is what a background
        sweeper (or a test) calls to make deadlines fire deterministically even
        when no live waiter is running (e.g. right after a restart).
        """
        async with self._guard:
            execution = self._executions.get(execution_id)
            if execution is None:
                try:
                    execution = await self._load_locked(execution_id)
                except (KeyError, MissingVersionError):
                    return 0
            expired = 0
            for aid, req in list(execution.pending_approvals.items()):
                if is_expired(req.get("deadline")):
                    if execution.resolve_approval(aid, DECISION_TIMEOUT, approver=None):
                        expired += 1
                        # If no live run task is blocked on this approval (e.g. we
                        # recovered but haven't relaunched), a top-level approval's
                        # awaiting_approval -> failed transition has no waiter to
                        # drive it, so drive it here as a backstop.
                        task = self._tasks.get(execution_id)
                        no_task = task is None or task.done()
                        if (
                            no_task
                            and req.get("branchId") is None
                            and execution.state == ExecState.AWAITING_APPROVAL
                        ):
                            execution._transition(ExecState.FAILED)
            return expired

    # ------------------------------------------------------------------
    # flow definition versioning
    # ------------------------------------------------------------------

    def register_flow(self, flow_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Create (or reuse) a version for ``flow_spec``; return its record."""
        return self.versions.create_version(flow_spec["id"], flow_spec)

    def edit_flow(self, flow_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Edit a flow.

        Editing never mutates an existing version -- it only ever creates a new
        one (or returns the identical latest version for a no-op edit). Any
        executions already running against an older version are entirely
        unaffected: they keep their pinned ``flowVersion`` in their header.
        """
        return self.versions.create_version(flow_spec["id"], flow_spec)

    def get_version(self, flow_id: str, version: int) -> Optional[Dict[str, Any]]:
        return self.versions.get_version(flow_id, version)

    def list_versions(self, flow_id: str) -> List[int]:
        return self.versions.list_versions(flow_id)

    def diff_versions(self, flow_id: str, from_v: int, to_v: int) -> Dict[str, Any]:
        return self.versions.diff_versions(flow_id, from_v, to_v)

    def executions_using(self, flow_id: str, version: int) -> List[str]:
        """Return the ids of all executions bound to a specific flow version.

        Scans persisted execution headers on disk (not just in-memory handles) so
        deletion protection holds across process restarts.
        """
        using: List[str] = []
        durable_dir = os.path.join(self.root_dir, "durable")
        if not os.path.isdir(durable_dir):
            return using
        for name in os.listdir(durable_dir):
            if name == "versions":
                continue
            exec_dir = os.path.join(durable_dir, name)
            if not os.path.isdir(exec_dir):
                continue
            header_path = os.path.join(exec_dir, "header.json")
            if not os.path.exists(header_path):
                continue
            try:
                import json as _json
                with open(header_path, "r", encoding="utf-8") as f:
                    header = _json.load(f)
            except Exception:
                continue
            if header.get("flowId") == flow_id and header.get("flowVersion") == version:
                using.append(header.get("executionId", name))
        return using

    def delete_version(self, flow_id: str, version: int, force: bool = False) -> Dict[str, Any]:
        """Delete a flow version, refusing if executions still bind to it.

        This protects the immutability guarantee: an execution must always be able
        to recover against its original definition, so a version in use cannot be
        removed unless ``force`` is explicitly set.
        """
        using = self.executions_using(flow_id, version)
        if using and not force:
            raise VersionInUseError(flow_id, version, using)
        self.versions.delete_version(flow_id, version)
        return {"flowId": flow_id, "version": version, "deleted": True, "wasUsedBy": using}

    def export_flow(self, flow_id: str) -> Dict[str, Any]:
        return self.versions.export_flow(flow_id)

    def import_flow(self, bundle: Dict[str, Any], overwrite: bool = False) -> Dict[str, Any]:
        return self.versions.import_flow(bundle, overwrite=overwrite)
