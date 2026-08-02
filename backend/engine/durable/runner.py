"""The durable runner and the process-wide execution engine.

``DurableEngine`` owns a registry of live executions in the current process. It
enforces the *single executor* invariant: for any ``executionId`` there is at
most one running asyncio task. Repeated ``start``/``resume`` commands, stale
commands, and duplicate ``pause``/``cancel`` requests can never spin up a second
executor -- they either no-op or flip a flag the existing runner observes.

Every meaningful moment is persisted through :class:`EventStore`, so:
  * WebSocket monitors can be served purely from the event log (snapshot + seq
    backfill), and
  * a fresh process can ``recover`` an execution and resume from the last
    completed node boundary without any in-memory state.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional

from engine.durable.event_store import EventStore, RecoveredState
from engine.durable.effects import EffectRunner, atomic_file_write
from engine.durable.models import DurableFlow, apply_ops
from engine.durable.approvals import (
    DECISION_APPROVED,
    DECISION_REJECTED,
    DECISION_TIMEOUT,
    approval_id,
)
from engine.durable.state_machine import (
    ExecState,
    TransitionError,
    allowed_commands,
    check_transition,
)


class NodeExecutionError(Exception):
    def __init__(self, node_id: str, message: str):
        self.node_id = node_id
        super().__init__(message)


class ApprovalSuspend(Exception):
    """Internal signal: a node (or a parallel branch) is blocked on a human
    approval. It is *not* a failure -- it unwinds the current node execution so
    the run loop can leave the execution durably in ``awaiting_approval`` until a
    decision (or a timeout) arrives.
    """


# Optional injectable HTTP performer so tests can count real outbound calls
# without hitting the network. Signature: (node, variables) -> result dict.
HttpPerformer = Callable[[Dict[str, Any], Dict[str, Any]], Awaitable[Dict[str, Any]]]


class DurableExecution:
    """Live, in-process handle for one durable execution."""

    def __init__(
        self,
        engine: "DurableEngine",
        execution_id: str,
        flow: DurableFlow,
        store: EventStore,
        recovered: RecoveredState,
        flow_version: Optional[int] = None,
    ):
        self.engine = engine
        self.execution_id = execution_id
        self.flow = flow
        self.store = store
        self.flow_version = flow_version

        # ---- durable, folded-from-log state ----
        self.state: str = recovered.state
        self.variables: Dict[str, Any] = dict(recovered.variables)
        self.completed_nodes: List[str] = list(recovered.completed_nodes)
        self.effects_ledger: Dict[str, Any] = dict(recovered.effects)
        self.attempts: Dict[str, int] = dict(recovered.attempts)
        self.started_attempt: Dict[str, int] = dict(recovered.started_attempt)
        self.failed_attempts: Dict[str, set] = {
            k: set(v) for k, v in recovered.failed_attempts.items()
        }
        self.boundary_flushed: set = set(recovered.boundary_flushed)
        self.branch_boundaries: Dict[str, int] = dict(recovered.branch_boundaries)
        self.last_boundary_node: Optional[str] = recovered.last_boundary_node

        # ---- approvals (durably folded; resolutions replayed on recovery) ----
        self.pending_approvals: Dict[str, Dict[str, Any]] = dict(
            recovered.pending_approvals
        )
        self.resolved_approvals: Dict[str, Dict[str, Any]] = dict(
            recovered.resolved_approvals
        )
        # Live per-approval waiter events (never persisted): a resolution wakes
        # only the matching approvalId, so a parallel branch's decision releases
        # that branch/generation alone.
        self._approval_waiters: Dict[str, asyncio.Event] = {}

        # ---- control flags (never persisted; recomputed each life) ----
        self._pause_requested = False
        self._cancel_requested = False
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

        self.effect_runner = EffectRunner(
            execution_id, self.effects_ledger, self._commit_effect
        )

    # ------------------------------------------------------------------
    # event emission
    # ------------------------------------------------------------------

    def _emit(self, kind: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        ev = self.store.append(kind, payload, time.time())
        self.engine._notify(self.execution_id, ev)
        return ev

    def _transition(self, to_state: str) -> None:
        """Persist a state transition, rejecting illegal ones."""
        check_transition(self.state, to_state)  # raises TransitionError if illegal
        prev = self.state
        self.state = to_state
        self._emit("state", {"prevState": prev, "state": to_state})

    def _commit_effect(self, key: str, node_id: str, attempt: int, result: Any) -> None:
        self._emit(
            "effect",
            {"key": key, "nodeId": node_id, "attempt": attempt, "result": result},
        )

    def _flush_boundary(self, node_id: str) -> None:
        if node_id not in self.completed_nodes:
            self.completed_nodes.append(node_id)
        self.last_boundary_node = node_id
        self._emit(
            "node_boundary",
            {"nodeId": node_id, "variables": dict(self.variables)},
        )

    # ------------------------------------------------------------------
    # commands (all idempotent / guarded)
    # ------------------------------------------------------------------

    def request_pause(self) -> bool:
        """Request a pause. Returns True if accepted given current state."""
        if "pause" not in allowed_commands(self.state):
            return False
        self._pause_requested = True
        return True

    def request_cancel(self) -> bool:
        if "cancel" not in allowed_commands(self.state):
            return False
        self._cancel_requested = True
        # Wake any approval waiters so a wait blocked on a deadline unblocks now
        # and the run loop can honour the cancellation promptly.
        for ev in self._approval_waiters.values():
            ev.set()
        # If not currently running under a task (e.g. paused/queued), finalize now.
        if self._task is None or self._task.done():
            if self.state != ExecState.CANCELLED:
                self._transition(ExecState.CANCELLED)
        return True

    def can(self, command: str) -> bool:
        return command in allowed_commands(self.state)

    # ------------------------------------------------------------------
    # the run loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Advance the flow until a terminal or paused state.

        Resumes from ``last_boundary_node`` if present, otherwise from start.
        Only ever called by the engine, which guarantees single ownership.
        """
        try:
            if self.state == ExecState.QUEUED:
                self._transition(ExecState.RUNNING)
            elif self.state in (ExecState.PAUSED, ExecState.RETRY_WAIT):
                self._transition(ExecState.RUNNING)
            elif self.state in (ExecState.RUNNING, ExecState.AWAITING_APPROVAL):
                # crashed while running, or resuming a durable approval wait: the
                # blocking node is re-entered below and re-arms its own wait.
                pass
            else:
                return  # terminal; nothing to do

            current = self._resume_point()

            while current is not None:
                node = self.flow.node(current)
                ntype = node.get("type")

                if ntype == "end":
                    self._flush_boundary(current)
                    self._transition(ExecState.SUCCEEDED)
                    return

                # --- honour cancel at each boundary ---
                if self._cancel_requested:
                    self._transition(ExecState.CANCELLED)
                    return

                # --- honour a pause that arrived while interruptible ---
                if self._pause_requested and self.state == ExecState.RUNNING:
                    # We're at a clean boundary between nodes -> pause directly.
                    self._transition(ExecState.PAUSED)
                    self._pause_requested = False
                    return

                if ntype == "start":
                    self._flush_boundary(current)
                    current = self.flow.next_of(current)
                    continue

                if ntype == "approval":
                    await self._run_approval_node(current, node)
                    if self.state in ExecState.TERMINAL:
                        return
                    if self.state == ExecState.AWAITING_APPROVAL:
                        # Still blocked (e.g. re-armed after restart); the wait
                        # returns here only on resolution, so this is unreachable
                        # in normal flow -- guard defensively.
                        return
                    current = self.flow.next_of(current)
                    continue

                # Execute the node (may be uninterruptible / may retry).
                await self._run_node(current, node)

                if self.state in ExecState.TERMINAL or self.state == ExecState.PAUSED:
                    return

                current = self.flow.next_of(current)

        except ApprovalSuspend:
            # A cancel interrupted an approval wait. Honour the cancellation as a
            # legal terminal transition (the approval stays unresolved).
            if self._cancel_requested and self.state not in ExecState.TERMINAL:
                self._transition(ExecState.CANCELLED)
            return
        except TransitionError:
            # An illegal transition is a programming error; surface as failure.
            if self.state not in ExecState.TERMINAL:
                try:
                    self._transition(ExecState.FAILED)
                except TransitionError:
                    pass
            raise

    def _resume_point(self) -> str:
        """Where to resume: the node after the last flushed boundary."""
        if self.last_boundary_node is None:
            return self.flow.start_id
        nxt = self.flow.next_of(self.last_boundary_node)
        return nxt if nxt is not None else self.last_boundary_node

    async def _run_node(self, node_id: str, node: Dict[str, Any]) -> None:
        ntype = node.get("type")
        retry_cfg = node.get("retry") or {}
        max_attempts = int(retry_cfg.get("maxAttempts", 1))
        delay = float(retry_cfg.get("delaySeconds", 0.0))

        # Recovery-safe attempt numbering: the next attempt is driven by how many
        # attempts have *durably failed*. If a prior life started attempt N,
        # committed its side effect, but crashed before the boundary flush, then
        # N is NOT in failed_attempts, so we re-run attempt N -> same idempotency
        # key -> the committed effect is replayed, never repeated.
        failed = self.failed_attempts.get(node_id, set())
        attempt = len(failed)

        while True:
            attempt += 1
            self.attempts[node_id] = attempt
            # Avoid a duplicate node_started for an attempt already recorded.
            if self.started_attempt.get(node_id, 0) < attempt:
                self._emit("node_started", {"nodeId": node_id, "attempt": attempt})
                self.started_attempt[node_id] = attempt

            uninterruptible = bool(node.get("uninterruptible"))
            try:
                await self._execute_node_body(node_id, node, attempt)
            except NodeExecutionError as exc:
                self._emit(
                    "node_failed",
                    {"nodeId": node_id, "attempt": attempt, "error": str(exc)},
                )
                self.failed_attempts.setdefault(node_id, set()).add(attempt)
                if attempt >= max_attempts:
                    self._transition(ExecState.FAILED)
                    return
                # Enter retry_wait, honour pause/cancel across the backoff.
                self._transition(ExecState.RETRY_WAIT)
                if self._cancel_requested:
                    self._transition(ExecState.CANCELLED)
                    return
                if self._pause_requested:
                    self._transition(ExecState.PAUSED)
                    self._pause_requested = False
                    return
                if delay > 0:
                    await asyncio.sleep(delay)
                if self._cancel_requested:
                    self._transition(ExecState.CANCELLED)
                    return
                self._transition(ExecState.RUNNING)
                continue

            # Node body succeeded. Flush the boundary durably.
            self._flush_boundary(node_id)

            # Honour a pause request observed for this node. If the node was
            # uninterruptible and the pause arrived mid-work, we are already in
            # PAUSING; otherwise transition RUNNING -> PAUSING now so the pending
            # intent is always visible in the event log before we settle on
            # PAUSED at this clean boundary.
            if self._pause_requested:
                if self.state == ExecState.RUNNING:
                    self._transition(ExecState.PAUSING)
                self._transition(ExecState.PAUSED)
                self._pause_requested = False
            return

    # ------------------------------------------------------------------
    # human approval node
    # ------------------------------------------------------------------

    async def _run_approval_node(self, node_id: str, node: Dict[str, Any]) -> None:
        """Suspend the execution on a top-level human approval node.

        The approval is bound to (executionId, nodeId, branchId=None, generation,
        attempt, flowVersion). Generation/attempt come from the durable attempt
        counter so a re-request after a restart reuses the *same* approvalId and
        matches any token already issued. Blocks in ``awaiting_approval`` until a
        decision or timeout arrives, then either resumes (approved) or fails.
        """
        attempt = self.attempts.get(node_id, 0) + 1
        self.attempts[node_id] = attempt
        decision = await self._await_approval(
            node_id, node, branch_id=None, generation=attempt, attempt=attempt
        )
        if decision == DECISION_APPROVED:
            # Approved: apply any ops and flush the boundary so recovery moves on.
            apply_ops(self.variables, node.get("ops", []))
            self._flush_boundary(node_id)
            if self.state != ExecState.RUNNING:
                self._transition(ExecState.RUNNING)
        else:
            # Rejected or timed out -> the execution fails.
            if self.state != ExecState.FAILED:
                self._transition(ExecState.FAILED)

    async def _await_approval(
        self,
        node_id: str,
        node: Dict[str, Any],
        branch_id: Optional[str],
        generation: int,
        attempt: int,
    ) -> str:
        """Emit (once) an approval request, then wait for its resolution.

        Returns the decision string. Recovery-safe: if a resolution for this exact
        approvalId is already in the durable log, it is returned immediately
        without re-requesting or re-waiting.
        """
        aid = approval_id(
            self.execution_id, node_id, branch_id, generation, attempt, self.flow_version
        )

        # Already resolved durably (e.g. resolved before a crash) -> replay it.
        resolved = self.resolved_approvals.get(aid)
        if resolved is not None:
            return resolved["decision"]

        # Compute / recover the deadline. If we already have a pending record
        # (recovered from disk), keep its original deadline; otherwise derive one.
        pending = self.pending_approvals.get(aid)
        now = time.time()
        if pending is not None and pending.get("deadline") is not None:
            deadline = pending["deadline"]
        else:
            timeout = node.get("timeoutSeconds")
            deadline = (now + float(timeout)) if timeout is not None else None

        # Emit the request only if it is not already pending on disk (idempotent
        # across restarts -- the folded state tells us whether it exists).
        if pending is None:
            self._emit(
                "approval_requested",
                {
                    "approvalId": aid,
                    "executionId": self.execution_id,
                    "nodeId": node_id,
                    "branchId": branch_id,
                    "generation": generation,
                    "attempt": attempt,
                    "flowVersion": self.flow_version,
                    "deadline": deadline,
                    "prompt": node.get("prompt"),
                },
            )
            self.pending_approvals[aid] = {
                "approvalId": aid,
                "nodeId": node_id,
                "branchId": branch_id,
                "generation": generation,
                "attempt": attempt,
                "deadline": deadline,
            }

        # Move the execution into awaiting_approval (only for the top-level node;
        # a branch approval keeps the parallel node running but the execution
        # state is set by the branch coordinator -- see _run_parallel).
        if branch_id is None and self.state == ExecState.RUNNING:
            self._transition(ExecState.AWAITING_APPROVAL)

        # Arm a waiter and block until resolved or the deadline elapses.
        event = self._approval_waiters.setdefault(aid, asyncio.Event())
        while aid not in self.resolved_approvals:
            if self._cancel_requested:
                # Cancellation wins; leave resolution to the run loop / engine.
                raise ApprovalSuspend()
            timeout_left = None
            if deadline is not None:
                timeout_left = max(0.0, deadline - time.time())
                if timeout_left <= 0:
                    # Deadline passed with no decision -> self-timeout.
                    self._resolve_locally(
                        aid, node_id, branch_id, generation, DECISION_TIMEOUT, approver=None
                    )
                    break
            try:
                await asyncio.wait_for(event.wait(), timeout=timeout_left)
            except asyncio.TimeoutError:
                # Loop back: the top of the loop will detect the elapsed deadline.
                continue
            event.clear()

        return self.resolved_approvals[aid]["decision"]

    def _resolve_locally(
        self,
        aid: str,
        node_id: str,
        branch_id: Optional[str],
        generation: int,
        decision: str,
        approver: Optional[str],
    ) -> None:
        """Persist a resolution for a pending approval (idempotent).

        A second call for an already-resolved approval is a no-op, so duplicate
        responses and races between multiple approvers collapse to the single
        first-writer-wins decision recorded in the log.
        """
        if aid in self.resolved_approvals:
            return
        if aid not in self.pending_approvals:
            # Unknown / stale approval id -> ignore (illegal per state machine).
            return
        self._emit(
            "approval_resolved",
            {
                "approvalId": aid,
                "nodeId": node_id,
                "branchId": branch_id,
                "generation": generation,
                "decision": decision,
                "approver": approver,
            },
        )
        self.resolved_approvals[aid] = {
            "approvalId": aid,
            "decision": decision,
            "approver": approver,
            "nodeId": node_id,
            "branchId": branch_id,
            "generation": generation,
        }
        self.pending_approvals.pop(aid, None)
        waiter = self._approval_waiters.get(aid)
        if waiter is not None:
            waiter.set()

    def resolve_approval(
        self, aid: str, decision: str, approver: Optional[str]
    ) -> bool:
        """Externally resolve a pending approval by its id.

        Returns True if this call is the one that recorded the decision, False if
        the approval was unknown/stale or already resolved (duplicate / late).
        """
        if aid in self.resolved_approvals or aid not in self.pending_approvals:
            return False
        pending = self.pending_approvals[aid]
        self._resolve_locally(
            aid,
            pending["nodeId"],
            pending.get("branchId"),
            pending.get("generation"),
            decision,
            approver,
        )
        return True

    async def _execute_node_body(
        self, node_id: str, node: Dict[str, Any], attempt: int
    ) -> None:
        ntype = node.get("type")

        if ntype == "task":
            fail_times = int(node.get("fail_times", 0))
            if attempt <= fail_times:
                raise NodeExecutionError(node_id, f"task failed on attempt {attempt}")
            await self._do_task_work(node)
            apply_ops(self.variables, node.get("ops", []))

        elif ntype == "http":
            await self._run_http(node_id, node, attempt)

        elif ntype == "file":
            await self._run_file(node_id, node, attempt)

        elif ntype == "parallel":
            await self._run_parallel(node_id, node, attempt)

        else:
            raise NodeExecutionError(node_id, f"Unknown node type: {ntype}")

    async def _do_task_work(self, node: Dict[str, Any]) -> None:
        """Perform a task node's (simulated) work.

        The work is a number of ``steps`` of ``stepDelay`` seconds. If the node is
        ``uninterruptible`` and a pause request lands *during* the work, we record
        RUNNING -> PAUSING immediately but do NOT abort -- we run every step to
        completion so the node reaches a clean boundary before pausing. This is
        the pause-race the durable engine must handle correctly.
        """
        uninterruptible = bool(node.get("uninterruptible"))
        work = float(node.get("work", 0.0))
        steps = int(node.get("steps", 1 if work > 0 else 0))
        step_delay = float(node.get("stepDelay", work if steps <= 1 else work / max(steps, 1)))

        for _ in range(steps):
            if (
                self._pause_requested
                and uninterruptible
                and self.state == ExecState.RUNNING
            ):
                # Pause arrived mid-node: mark pending, keep working to boundary.
                self._transition(ExecState.PAUSING)
            if step_delay > 0:
                await asyncio.sleep(step_delay)
        # Non-stepped fixed work (e.g. simple sleep).
        if steps == 0 and work > 0:
            await asyncio.sleep(work)

    # ------------------------------------------------------------------
    # side-effect nodes (idempotent)
    # ------------------------------------------------------------------

    async def _run_http(self, node_id: str, node: Dict[str, Any], attempt: int) -> None:
        fail_times = int(node.get("fail_times", 0))
        if attempt <= fail_times:
            # Failure happens *before* the effect commits, so no ledger entry and
            # the outbound call is counted only when it actually succeeds.
            raise NodeExecutionError(node_id, f"http failed on attempt {attempt}")

        async def _do() -> Dict[str, Any]:
            performer = self.engine.http_performer
            if performer is not None:
                return await performer(node, self.variables)
            # Default: no real network; echo a deterministic result.
            return {"status": 200, "url": node.get("url"), "attempt": attempt}

        result = await self.effect_runner.run(node_id, attempt, _do, is_async=True)
        result_var = node.get("resultVar", f"{node_id}_result")
        self.variables[result_var] = result

    async def _run_file(self, node_id: str, node: Dict[str, Any], attempt: int) -> None:
        fail_times = int(node.get("fail_times", 0))
        if attempt <= fail_times:
            raise NodeExecutionError(node_id, f"file write failed on attempt {attempt}")

        from engine.durable.effects import idempotency_key

        path = node["path"]
        content = node.get("content", "")

        def _do() -> Dict[str, Any]:
            key = idempotency_key(self.execution_id, node_id, attempt)
            return atomic_file_write(path, content, key)

        result = await self.effect_runner.run(node_id, attempt, _do, is_async=False)
        result_var = node.get("resultVar", f"{node_id}_result")
        self.variables[result_var] = result

    # ------------------------------------------------------------------
    # parallel node with generation-aware join
    # ------------------------------------------------------------------

    async def _run_parallel(self, node_id: str, node: Dict[str, Any], attempt: int) -> None:
        """Run branches concurrently; join waits for *required* branches of the
        *current generation*. A whole-node retry bumps the generation so results
        from a previous generation are never consumed by the join.
        """
        # Generation == this node's attempt number. Each retry is a new gen.
        generation = attempt
        branches: Dict[str, Any] = node.get("branches", {})
        required: List[str] = node.get("required", list(branches.keys()))

        results: Dict[str, Any] = {}
        errors: Dict[str, str] = {}

        # If any branch requires approval and is not already resolved for this
        # generation, the execution as a whole is awaiting_approval. A branch
        # approval is keyed by (node_id, branch_id, generation), so a decision
        # releases only its own branch/generation and never a sibling or a stale
        # generation.
        needs_approval = False
        for bid, spec in branches.items():
            if spec.get("requiresApproval"):
                aid = approval_id(
                    self.execution_id, node_id, bid, generation, generation, self.flow_version
                )
                if aid not in self.resolved_approvals:
                    needs_approval = True
        if needs_approval and self.state == ExecState.RUNNING:
            self._transition(ExecState.AWAITING_APPROVAL)

        async def _run_branch(branch_id: str, spec: Dict[str, Any]) -> None:
            fail_times = int(spec.get("fail_times", 0))
            # Branch failure is per-generation: a branch retry (via node retry)
            # sees a fresh generation and does not read prior-gen output.
            if generation <= fail_times:
                errors[branch_id] = f"branch {branch_id} failed at gen {generation}"
                return
            # Human approval for this specific branch + generation.
            if spec.get("requiresApproval"):
                decision = await self._await_approval(
                    node_id, spec, branch_id=branch_id,
                    generation=generation, attempt=generation,
                )
                if decision != DECISION_APPROVED:
                    errors[branch_id] = (
                        f"branch {branch_id} {decision} at gen {generation}"
                    )
                    return
            work = float(spec.get("work", 0.0))
            if work > 0:
                await asyncio.sleep(work)
            value = spec.get("value")
            results[branch_id] = {"generation": generation, "value": value}
            self._emit(
                "branch_boundary",
                {
                    "parallelNodeId": node_id,
                    "branchId": branch_id,
                    "generation": generation,
                    "value": value,
                },
            )

        await asyncio.gather(
            *[_run_branch(bid, branches[bid]) for bid in branches]
        )

        # All branch approvals (if any) are resolved now; return to running so the
        # join / boundary flush proceed under a legal state.
        if self.state == ExecState.AWAITING_APPROVAL:
            self._transition(ExecState.RUNNING)

        # Join: every required branch must have produced a result for THIS gen.
        missing = [
            bid
            for bid in required
            if results.get(bid, {}).get("generation") != generation
        ]
        if missing:
            raise NodeExecutionError(
                node_id,
                f"parallel join incomplete at gen {generation}; missing {missing}; "
                f"errors={errors}",
            )

        merged = {bid: results[bid]["value"] for bid in required}
        result_var = node.get("resultVar", f"{node_id}_result")
        self.variables[result_var] = {"generation": generation, "branches": merged}
