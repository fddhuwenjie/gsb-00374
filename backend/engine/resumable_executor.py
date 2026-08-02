import asyncio
import hashlib
import os
import sqlite3
import time
import uuid
from typing import Any, Dict, List, Optional

import httpx

from models.flow import FlowNode
from engine.ast_eval import evaluate_expression, ASTEvaluationError
from engine.sandbox import execute_python_sandbox, SandboxError
from engine.executor import ExecutionError
from engine.state_machine import ExecutionStateMachine
from storage.execution_journal import ExecutionJournal, RecoveryState

MAX_LOOP_COUNT = 10000


class ExecutionCancelled(Exception):
    pass


class RunnerControl:
    """In-process signals from the command side to a live runner. Never used
    as a source of truth for recovery — the journal is."""

    def __init__(self):
        self.resume_event = asyncio.Event()
        self.cancel_event = asyncio.Event()

    def signal_resume(self) -> None:
        self.resume_event.set()

    def signal_cancel(self) -> None:
        self.cancel_event.set()
        self.resume_event.set()


class ApprovalRegistry:
    """In-process waiters for pending approvals, keyed by
    (execution_id, token). A resolved token releases exactly its own waiter —
    a response can never release another branch's or generation's approval."""

    def __init__(self):
        self._events: Dict[Any, asyncio.Event] = {}

    def register(self, key: Any) -> asyncio.Event:
        event = asyncio.Event()
        self._events[key] = event
        return event

    def signal(self, key: Any) -> None:
        event = self._events.get(key)
        if event is not None:
            event.set()

    def signal_all(self, execution_id: str) -> None:
        for key, event in self._events.items():
            if isinstance(key, tuple) and key[0] == execution_id:
                event.set()

    def unregister(self, key: Any) -> None:
        self._events.pop(key, None)


class SideEffectGateway:
    """Performs real side effects. Tests substitute a counting gateway to
    assert exactly how many times each effect physically happened."""

    async def perform(self, node: FlowNode, idempotency_key: str,
                      variables: Dict[str, Any]) -> Any:
        if node.type == 'http':
            return await self._perform_http(node, idempotency_key)
        if node.type == 'sql':
            return await self._perform_sql(node)
        if node.type == 'filewrite':
            return self._perform_filewrite(node)
        raise ExecutionError(f"Node type {node.type} has no side effect handler")

    async def _perform_http(self, node: FlowNode, idempotency_key: str) -> Any:
        config = node.data.httpConfig
        if not config:
            raise ExecutionError("HTTP config is empty")
        headers = dict(config.headers or {})
        headers['Idempotency-Key'] = idempotency_key
        timeout = config.timeout or 30.0
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.request(
                    method=config.method,
                    url=config.url,
                    headers=headers,
                    content=config.body,
                )
                result = {
                    'status_code': response.status_code,
                    'text': response.text,
                    'json': None,
                }
                try:
                    result['json'] = response.json()
                except Exception:
                    pass
                return result
        except Exception as e:
            raise ExecutionError(f"HTTP request failed: {e}")

    async def _perform_sql(self, node: FlowNode) -> Any:
        config = node.data.sqlConfig
        if not config:
            raise ExecutionError("SQL config is empty")

        loop = asyncio.get_event_loop()

        def _run():
            conn = sqlite3.connect(config.connectionString)
            conn.row_factory = sqlite3.Row
            try:
                cursor = conn.cursor()
                cursor.execute(config.query, config.params or [])
                if config.query.strip().upper().startswith('SELECT'):
                    result = [dict(row) for row in cursor.fetchall()]
                else:
                    conn.commit()
                    result = {'rowcount': cursor.rowcount, 'lastrowid': cursor.lastrowid}
                cursor.close()
                return result
            finally:
                conn.close()

        try:
            return await loop.run_in_executor(None, _run)
        except Exception as e:
            raise ExecutionError(f"SQL execution failed: {e}")

    def _perform_filewrite(self, node: FlowNode) -> Any:
        config = node.data.fileConfig
        if not config:
            raise ExecutionError("FileWrite config is empty")
        directory = os.path.dirname(os.path.abspath(config.path))
        os.makedirs(directory, exist_ok=True)
        tmp_path = config.path + '.tmp'
        with open(tmp_path, 'w', encoding='utf-8') as f:
            f.write(config.content or '')
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, config.path)
        return {'path': config.path, 'bytes': len((config.content or '').encode('utf-8'))}


SIDE_EFFECT_NODE_TYPES = {'http', 'sql', 'filewrite'}


class ResumableExecutor:
    """Executes a flow node by node, persisting a journal boundary after every
    completed node. Pause/cancel are honored only at persisted boundaries
    (nodes are uninterruptible); recovery replays the journal and resumes from
    the most recent complete boundary."""

    def __init__(self, journal: ExecutionJournal, sm: ExecutionStateMachine,
                 gateway: Optional[SideEffectGateway] = None,
                 approval_registry: Optional[ApprovalRegistry] = None):
        self.journal = journal
        self.sm = sm
        self.gateway = gateway or SideEffectGateway()
        self.approval_registry = approval_registry or ApprovalRegistry()
        self.flow = journal.flow()
        self.nodes = {n.id: n for n in self.flow.nodes}
        self.outgoing: Dict[str, list] = {}
        for edge in self.flow.edges:
            self.outgoing.setdefault(edge.source, []).append(edge)

        self.variables: Dict[str, Any] = {}
        self.loop_counts: Dict[str, int] = {}

    # ------------------------------------------------------------------
    async def run(self, control: RunnerControl) -> RecoveryState:
        recovery = self.journal.recover()
        self.variables = dict(recovery.boundary_variables) if recovery.boundary_node_id \
            else self.journal.initial_variables()
        self.loop_counts = dict(recovery.boundary_loop_counts)

        if recovery.boundary_node_id is None:
            current = self._start_node()
        else:
            boundary_node = self.nodes.get(recovery.boundary_node_id)
            if boundary_node and boundary_node.type == 'end':
                self._finish()
                return self.journal.recover()
            if not recovery.boundary_next_node_id:
                self._finish()
                return self.journal.recover()
            current = self.nodes[recovery.boundary_next_node_id]

        while current.type != 'end':
            await self._boundary_control(control)

            attempt = self.journal.completed_attempts(current.id) + 1
            self.journal.append_node_started(current.id, attempt)
            try:
                next_node = await self._execute_with_retry(current, attempt, control)
            except ExecutionCancelled:
                return self.journal.recover()
            except Exception as e:
                self.journal.append_node_failed(current.id, attempt, str(e))
                # status may be running or pausing here; both may fail
                if self.sm.status != 'failed':
                    self.sm.transition('failed', reason=str(e), node_id=current.id)
                return self.journal.recover()

            self.journal.append_node_completed(
                current.id, attempt,
                next_node_id=next_node.id if next_node else None,
                variables=self.variables,
                loop_counts=self.loop_counts,
            )
            # Pause only takes effect after the boundary is durable.
            await self._boundary_control(control)
            if next_node is None:
                self._finish()
                return self.journal.recover()
            current = next_node

        # end node boundary
        attempt = self.journal.completed_attempts(current.id) + 1
        self.journal.append_node_started(current.id, attempt)
        self.journal.append_node_completed(
            current.id, attempt, next_node_id=None,
            variables=self.variables, loop_counts=self.loop_counts,
        )
        self._finish()
        return self.journal.recover()

    def _finish(self) -> None:
        if self.sm.status == 'cancelled':
            return
        if self.sm.status == 'pausing':
            # Pause request raced with the final boundary: pause wins.
            self.sm.transition('paused', reason='pause requested at final boundary')
            return
        if self.sm.status != 'succeeded':
            self.sm.transition('succeeded')

    # ------------------------------------------------------------------
    async def _boundary_control(self, control: RunnerControl) -> None:
        if self.sm.status == 'pausing':
            self.sm.transition('paused', reason='node boundary persisted')
        while self.sm.status == 'paused':
            control.resume_event.clear()
            await control.resume_event.wait()
            # resume command moves paused -> running before signalling;
            # cancel moves -> cancelled.
        if self.sm.status == 'cancelled' or control.cancel_event.is_set():
            raise ExecutionCancelled()

    async def _interruptible_sleep(self, seconds: float, control: RunnerControl) -> None:
        remaining = seconds
        while remaining > 0:
            if control.cancel_event.is_set() or self.sm.status == 'cancelled':
                raise ExecutionCancelled()
            slice_s = min(0.02, remaining)
            await asyncio.sleep(slice_s)
            remaining -= slice_s

    # ------------------------------------------------------------------
    async def _execute_with_retry(self, node: FlowNode, attempt: int,
                                  control: RunnerControl) -> Optional[FlowNode]:
        retry = node.data.retry
        max_attempts = retry.maxAttempts if retry else 1
        delay = retry.delaySeconds if retry else 0.0

        last_error: Optional[Exception] = None
        for i in range(max_attempts):
            try:
                return await self._execute_node(node, attempt, control, sub_attempt=i)
            except ExecutionCancelled:
                raise
            except Exception as e:
                last_error = e
                if i < max_attempts - 1:
                    if self.sm.status in ('running', 'awaiting_approval'):
                        self.sm.transition('retry_wait', reason=str(e), node_id=node.id)
                    if retry and retry.backoff == 'exponential':
                        delay = min(delay * 2, retry.maxDelaySeconds)
                    await self._interruptible_sleep(delay, control)
                    if self.sm.status == 'retry_wait':
                        self.sm.transition('running', reason='retry delay elapsed', node_id=node.id)
        raise last_error  # type: ignore[misc]

    async def _execute_node(self, node: FlowNode, attempt: int,
                            control: RunnerControl,
                            sub_attempt: int = 0) -> Optional[FlowNode]:
        ntype = node.type
        if ntype == 'start':
            return self._next(node)
        if ntype == 'task':
            await self._execute_task(node)
            return self._next(node)
        if ntype in SIDE_EFFECT_NODE_TYPES:
            await self._execute_side_effect(node, attempt, generation=attempt)
            return self._next(node)
        if ntype == 'wait':
            await self._interruptible_sleep(node.data.seconds or 0, control)
            return self._next(node)
        if ntype == 'condition':
            result = self._eval_bool(node)
            return self._next(node, result=result)
        if ntype == 'loop':
            count = self.loop_counts.get(node.id, 0)
            if count >= MAX_LOOP_COUNT:
                raise ExecutionError(f"Infinite loop detected at node {node.id}")
            result = self._eval_bool(node)
            if result:
                self.loop_counts[node.id] = count + 1
                return self._next(node, handle='loop')
            return self._next(node, handle='exit')
        if ntype == 'parallel':
            await self._execute_parallel(node, attempt, sub_attempt, control)
            return self._next(node)
        if ntype == 'approval':
            result = await self._execute_approval(node, attempt, attempt, control)
            self.variables[node.id + '_result'] = result
            return self._next(node)
        raise ExecutionError(f"Unsupported node type in resumable executor: {ntype}")

    # ------------------------------------------------------------------
    async def _execute_approval(self, node: FlowNode, attempt: Any,
                                generation: Any,
                                control: RunnerControl) -> None:
        """Human approval gate. The request is bound to
        executionId + nodeId + attempt + flowVersion with an expiry deadline,
        all persisted in the journal. A restart re-arms the wait with the
        original token and deadline."""
        exec_id = self.journal.execution_id
        flow_version = self.journal.flow_version()

        # Already decided (e.g. resolved just before a crash)?
        resolved = self.journal.approval_resolved_for(node.id, attempt)
        if resolved is None:
            pending = self.journal.pending_approval_for(node.id, attempt)
            if pending is not None:
                token, deadline = pending['token'], pending['deadline']
            else:
                config = node.data.approvalConfig
                approvers = config.approvers if config else []
                timeout_s = config.timeoutSeconds if config else 300.0
                token = hashlib.sha256(
                    f"{exec_id}:{node.id}:{attempt}:{flow_version}:{uuid.uuid4().hex}".encode()
                ).hexdigest()
                deadline = time.time() + timeout_s
                self.journal.append_approval_requested(
                    node.id, attempt, generation, token, approvers,
                    deadline, flow_version,
                )
            if self.sm.status == 'running':
                self.sm.transition('awaiting_approval',
                                   reason='approval requested', node_id=node.id)

            event = self.approval_registry.register((exec_id, token))
            try:
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise asyncio.TimeoutError()
                await asyncio.wait_for(event.wait(), remaining)
            except asyncio.TimeoutError:
                self.journal.append_approval_resolved(
                    node.id, attempt, generation, token, 'expired', None,
                )
                resolved = {'decision': 'expired', 'by': None}
            finally:
                self.approval_registry.unregister((exec_id, token))

            if control.cancel_event.is_set() or self.sm.status == 'cancelled':
                raise ExecutionCancelled()
            if resolved is None:
                resolved = self.journal.approval_resolved_for(node.id, attempt)
            if resolved is None:
                # spurious wake with no decision recorded: keep waiting is
                # impossible here safely, treat as expired
                resolved = {'decision': 'expired', 'by': None}

        if resolved.get('decision') == 'approved':
            # Release the gate only when no other approval is still pending.
            if not self.journal.pending_approvals() and \
                    self.sm.status == 'awaiting_approval':
                self.sm.transition('running', reason='approval granted',
                                   node_id=node.id)
            return {'decision': 'approved', 'by': resolved.get('by')}
        raise ExecutionError(
            f"Approval {resolved.get('decision')} for node {node.id}"
        )

    # ------------------------------------------------------------------
    async def _execute_task(self, node: FlowNode) -> None:
        code = node.data.code
        if not code:
            return
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None, execute_python_sandbox, code, self.variables, 5
            )
        except SandboxError as e:
            raise ExecutionError(f"Task execution failed: {e}")

    def _eval_bool(self, node: FlowNode) -> bool:
        expression = node.data.expression
        if not expression:
            raise ExecutionError(f"Expression is empty for node {node.id}")
        try:
            ctx = dict(self.variables)
            ctx['ctx'] = self.variables
            return bool(evaluate_expression(expression, ctx))
        except ASTEvaluationError as e:
            raise ExecutionError(f"Expression evaluation failed: {e}")

    async def _execute_side_effect(self, node: FlowNode, attempt: Any,
                                   generation: Any,
                                   variables: Optional[Dict[str, Any]] = None,
                                   journal_result: bool = True) -> Any:
        """Idempotency key derived from executionId + nodeId + attempt. A
        recorded success is reused instead of re-performing the effect, so
        crash recovery can never duplicate a successful side effect."""
        key = f"{self.journal.execution_id}:{node.id}:{attempt}"
        record = self.journal.side_effect(key)
        if record and record.get('status') == 'success':
            result = record['result']
        else:
            result = await self.gateway.perform(node, key, variables or self.variables)
            self.journal.append_side_effect(
                key, node.id, attempt, generation, node.type, result
            )
        if journal_result:
            self.variables[node.id + '_result'] = result
        return result

    # ------------------------------------------------------------------
    async def _execute_parallel(self, node: FlowNode, attempt: int,
                                sub_attempt: int,
                                control: RunnerControl) -> None:
        config = node.data.parallelConfig
        if not config or not config.branchNodeIds:
            return
        # Generation identifies this attempt of the parallel node. A retry
        # (internal or after failure) starts a new generation, and the join
        # below only ever merges results produced inside this gather() —
        # results of a previous generation are never consumed.
        generation = f"{attempt}.{sub_attempt}"

        async def run_branch(branch_id: str) -> Dict[str, Any]:
            branch_vars = dict(self.variables)
            current = self.nodes.get(branch_id)
            while current and current.type != 'end' and not control.cancel_event.is_set():
                if current.type == 'task':
                    code = current.data.code
                    if code:
                        loop = asyncio.get_event_loop()
                        try:
                            await loop.run_in_executor(
                                None, execute_python_sandbox, code, branch_vars, 5
                            )
                        except SandboxError as e:
                            raise ExecutionError(f"Task execution failed: {e}")
                elif current.type in SIDE_EFFECT_NODE_TYPES:
                    # Key is generation-scoped: a retry of the parallel node
                    # starts a new generation and must not consume results
                    # recorded by the previous generation.
                    result = await self._execute_side_effect(
                        current, generation, generation,
                        variables=branch_vars, journal_result=False,
                    )
                    branch_vars[current.id + '_result'] = result
                elif current.type == 'approval':
                    result = await self._execute_approval(
                        current, generation, generation, control)
                    branch_vars[current.id + '_result'] = result
                elif current.type == 'wait':
                    await self._interruptible_sleep(current.data.seconds or 0, control)
                elif current.type == 'condition':
                    ctx = dict(branch_vars)
                    ctx['ctx'] = branch_vars
                    try:
                        result = bool(evaluate_expression(current.data.expression or '', ctx))
                    except ASTEvaluationError as e:
                        raise ExecutionError(f"Condition evaluation failed: {e}")
                    current = self._next(current, result=result)
                    continue
                if current.data.anchorId:
                    break
                nxt = self._next(current)
                if nxt is None or nxt.type == 'parallel':
                    break
                current = nxt
            return branch_vars

        tasks = [asyncio.ensure_future(run_branch(bid))
                 for bid in config.branchNodeIds]
        # Join waits for this generation's branches only. On the first branch
        # failure the remaining branches (e.g. ones blocked on approval) are
        # cancelled so the parallel node can retry as a NEW generation; their
        # pending approval tokens become stale and can never release the new
        # generation's requests.
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        first_error: Optional[BaseException] = None
        for task in done:
            if task.exception() is not None and first_error is None:
                first_error = task.exception()
        if first_error is not None:
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            raise ExecutionError(
                f"Parallel branch failed (generation {generation}): {first_error}"
            )
        if control.cancel_event.is_set():
            raise ExecutionCancelled()

        results = [task.result() for task in tasks]
        merged: Dict[str, Any] = {}
        for i, branch_vars in enumerate(results):
            merged[f'branch_{i}'] = branch_vars
            for key, value in branch_vars.items():
                if key.endswith('_result'):
                    merged[key] = value
        merged['_generation'] = generation
        self.variables[node.id + '_result'] = merged
        self.variables.update(merged)

    # ------------------------------------------------------------------
    def _start_node(self) -> FlowNode:
        for node in self.flow.nodes:
            if node.type == 'start':
                return node
        raise ExecutionError("No Start node found")

    def _next(self, node: FlowNode, result: Optional[bool] = None,
              handle: Optional[str] = None) -> Optional[FlowNode]:
        outgoing = self.outgoing.get(node.id, [])
        if node.type == 'condition':
            want = 'true' if result else 'false'
            for edge in outgoing:
                if edge.sourceHandle == want:
                    return self.nodes[edge.target]
            raise ExecutionError(f"No {want} branch found for condition node {node.id}")
        if node.type == 'loop':
            for edge in outgoing:
                if handle == 'loop':
                    if edge.sourceHandle == 'loop':
                        return self.nodes[edge.target]
                else:
                    if edge.sourceHandle != 'loop':
                        return self.nodes[edge.target]
            raise ExecutionError(f"No appropriate edge found for loop node {node.id}")
        if len(outgoing) == 1:
            return self.nodes[outgoing[0].target]
        if len(outgoing) == 0:
            return None
        raise ExecutionError(f"Expected exactly 1 outgoing edge for {node.id}")
