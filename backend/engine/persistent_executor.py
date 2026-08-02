import asyncio
import copy
import time
import uuid
from typing import Any, Dict, List, Optional

import httpx

from engine import state_machine as sm
from engine.ast_eval import evaluate_expression, ASTEvaluationError
from engine.sandbox import execute_python_sandbox, SandboxError
from models.flow import (
    ApprovalRequest,
    FlowDefinition,
    FlowNode,
    StateEvent,
    SideEffectRecord,
    PersistedExecutionRecord,
    TraceLog,
)


class NodeInterrupted(Exception):
    """Raised when an interruptible node is interrupted by pause/cancel."""


class PersistentExecutor:
    """Event-sourced, recoverable workflow executor.

    All state changes are persisted as monotonic events through the
    EventStore. Recovery resumes from the most recent node checkpoint;
    in-memory task state is never trusted after a restart.
    """

    def __init__(self, event_store, execution_id: str, flow_store=None):
        self.event_store = event_store
        self.execution_id = execution_id
        self.flow_store = flow_store

        self.pause_event = asyncio.Event()
        self.resume_event = asyncio.Event()
        self.cancel_event = asyncio.Event()

        self._approval_events: Dict[str, asyncio.Event] = {}
        self._approval_decisions: Dict[str, Dict[str, Any]] = {}

        self.nodes: Dict[str, FlowNode] = {}
        self.edges: List = []
        self._callbacks: Dict[str, Any] = {}

    def set_callback(self, name: str, fn) -> None:
        self._callbacks[name] = fn

    async def _emit(self, name: str, *args) -> None:
        fn = self._callbacks.get(name)
        if fn:
            try:
                await fn(*args)
            except Exception:
                pass

    # ----- persistence helpers -----

    def _load(self) -> PersistedExecutionRecord:
        record = self.event_store.load_record(self.execution_id)
        if record is None:
            raise RuntimeError(f"Execution {self.execution_id} not found")
        return record

    def _save_snapshot(self, record: PersistedExecutionRecord) -> None:
        snap = record.snapshot
        snap.allowedActions = sm.allowed_actions(snap.status)
        record.status = snap.status
        record.updatedAt = time.time()
        self.event_store.save_record(record)

    def _append(
        self,
        record: PersistedExecutionRecord,
        event_type: str,
        *,
        from_state=None,
        to_state=None,
        node_id: Optional[str] = None,
        attempt: Optional[int] = None,
        generation: Optional[int] = None,
        idempotency_key: Optional[str] = None,
        request_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> StateEvent:
        event = StateEvent(
            seq=0,
            executionId=self.execution_id,
            timestamp=0,
            eventType=event_type,
            fromState=from_state,
            toState=to_state,
            nodeId=node_id,
            attempt=attempt,
            generation=generation,
            idempotencyKey=idempotency_key,
            requestId=request_id,
            payload=payload or {},
        )
        self.event_store.append_event(event)
        record.seq = event.seq
        record.snapshot.seq = event.seq
        if to_state is not None:
            record.snapshot.status = to_state
            record.status = to_state
        return event

    def _transition(self, record, from_state, to_state, **payload) -> StateEvent:
        sm.assert_transition(from_state, to_state)
        ev = self._append(
            record, 'transition',
            from_state=from_state, to_state=to_state, payload=payload,
        )
        self._save_snapshot(record)
        return ev

    def _trace(self, record, node: FlowNode, action: str,
               variables: Dict[str, Any], message: Optional[str] = None) -> None:
        log = TraceLog(
            timestamp=time.time(),
            nodeId=node.id,
            nodeType=node.type,
            action=action,
            variables=copy.deepcopy(variables),
            message=message,
        )
        record.snapshot.trace.append(log)
        self._append(record, 'trace', node_id=node.id,
                     payload={'action': action, 'message': message})
        self._save_snapshot(record)

    # ----- control signals -----

    def request_pause(self) -> None:
        self.pause_event.set()

    def request_resume(self) -> None:
        self.resume_event.set()

    def request_cancel(self) -> None:
        self.cancel_event.set()
        self.resume_event.set()

    def sync_control_from_snapshot(self, snap) -> None:
        if snap.pauseRequested or snap.status == sm.PAUSING:
            self.pause_event.set()
        if snap.cancelRequested:
            self.cancel_event.set()

    def inject_approval_response(self, token: str, decision: str,
                                  responder: Optional[str] = None,
                                  comment: Optional[str] = None) -> None:
        """Called by ExecutionManager when an approval response arrives.
        Only signals the waiting coroutine; the manager handles all
        persistence and state transitions."""
        self._approval_decisions[token] = {
            'decision': decision,
            'responder': responder,
            'comment': comment,
        }
        event = self._approval_events.get(token)
        if event is not None:
            event.set()

    def _get_approval_event(self, token: str) -> asyncio.Event:
        if token not in self._approval_events:
            self._approval_events[token] = asyncio.Event()
        return self._approval_events[token]

    async def _wait_resume_or_cancel(self) -> None:
        self.resume_event.clear()
        while not self.cancel_event.is_set():
            record = self._load()
            if record.snapshot.cancelRequested:
                return
            if record.snapshot.status in (sm.RUNNING, sm.PAUSING):
                return
            try:
                await asyncio.wait_for(self.resume_event.wait(), timeout=0.2)
            except asyncio.TimeoutError:
                pass

    async def _wait_retry(self, record) -> None:
        snap = record.snapshot
        until = snap.retryUntil or 0
        self.resume_event.clear()
        while not self.cancel_event.is_set():
            record = self._load()
            if record.snapshot.cancelRequested:
                return
            if record.snapshot.status != sm.RETRY_WAIT:
                return
            now = time.time()
            if now >= until:
                return
            try:
                await asyncio.wait_for(
                    self.resume_event.wait(),
                    timeout=min(0.2, max(0.0, until - now)),
                )
                if record.snapshot.status == sm.RETRY_WAIT:
                    return
            except asyncio.TimeoutError:
                pass

    async def _wait_for_approval(self, record, advance_on_approve: bool = False) -> None:
        """Wait for an approval response, timeout, or cancellation.
        Used both by the main run loop (for recovery, with
        advance_on_approve=True) and by _exec_approval when first entering
        the approval node (advance_on_approve=False)."""
        snap = record.snapshot
        pending = snap.pendingApproval
        if pending is None:
            if snap.status == sm.AWAITING_APPROVAL:
                self._transition(record, sm.AWAITING_APPROVAL, sm.RUNNING)
            return

        token = pending.token
        event = self._get_approval_event(token)

        while not self.cancel_event.is_set():
            record = self._load()
            snap = record.snapshot
            if snap.cancelRequested or snap.status == sm.CANCELLED:
                return
            if snap.status != sm.AWAITING_APPROVAL:
                if snap.pendingApproval is not None:
                    pa = snap.pendingApproval
                    if advance_on_approve and pa.nodeId in self.nodes and pa.status == 'approved':
                        next_id = self._follow_edge(self.nodes[pa.nodeId], None)
                        if pa.nodeId not in snap.completedNodes:
                            snap.completedNodes.append(pa.nodeId)
                        snap.resumeFromNodeId = next_id
                        snap.currentNodeId = next_id
                    snap.pendingApproval = None
                    self._save_snapshot(record)
                return
            pending = snap.pendingApproval
            if pending is None:
                self._transition(record, sm.AWAITING_APPROVAL, sm.RUNNING)
                return
            if pending.status != 'pending':
                if pending.status == 'approved':
                    self._append(record, 'approvalResponded', node_id=pending.nodeId,
                                 attempt=pending.attempt, generation=pending.generation,
                                 payload={'token': token, 'decision': 'approved',
                                          'responder': pending.respondedBy,
                                          'comment': pending.comment})
                    if advance_on_approve and pending.nodeId in self.nodes:
                        next_id = self._follow_edge(self.nodes[pending.nodeId], None)
                        if pending.nodeId not in snap.completedNodes:
                            snap.completedNodes.append(pending.nodeId)
                        snap.resumeFromNodeId = next_id
                        snap.currentNodeId = next_id
                    snap.pendingApproval = None
                    self._save_snapshot(record)
                    self._transition(record, sm.AWAITING_APPROVAL, sm.RUNNING)
                elif pending.status in ('rejected', 'expired'):
                    self._append(record, 'approvalResponded', node_id=pending.nodeId,
                                 attempt=pending.attempt, generation=pending.generation,
                                 payload={'token': token, 'decision': pending.status,
                                          'responder': pending.respondedBy,
                                          'comment': pending.comment})
                    snap.pendingApproval = None
                    snap.lastError = f"Approval {pending.status}"
                    self._save_snapshot(record)
                    self._transition(record, sm.AWAITING_APPROVAL, sm.FAILED,
                                     nodeId=pending.nodeId,
                                     error=f"Approval {pending.status}")
                elif pending.status == 'cancelled':
                    snap.pendingApproval = None
                    self._save_snapshot(record)
                return

            now = time.time()
            if pending.deadline and now >= pending.deadline:
                pending.status = 'expired'
                pending.respondedAt = now
                for a in record.approvals:
                    if a.token == token:
                        a.status = 'expired'
                        a.respondedAt = now
                        break
                self._append(record, 'approvalExpired', node_id=pending.nodeId,
                             attempt=pending.attempt, generation=pending.generation,
                             payload={'token': token})
                snap.lastError = "Approval expired"
                self._save_snapshot(record)
                self._transition(record, sm.AWAITING_APPROVAL, sm.FAILED,
                                 nodeId=pending.nodeId, error="Approval expired")
                return

            timeout = min(0.2, max(0.01, pending.deadline - now)) if pending.deadline else 0.2
            try:
                await asyncio.wait_for(event.wait(), timeout=timeout)
                event.clear()
            except asyncio.TimeoutError:
                pass

    # ----- main loop -----

    async def run(self) -> None:
        record = self._load()
        self.sync_control_from_snapshot(record.snapshot)
        flow: FlowDefinition = record.flow
        self.nodes = {n.id: n for n in flow.nodes}
        self.edges = list(flow.edges)

        while True:
            record = self._load()
            snap = record.snapshot

            if snap.cancelRequested or snap.status == sm.CANCELLED:
                if snap.status != sm.CANCELLED:
                    self._transition(record, snap.status, sm.CANCELLED)
                return
            if sm.is_terminal(snap.status):
                return
            if snap.status == sm.PAUSED:
                await self._wait_resume_or_cancel()
                continue
            if snap.status == sm.PAUSING:
                self._transition(record, sm.PAUSING, sm.PAUSED)
                continue
            if snap.status == sm.RETRY_WAIT:
                await self._wait_retry(record)
                record = self._load()
                if record.snapshot.status == sm.RETRY_WAIT:
                    self._transition(record, sm.RETRY_WAIT, sm.RUNNING)
                continue
            if snap.status == sm.AWAITING_APPROVAL:
                await self._wait_for_approval(record, advance_on_approve=True)
                continue
            if snap.status == sm.QUEUED:
                self._transition(record, sm.QUEUED, sm.RUNNING)
                continue
            if snap.status == sm.RUNNING:
                advanced = await self._step_next_node()
                if not advanced:
                    record = self._load()
                    if record.snapshot.status == sm.RUNNING:
                        self._transition(record, sm.RUNNING, sm.SUCCEEDED)
                    return
                continue
            return

    # ----- node stepping -----

    def _follow_edge(self, node: FlowNode, result) -> Optional[str]:
        out = [e for e in self.edges if e.source == node.id]
        if node.type == 'condition':
            handle = 'true' if result else 'false'
            for e in out:
                if e.sourceHandle == handle:
                    return e.target
            return None
        if node.type == 'loop':
            if result:
                for e in out:
                    if e.sourceHandle == 'loop':
                        return e.target
                return None
            for e in out:
                if e.sourceHandle != 'loop':
                    return e.target
            return None
        return out[0].target if out else None

    async def _step_next_node(self) -> bool:
        record = self._load()
        snap = record.snapshot

        current_id = snap.resumeFromNodeId
        if current_id is None:
            for n in record.flow.nodes:
                if n.type == 'start':
                    current_id = n.id
                    break
        if current_id is None:
            raise RuntimeError("No start node")

        current = self.nodes.get(current_id)
        if current is None:
            raise RuntimeError(f"Node {current_id} not found")
        if current.type == 'end':
            snap.currentNodeId = current.id
            self._save_snapshot(record)
            return False

        snap.currentNodeId = current.id
        self._append(record, 'nodeEnter', node_id=current.id, from_state=sm.RUNNING)
        self._trace(record, current, 'enter', snap.variables)
        self._save_snapshot(record)
        await self._emit('nodeEnter', current.id, copy.deepcopy(snap.variables))

        done_attempts = snap.nodeAttempts.get(current.id, 0)
        max_attempts = 1
        if current.data.retry and current.data.retry.maxAttempts and current.data.retry.maxAttempts > 1:
            max_attempts = current.data.retry.maxAttempts

        try:
            result = await self._execute_node_with_retry(current, done_attempts, max_attempts)
        except NodeInterrupted:
            return True
        except Exception as exc:
            record = self._load()
            snap = record.snapshot
            self._append(record, 'nodeError', node_id=current.id,
                         attempt=snap.nodeAttempts.get(current.id, 0),
                         payload={'error': str(exc)})
            self._trace(record, current, 'error', snap.variables, str(exc))
            await self._emit('nodeError', current.id, str(exc), copy.deepcopy(snap.variables))
            snap.lastError = str(exc)
            self._save_snapshot(record)
            if not sm.is_terminal(snap.status) and not snap.cancelRequested:
                self._transition(record, snap.status, sm.FAILED,
                                 nodeId=current.id, error=str(exc))
            return False

        record = self._load()
        snap = record.snapshot
        next_id = self._follow_edge(current, result)

        self._append(record, 'nodeExit', node_id=current.id, payload={'next': next_id})
        self._trace(record, current, 'exit', snap.variables)

        if current.id not in snap.completedNodes:
            snap.completedNodes.append(current.id)
        snap.resumeFromNodeId = next_id
        snap.nodeAttempts[current.id] = snap.nodeAttempts.get(current.id, 0)
        snap.currentNodeId = next_id
        if snap.retryNodeId == current.id:
            snap.retryNodeId = None
            snap.retryUntil = None
            snap.retryAttempt = None
            snap.retryDelay = None
            snap.lastError = None

        self._append(record, 'checkpoint', node_id=current.id,
                     generation=snap.generation,
                     payload={'resumeFromNodeId': next_id,
                              'completedNodes': list(snap.completedNodes)})
        self._save_snapshot(record)
        await self._emit('nodeExit', current.id, copy.deepcopy(snap.variables))

        # Safety net: if pause was requested but manager has not yet
        # flipped the state, do it now after the checkpoint is durable.
        if self.pause_event.is_set() and snap.status == sm.RUNNING:
            record = self._load()
            if record.snapshot.status == sm.RUNNING:
                record.snapshot.pauseRequested = True
                self._save_snapshot(record)
                self._transition(record, sm.RUNNING, sm.PAUSING)
        return next_id is not None

    def _retry_delay(self, node: FlowNode, failed_attempts: int) -> float:
        cfg = node.data.retry
        if not cfg:
            return 0.0
        delay = cfg.delaySeconds or 0.0
        if cfg.backoff == 'exponential':
            delay = delay * (2 ** failed_attempts)
            if cfg.maxDelaySeconds:
                delay = min(delay, cfg.maxDelaySeconds)
        return delay

    async def _execute_node_with_retry(self, node: FlowNode, done_attempts: int, max_attempts: int):
        attempt = done_attempts
        while True:
            attempt += 1
            record = self._load()
            record.snapshot.nodeAttempts[node.id] = attempt - 1
            self._save_snapshot(record)
            try:
                result = await self._execute_node(record, node, attempt)
                fresh = self._load()
                fresh.snapshot.nodeAttempts[node.id] = attempt - 1
                fresh.snapshot.variables.update(record.snapshot.variables)
                self._save_snapshot(fresh)
                return result
            except NodeInterrupted:
                raise
            except Exception as exc:
                if attempt >= max_attempts:
                    raise
                delay = self._retry_delay(node, attempt)
                record = self._load()
                snap = record.snapshot
                snap.nodeAttempts[node.id] = attempt
                snap.retryNodeId = node.id
                snap.retryAttempt = attempt + 1
                snap.retryDelay = delay
                snap.retryUntil = time.time() + delay
                snap.lastError = str(exc)
                self._save_snapshot(record)
                self._transition(record, sm.RUNNING, sm.RETRY_WAIT,
                                 nodeId=node.id, attempt=attempt)
                await self._wait_retry(self._load())
                record = self._load()
                if record.snapshot.status == sm.CANCELLED or record.snapshot.cancelRequested:
                    raise NodeInterrupted()
                if record.snapshot.status == sm.RETRY_WAIT:
                    self._transition(record, sm.RETRY_WAIT, sm.RUNNING)

    async def _execute_node(self, record, node: FlowNode, attempt: int):
        ctx = record.snapshot.variables
        if node.type in ('start', 'end'):
            return None
        if node.type == 'task':
            return await self._exec_task(node, ctx)
        if node.type == 'condition':
            return await self._exec_condition(node, ctx)
        if node.type == 'loop':
            return await self._exec_loop(node, ctx, record.snapshot)
        if node.type == 'wait':
            return await self._exec_wait(node)
        if node.type in ('http', 'sql', 'file'):
            return await self._exec_side_effect(record, node, attempt)
        if node.type == 'approval':
            return await self._exec_approval(record, node, attempt)
        if node.type == 'parallel':
            return await self._exec_parallel(record, node)
        if node.type == 'subflow':
            return await self._exec_subflow(record, node)
        if node.type == 'trycatch':
            return await self._exec_trycatch(record, node)
        raise RuntimeError(f"Unsupported node type: {node.type}")

    # ----- primitive executors -----

    async def _exec_task(self, node: FlowNode, ctx: Dict[str, Any]):
        code = node.data.code
        if not code:
            return None
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, execute_python_sandbox, code, ctx, 5)
        except SandboxError as e:
            raise RuntimeError(f"Task execution failed: {e}")
        return None

    async def _exec_condition(self, node: FlowNode, ctx: Dict[str, Any]) -> bool:
        if not node.data.expression:
            raise RuntimeError("Condition expression is empty")
        try:
            return bool(evaluate_expression(node.data.expression, ctx))
        except ASTEvaluationError as e:
            raise RuntimeError(f"Condition evaluation failed: {e}")

    async def _exec_loop(self, node: FlowNode, ctx: Dict[str, Any], snap) -> bool:
        if snap.loopCounts.get(node.id, 0) >= 10000:
            raise RuntimeError("Infinite loop detected")
        if not node.data.expression:
            raise RuntimeError("Loop expression is empty")
        try:
            result = bool(evaluate_expression(node.data.expression, ctx))
        except ASTEvaluationError as e:
            raise RuntimeError(f"Loop evaluation failed: {e}")
        if result:
            snap.loopCounts[node.id] = snap.loopCounts.get(node.id, 0) + 1
        return result

    async def _exec_wait(self, node: FlowNode) -> None:
        seconds = node.data.seconds or 0
        if seconds <= 0:
            return
        deadline = time.time() + seconds
        pause_task = asyncio.ensure_future(self.pause_event.wait())
        cancel_task = asyncio.ensure_future(self.cancel_event.wait())
        try:
            while True:
                if self.cancel_event.is_set() or self.pause_event.is_set():
                    raise NodeInterrupted()
                remaining = deadline - time.time()
                if remaining <= 0:
                    return
                done, _ = await asyncio.wait(
                    [pause_task, cancel_task],
                    timeout=min(0.1, remaining),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if done:
                    raise NodeInterrupted()
        finally:
            if not pause_task.done():
                pause_task.cancel()
            if not cancel_task.done():
                cancel_task.cancel()

    # ----- side effects with idempotency -----

    def _idempotency_key(self, node: FlowNode, attempt: int) -> str:
        return sm.idempotency_key(self.execution_id, node.id, attempt)

    async def _exec_side_effect(self, record, node: FlowNode, attempt: int):
        key = self._idempotency_key(node, attempt)
        if key in record.sideEffects:
            cached = record.sideEffects[key].result
            if isinstance(cached, dict):
                record.snapshot.variables.update(cached)
            self._append(record, 'sideEffect', node_id=node.id, attempt=attempt,
                         generation=record.snapshot.generation,
                         idempotency_key=key, payload={'reused': True})
            self._save_snapshot(record)
            return None

        if node.type == 'http':
            result = await self._do_http(node, attempt)
        elif node.type == 'sql':
            result = await self._do_sql(node)
        elif node.type == 'file':
            result = await self._do_file(node)
        else:
            raise RuntimeError(f"Unknown side-effect node: {node.type}")

        record = self._load()
        record.sideEffects[key] = SideEffectRecord(
            key=key, executionId=self.execution_id, nodeId=node.id,
            attempt=attempt, generation=record.snapshot.generation,
            timestamp=time.time(), result=result,
        )
        if isinstance(result, dict):
            record.snapshot.variables.update(result)
        self._append(record, 'sideEffect', node_id=node.id, attempt=attempt,
                     generation=record.snapshot.generation,
                     idempotency_key=key, payload={'reused': False})
        self._save_snapshot(record)
        return None

    async def _do_http(self, node: FlowNode, attempt: int, idem_key: Optional[str] = None) -> Dict[str, Any]:
        config = node.data.httpConfig
        if not config:
            raise RuntimeError("HTTP config is empty")
        timeout = config.timeout or 30.0
        headers = dict(config.headers or {})
        headers.setdefault('Idempotency-Key', idem_key or self._idempotency_key(node, attempt))
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(
                method=config.method, url=config.url,
                headers=headers, content=config.body,
            )
            if response.status_code >= 400:
                raise RuntimeError(
                    f"HTTP request failed with status {response.status_code}: {response.text[:200]}"
                )
            result = {
                'status_code': response.status_code,
                'headers': dict(response.headers),
                'text': response.text,
                'json': None,
            }
            try:
                result['json'] = response.json()
            except Exception:
                pass
            return {node.id + '_result': result}

    async def _do_sql(self, node: FlowNode) -> Dict[str, Any]:
        import sqlite3
        config = node.data.sqlConfig
        if not config:
            raise RuntimeError("SQL config is empty")
        loop = asyncio.get_event_loop()

        def _run():
            conn = sqlite3.connect(config.connectionString)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(config.query, config.params or [])
            q = config.query.strip().upper()
            if q.startswith('SELECT'):
                out = {'rows': [dict(r) for r in cursor.fetchall()]}
            else:
                conn.commit()
                out = {'rowcount': cursor.rowcount, 'lastrowid': cursor.lastrowid}
            cursor.close()
            conn.close()
            return out

        return {node.id + '_result': await loop.run_in_executor(None, _run)}

    async def _do_file(self, node: FlowNode) -> Dict[str, Any]:
        config = node.data.fileConfig
        if not config:
            raise RuntimeError("File config is empty")

        def _write():
            mode = 'a' if config.mode == 'append' else 'w'
            with open(config.path, mode, encoding='utf-8') as f:
                f.write(config.content)
            return {'path': config.path, 'bytes': len(config.content), 'mode': config.mode}

        loop = asyncio.get_event_loop()
        return {node.id + '_result': await loop.run_in_executor(None, _write)}

    # ----- approval with expirable tokens -----

    async def _exec_approval(self, record, node: FlowNode, attempt: int) -> None:
        config = node.data.approvalConfig
        now = time.time()
        timeout_seconds = config.timeoutSeconds if config and config.timeoutSeconds is not None else 600
        deadline = now + timeout_seconds if timeout_seconds > 0 else 0
        token = str(uuid.uuid4())
        generation = record.snapshot.generation

        existing = record.snapshot.pendingApproval
        if existing is not None and existing.nodeId == node.id and existing.status == 'pending':
            await self._wait_for_approval(record)
            return

        approval = ApprovalRequest(
            token=token,
            executionId=self.execution_id,
            nodeId=node.id,
            attempt=attempt,
            flowVersion=record.flowVersion,
            generation=generation,
            approvers=list(config.approvers) if config else [],
            description=config.description if config else None,
            createdAt=now,
            deadline=deadline,
            status='pending',
        )

        record.snapshot.pendingApproval = approval
        record.approvals.append(approval)
        self._append(record, 'approvalRequested', node_id=node.id, attempt=attempt,
                     generation=generation,
                     payload={
                         'token': token,
                         'flowVersion': record.flowVersion,
                         'deadline': deadline,
                         'approvers': approval.approvers,
                         'description': approval.description,
                     })
        self._save_snapshot(record)
        await self._emit('approvalRequested', approval.model_dump())

        self._transition(record, sm.RUNNING, sm.AWAITING_APPROVAL,
                         nodeId=node.id, token=token)

        await self._wait_for_approval(self._load())
        return None

    # ----- parallel with generation-gated join -----

    async def _exec_parallel(self, record, node: FlowNode) -> None:
        config = node.data.parallelConfig
        if not config or not config.branchNodeIds:
            return None

        record = self._load()
        snap = record.snapshot
        snap.generation = snap.generation + 1
        generation = snap.generation
        state = {
            'generation': generation,
            'required': list(config.branchNodeIds),
            'completed': {},
            'results': {},
        }
        snap.parallelBranches[node.id] = state
        self._append(record, 'parallelJoin', node_id=node.id, generation=generation,
                     payload={'phase': 'start', 'required': list(config.branchNodeIds)})
        self._save_snapshot(record)

        ctx_snapshot = copy.deepcopy(snap.variables)
        branch_tasks = {
            bid: asyncio.ensure_future(
                self._run_branch(bid, copy.deepcopy(ctx_snapshot), generation)
            )
            for bid in config.branchNodeIds
        }

        pending = set(branch_tasks.values())
        first_exception = None
        while pending and first_exception is None:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_EXCEPTION,
            )
            for t in done:
                exc = t.exception() if not t.cancelled() else asyncio.CancelledError()
                if exc is not None:
                    first_exception = exc
                    break

        if first_exception is not None:
            for t in pending:
                t.cancel()
            for t in branch_tasks.values():
                if not t.done():
                    t.cancel()
            for t in branch_tasks.values():
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
            record = self._load()
            state = record.snapshot.parallelBranches.get(node.id, {})
            for bid, t in branch_tasks.items():
                state.setdefault('completed', {})[bid] = generation
                exc = t.exception() if not t.cancelled() else None
                if exc:
                    state.setdefault('results', {})[bid] = {'error': str(exc)}
            self._save_snapshot(record)
            raise first_exception

        outcomes = {}
        for bid, t in branch_tasks.items():
            outcomes[bid] = t.result()

        record = self._load()
        snap = record.snapshot
        state = snap.parallelBranches[node.id]
        merged: Dict[str, Any] = {}
        for bid in config.branchNodeIds:
            outcome = outcomes[bid]
            state['completed'][bid] = generation
            state['results'][bid] = outcome
            merged[f'branch_{bid}'] = outcome
            for k, v in outcome.items():
                if k.endswith('_result'):
                    merged[k] = v

        for bid in config.branchNodeIds:
            if state['completed'].get(bid) != generation:
                raise RuntimeError(
                    f"Parallel join failed: branch {bid} generation mismatch "
                    f"(expected {generation}, got {state['completed'].get(bid)})"
                )

        snap.variables.update(merged)
        snap.variables[node.id + '_result'] = merged
        self._append(record, 'parallelJoin', node_id=node.id, generation=generation,
                     payload={'phase': 'join',
                              'completed': list(state['completed'].keys())})
        self._save_snapshot(record)
        return None

    async def _run_branch(self, start_node_id: str, ctx: Dict[str, Any],
                          generation: int) -> Dict[str, Any]:
        current_id: Optional[str] = start_node_id
        guard = 0
        while current_id and guard < 10000:
            if self.cancel_event.is_set():
                raise NodeInterrupted()
            node = self.nodes.get(current_id)
            if node is None:
                break
            if node.type == 'end':
                break
            if sm.is_side_effect_node(node.type):
                await self._branch_side_effect(node, ctx, generation)
            elif node.type == 'task':
                await self._exec_task(node, ctx)
            elif node.type == 'condition':
                res = await self._exec_condition(node, ctx)
                current_id = self._follow_edge(node, res)
                guard += 1
                continue
            elif node.type == 'wait':
                await self._exec_wait(node)
            elif node.type == 'loop':
                res = await self._exec_loop(node, ctx, self._load().snapshot)
                current_id = self._follow_edge(node, res)
                guard += 1
                continue
            elif node.type == 'approval':
                await self._branch_approval(node, ctx, generation)
            else:
                break
            if node.data.anchorId:
                break
            current_id = self._follow_edge(node, None)
            guard += 1
        return ctx

    async def _branch_side_effect(self, node, ctx, generation):
        record = self._load()
        attempt = 1
        key = sm.branch_idempotency_key(self.execution_id, node.id, attempt, generation)
        if key in record.sideEffects:
            cached = record.sideEffects[key].result
            if isinstance(cached, dict):
                ctx.update(cached)
            self._append(record, 'sideEffect', node_id=node.id, attempt=attempt,
                         generation=generation, idempotency_key=key,
                         payload={'reused': True, 'branch': True})
            self._save_snapshot(record)
            return
        if node.type == 'http':
            result = await self._do_http(node, attempt, idem_key=key)
        elif node.type == 'sql':
            result = await self._do_sql(node)
        elif node.type == 'file':
            result = await self._do_file(node)
        else:
            raise RuntimeError(f"Unknown side effect: {node.type}")
        record = self._load()
        record.sideEffects[key] = SideEffectRecord(
            key=key, executionId=self.execution_id, nodeId=node.id,
            attempt=attempt, generation=generation,
            timestamp=time.time(), result=result,
        )
        if isinstance(result, dict):
            ctx.update(result)
        self._append(record, 'sideEffect', node_id=node.id, attempt=attempt,
                     generation=generation, idempotency_key=key,
                     payload={'reused': False, 'branch': True})
        self._save_snapshot(record)

    async def _branch_approval(self, node: FlowNode, ctx: Dict[str, Any],
                                generation: int) -> None:
        """Approval inside a parallel branch. Uses generation-scoped tokens
        so a response can only release the matching branch execution."""
        record = self._load()
        config = node.data.approvalConfig
        attempt = 1

        for existing in record.approvals:
            if (existing.nodeId == node.id and existing.generation == generation
                    and existing.status == 'pending'):
                return await self._wait_branch_approval(existing.token, node, generation)

        now = time.time()
        timeout_seconds = config.timeoutSeconds if config and config.timeoutSeconds is not None else 600
        deadline = now + timeout_seconds if timeout_seconds > 0 else 0
        token = str(uuid.uuid4())

        approval = ApprovalRequest(
            token=token,
            executionId=self.execution_id,
            nodeId=node.id,
            attempt=attempt,
            flowVersion=record.flowVersion,
            generation=generation,
            approvers=list(config.approvers) if config else [],
            description=config.description if config else None,
            createdAt=now,
            deadline=deadline,
            status='pending',
        )

        record = self._load()
        record.approvals.append(approval)
        self._append(record, 'approvalRequested', node_id=node.id, attempt=attempt,
                     generation=generation,
                     payload={
                         'token': token,
                         'flowVersion': record.flowVersion,
                         'deadline': deadline,
                         'approvers': approval.approvers,
                         'branch': True,
                     })
        self._save_snapshot(record)
        await self._emit('approvalRequested', approval.model_dump())

        await self._wait_branch_approval(token, node, generation)

    async def _wait_branch_approval(self, token: str, node: FlowNode,
                                     generation: int) -> None:
        event = self._get_approval_event(token)
        while not self.cancel_event.is_set():
            record = self._load()
            if record.snapshot.cancelRequested:
                raise NodeInterrupted()
            approval = None
            for a in record.approvals:
                if a.token == token:
                    approval = a
                    break
            if approval is None:
                return
            if approval.status != 'pending':
                if approval.status == 'rejected':
                    raise RuntimeError(f"Approval rejected by {approval.respondedBy}")
                if approval.status == 'expired':
                    raise RuntimeError("Approval expired")
                if approval.status == 'cancelled':
                    raise NodeInterrupted()
                return
            now = time.time()
            if approval.deadline and now >= approval.deadline:
                approval.status = 'expired'
                approval.respondedAt = now
                self._append(record, 'approvalExpired', node_id=node.id,
                             generation=generation, payload={'token': token, 'branch': True})
                self._save_snapshot(record)
                raise RuntimeError("Approval expired")
            timeout = min(0.2, max(0.01, approval.deadline - now)) if approval.deadline else 0.2
            try:
                await asyncio.wait_for(event.wait(), timeout=timeout)
                event.clear()
            except asyncio.TimeoutError:
                pass

    # ----- subflow / trycatch -----

    async def _exec_subflow(self, record, node: FlowNode) -> None:
        from engine.executor import FlowExecutor
        config = node.data.subflowConfig
        if not config:
            raise RuntimeError("Subflow config is empty")
        if not self.flow_store:
            raise RuntimeError("Flow store unavailable for subflow")
        subflow = self.flow_store.get_flow(config.subflowId)
        if not subflow:
            raise RuntimeError(f"Subflow {config.subflowId} not found")
        sub = FlowExecutor(subflow, flow_store=self.flow_store)
        sub.state.variables = copy.deepcopy(record.snapshot.variables)
        await sub.execute()
        record = self._load()
        record.snapshot.variables.update(sub.state.variables)
        record.snapshot.variables[node.id + '_result'] = {
            'subflowId': config.subflowId,
            'status': sub.state.status,
        }
        self._save_snapshot(record)
        return None

    async def _exec_trycatch(self, record, node: FlowNode) -> None:
        config = node.data.tryCatchConfig
        if not config:
            raise RuntimeError("TryCatch config is empty")
        ctx_snapshot = copy.deepcopy(record.snapshot.variables)
        caught_error = None
        try:
            for nid in (config.tryNodeIds or []):
                if self.cancel_event.is_set():
                    raise NodeInterrupted()
                await self._run_branch(nid, record.snapshot.variables,
                                       record.snapshot.generation)
        except NodeInterrupted:
            raise
        except Exception as e:
            caught_error = str(e)
            record = self._load()
            record.snapshot.variables.clear()
            record.snapshot.variables.update(copy.deepcopy(ctx_snapshot))
            record.snapshot.variables[node.id + '_error'] = caught_error
            self._save_snapshot(record)
            try:
                for nid in (config.catchNodeIds or []):
                    if self.cancel_event.is_set():
                        raise NodeInterrupted()
                    await self._run_branch(nid, record.snapshot.variables,
                                           record.snapshot.generation)
            except NodeInterrupted:
                raise
            except Exception as catch_e:
                caught_error = f"{caught_error}; Catch failed: {catch_e}"

        record = self._load()
        record.snapshot.variables[node.id + '_result'] = {
            'caught_error': caught_error,
            'success': caught_error is None,
        }
        self._save_snapshot(record)
        return None
