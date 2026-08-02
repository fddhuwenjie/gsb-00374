import asyncio
import time
import uuid
from typing import Any, Dict, List, Optional

from engine import state_machine as sm
from engine.flow_version import compute_node_config_hash
from engine.persistent_executor import PersistentExecutor
from models.flow import (
    ApprovalResponse,
    ControlCommand,
    ExecutionSnapshot,
    FlowDefinition,
    PersistedExecutionRecord,
    StateEvent,
)


class CommandRejected(Exception):
    def __init__(self, reason: str, status: Optional[str] = None):
        self.reason = reason
        self.status = status
        super().__init__(reason)


class ExecutionManager:
    """Owns execution lifecycles, enforces single-runner semantics,
    deduplicates commands, and recovers in-flight executions on startup."""

    MAX_SEEN_REQUESTS = 200

    def __init__(self, event_store, flow_store=None):
        self.event_store = event_store
        self.flow_store = flow_store
        self._tasks: Dict[str, asyncio.Task] = {}
        self._executors: Dict[str, PersistentExecutor] = {}
        self._tasks_guard = asyncio.Lock()

    async def shutdown(self) -> None:
        for task in list(self._tasks.values()):
            task.cancel()
        for task in list(self._tasks.values()):
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks.clear()
        self._executors.clear()

    # ----- creation -----

    def create_execution_id(self) -> str:
        return self.event_store.create_execution_id()

    async def start_execution(
        self,
        flow: FlowDefinition,
        variables: Optional[Dict[str, Any]] = None,
        request_id: Optional[str] = None,
        execution_id: Optional[str] = None,
        flow_version: Optional[int] = None,
    ) -> str:
        execution_id = execution_id or self.create_execution_id()
        request_id = request_id or str(uuid.uuid4())

        bound_flow, bound_version, cfg_hash = self._resolve_flow_version(
            flow, flow_version
        )

        snapshot = ExecutionSnapshot(
            executionId=execution_id,
            flowId=bound_flow.id,
            flowVersion=bound_version,
            nodeConfigHash=cfg_hash,
            status=sm.QUEUED,
            seq=0,
            timestamp=time.time(),
            variables=dict(variables or {}),
        )
        record = PersistedExecutionRecord(
            executionId=execution_id,
            flowId=bound_flow.id,
            flow=bound_flow,
            flowVersion=bound_version,
            nodeConfigHash=cfg_hash,
            status=sm.QUEUED,
            seq=0,
            snapshot=snapshot,
        )
        self.event_store.init_record(record)
        self._append_event(
            execution_id, 'queued',
            to_state=sm.QUEUED,
            request_id=request_id,
            payload={
                'flowId': bound_flow.id,
                'flowName': bound_flow.name,
                'flowVersion': bound_version,
                'nodeConfigHash': cfg_hash,
            },
        )
        await self._ensure_runner(execution_id)
        return execution_id

    def _resolve_flow_version(
        self, flow: FlowDefinition, flow_version: Optional[int]
    ):
        """Return (definition, version, config_hash) that the execution
        must bind to immutably.

        * If a versioned flow store is available and the flow exists, a
          specific *flow_version* (or the current latest when None) is
          loaded.  If the requested version is missing a ``CommandRejected``
          is raised.
        * If no versioned store is present (e.g. in unit tests that only
          pass an ``EventStore``), the provided *flow* object is used
          directly with version 1 and a config hash computed from it.
        """
        cfg_hash = compute_node_config_hash(flow)

        if self.flow_store is None:
            version = flow_version or flow.version or 1
            return flow, version, cfg_hash

        store = self.flow_store
        if hasattr(store, 'get_version'):
            requested = flow_version if flow_version is not None else flow.version
            if requested is None:
                latest_num = store.get_latest_version_number(flow.id)
                if latest_num == 0:
                    saved, fv = store.create_flow(flow)
                    return saved, fv.meta.version, fv.meta.nodeConfigHash
                fv = store.get_version(flow.id, latest_num)
                if fv is None:
                    raise CommandRejected(
                        f"Flow version v{latest_num} not found"
                    )
                return fv.definition, fv.meta.version, fv.meta.nodeConfigHash
            fv = store.get_version(flow.id, requested)
            if fv is None:
                raise CommandRejected(
                    f"Flow version v{requested} not found for flow {flow.id}"
                )
            return fv.definition, fv.meta.version, fv.meta.nodeConfigHash

        return flow, flow_version or 1, cfg_hash

    # ----- commands -----

    async def send_command(self, command: ControlCommand) -> ExecutionSnapshot:
        return await self.control(
            command.executionId, command.command, command.requestId
        )

    async def control(
        self, execution_id: str, action: str, request_id: Optional[str] = None
    ) -> ExecutionSnapshot:
        request_id = request_id or str(uuid.uuid4())
        lock = self.event_store._get_lock(execution_id)
        with lock:
            record = self.event_store.load_record(execution_id)
            if record is None:
                raise CommandRejected("Execution not found")
            snap = record.snapshot

            if request_id in record.seenRequestIds:
                return self._snapshot_safe(snap)

            if sm.is_terminal(snap.status):
                self._remember_request(record, request_id)
                self.event_store.save_record(record)
                return self._snapshot_safe(snap)

            if action == 'pause':
                self._handle_pause(record)
            elif action == 'resume':
                self._handle_resume(record)
            elif action == 'cancel':
                self._handle_cancel(record)
            elif action == 'step':
                self._handle_step(record)
            else:
                raise CommandRejected(f"Unknown command: {action}")

            self._append_event(
                execution_id, 'command',
                request_id=request_id,
                payload={'action': action},
            )
            self._remember_request(record, request_id)
            self.event_store.save_record(record)

        executor = self._executors.get(execution_id)
        if action == 'pause' and executor:
            executor.request_pause()
        elif action in ('resume', 'step') and executor:
            executor.request_resume()
        elif action == 'cancel' and executor:
            executor.request_cancel()

        if action in ('resume', 'step', 'cancel'):
            await self._ensure_runner(execution_id)

        return self.get_snapshot(execution_id)

    def _handle_pause(self, record) -> None:
        snap = record.snapshot
        if snap.status == sm.RUNNING:
            snap.pauseRequested = True
            self._transition(record, sm.RUNNING, sm.PAUSING)
        elif snap.status == sm.PAUSING:
            snap.pauseRequested = True
        elif snap.status == sm.PAUSED:
            snap.pauseRequested = True
        else:
            raise CommandRejected(
                f"Cannot pause from state {snap.status}", snap.status
            )

    def _handle_resume(self, record) -> None:
        snap = record.snapshot
        if snap.status in (sm.PAUSED, sm.PAUSING, sm.RETRY_WAIT):
            snap.pauseRequested = False
            self._transition(record, snap.status, sm.RUNNING)
        elif snap.status == sm.RUNNING:
            return
        else:
            raise CommandRejected(
                f"Cannot resume from state {snap.status}", snap.status
            )

    def _handle_cancel(self, record) -> None:
        snap = record.snapshot
        snap.cancelRequested = True
        if snap.status in (sm.QUEUED, sm.PAUSED, sm.RETRY_WAIT, sm.AWAITING_APPROVAL):
            now = time.time()
            if snap.pendingApproval is not None:
                snap.pendingApproval.status = 'cancelled'
                snap.pendingApproval.respondedAt = now
                for a in record.approvals:
                    if a.token == snap.pendingApproval.token:
                        a.status = 'cancelled'
                        a.respondedAt = now
                        break
            for a in record.approvals:
                if a.status == 'pending':
                    a.status = 'cancelled'
                    a.respondedAt = now
            self._transition(record, snap.status, sm.CANCELLED)
        elif snap.status in (sm.RUNNING, sm.PAUSING):
            for a in record.approvals:
                if a.status == 'pending':
                    a.status = 'cancelled'
                    a.respondedAt = time.time()
            snap.status = snap.status
            record.status = snap.status
            self.event_store.save_record(record)
        else:
            raise CommandRejected(
                f"Cannot cancel from state {snap.status}", snap.status
            )

    def _handle_step(self, record) -> None:
        snap = record.snapshot
        if snap.status == sm.PAUSED:
            snap.pauseRequested = False
            snap.stepMode = True
            self._transition(record, sm.PAUSED, sm.RUNNING)
        elif snap.status == sm.RUNNING:
            return
        else:
            raise CommandRejected(
                f"Cannot step from state {snap.status}", snap.status
            )

    async def respond_to_approval(self, response: ApprovalResponse) -> ExecutionSnapshot:
        """Process an approval response (approve/reject).

        Validates that:
        - The execution exists
        - The token matches a pending approval bound to this execution
        - The bound flowVersion matches the execution's flowVersion
        - The token is not stale (approval already resolved or different node)
        - Duplicate responses are idempotent (return current state)
        """
        request_id = response.requestId or str(uuid.uuid4())
        lock = self.event_store._get_lock(response.executionId)
        with lock:
            record = self.event_store.load_record(response.executionId)
            if record is None:
                raise CommandRejected("Execution not found")
            snap = record.snapshot

            if request_id in record.seenRequestIds:
                return self._snapshot_safe(snap)

            if sm.is_terminal(snap.status):
                self._remember_request(record, request_id)
                self.event_store.save_record(record)
                raise CommandRejected(
                    f"Execution is in terminal state {snap.status}", snap.status
                )

            approval = None
            for a in record.approvals:
                if a.token == response.token:
                    approval = a
                    break

            if approval is None:
                if snap.pendingApproval and snap.pendingApproval.token == response.token:
                    approval = snap.pendingApproval
                else:
                    raise CommandRejected("Invalid or unknown approval token")

            if approval.executionId != response.executionId:
                raise CommandRejected("Token does not belong to this execution")

            if approval.flowVersion != record.flowVersion:
                raise CommandRejected(
                    f"Token flow version mismatch: token bound to v{approval.flowVersion}, "
                    f"execution is on v{record.flowVersion}"
                )

            if approval.status != 'pending':
                self._remember_request(record, request_id)
                self.event_store.save_record(record)
                return self._snapshot_safe(snap)

            now = time.time()
            if approval.deadline and now >= approval.deadline:
                approval.status = 'expired'
                approval.respondedAt = now
                if snap.pendingApproval and snap.pendingApproval.token == response.token:
                    snap.pendingApproval.status = 'expired'
                    snap.pendingApproval.respondedAt = now
                self.event_store.save_record(record)
                event = self._append_event(
                    response.executionId, 'approvalExpired',
                    node_id=approval.nodeId, attempt=approval.attempt,
                    generation=approval.generation,
                    request_id=request_id,
                    payload={'token': response.token},
                )
                record.seq = event.seq
                record.snapshot.seq = event.seq
                if snap.status == sm.AWAITING_APPROVAL and snap.pendingApproval and snap.pendingApproval.token == response.token:
                    snap.lastError = "Approval expired"
                    self._transition(record, sm.AWAITING_APPROVAL, sm.FAILED,
                                     nodeId=approval.nodeId, error="Approval expired")
                self._remember_request(record, request_id)
                self.event_store.save_record(record)
                executor = self._executors.get(response.executionId)
                if executor:
                    executor.inject_approval_response(
                        response.token, 'expired'
                    )
                return self.get_snapshot(response.executionId)

            approval.status = response.decision
            approval.respondedBy = response.responder
            approval.respondedAt = now
            approval.comment = response.comment

            is_main_approval = (
                snap.pendingApproval is not None
                and snap.pendingApproval.token == response.token
            )

            if is_main_approval:
                snap.pendingApproval.status = response.decision
                snap.pendingApproval.respondedBy = response.responder
                snap.pendingApproval.respondedAt = now
                snap.pendingApproval.comment = response.comment

            self.event_store.save_record(record)
            event = self._append_event(
                response.executionId, 'approvalResponded',
                node_id=approval.nodeId, attempt=approval.attempt,
                generation=approval.generation,
                request_id=request_id,
                payload={
                    'token': response.token,
                    'decision': response.decision,
                    'responder': response.responder,
                    'comment': response.comment,
                },
            )
            record.seq = event.seq
            record.snapshot.seq = event.seq

            if is_main_approval and snap.status == sm.AWAITING_APPROVAL:
                if response.decision == 'approved':
                    self._transition(record, sm.AWAITING_APPROVAL, sm.RUNNING,
                                     nodeId=approval.nodeId)
                else:
                    snap.lastError = f"Approval {response.decision}"
                    self._transition(record, sm.AWAITING_APPROVAL, sm.FAILED,
                                     nodeId=approval.nodeId,
                                     error=f"Approval {response.decision}")

            self._remember_request(record, request_id)
            self.event_store.save_record(record)

        executor = self._executors.get(response.executionId)
        if executor:
            executor.inject_approval_response(
                response.token, response.decision,
                responder=response.responder, comment=response.comment,
            )

        if response.decision == 'approved':
            await self._ensure_runner(response.executionId)

        return self.get_snapshot(response.executionId)

    # ----- runner management -----

    async def _ensure_runner(self, execution_id: str) -> None:
        async with self._tasks_guard:
            existing = self._tasks.get(execution_id)
            if existing and not existing.done():
                return
            executor = PersistentExecutor(
                self.event_store, execution_id, flow_store=self.flow_store
            )
            self._executors[execution_id] = executor
            task = asyncio.create_task(self._run_execution(execution_id, executor))
            self._tasks[execution_id] = task

            def _cleanup(_task):
                self._tasks.pop(execution_id, None)
                self._executors.pop(execution_id, None)

            task.add_done_callback(_cleanup)

    async def _run_execution(self, execution_id: str, executor: PersistentExecutor) -> None:
        try:
            await executor.run()
        except Exception:
            import traceback
            traceback.print_exc()
            try:
                record = self.event_store.load_record(execution_id)
                if record and not sm.is_terminal(record.snapshot.status):
                    lock = self.event_store._get_lock(execution_id)
                    with lock:
                        record = self.event_store.load_record(execution_id)
                        if record and not sm.is_terminal(record.snapshot.status):
                            cur = record.snapshot.status
                            record.snapshot.lastError = "executor crashed"
                            if sm.can_transition(cur, sm.FAILED):
                                self._transition(record, cur, sm.FAILED,
                                                 error="executor crashed")
                            else:
                                record.snapshot.status = sm.FAILED
                                record.status = sm.FAILED
                                self.event_store.save_record(record)
            except Exception:
                pass

    # ----- recovery -----

    async def recover_all(self) -> int:
        """Recover executions left in-flight by a previous process.
        Returns the number of executions resumed.

        Each execution is bound to an immutable flow version (stored in
        its ``PersistedExecutionRecord``).  Recovery never consults the
        latest flow definition — it always uses the version captured at
        start time.  If that version is no longer present in the flow
        store the execution is transitioned to FAILED.
        """
        recovered = 0
        for execution_id in self.event_store.list_execution_ids():
            record = self.event_store.reconcile_record(execution_id)
            if record is None:
                continue
            snap = record.snapshot
            if sm.is_terminal(snap.status):
                continue
            if snap.status == sm.PAUSED:
                continue
            if snap.status == sm.FAILED:
                continue

            if not self._verify_bound_version(record):
                lock = self.event_store._get_lock(execution_id)
                with lock:
                    record = self.event_store.load_record(execution_id)
                    if record is not None and not sm.is_terminal(record.snapshot.status):
                        record.snapshot.lastError = (
                            f"Bound flow version v{record.flowVersion} "
                            f"not found"
                        )
                        self._transition(
                            record, record.snapshot.status, sm.FAILED,
                            error="bound flow version missing",
                        )
                        self.event_store.save_record(record)
                continue

            lock = self.event_store._get_lock(execution_id)
            with lock:
                record = self.event_store.load_record(execution_id)
                snap = record.snapshot
                if snap.status in (sm.RUNNING, sm.PAUSING):
                    was_pausing = snap.status == sm.PAUSING
                    self._append_event(
                        execution_id, 'recover',
                        from_state=snap.status, to_state=sm.RUNNING,
                        payload={'reason': 'process_restart',
                                 'pauseRequested': was_pausing,
                                 'flowVersion': record.flowVersion},
                    )
                    record = self.event_store.load_record(execution_id)
                    record.snapshot.status = sm.RUNNING
                    record.status = sm.RUNNING
                    if was_pausing:
                        record.snapshot.pauseRequested = True
                    self.event_store.save_record(record)
                elif snap.status == sm.QUEUED:
                    pass
                elif snap.status == sm.RETRY_WAIT:
                    pass
                elif snap.status == sm.AWAITING_APPROVAL:
                    if snap.pendingApproval is not None:
                        now = time.time()
                        if snap.pendingApproval.deadline and now >= snap.pendingApproval.deadline:
                            snap.pendingApproval.status = 'expired'
                            snap.pendingApproval.respondedAt = now
                            for a in record.approvals:
                                if a.token == snap.pendingApproval.token:
                                    a.status = 'expired'
                                    a.respondedAt = now
                                    break
                            self.event_store.save_record(record)
                            self._append_event(
                                execution_id, 'approvalExpired',
                                node_id=snap.pendingApproval.nodeId,
                                attempt=snap.pendingApproval.attempt,
                                generation=snap.pendingApproval.generation,
                                payload={'token': snap.pendingApproval.token,
                                         'reason': 'recovery_deadline_passed'},
                            )
                            record = self.event_store.load_record(execution_id)
                            record.snapshot.lastError = "Approval expired before recovery"
                            self._transition(record, sm.AWAITING_APPROVAL, sm.FAILED,
                                             error="approval expired")
                        else:
                            self._append_event(
                                execution_id, 'recover',
                                from_state=sm.AWAITING_APPROVAL,
                                to_state=sm.AWAITING_APPROVAL,
                                payload={'reason': 'process_restart',
                                         'token': snap.pendingApproval.token,
                                         'deadline': snap.pendingApproval.deadline,
                                         'flowVersion': record.flowVersion},
                            )
                    else:
                        self._transition(record, sm.AWAITING_APPROVAL, sm.RUNNING)
                # PAUSED stays paused (waits for explicit resume).
            await self._ensure_runner(execution_id)
            recovered += 1
        return recovered

    def _verify_bound_version(self, record) -> bool:
        """Confirm that the flow version bound to *record* still exists.

        When no versioned flow store is configured this always returns
        True because the definition itself is embedded in the record.
        When a versioned store *is* present, the bound version must
        exist there — an embedded copy is never considered sufficient
        once explicit versioning is in use (so deleting a version
        correctly orphans executions).
        """
        if record.flow is None:
            return False
        if self.flow_store is None:
            return True
        if not hasattr(self.flow_store, 'get_version'):
            return True
        fv = self.flow_store.get_version(record.flowId, record.flowVersion)
        return fv is not None

    def count_active_executions_for_version(
        self, flow_id: str, version: int
    ) -> int:
        """Return the number of non-terminal executions bound to a
        particular flow version.  Used for deletion protection."""
        count = 0
        for eid in self.event_store.list_execution_ids():
            rec = self.event_store.load_record(eid)
            if rec is None:
                continue
            if rec.flowId != flow_id:
                continue
            if rec.flowVersion != version:
                continue
            if not sm.is_terminal(rec.snapshot.status):
                count += 1
        return count

    # ----- queries -----

    def get_snapshot(self, execution_id: str) -> ExecutionSnapshot:
        record = self.event_store.load_record(execution_id)
        if record is None:
            raise CommandRejected("Execution not found")
        return self._snapshot_safe(record.snapshot)

    def get_events(self, execution_id: str, after_seq: int = 0) -> List[StateEvent]:
        return self.event_store.read_events(execution_id, after_seq)

    def list_executions(self) -> List[Dict[str, Any]]:
        out = []
        for eid in self.event_store.list_execution_ids():
            record = self.event_store.load_record(eid)
            if record is None:
                continue
            out.append({
                'executionId': record.executionId,
                'flowId': record.flowId,
                'flowName': record.flow.name,
                'status': record.status,
                'seq': record.seq,
                'createdAt': record.createdAt,
                'updatedAt': record.updatedAt,
            })
        out.sort(key=lambda x: x['createdAt'], reverse=True)
        return out

    def subscribe(self, callback):
        return self.event_store.subscribe(callback)

    # ----- internal helpers -----

    def _snapshot_safe(self, snap: ExecutionSnapshot) -> ExecutionSnapshot:
        snap.allowedActions = sm.allowed_actions(snap.status)
        return snap

    def _remember_request(self, record, request_id: str) -> None:
        if request_id not in record.seenRequestIds:
            record.seenRequestIds.append(request_id)
            if len(record.seenRequestIds) > self.MAX_SEEN_REQUESTS:
                record.seenRequestIds = record.seenRequestIds[-self.MAX_SEEN_REQUESTS:]

    def _transition(self, record, from_state, to_state, **payload) -> None:
        sm.assert_transition(from_state, to_state)
        event = StateEvent(
            seq=0, executionId=record.executionId, timestamp=0,
            eventType='transition', fromState=from_state, toState=to_state,
            payload=payload,
        )
        self.event_store.append_event(event)
        record.seq = event.seq
        record.snapshot.seq = event.seq
        record.snapshot.status = to_state
        record.status = to_state

    def _append_event(
        self, execution_id: str, event_type: str, *,
        to_state=None, from_state=None, request_id=None, payload=None,
        node_id=None, attempt=None, generation=None,
    ) -> StateEvent:
        event = StateEvent(
            seq=0, executionId=execution_id, timestamp=0,
            eventType=event_type, fromState=from_state, toState=to_state,
            requestId=request_id, payload=payload or {},
            nodeId=node_id, attempt=attempt, generation=generation,
        )
        self.event_store.append_event(event)
        record = self.event_store.load_record(execution_id)
        if record is not None:
            record.seq = event.seq
            record.snapshot.seq = event.seq
            if to_state is not None:
                record.snapshot.status = to_state
                record.status = to_state
            self.event_store.save_record(record)
        return event
