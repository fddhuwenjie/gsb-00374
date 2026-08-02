import asyncio
import time
from typing import Any, Dict, List, Optional, Set

from models.flow import FlowDefinition
from engine.state_machine import (
    ExecutionStateMachine, IllegalTransitionError,
    COMMAND_ALLOWED_STATUSES, ALLOWED_TRANSITIONS,
    TERMINAL_STATES, ALL_COMMANDS,
)
from engine.resumable_executor import (
    ResumableExecutor, RunnerControl, SideEffectGateway, ApprovalRegistry,
)
from storage.execution_journal import ExecutionJournal
from storage.flow_version_store import FlowVersionStore


class ExecutionManager:
    """Owns journals, state machines and runner tasks.

    Guarantees:
    - at most one live runner per execution, regardless of duplicate,
      repeated or stale commands;
    - commands are deduplicated by commandId (persisted in the journal, so
      dedupe survives restarts);
    - recovery after a process restart resumes from the most recent complete
      node boundary recorded in the journal.
    """

    def __init__(self, storage_dir: str, gateway: Optional[SideEffectGateway] = None,
                 version_store: Optional[FlowVersionStore] = None):
        self.storage_dir = storage_dir
        self.gateway = gateway or SideEffectGateway()
        self.version_store = version_store
        self._journals: Dict[str, ExecutionJournal] = {}
        self._sms: Dict[str, ExecutionStateMachine] = {}
        self._controls: Dict[str, RunnerControl] = {}
        self._runners: Dict[str, asyncio.Task] = {}
        self._subscribers: Dict[str, Set[asyncio.Queue]] = {}
        self._lock = asyncio.Lock()
        self.recovery_refused: List[str] = []
        self.approval_registry = ApprovalRegistry()

    # ------------------------------------------------------------------
    def _get_journal(self, execution_id: str) -> Optional[ExecutionJournal]:
        if execution_id in self._journals:
            return self._journals[execution_id]
        if not ExecutionJournal.exists(self.storage_dir, execution_id):
            return None
        journal = ExecutionJournal(self.storage_dir, execution_id)
        self._journals[execution_id] = journal
        journal.add_listener(lambda event, eid=execution_id: self._broadcast(eid, event))
        return journal

    def _get_sm(self, execution_id: str) -> Optional[ExecutionStateMachine]:
        if execution_id in self._sms:
            return self._sms[execution_id]
        journal = self._get_journal(execution_id)
        if journal is None:
            return None
        sm = ExecutionStateMachine(journal)
        self._sms[execution_id] = sm
        return sm

    # ------------------------------------------------------------------
    def _version_available(self, journal: ExecutionJournal) -> bool:
        """An execution may only run / resume / retry if the immutable flow
        version it was bound to still exists."""
        if self.version_store is None:
            return True
        version = journal.flow_version()
        if version is None:
            return True  # legacy journal without version binding
        return self.version_store.get(journal.flow_id, version) is not None

    def referenced_versions(self) -> set:
        """(flowId, version) pairs referenced by any execution journal."""
        referenced = set()
        for execution_id in ExecutionJournal.list_execution_ids(self.storage_dir):
            journal = self._get_journal(execution_id)
            if journal is None:
                continue
            version = journal.flow_version()
            if version is not None:
                referenced.add((journal.flow_id, version))
        return referenced

    # ------------------------------------------------------------------
    async def start_execution(self, flow: FlowDefinition,
                              variables: Optional[Dict[str, Any]] = None,
                              execution_id: Optional[str] = None) -> Dict[str, Any]:
        async with self._lock:
            flow_version = None
            config_hash = None
            if self.version_store is not None:
                record = self.version_store.save_version(flow)
                flow_version = record['version']
                config_hash = record['configHash']
            journal = ExecutionJournal.create(
                self.storage_dir, flow, variables, execution_id,
                flow_version=flow_version, config_hash=config_hash,
            )
            self._journals[journal.execution_id] = journal
            journal.add_listener(
                lambda event, eid=journal.execution_id: self._broadcast(eid, event)
            )
            sm = ExecutionStateMachine(journal)
            self._sms[journal.execution_id] = sm
            self._spawn_runner_locked(journal.execution_id)
            return self.snapshot(journal.execution_id)

    async def command(self, execution_id: str, command_id: str, command: str,
                      expected_status: Optional[str] = None,
                      token: Optional[str] = None,
                      approver: Optional[str] = None,
                      node_id: Optional[str] = None,
                      attempt: Optional[Any] = None,
                      flow_version: Optional[int] = None) -> Dict[str, Any]:
        """Process a control command idempotently. Returns a result dict;
        never raises for duplicate/stale/illegal requests."""
        if command not in ALL_COMMANDS:
            return {'accepted': False, 'result': 'rejected',
                    'detail': f'unknown command: {command}'}
        async with self._lock:
            journal = self._get_journal(execution_id)
            sm = self._get_sm(execution_id)
            if journal is None or sm is None:
                return {'accepted': False, 'result': 'rejected',
                        'detail': 'execution not found'}

            previous = journal.processed_command(command_id)
            if previous is not None:
                # Duplicate delivery of an already processed command:
                # acknowledge without re-applying and without spawning
                # anything.
                return {'accepted': previous.get('result') == 'accepted',
                        'result': 'duplicate',
                        'detail': previous.get('detail'),
                        'status': sm.status}

            status = sm.status
            if expected_status is not None and expected_status != status:
                journal.append_command(command_id, command, 'stale',
                                       f'expected {expected_status}, current {status}')
                return {'accepted': False, 'result': 'stale',
                        'detail': f'expected {expected_status}, current {status}',
                        'status': status}

            if status not in COMMAND_ALLOWED_STATUSES[command]:
                journal.append_command(command_id, command, 'rejected',
                                       f'{command} not allowed from {status}')
                return {'accepted': False, 'result': 'rejected',
                        'detail': f'{command} not allowed from {status}',
                        'status': status}

            if command in ('resume', 'retry') and not self._version_available(journal):
                # The immutable flow version this execution was bound to is
                # gone: refuse to resume/retry rather than run anything else.
                journal.append_command(command_id, command, 'rejected',
                                       'bound flow version is missing')
                return {'accepted': False, 'result': 'rejected',
                        'detail': 'bound flow version is missing',
                        'status': status}

            if command in ('approve', 'reject'):
                check = self._validate_approval_locked(
                    journal, command, token, approver, node_id, attempt, flow_version
                )
                if check is not None:
                    journal.append_command(command_id, command, 'rejected', check)
                    return {'accepted': False, 'result': 'rejected',
                            'detail': check, 'status': status}
                pending = next(
                    p for p in journal.pending_approvals() if p['token'] == token
                )
                journal.append_approval_resolved(
                    pending['nodeId'], pending['attempt'], pending.get('generation'),
                    token, 'approved' if command == 'approve' else 'rejected',
                    approver,
                )
                journal.append_command(command_id, command, 'accepted')
                # Release exactly this token's waiter; other branches or
                # generations keep waiting.
                self.approval_registry.signal((execution_id, token))
                return {'accepted': True, 'result': 'accepted',
                        'status': self._get_sm(execution_id).status}

            self._apply_command_locked(execution_id, command)
            journal.append_command(command_id, command, 'accepted')
            return {'accepted': True, 'result': 'accepted',
                    'status': self._get_sm(execution_id).status}

    def _validate_approval_locked(self, journal: ExecutionJournal, command: str,
                                  token: Optional[str], approver: Optional[str],
                                  node_id: Optional[str], attempt: Optional[Any],
                                  flow_version: Optional[int]) -> Optional[str]:
        """Returns a rejection reason, or None if the response is valid.
        The token must match the CURRENT pending request bound to the same
        executionId + nodeId + attempt + flowVersion; superseded tokens
        (older generation) and expired requests are stale."""
        if not token:
            return 'approval token is required'
        pending = next(
            (p for p in journal.pending_approvals() if p['token'] == token), None
        )
        if pending is None:
            return 'stale or unknown approval token'
        if node_id is not None and node_id != pending['nodeId']:
            return f"token/nodeId mismatch: {node_id}"
        if attempt is not None and str(attempt) != str(pending['attempt']):
            return f"token/attempt mismatch: {attempt}"
        if flow_version is not None and flow_version != pending.get('flowVersion'):
            return f"token/flowVersion mismatch: {flow_version}"
        if time.time() > pending['deadline']:
            return 'approval request expired'
        approvers = pending.get('approvers') or []
        if approvers and approver not in approvers:
            return f"not an approver: {approver}"
        return None

    def _apply_command_locked(self, execution_id: str, command: str) -> None:
        sm = self._sms[execution_id]
        control = self._controls.get(execution_id)
        status = sm.status

        if command == 'pause':
            # Node in flight is uninterruptible: pausing first, paused only
            # after the node boundary is persisted.
            sm.transition('pausing', reason='pause command')
        elif command == 'resume':
            was_paused = status == 'paused'
            sm.transition('running', reason='resume command')
            if control:
                control.signal_resume()
            if was_paused:
                # Resume after a crash (or after the previous runner exited):
                # continue from the last persisted boundary.
                self._spawn_runner_locked(execution_id)
        elif command == 'cancel':
            sm.transition('cancelled', reason='cancel command')
            if control:
                control.signal_cancel()
            # wake any approval waiters so the runner can exit
            self.approval_registry.signal_all(execution_id)
        elif command == 'retry':
            sm.transition('queued', reason='retry command')
            self._spawn_runner_locked(execution_id)

    # ------------------------------------------------------------------
    def _spawn_runner_locked(self, execution_id: str) -> bool:
        """Start a runner iff none is live. Returns True if one was started."""
        existing = self._runners.get(execution_id)
        if existing is not None and not existing.done():
            return False
        control = RunnerControl()
        self._controls[execution_id] = control
        self._runners[execution_id] = asyncio.create_task(
            self._runner_main(execution_id, control)
        )
        return True

    def has_live_runner(self, execution_id: str) -> bool:
        task = self._runners.get(execution_id)
        return task is not None and not task.done()

    async def _runner_main(self, execution_id: str, control: RunnerControl) -> None:
        journal = self._journals[execution_id]
        sm = self._sms[execution_id]
        try:
            if sm.status == 'queued':
                sm.transition('running', reason='runner started')
            executor = ResumableExecutor(journal, sm, self.gateway,
                                         self.approval_registry)
            await executor.run(control)
        except Exception as e:
            try:
                if sm.status not in TERMINAL_STATES:
                    sm.transition('failed', reason=f'runner error: {e}')
            except IllegalTransitionError:
                pass

    # ------------------------------------------------------------------
    async def recover_all(self) -> List[str]:
        """Call after process start. Executions interrupted mid-flight are
        requeued and resumed from their last complete boundary; pause intent
        recorded before the crash is honored. Executions whose bound flow
        version is missing are refused and left untouched."""
        recovered: List[str] = []
        self.recovery_refused = []
        async with self._lock:
            for execution_id in ExecutionJournal.list_execution_ids(self.storage_dir):
                sm = self._get_sm(execution_id)
                if sm is None:
                    continue
                status = sm.status
                if status in TERMINAL_STATES or status == 'paused':
                    continue
                journal = self._journals[execution_id]
                if not self._version_available(journal):
                    self.recovery_refused.append(execution_id)
                    continue
                if status in ('running', 'retry_wait'):
                    sm.transition('queued', reason='process restart recovery')
                    self._spawn_runner_locked(execution_id)
                    recovered.append(execution_id)
                elif status == 'awaiting_approval':
                    # Re-arm the approval wait with the persisted token and
                    # deadline; the runner re-enters the approval node and
                    # reuses the pending request from the journal.
                    self._spawn_runner_locked(execution_id)
                    recovered.append(execution_id)
                elif status == 'pausing':
                    # The pause request arrived before the crash; honor it.
                    sm.transition('paused', reason='process restart recovery')
                    recovered.append(execution_id)
        return recovered

    # ------------------------------------------------------------------
    def snapshot(self, execution_id: str) -> Dict[str, Any]:
        journal = self._get_journal(execution_id)
        sm = self._get_sm(execution_id)
        if journal is None or sm is None:
            raise KeyError(execution_id)
        recovery = journal.recover()
        variables = dict(recovery.boundary_variables) if recovery.boundary_node_id \
            else journal.initial_variables()
        return {
            'executionId': execution_id,
            'flowId': journal.flow_id,
            'flowVersion': journal.flow_version(),
            'configHash': journal.config_hash(),
            'status': sm.status,
            'seq': journal.last_seq,
            'allowedTransitions': sm.allowed_transitions(),
            'allowedCommands': sm.allowed_commands(),
            'variables': variables,
            'currentNodeId': recovery.boundary_node_id,
            'completedNodes': recovery.completed_nodes,
            'lastError': recovery.last_error,
            'recoveryRefused': not self._version_available(journal),
            'pendingApprovals': [
                {
                    'nodeId': p['nodeId'],
                    'attempt': p['attempt'],
                    'generation': p.get('generation'),
                    'token': p['token'],
                    'approvers': p.get('approvers', []),
                    'deadline': p['deadline'],
                    'flowVersion': p.get('flowVersion'),
                }
                for p in journal.pending_approvals()
            ],
        }

    def events_since(self, execution_id: str, seq: int) -> List[Dict[str, Any]]:
        journal = self._get_journal(execution_id)
        if journal is None:
            raise KeyError(execution_id)
        return journal.events_since(seq)

    # ------------------------------------------------------------------
    def subscribe(self, execution_id: str) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue()
        self._subscribers.setdefault(execution_id, set()).add(queue)
        return queue

    def unsubscribe(self, execution_id: str, queue: asyncio.Queue) -> None:
        self._subscribers.get(execution_id, set()).discard(queue)

    def _broadcast(self, execution_id: str, event: Dict[str, Any]) -> None:
        # Enrich status events with the server-side allowed commands /
        # transitions for the NEW status, so monitoring clients can drive
        # their button state purely from what the server allows.
        if event.get('type') == 'status':
            to_status = event.get('toStatus', '')
            event = {
                **event,
                'allowedCommands': sorted(
                    cmd for cmd, statuses in COMMAND_ALLOWED_STATUSES.items()
                    if to_status in statuses
                ),
                'allowedTransitions': sorted(
                    ALLOWED_TRANSITIONS.get(to_status, set())
                ),
            }
        for queue in list(self._subscribers.get(execution_id, set())):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass
