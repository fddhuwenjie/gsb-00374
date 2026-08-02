import json
import os
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

from models.flow import FlowDefinition


class JournalError(Exception):
    pass


class RecoveryState:
    """State rebuilt purely from the persisted journal. Never depends on
    in-memory task state, so it survives process restarts."""

    def __init__(self):
        self.status: str = 'queued'
        self.last_seq: int = 0
        # Last fully completed node boundary (None before any node finishes).
        self.boundary_node_id: Optional[str] = None
        self.boundary_next_node_id: Optional[str] = None
        self.boundary_variables: Dict[str, Any] = {}
        self.boundary_loop_counts: Dict[str, int] = {}
        # nodeId -> number of completed attempts (drives attempt numbering)
        self.completed_attempts: Dict[str, int] = {}
        # idempotency key -> side effect record (only successful effects)
        self.side_effects: Dict[str, Dict[str, Any]] = {}
        # commandId -> command processing result (for duplicate detection)
        self.processed_commands: Dict[str, Dict[str, Any]] = {}
        self.last_error: Optional[str] = None
        self.completed_nodes: List[str] = []


class ExecutionJournal:
    """Append-only JSONL journal for one execution.

    Every event gets a strictly monotonic `seq`. Appends are flushed and
    fsynced before returning, so a completed write is durable even if the
    process dies immediately afterwards.
    """

    def __init__(self, storage_dir: str, execution_id: str):
        self.storage_dir = storage_dir
        self.execution_id = execution_id
        os.makedirs(storage_dir, exist_ok=True)
        self._path = os.path.join(storage_dir, f"{self._safe(execution_id)}.journal.jsonl")
        self._events: List[Dict[str, Any]] = []
        self._last_seq = 0
        self._listeners: List[Callable[[Dict[str, Any]], None]] = []
        self._load()

    @staticmethod
    def _safe(execution_id: str) -> str:
        return execution_id.replace('/', '_').replace('\\', '_').replace('..', '_')

    # ------------------------------------------------------------------
    @classmethod
    def create(cls, storage_dir: str, flow: FlowDefinition,
               variables: Optional[Dict[str, Any]] = None,
               execution_id: Optional[str] = None,
               flow_version: Optional[int] = None,
               config_hash: Optional[str] = None) -> 'ExecutionJournal':
        execution_id = execution_id or f"exec_{uuid.uuid4().hex[:16]}"
        journal = cls(storage_dir, execution_id)
        if journal._events:
            raise JournalError(f"Execution {execution_id} already exists")
        journal.append({
            'type': 'created',
            'executionId': execution_id,
            'flowId': flow.id,
            'flow': flow.model_dump(),
            'flowVersion': flow_version,
            'configHash': config_hash,
            'variables': dict(variables or {}),
            'status': 'queued',
        })
        return journal

    @classmethod
    def exists(cls, storage_dir: str, execution_id: str) -> bool:
        path = os.path.join(storage_dir, f"{cls._safe(execution_id)}.journal.jsonl")
        return os.path.exists(path)

    @classmethod
    def list_execution_ids(cls, storage_dir: str) -> List[str]:
        if not os.path.isdir(storage_dir):
            return []
        ids = []
        for name in os.listdir(storage_dir):
            if name.endswith('.journal.jsonl'):
                ids.append(name[:-len('.journal.jsonl')])
        return sorted(ids)

    # ------------------------------------------------------------------
    def _load(self) -> None:
        if not os.path.exists(self._path):
            return
        with open(self._path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    # Tolerate a torn final line from a crash mid-write.
                    continue
                seq = event.get('seq')
                if not isinstance(seq, int) or seq <= self._last_seq:
                    continue
                self._events.append(event)
                self._last_seq = seq

    def append(self, event: Dict[str, Any]) -> Dict[str, Any]:
        event = dict(event)
        event['seq'] = self._last_seq + 1
        event['executionId'] = self.execution_id
        event.setdefault('ts', time.time())
        line = json.dumps(event, ensure_ascii=False)
        with open(self._path, 'a', encoding='utf-8') as f:
            f.write(line + '\n')
            f.flush()
            os.fsync(f.fileno())
        self._events.append(event)
        self._last_seq = event['seq']
        for listener in list(self._listeners):
            try:
                listener(event)
            except Exception:
                pass
        return event

    def add_listener(self, listener: Callable[[Dict[str, Any]], None]) -> None:
        self._listeners.append(listener)

    # ------------------------------------------------------------------
    @property
    def last_seq(self) -> int:
        return self._last_seq

    def events_since(self, seq: int) -> List[Dict[str, Any]]:
        return [e for e in self._events if e['seq'] > seq]

    def all_events(self) -> List[Dict[str, Any]]:
        return list(self._events)

    def flow(self) -> FlowDefinition:
        created = next((e for e in self._events if e['type'] == 'created'), None)
        if not created:
            raise JournalError(f"Journal {self.execution_id} has no created event")
        return FlowDefinition(**created['flow'])

    @property
    def flow_id(self) -> str:
        created = next((e for e in self._events if e['type'] == 'created'), None)
        return created['flowId'] if created else ''

    def flow_version(self) -> Optional[int]:
        created = next((e for e in self._events if e['type'] == 'created'), None)
        return created.get('flowVersion') if created else None

    def config_hash(self) -> Optional[str]:
        created = next((e for e in self._events if e['type'] == 'created'), None)
        return created.get('configHash') if created else None

    def initial_variables(self) -> Dict[str, Any]:
        created = next((e for e in self._events if e['type'] == 'created'), None)
        return dict(created.get('variables', {})) if created else {}

    def current_status(self) -> str:
        for event in reversed(self._events):
            if event['type'] == 'status':
                return event['toStatus']
        return 'queued'

    # ------------------------------------------------------------------
    def append_status_event(self, from_status: Optional[str], to_status: str,
                            reason: Optional[str] = None,
                            node_id: Optional[str] = None,
                            extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.append({
            'type': 'status',
            'fromStatus': from_status,
            'toStatus': to_status,
            'reason': reason,
            'nodeId': node_id,
            **(extra or {}),
        })

    def append_node_started(self, node_id: str, attempt: int) -> Dict[str, Any]:
        return self.append({'type': 'node_started', 'nodeId': node_id, 'attempt': attempt})

    def append_node_completed(self, node_id: str, attempt: int,
                              next_node_id: Optional[str],
                              variables: Dict[str, Any],
                              loop_counts: Dict[str, int]) -> Dict[str, Any]:
        return self.append({
            'type': 'node_completed',
            'nodeId': node_id,
            'attempt': attempt,
            'nextNodeId': next_node_id,
            'variables': dict(variables),
            'loopCounts': dict(loop_counts),
        })

    def append_node_failed(self, node_id: str, attempt: int, error: str) -> Dict[str, Any]:
        return self.append({
            'type': 'node_failed',
            'nodeId': node_id,
            'attempt': attempt,
            'error': error,
        })

    def append_side_effect(self, key: str, node_id: str, attempt: int,
                           generation: int, kind: str,
                           result: Any) -> Dict[str, Any]:
        return self.append({
            'type': 'side_effect',
            'key': key,
            'nodeId': node_id,
            'attempt': attempt,
            'generation': generation,
            'kind': kind,
            'status': 'success',
            'result': result,
        })

    def append_command(self, command_id: str, command: str,
                       result: str, detail: Optional[str] = None) -> Dict[str, Any]:
        return self.append({
            'type': 'command',
            'commandId': command_id,
            'command': command,
            'result': result,
            'detail': detail,
        })

    def append_approval_requested(self, node_id: str, attempt: Any,
                                  generation: Any, token: str,
                                  approvers: List[str], deadline: float,
                                  flow_version: Optional[int]) -> Dict[str, Any]:
        return self.append({
            'type': 'approval_requested',
            'nodeId': node_id,
            'attempt': attempt,
            'generation': generation,
            'token': token,
            'approvers': list(approvers),
            'deadline': deadline,
            'flowVersion': flow_version,
        })

    def append_approval_resolved(self, node_id: str, attempt: Any,
                                 generation: Any, token: str,
                                 decision: str, by: Optional[str]) -> Dict[str, Any]:
        return self.append({
            'type': 'approval_resolved',
            'nodeId': node_id,
            'attempt': attempt,
            'generation': generation,
            'token': token,
            'decision': decision,
            'by': by,
        })

    # ------------------------------------------------------------------
    def approval_resolved_for(self, node_id: str, attempt: Any) -> Optional[Dict[str, Any]]:
        for event in reversed(self._events):
            if (event['type'] == 'approval_resolved'
                    and event.get('nodeId') == node_id
                    and str(event.get('attempt')) == str(attempt)):
                return event
        return None

    def pending_approvals(self) -> List[Dict[str, Any]]:
        """Latest approval request per nodeId that has no matching resolution.
        A superseded request (a newer request exists for the same node, e.g.
        from a newer parallel generation) is stale and never returned."""
        latest_request: Dict[str, Dict[str, Any]] = {}
        resolved_tokens = set()
        for event in self._events:
            if event['type'] == 'approval_requested':
                latest_request[event['nodeId']] = event
            elif event['type'] == 'approval_resolved':
                resolved_tokens.add(event.get('token'))
        return [
            e for e in latest_request.values()
            if e.get('token') not in resolved_tokens
        ]

    def pending_approval_for(self, node_id: str, attempt: Any) -> Optional[Dict[str, Any]]:
        for event in self.pending_approvals():
            if event['nodeId'] == node_id and str(event.get('attempt')) == str(attempt):
                return event
        return None

    # ------------------------------------------------------------------
    def side_effect(self, key: str) -> Optional[Dict[str, Any]]:
        for event in reversed(self._events):
            if event['type'] == 'side_effect' and event.get('key') == key:
                return event
        return None

    def completed_attempts(self, node_id: str) -> int:
        return sum(
            1 for e in self._events
            if e['type'] == 'node_completed' and e.get('nodeId') == node_id
        )

    def processed_command(self, command_id: str) -> Optional[Dict[str, Any]]:
        for event in self._events:
            if event['type'] == 'command' and event.get('commandId') == command_id:
                return event
        return None

    # ------------------------------------------------------------------
    def recover(self) -> RecoveryState:
        """Rebuild resumable state from the journal only. The resume point is
        the most recent *complete* node boundary; a node that started (or had
        a recorded side effect) but never completed is re-attempted, with its
        successful side effects reused via idempotency keys."""
        state = RecoveryState()
        state.status = self.current_status()
        state.last_seq = self._last_seq

        for event in self._events:
            etype = event['type']
            if etype == 'node_completed':
                nid = event['nodeId']
                state.boundary_node_id = nid
                state.boundary_next_node_id = event.get('nextNodeId')
                state.boundary_variables = dict(event.get('variables', {}))
                state.boundary_loop_counts = dict(event.get('loopCounts', {}))
                state.completed_attempts[nid] = state.completed_attempts.get(nid, 0) + 1
                state.completed_nodes.append(nid)
                state.last_error = None
            elif etype == 'node_failed':
                state.last_error = event.get('error')
            elif etype == 'side_effect':
                if event.get('status') == 'success':
                    state.side_effects[event['key']] = event
            elif etype == 'command':
                state.processed_commands[event['commandId']] = event

        return state
