import json
import os
import threading
import time
import uuid
from typing import Callable, Dict, List, Optional

from models.flow import StateEvent, PersistedExecutionRecord, ExecutionSnapshot


def _safe_name(execution_id: str) -> str:
    return execution_id.replace('/', '_').replace('\\', '_').replace('..', '_')


class EventStore:
    """Append-only event store with per-execution locks and atomic writes.

    Layout: <storage_dir>/<execution_id>/record.json and events.jsonl
    """

    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)
        self._locks: Dict[str, threading.RLock] = {}
        self._locks_guard = threading.Lock()
        self._subscribers: List[Callable[[StateEvent], None]] = []
        self._subscribers_lock = threading.Lock()

    def _get_lock(self, execution_id: str) -> threading.RLock:
        with self._locks_guard:
            lock = self._locks.get(execution_id)
            if lock is None:
                lock = threading.RLock()
                self._locks[execution_id] = lock
            return lock

    def _exec_dir(self, execution_id: str) -> str:
        path = os.path.join(self.storage_dir, _safe_name(execution_id))
        os.makedirs(path, exist_ok=True)
        return path

    def _record_path(self, execution_id: str) -> str:
        return os.path.join(self._exec_dir(execution_id), 'record.json')

    def _events_path(self, execution_id: str) -> str:
        return os.path.join(self._exec_dir(execution_id), 'events.jsonl')

    def subscribe(self, callback: Callable[[StateEvent], None]) -> Callable[[], None]:
        with self._subscribers_lock:
            self._subscribers.append(callback)

        def unsubscribe():
            with self._subscribers_lock:
                if callback in self._subscribers:
                    self._subscribers.remove(callback)

        return unsubscribe

    def _publish(self, event: StateEvent) -> None:
        with self._subscribers_lock:
            subs = list(self._subscribers)
        for cb in subs:
            try:
                cb(event)
            except Exception:
                pass

    def create_execution_id(self) -> str:
        return f"exec_{int(time.time() * 1000)}_{uuid.uuid4().hex[:10]}"

    def init_record(self, record: PersistedExecutionRecord) -> None:
        lock = self._get_lock(record.executionId)
        with lock:
            path = self._record_path(record.executionId)
            self._atomic_write(path, record.model_dump_json())
            events_path = self._events_path(record.executionId)
            if not os.path.exists(events_path):
                open(events_path, 'a', encoding='utf-8').close()

    def save_record(self, record: PersistedExecutionRecord) -> None:
        lock = self._get_lock(record.executionId)
        with lock:
            self._atomic_write(self._record_path(record.executionId), record.model_dump_json())

    def load_record(self, execution_id: str) -> Optional[PersistedExecutionRecord]:
        path = self._record_path(execution_id)
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return PersistedExecutionRecord(**data)
        except Exception:
            return None

    def list_execution_ids(self) -> List[str]:
        ids: List[str] = []
        if not os.path.isdir(self.storage_dir):
            return ids
        for name in os.listdir(self.storage_dir):
            full = os.path.join(self.storage_dir, name)
            if os.path.isdir(full) and os.path.exists(os.path.join(full, 'record.json')):
                ids.append(name)
        return ids

    def next_seq(self, execution_id: str) -> int:
        record = self.load_record(execution_id)
        if record is None:
            return 1
        return record.seq + 1

    def append_event(self, event: StateEvent) -> StateEvent:
        lock = self._get_lock(event.executionId)
        with lock:
            record = self.load_record(event.executionId)
            next_seq = (record.seq if record else 0) + 1
            event.seq = next_seq
            if event.timestamp is None or event.timestamp == 0:
                event.timestamp = time.time()

            events_path = self._events_path(event.executionId)
            with open(events_path, 'a', encoding='utf-8') as f:
                f.write(event.model_dump_json() + '\n')
                f.flush()
                os.fsync(f.fileno())

            if record is not None:
                record.seq = next_seq
                if event.toState is not None:
                    record.status = event.toState
                record.updatedAt = time.time()
                self._atomic_write(self._record_path(event.executionId), record.model_dump_json())

        self._publish(event)
        return event

    def append_events(self, execution_id: str, events: List[StateEvent]) -> List[StateEvent]:
        results: List[StateEvent] = []
        if not events:
            return results
        lock = self._get_lock(execution_id)
        with lock:
            record = self.load_record(execution_id)
            base_seq = record.seq if record else 0

            events_path = self._events_path(execution_id)
            now = time.time()
            with open(events_path, 'a', encoding='utf-8') as f:
                for event in events:
                    base_seq += 1
                    event.seq = base_seq
                    if event.timestamp is None or event.timestamp == 0:
                        event.timestamp = now
                    f.write(event.model_dump_json() + '\n')
                    results.append(event)
                f.flush()
                os.fsync(f.fileno())

            if record is not None:
                record.seq = base_seq
                for event in reversed(results):
                    if event.toState is not None:
                        record.status = event.toState
                        break
                record.updatedAt = now
                self._atomic_write(self._record_path(execution_id), record.model_dump_json())

        for event in results:
            self._publish(event)
        return results

    def read_events(self, execution_id: str, after_seq: int = 0) -> List[StateEvent]:
        events_path = self._events_path(execution_id)
        if not os.path.exists(events_path):
            return []
        events: List[StateEvent] = []
        with open(events_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = StateEvent(**json.loads(line))
                    if event.seq > after_seq:
                        events.append(event)
                except Exception:
                    continue
        return events

    def read_all_events(self, execution_id: str) -> List[StateEvent]:
        return self.read_events(execution_id, after_seq=0)

    def reconcile_record(self, execution_id: str) -> Optional[PersistedExecutionRecord]:
        """On recovery, ensure record.seq matches the actual event log length."""
        lock = self._get_lock(execution_id)
        with lock:
            record = self.load_record(execution_id)
            if record is None:
                return None
            events = self.read_all_events(execution_id)
            if events:
                max_seq = max(e.seq for e in events)
                if max_seq > record.seq:
                    record.seq = max_seq
                for event in reversed(events):
                    if event.toState is not None:
                        record.status = event.toState
                        break
                self._atomic_write(self._record_path(execution_id), record.model_dump_json())
            return record

    def delete_execution(self, execution_id: str) -> bool:
        path = os.path.join(self.storage_dir, _safe_name(execution_id))
        if not os.path.exists(path):
            return False
        import shutil
        shutil.rmtree(path, ignore_errors=True)
        return True

    @staticmethod
    def _atomic_write(path: str, content: str) -> None:
        tmp_path = path + '.tmp'
        with open(tmp_path, 'w', encoding='utf-8') as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
