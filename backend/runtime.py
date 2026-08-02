import os
from typing import Optional

from storage.event_store import EventStore
from storage.versioned_flow_store import VersionedFlowStore
from storage.trace_store import TraceStore
from engine.execution_manager import ExecutionManager


_BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def get_flows_dir() -> str:
    return os.environ.get('FLOWS_DIR', os.path.join(_BASE_DIR, 'flows'))


class Runtime:
    def __init__(self, flows_dir: Optional[str] = None):
        self.flows_dir = flows_dir or get_flows_dir()
        os.makedirs(self.flows_dir, exist_ok=True)
        self.event_store = EventStore(os.path.join(self.flows_dir, 'event_store'))
        self.flow_store = VersionedFlowStore(self.flows_dir)
        self.trace_store = TraceStore(os.path.join(self.flows_dir, 'traces'))
        self.manager = ExecutionManager(self.event_store, self.flow_store)


_runtime: Optional[Runtime] = None


def get_runtime() -> Runtime:
    global _runtime
    if _runtime is None:
        _runtime = Runtime()
    return _runtime


def set_runtime(runtime: Optional[Runtime]) -> None:
    global _runtime
    _runtime = runtime


def reset_runtime() -> None:
    global _runtime
    _runtime = None


def get_manager() -> ExecutionManager:
    return get_runtime().manager
