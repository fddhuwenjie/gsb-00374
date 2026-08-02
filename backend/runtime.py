import os
from typing import Optional

from engine.execution_manager import ExecutionManager
from storage.flow_version_store import FlowVersionStore

_manager: Optional[ExecutionManager] = None

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_STORAGE_DIR = os.path.join(BASE_DIR, 'flows', 'v2_executions')
DEFAULT_VERSION_DIR = os.path.join(BASE_DIR, 'flows', 'versions')


def init_manager(storage_dir: Optional[str] = None,
                 version_dir: Optional[str] = None) -> ExecutionManager:
    global _manager
    _manager = ExecutionManager(
        storage_dir or DEFAULT_STORAGE_DIR,
        version_store=FlowVersionStore(version_dir or DEFAULT_VERSION_DIR),
    )
    return _manager


def get_manager() -> ExecutionManager:
    global _manager
    if _manager is None:
        _manager = init_manager()
    return _manager
