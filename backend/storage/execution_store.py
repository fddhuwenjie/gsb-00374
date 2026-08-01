import json
import os
import time
from typing import List, Optional
from models.flow import Execution


class ExecutionStore:
    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)

    def _get_file_path(self, execution_id: str) -> str:
        safe_id = execution_id.replace('/', '_').replace('\\', '_').replace('..', '_')
        return os.path.join(self.storage_dir, f"exec_{safe_id}.json")

    def save_execution(self, execution: Execution) -> str:
        filepath = self._get_file_path(execution.id)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(execution.model_dump(), f, indent=2, ensure_ascii=False)
        return execution.id

    def get_execution(self, execution_id: str) -> Optional[Execution]:
        filepath = self._get_file_path(execution_id)
        if not os.path.exists(filepath):
            return None
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return Execution(**data)
        except Exception:
            return None

    def list_executions(self, flow_id: Optional[str] = None) -> List[Execution]:
        executions = []
        for filename in os.listdir(self.storage_dir):
            if filename.startswith('exec_') and filename.endswith('.json'):
                try:
                    filepath = os.path.join(self.storage_dir, filename)
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        execution = Execution(**data)
                        if flow_id is None or execution.flowId == flow_id:
                            executions.append(execution)
                except Exception:
                    continue
        return sorted(executions, key=lambda e: e.startedAt, reverse=True)

    def delete_execution(self, execution_id: str) -> bool:
        filepath = self._get_file_path(execution_id)
        if not os.path.exists(filepath):
            return False
        os.remove(filepath)
        return True

    def create_execution_id(self) -> str:
        return f"exec_{int(time.time() * 1000)}_{int(time.time() % 1000000)}"
