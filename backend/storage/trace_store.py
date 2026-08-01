import json
import os
import time
from typing import List, Optional
from models.flow import TraceLog


class TraceStore:
    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)

    def _get_file_path(self, trace_id: str) -> str:
        safe_id = trace_id.replace('/', '_').replace('\\', '_').replace('..', '_')
        return os.path.join(self.storage_dir, f"trace_{safe_id}.json")

    def save_trace(self, flow_id: str, trace: List[TraceLog]) -> str:
        trace_id = f"{flow_id}_{int(time.time() * 1000)}"
        filepath = self._get_file_path(trace_id)
        data = {
            'traceId': trace_id,
            'flowId': flow_id,
            'createdAt': time.time(),
            'trace': [log.model_dump() for log in trace]
        }
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return trace_id

    def get_trace(self, trace_id: str) -> Optional[dict]:
        filepath = self._get_file_path(trace_id)
        if not os.path.exists(filepath):
            return None
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)

    def list_traces(self, flow_id: Optional[str] = None) -> List[dict]:
        traces = []
        for filename in os.listdir(self.storage_dir):
            if filename.startswith('trace_') and filename.endswith('.json'):
                try:
                    filepath = os.path.join(self.storage_dir, filename)
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if flow_id is None or data.get('flowId') == flow_id:
                            traces.append(data)
                except Exception:
                    continue
        return sorted(traces, key=lambda t: t.get('createdAt', 0), reverse=True)
