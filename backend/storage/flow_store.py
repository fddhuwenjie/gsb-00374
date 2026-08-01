import json
import os
import time
from typing import List, Optional
from models.flow import FlowDefinition


class FlowStore:
    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)

    def _get_file_path(self, flow_id: str) -> str:
        safe_id = flow_id.replace('/', '_').replace('\\', '_').replace('..', '_')
        return os.path.join(self.storage_dir, f"{safe_id}.json")

    def list_flows(self) -> List[FlowDefinition]:
        flows = []
        for filename in os.listdir(self.storage_dir):
            if filename.endswith('.json'):
                try:
                    filepath = os.path.join(self.storage_dir, filename)
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        flows.append(FlowDefinition(**data))
                except Exception:
                    continue
        return sorted(flows, key=lambda f: f.updatedAt, reverse=True)

    def get_flow(self, flow_id: str) -> Optional[FlowDefinition]:
        filepath = self._get_file_path(flow_id)
        if not os.path.exists(filepath):
            return None
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return FlowDefinition(**data)
        except Exception:
            return None

    def create_flow(self, flow: FlowDefinition) -> FlowDefinition:
        flow.createdAt = time.time()
        flow.updatedAt = flow.createdAt
        filepath = self._get_file_path(flow.id)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(flow.model_dump(), f, indent=2, ensure_ascii=False)
        return flow

    def update_flow(self, flow_id: str, flow: FlowDefinition) -> Optional[FlowDefinition]:
        if not os.path.exists(self._get_file_path(flow_id)):
            return None
        flow.id = flow_id
        flow.updatedAt = time.time()
        existing = self.get_flow(flow_id)
        if existing:
            flow.createdAt = existing.createdAt
        filepath = self._get_file_path(flow_id)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(flow.model_dump(), f, indent=2, ensure_ascii=False)
        return flow

    def delete_flow(self, flow_id: str) -> bool:
        filepath = self._get_file_path(flow_id)
        if not os.path.exists(filepath):
            return False
        os.remove(filepath)
        return True
