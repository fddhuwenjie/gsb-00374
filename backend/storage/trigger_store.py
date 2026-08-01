import json
import os
import time
from typing import List, Optional
from models.flow import Trigger


class TriggerStore:
    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)
        self._init_db_file()

    def _init_db_file(self):
        db_file = os.path.join(self.storage_dir, 'triggers.json')
        if not os.path.exists(db_file):
            with open(db_file, 'w', encoding='utf-8') as f:
                json.dump([], f)

    def _get_db_path(self) -> str:
        return os.path.join(self.storage_dir, 'triggers.json')

    def _read_all(self) -> List[dict]:
        try:
            with open(self._get_db_path(), 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return []

    def _write_all(self, data: List[dict]) -> None:
        with open(self._get_db_path(), 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def list_triggers(self, flow_id: Optional[str] = None) -> List[Trigger]:
        data = self._read_all()
        triggers = []
        for item in data:
            try:
                t = Trigger(**item)
                if flow_id is None or t.flowId == flow_id:
                    triggers.append(t)
            except Exception:
                continue
        return sorted(triggers, key=lambda t: t.createdAt, reverse=True)

    def get_trigger(self, trigger_id: str) -> Optional[Trigger]:
        data = self._read_all()
        for item in data:
            if item.get('id') == trigger_id:
                try:
                    return Trigger(**item)
                except Exception:
                    return None
        return None

    def get_trigger_by_webhook_path(self, webhook_path: str) -> Optional[Trigger]:
        data = self._read_all()
        for item in data:
            if item.get('webhookPath') == webhook_path and item.get('type') == 'webhook':
                try:
                    return Trigger(**item)
                except Exception:
                    return None
        return None

    def get_triggers_by_source_flow(self, source_flow_id: str) -> List[Trigger]:
        data = self._read_all()
        triggers = []
        for item in data:
            if (item.get('type') == 'flow_completed' and
                item.get('sourceFlowId') == source_flow_id and
                item.get('enabled', True)):
                try:
                    triggers.append(Trigger(**item))
                except Exception:
                    continue
        return triggers

    def create_trigger(self, trigger: Trigger) -> Trigger:
        trigger.createdAt = time.time()
        data = self._read_all()
        data.append(trigger.model_dump())
        self._write_all(data)
        return trigger

    def update_trigger(self, trigger_id: str, trigger: Trigger) -> Optional[Trigger]:
        data = self._read_all()
        for i, item in enumerate(data):
            if item.get('id') == trigger_id:
                existing = Trigger(**item)
                trigger.id = trigger_id
                trigger.createdAt = existing.createdAt
                data[i] = trigger.model_dump()
                self._write_all(data)
                return trigger
        return None

    def delete_trigger(self, trigger_id: str) -> bool:
        data = self._read_all()
        new_data = [item for item in data if item.get('id') != trigger_id]
        if len(new_data) != len(data):
            self._write_all(new_data)
            return True
        return False

    def toggle_trigger(self, trigger_id: str, enabled: bool) -> Optional[Trigger]:
        data = self._read_all()
        for i, item in enumerate(data):
            if item.get('id') == trigger_id:
                item['enabled'] = enabled
                self._write_all(data)
                try:
                    return Trigger(**item)
                except Exception:
                    return None
        return None
