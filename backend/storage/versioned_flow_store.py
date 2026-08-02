import json
import os
import time
from typing import Dict, List, Optional, Tuple

from models.flow import (
    FlowDefinition,
    FlowVersion,
    FlowVersionMeta,
)
from engine.flow_version import compute_node_config_hash, diff_flows, is_structurally_identical


def _safe_name(flow_id: str) -> str:
    return flow_id.replace('/', '_').replace('\\', '_').replace('..', '_')


class VersionNotFound(Exception):
    pass


class VersionInUse(Exception):
    """Raised when attempting to delete a version referenced by executions."""


class VersionedFlowStore:
    """Stores flows with immutable, append-only version history.

    Layout:
      <storage_dir>/<flow_id>.json              -> latest FlowDefinition
      <storage_dir>/<flow_id>.versions.json     -> version index + metadata
      <storage_dir>/versions/<flow_id>/v<N>.json -> immutable FlowVersion snapshot
    """

    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)
        self._versions_dir = os.path.join(storage_dir, 'versions')
        os.makedirs(self._versions_dir, exist_ok=True)

    # ----- paths -----

    def _flow_path(self, flow_id: str) -> str:
        return os.path.join(self.storage_dir, f"{_safe_name(flow_id)}.json")

    def _index_path(self, flow_id: str) -> str:
        return os.path.join(self.storage_dir, f"{_safe_name(flow_id)}.versions.json")

    def _version_dir(self, flow_id: str) -> str:
        d = os.path.join(self._versions_dir, _safe_name(flow_id))
        os.makedirs(d, exist_ok=True)
        return d

    def _version_path(self, flow_id: str, version: int) -> str:
        return os.path.join(self._version_dir(flow_id), f"v{version}.json")

    # ----- low-level helpers -----

    @staticmethod
    def _atomic_write(path: str, content: str) -> None:
        tmp = path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            f.write(content)
        os.replace(tmp, path)

    def _read_index(self, flow_id: str) -> Optional[Dict]:
        path = self._index_path(flow_id)
        if not os.path.exists(path):
            return None
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def _write_index(self, flow_id: str, index: Dict) -> None:
        self._atomic_write(self._index_path(flow_id),
                           json.dumps(index, indent=2, ensure_ascii=False))

    def _persist_version(self, flow: FlowDefinition, version: int,
                         comment: Optional[str] = None) -> FlowVersion:
        cfg_hash = compute_node_config_hash(flow)
        flow.version = version
        meta = FlowVersionMeta(
            version=version,
            flowId=flow.id,
            name=flow.name,
            createdAt=time.time(),
            nodeConfigHash=cfg_hash,
            comment=comment,
        )
        fv = FlowVersion(meta=meta, definition=flow)
        self._atomic_write(
            self._version_path(flow.id, version),
            fv.model_dump_json(indent=2),
        )
        return fv

    # ----- public API: latest -----

    def list_flows(self) -> List[FlowDefinition]:
        flows: List[FlowDefinition] = []
        for filename in os.listdir(self.storage_dir):
            if not filename.endswith('.json') or filename.endswith('.versions.json'):
                continue
            try:
                with open(os.path.join(self.storage_dir, filename),
                          'r', encoding='utf-8') as f:
                    flows.append(FlowDefinition(**json.load(f)))
            except Exception:
                continue
        return sorted(flows, key=lambda f: f.updatedAt, reverse=True)

    def get_flow(self, flow_id: str) -> Optional[FlowDefinition]:
        path = self._flow_path(flow_id)
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return FlowDefinition(**json.load(f))
        except Exception:
            return None

    def get_latest_version_number(self, flow_id: str) -> int:
        idx = self._read_index(flow_id)
        if not idx:
            return 0
        return idx.get('latest', 0)

    def create_flow(self, flow: FlowDefinition,
                    comment: Optional[str] = None) -> Tuple[FlowDefinition, FlowVersion]:
        now = time.time()
        flow.createdAt = now
        flow.updatedAt = now
        version = 1
        fv = self._persist_version(flow, version, comment=comment)
        flow.version = version

        self._atomic_write(self._flow_path(flow.id),
                           flow.model_dump_json(indent=2))
        self._write_index(flow.id, {
            'flowId': flow.id,
            'latest': version,
            'versions': [fv.meta.model_dump()],
        })
        return flow, fv

    def update_flow(self, flow_id: str, flow: FlowDefinition,
                    comment: Optional[str] = None,
                    force: bool = False
                    ) -> Tuple[Optional[FlowDefinition], Optional[FlowVersion]]:
        """Save a new version of the flow.

        If the new definition is structurally identical to the current
        latest version, no new version is created (unless *force* is True)
        and the existing latest is returned.

        Returns (updated_flow, new_or_existing_version).  ``None`` if the
        flow does not exist.
        """
        existing = self.get_flow(flow_id)
        if existing is None:
            return None, None

        flow.id = flow_id
        idx = self._read_index(flow_id) or {'versions': [], 'latest': 0}
        latest_num = idx.get('latest', 0)

        if not force and latest_num > 0 and is_structurally_identical(existing, flow):
            flow.version = latest_num
            flow.createdAt = existing.createdAt
            flow.updatedAt = time.time()
            self._atomic_write(self._flow_path(flow_id),
                               flow.model_dump_json(indent=2))
            fv = self.get_version(flow_id, latest_num)
            return flow, fv

        new_version = latest_num + 1
        flow.createdAt = existing.createdAt
        flow.updatedAt = time.time()
        fv = self._persist_version(flow, new_version, comment=comment)
        flow.version = new_version

        self._atomic_write(self._flow_path(flow_id),
                           flow.model_dump_json(indent=2))
        versions = idx.get('versions', [])
        versions.append(fv.meta.model_dump())
        idx['latest'] = new_version
        idx['versions'] = versions
        self._write_index(flow_id, idx)
        return flow, fv

    # ----- public API: versions -----

    def list_versions(self, flow_id: str) -> List[FlowVersionMeta]:
        idx = self._read_index(flow_id)
        if not idx:
            return []
        return [FlowVersionMeta(**v) for v in idx.get('versions', [])]

    def get_version(self, flow_id: str, version: int) -> Optional[FlowVersion]:
        path = self._version_path(flow_id, version)
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return FlowVersion(**json.load(f))
        except Exception:
            return None

    def get_version_definition(self, flow_id: str,
                               version: int) -> Optional[FlowDefinition]:
        fv = self.get_version(flow_id, version)
        return fv.definition if fv else None

    def diff_versions(self, flow_id: str,
                      from_version: int, to_version: int
                      ) -> Optional[dict]:
        a = self.get_version_definition(flow_id, from_version)
        b = self.get_version_definition(flow_id, to_version)
        if a is None or b is None:
            return None
        return diff_flows(a, b).model_dump()

    def delete_version(self, flow_id: str, version: int,
                       active_version_check=None) -> bool:
        """Delete a single immutable version.

        The latest version cannot be deleted.  If *active_version_check*
        is provided, it is called with (flow_id, version) and must return
        the number of active executions referencing that version; a
        non-zero count raises :class:`VersionInUse`.
        """
        idx = self._read_index(flow_id)
        if not idx:
            return False
        latest = idx.get('latest', 0)
        if version == latest:
            raise VersionInUse(
                f"Cannot delete latest version v{version} of flow {flow_id}"
            )
        if version < 1 or version > latest:
            return False

        if active_version_check is not None:
            count = active_version_check(flow_id, version)
            if count:
                raise VersionInUse(
                    f"Version v{version} of flow {flow_id} is referenced by "
                    f"{count} active execution(s)"
                )

        path = self._version_path(flow_id, version)
        if os.path.exists(path):
            os.remove(path)

        idx['versions'] = [
            v for v in idx.get('versions', []) if v.get('version') != version
        ]
        self._write_index(flow_id, idx)
        return True

    def delete_flow(self, flow_id: str,
                    active_version_check=None) -> bool:
        """Delete the flow and *all* its versions.

        If *active_version_check* is provided, deletion is refused when
        any version still has active executions.
        """
        idx = self._read_index(flow_id)
        if idx is None and not os.path.exists(self._flow_path(flow_id)):
            return False

        if active_version_check is not None and idx:
            for vmeta in idx.get('versions', []):
                v = vmeta.get('version', 0)
                count = active_version_check(flow_id, v)
                if count:
                    raise VersionInUse(
                        f"Flow {flow_id} v{v} is referenced by "
                        f"{count} active execution(s)"
                    )

        fp = self._flow_path(flow_id)
        if os.path.exists(fp):
            os.remove(fp)
        ip = self._index_path(flow_id)
        if os.path.exists(ip):
            os.remove(ip)
        vdir = os.path.join(self._versions_dir, _safe_name(flow_id))
        if os.path.isdir(vdir):
            import shutil
            shutil.rmtree(vdir, ignore_errors=True)
        return True

    # ----- backwards-compatible wrapper used by older code -----

    def create_or_update(self, flow: FlowDefinition,
                         comment: Optional[str] = None
                         ) -> Tuple[FlowDefinition, FlowVersion]:
        """Idempotent helper used by callers that don't know if a flow
        already exists."""
        if self.get_flow(flow.id) is None:
            return self.create_flow(flow, comment=comment)
        updated, fv = self.update_flow(flow.id, flow, comment=comment)
        return updated, fv
