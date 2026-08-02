import hashlib
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from models.flow import FlowDefinition


class VersionError(Exception):
    pass


class VersionInUseError(VersionError):
    pass


class VersionNotFoundError(VersionError):
    pass


def compute_config_hash(flow: FlowDefinition) -> str:
    """Stable hash over node identities + full node configs + edge topology.
    Position/labels-only cosmetic fields are part of data, matching what the
    executor consumes; ordering is normalized so the hash is deterministic."""
    nodes = sorted(
        (
            {'id': n.id, 'type': n.type, 'data': n.data.model_dump()}
            for n in flow.nodes
        ),
        key=lambda x: x['id'],
    )
    edges = sorted(
        (
            {'source': e.source, 'target': e.target,
             'sourceHandle': e.sourceHandle or ''}
            for e in flow.edges
        ),
        key=lambda x: (x['source'], x['target'], x['sourceHandle']),
    )
    canonical = json.dumps(
        {'nodes': nodes, 'edges': edges},
        sort_keys=True, ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def _edge_key(edge: Dict[str, Any]) -> str:
    return f"{edge['source']}->{edge['target']}:{edge.get('sourceHandle') or ''}"


def diff_flows(old: FlowDefinition, new: FlowDefinition) -> Dict[str, List[str]]:
    """Stable categorization of definition changes: node added / removed /
    config changed, and edge (connection) added / removed."""
    old_nodes = {n.id: n for n in old.nodes}
    new_nodes = {n.id: n for n in new.nodes}

    nodes_added = sorted(set(new_nodes) - set(old_nodes))
    nodes_removed = sorted(set(old_nodes) - set(new_nodes))
    nodes_config_changed = sorted(
        nid for nid in set(old_nodes) & set(new_nodes)
        if old_nodes[nid].type != new_nodes[nid].type
        or old_nodes[nid].data.model_dump() != new_nodes[nid].data.model_dump()
    )

    old_edges = {
        _edge_key(e.model_dump()) for e in old.edges
    }
    new_edges = {
        _edge_key(e.model_dump()) for e in new.edges
    }
    return {
        'nodesAdded': nodes_added,
        'nodesRemoved': nodes_removed,
        'nodesConfigChanged': nodes_config_changed,
        'edgesAdded': sorted(new_edges - old_edges),
        'edgesRemoved': sorted(old_edges - new_edges),
    }


class FlowVersionStore:
    """Immutable, append-only store of flow definition versions.

    Saving a flow never mutates an existing version: identical content binds
    to the existing version, changed content creates version max+1. Every
    version carries the config hash that executions bind to.
    """

    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)

    @staticmethod
    def _safe(flow_id: str) -> str:
        return flow_id.replace('/', '_').replace('\\', '_').replace('..', '_')

    def _flow_dir(self, flow_id: str) -> str:
        return os.path.join(self.storage_dir, self._safe(flow_id))

    def _path(self, flow_id: str, version: int) -> str:
        return os.path.join(self._flow_dir(flow_id), f"v{version}.json")

    # ------------------------------------------------------------------
    def save_version(self, flow: FlowDefinition) -> Dict[str, Any]:
        config_hash = compute_config_hash(flow)
        existing = self.get_by_hash(flow.id, config_hash)
        if existing is not None:
            return existing
        version = self.next_version(flow.id)
        record = {
            'flowId': flow.id,
            'version': version,
            'configHash': config_hash,
            'flow': flow.model_dump(),
            'createdAt': time.time(),
        }
        flow_dir = self._flow_dir(flow.id)
        os.makedirs(flow_dir, exist_ok=True)
        path = self._path(flow.id, version)
        if os.path.exists(path):
            raise VersionError(f"Version file already exists: {path}")
        tmp_path = path + '.tmp'
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(record, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
        return record

    def next_version(self, flow_id: str) -> int:
        versions = [v['version'] for v in self.list_versions(flow_id)]
        return max(versions, default=0) + 1

    def get(self, flow_id: str, version: int) -> Optional[Dict[str, Any]]:
        path = self._path(flow_id, version)
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return None

    def get_by_hash(self, flow_id: str, config_hash: str) -> Optional[Dict[str, Any]]:
        for record in self.list_versions(flow_id):
            if record.get('configHash') == config_hash:
                return record
        return None

    def list_versions(self, flow_id: str) -> List[Dict[str, Any]]:
        flow_dir = self._flow_dir(flow_id)
        if not os.path.isdir(flow_dir):
            return []
        records = []
        for name in os.listdir(flow_dir):
            if name.startswith('v') and name.endswith('.json'):
                try:
                    with open(os.path.join(flow_dir, name), 'r', encoding='utf-8') as f:
                        records.append(json.load(f))
                except Exception:
                    continue
        return sorted(records, key=lambda r: r['version'])

    def list_flow_ids(self) -> List[str]:
        if not os.path.isdir(self.storage_dir):
            return []
        return sorted(
            name for name in os.listdir(self.storage_dir)
            if os.path.isdir(os.path.join(self.storage_dir, name))
        )

    # ------------------------------------------------------------------
    def delete(self, flow_id: str, version: int,
               referenced: Optional[set] = None) -> None:
        """Deletion protection: a version referenced by any execution journal
        can never be deleted."""
        referenced = referenced or set()
        if (flow_id, version) in referenced:
            raise VersionInUseError(
                f"Version {version} of flow {flow_id} is referenced by executions"
            )
        path = self._path(flow_id, version)
        if not os.path.exists(path):
            raise VersionNotFoundError(f"Version {version} of {flow_id} not found")
        os.remove(path)

    # ------------------------------------------------------------------
    def diff(self, flow_id: str, from_version: int, to_version: int) -> Dict[str, Any]:
        old = self.get(flow_id, from_version)
        new = self.get(flow_id, to_version)
        if old is None or new is None:
            raise VersionNotFoundError(
                f"Missing version(s) for diff: {from_version}, {to_version}"
            )
        result = diff_flows(
            FlowDefinition(**old['flow']), FlowDefinition(**new['flow'])
        )
        result['fromVersion'] = from_version
        result['toVersion'] = to_version
        result['fromConfigHash'] = old['configHash']
        result['toConfigHash'] = new['configHash']
        return result

    # ------------------------------------------------------------------
    def export_version(self, flow_id: str, version: int) -> Dict[str, Any]:
        record = self.get(flow_id, version)
        if record is None:
            raise VersionNotFoundError(f"Version {version} of {flow_id} not found")
        return {
            'format': 'flow-version-bundle/v1',
            'flowId': record['flowId'],
            'configHash': record['configHash'],
            'flow': record['flow'],
            'exportedAt': time.time(),
        }

    def import_bundle(self, bundle: Dict[str, Any]) -> Dict[str, Any]:
        if bundle.get('format') != 'flow-version-bundle/v1':
            raise VersionError('Unsupported bundle format')
        flow = FlowDefinition(**bundle['flow'])
        # Content defines identity: identical content binds to the existing
        # version, otherwise a new version is appended. Hash is recomputed
        # so a tampered bundle cannot smuggle a mismatched hash.
        record = self.save_version(flow)
        return record
