"""Immutable workflow definition versioning for the durable engine.

Every durable execution is bound at creation time to an *immutable* flow version:
a monotonically increasing integer ``flowVersion`` plus content hashes that pin
the exact node configuration and wiring the execution ran against. Editing a flow
whose id already has versions never mutates an existing version -- it always
creates a new one. Old executions therefore keep recovering, retrying and
backfilling against their *original* version, even while newer versions exist.

Layout (under ``<root>/durable/versions/<flowId>/``)::

    v1.json, v2.json, ...   immutable version records (never overwritten)
    latest.json             pointer {"latest": N}

A version record::

    {
      "flowId": ...,
      "version": N,
      "createdAt": ts,
      "spec": {...},                     # the exact DurableFlow spec
      "contentHash": "sha256:...",       # hash of the whole canonical spec
      "nodeHashes": {nodeId: "sha256:.."},   # per-node config hash (no wiring)
      "edges": [[src, "next", dst], ...],    # normalized wiring set
    }

Hashing is *canonical*: dict keys are sorted and JSON is emitted deterministically
so logically-identical specs always hash identically regardless of key order.

Per-node ``nodeHashes`` deliberately exclude the ``next`` pointer (wiring) so the
diff can cleanly separate a node *configuration* change from a *connection*
change: the ``next`` field lives in the edge set, not the node config hash.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
from typing import Any, Dict, List, Optional, Tuple


# Fields that describe wiring rather than node configuration. They are hashed as
# part of the edge set, not the per-node config hash.
_WIRING_FIELDS = ("next",)


class MissingVersionError(Exception):
    """Raised when an execution references a flow version that is not on disk."""


class VersionInUseError(Exception):
    """Raised when attempting to delete a version still bound to executions."""

    def __init__(self, flow_id: str, version: int, execution_ids: List[str]):
        self.flow_id = flow_id
        self.version = version
        self.execution_ids = execution_ids
        super().__init__(
            f"Flow {flow_id} v{version} is still used by executions: {execution_ids}"
        )


def _canonical_json(obj: Any) -> str:
    """Deterministic JSON: sorted keys, no incidental whitespace."""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def content_hash(spec: Dict[str, Any]) -> str:
    """Hash of the entire canonical flow spec (config + wiring)."""
    return _sha256(_canonical_json(spec))


def node_config_hash(node: Dict[str, Any]) -> str:
    """Hash of a single node's *configuration*, excluding wiring fields.

    Stripping ``next`` means re-pointing an edge does not change a node's config
    hash, so the diff attributes it to a connection change rather than a config
    change.
    """
    config = {k: v for k, v in node.items() if k not in _WIRING_FIELDS}
    return _sha256(_canonical_json(config))


def node_hashes(spec: Dict[str, Any]) -> Dict[str, str]:
    return {nid: node_config_hash(node) for nid, node in spec.get("nodes", {}).items()}


def edge_set(spec: Dict[str, Any]) -> List[Tuple[str, str, str]]:
    """Normalized, sorted wiring set derived from node ``next`` pointers.

    Each edge is ``(source, handle, target)``. Only ``next`` is modelled by the
    durable flow, so the handle is always ``"next"``; the tuple shape leaves room
    for labelled edges without breaking the diff.
    """
    edges: List[Tuple[str, str, str]] = []
    for nid, node in spec.get("nodes", {}).items():
        nxt = node.get("next")
        if nxt is not None:
            edges.append((nid, "next", nxt))
    return sorted(edges)


class FlowDiff:
    """Stable classification of the difference between two flow specs."""

    def __init__(
        self,
        added_nodes: List[str],
        removed_nodes: List[str],
        changed_nodes: List[str],
        added_edges: List[Tuple[str, str, str]],
        removed_edges: List[Tuple[str, str, str]],
    ):
        self.added_nodes = added_nodes
        self.removed_nodes = removed_nodes
        self.changed_nodes = changed_nodes
        self.added_edges = added_edges
        self.removed_edges = removed_edges

    @property
    def is_empty(self) -> bool:
        return not (
            self.added_nodes
            or self.removed_nodes
            or self.changed_nodes
            or self.added_edges
            or self.removed_edges
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "addedNodes": sorted(self.added_nodes),
            "removedNodes": sorted(self.removed_nodes),
            "changedNodes": sorted(self.changed_nodes),
            "addedEdges": [list(e) for e in sorted(self.added_edges)],
            "removedEdges": [list(e) for e in sorted(self.removed_edges)],
            "empty": self.is_empty,
        }


def diff_specs(old_spec: Dict[str, Any], new_spec: Dict[str, Any]) -> FlowDiff:
    """Compute a stable diff between two flow specs.

    Node changes are split into three disjoint buckets -- added, removed and
    (config-)changed -- by comparing per-node config hashes. Wiring changes are
    reported separately as added/removed edges. Re-pointing an edge therefore
    shows up only under edges, never as a spurious node config change.
    """
    old_nodes = old_spec.get("nodes", {})
    new_nodes = new_spec.get("nodes", {})
    old_ids = set(old_nodes)
    new_ids = set(new_nodes)

    added = list(new_ids - old_ids)
    removed = list(old_ids - new_ids)

    old_hashes = node_hashes(old_spec)
    new_hashes = node_hashes(new_spec)
    changed = [
        nid for nid in (old_ids & new_ids) if old_hashes[nid] != new_hashes[nid]
    ]

    old_edges = set(edge_set(old_spec))
    new_edges = set(edge_set(new_spec))
    added_edges = list(new_edges - old_edges)
    removed_edges = list(old_edges - new_edges)

    return FlowDiff(added, removed, changed, added_edges, removed_edges)


class FlowVersionStore:
    """Append-only, immutable store of flow definition versions.

    Creating a version with content identical to the current latest returns that
    latest version unchanged (idempotent edits), so a no-op "edit" never inflates
    the version counter.
    """

    _locks: Dict[str, threading.Lock] = {}
    _locks_guard = threading.Lock()

    def __init__(self, root_dir: str):
        self.root_dir = root_dir
        self.versions_dir = os.path.join(root_dir, "durable", "versions")
        os.makedirs(self.versions_dir, exist_ok=True)

    def _flow_dir(self, flow_id: str) -> str:
        safe = flow_id.replace("/", "_").replace("\\", "_").replace("..", "_")
        d = os.path.join(self.versions_dir, safe)
        os.makedirs(d, exist_ok=True)
        return d

    def _lock_for(self, flow_id: str) -> threading.Lock:
        with FlowVersionStore._locks_guard:
            if flow_id not in FlowVersionStore._locks:
                FlowVersionStore._locks[flow_id] = threading.Lock()
            return FlowVersionStore._locks[flow_id]

    def _version_path(self, flow_id: str, version: int) -> str:
        return os.path.join(self._flow_dir(flow_id), f"v{version}.json")

    def _latest_path(self, flow_id: str) -> str:
        return os.path.join(self._flow_dir(flow_id), "latest.json")

    def latest_version(self, flow_id: str) -> int:
        path = self._latest_path(flow_id)
        if not os.path.exists(path):
            return 0
        with open(path, "r", encoding="utf-8") as f:
            return int(json.load(f).get("latest", 0))

    def list_versions(self, flow_id: str) -> List[int]:
        d = self._flow_dir(flow_id)
        versions = []
        for name in os.listdir(d):
            if name.startswith("v") and name.endswith(".json"):
                try:
                    versions.append(int(name[1:-5]))
                except ValueError:
                    continue
        return sorted(versions)

    def get_version(self, flow_id: str, version: int) -> Optional[Dict[str, Any]]:
        path = self._version_path(flow_id, version)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def require_version(self, flow_id: str, version: int) -> Dict[str, Any]:
        record = self.get_version(flow_id, version)
        if record is None:
            raise MissingVersionError(
                f"Flow version not found: {flow_id} v{version}"
            )
        return record

    def _write_version(self, flow_id: str, version: int, record: Dict[str, Any]) -> None:
        path = self._version_path(flow_id, version)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def _write_latest(self, flow_id: str, version: int) -> None:
        path = self._latest_path(flow_id)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"latest": version}, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def create_version(self, flow_id: str, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new immutable version (or return the identical latest one)."""
        lock = self._lock_for(flow_id)
        with lock:
            latest = self.latest_version(flow_id)
            new_hash = content_hash(spec)
            if latest > 0:
                current = self.get_version(flow_id, latest)
                if current and current["contentHash"] == new_hash:
                    # Identical content -> no new version (idempotent edit).
                    return current
            version = latest + 1
            record = {
                "flowId": flow_id,
                "version": version,
                "createdAt": __import__("time").time(),
                "spec": spec,
                "contentHash": new_hash,
                "nodeHashes": node_hashes(spec),
                "edges": [list(e) for e in edge_set(spec)],
            }
            self._write_version(flow_id, version, record)
            self._write_latest(flow_id, version)
            return record

    def diff_versions(self, flow_id: str, from_v: int, to_v: int) -> Dict[str, Any]:
        a = self.require_version(flow_id, from_v)
        b = self.require_version(flow_id, to_v)
        d = diff_specs(a["spec"], b["spec"])
        return {
            "flowId": flow_id,
            "fromVersion": from_v,
            "toVersion": to_v,
            **d.to_dict(),
        }

    def delete_version(self, flow_id: str, version: int) -> None:
        path = self._version_path(flow_id, version)
        if os.path.exists(path):
            os.remove(path)

    def export_flow(self, flow_id: str) -> Dict[str, Any]:
        """Export every version of a flow as a portable bundle."""
        versions = [self.get_version(flow_id, v) for v in self.list_versions(flow_id)]
        return {
            "flowId": flow_id,
            "latest": self.latest_version(flow_id),
            "versions": [v for v in versions if v is not None],
        }

    def import_flow(self, bundle: Dict[str, Any], overwrite: bool = False) -> Dict[str, Any]:
        """Import a bundle, re-verifying each version's content hash.

        Importing never silently mutates existing versions: if a version already
        exists and ``overwrite`` is False, it is left as-is. Hash mismatches in the
        bundle are rejected so a corrupted/tampered export cannot pollute the
        store.
        """
        flow_id = bundle["flowId"]
        lock = self._lock_for(flow_id)
        imported = []
        with lock:
            for record in bundle.get("versions", []):
                version = record["version"]
                expected = content_hash(record["spec"])
                if record.get("contentHash") != expected:
                    raise ValueError(
                        f"Import rejected: content hash mismatch for "
                        f"{flow_id} v{version}"
                    )
                exists = self.get_version(flow_id, version) is not None
                if exists and not overwrite:
                    continue
                self._write_version(flow_id, version, record)
                imported.append(version)
            if imported or bundle.get("latest"):
                target_latest = max(
                    [self.latest_version(flow_id), int(bundle.get("latest", 0))]
                )
                if target_latest > 0:
                    self._write_latest(flow_id, target_latest)
        return {"flowId": flow_id, "importedVersions": imported}
