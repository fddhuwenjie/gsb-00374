import hashlib
import json
from typing import Any, Dict, List, Optional, Tuple

from models.flow import FlowDefinition, FlowEdge, FlowNode


NODE_CONFIG_FIELDS = (
    "type",
    "code",
    "expression",
    "seconds",
    "anchorId",
    "retry",
    "httpConfig",
    "sqlConfig",
    "fileWriteConfig",
    "parallelConfig",
    "subflowConfig",
    "tryCatchConfig",
)


def _canonical(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _canonical(value[k]) for k in sorted(value.keys())}
    if isinstance(value, list):
        return [_canonical(v) for v in value]
    return value


def node_config_hash(node: FlowNode) -> str:
    config: Dict[str, Any] = {"type": node.type}
    for field in NODE_CONFIG_FIELDS:
        if field == "type":
            continue
        value = getattr(node.data, field, None)
        if value is not None:
            if hasattr(value, "model_dump"):
                config[field] = value.model_dump(exclude_none=True)
            else:
                config[field] = value
    canonical = json.dumps(_canonical(config), sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def flow_config_hash(flow: FlowDefinition) -> str:
    node_hashes = {n.id: node_config_hash(n) for n in flow.nodes}
    edges_sorted = sorted(
        (
            {
                "id": e.id,
                "source": e.source,
                "target": e.target,
                "sourceHandle": e.sourceHandle,
            }
            for e in flow.edges
        ),
        key=lambda x: x["id"],
    )
    payload = {
        "nodes": {nid: node_hashes[nid] for nid in sorted(node_hashes.keys())},
        "edges": edges_sorted,
    }
    canonical = json.dumps(_canonical(payload), sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def diff_versions(old: FlowDefinition, new: FlowDefinition) -> Dict[str, Any]:
    old_nodes = {n.id: n for n in old.nodes}
    new_nodes = {n.id: n for n in new.nodes}

    old_node_ids = set(old_nodes.keys())
    new_node_ids = set(new_nodes.keys())

    added = sorted(new_node_ids - old_node_ids)
    removed = sorted(old_node_ids - new_node_ids)

    config_changed: List[Dict[str, Any]] = []
    position_changed: List[str] = []
    for nid in sorted(old_node_ids & new_node_ids):
        on = old_nodes[nid]
        nn = new_nodes[nid]
        if node_config_hash(on) != node_config_hash(nn):
            config_changed.append({
                "nodeId": nid,
                "oldHash": node_config_hash(on),
                "newHash": node_config_hash(nn),
            })
        if on.position.x != nn.position.x or on.position.y != nn.position.y:
            position_changed.append(nid)

    old_edges = {e.id: e for e in old.edges}
    new_edges = {e.id: e for e in new.edges}
    old_edge_ids = set(old_edges.keys())
    new_edge_ids = set(new_edges.keys())

    edges_added = sorted(new_edge_ids - old_edge_ids)
    edges_removed = sorted(old_edge_ids - new_edge_ids)
    edges_changed: List[Dict[str, Any]] = []
    for eid in sorted(old_edge_ids & new_edge_ids):
        oe = old_edges[eid]
        ne = new_edges[eid]
        if (oe.source != ne.source or oe.target != ne.target
                or oe.sourceHandle != ne.sourceHandle):
            edges_changed.append({
                "edgeId": eid,
                "old": {"source": oe.source, "target": oe.target, "sourceHandle": oe.sourceHandle},
                "new": {"source": ne.source, "target": ne.target, "sourceHandle": ne.sourceHandle},
            })

    return {
        "addedNodes": added,
        "removedNodes": removed,
        "configChangedNodes": config_changed,
        "positionChangedNodes": position_changed,
        "addedEdges": edges_added,
        "removedEdges": edges_removed,
        "changedEdges": edges_changed,
        "oldHash": flow_config_hash(old),
        "newHash": flow_config_hash(new),
        "hasStructuralChange": bool(added or removed or config_changed or edges_added or edges_removed or edges_changed),
    }
