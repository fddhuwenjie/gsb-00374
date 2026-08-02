"""Flow definition versioning utilities: config hashing and structural diff."""
import hashlib
import json
from typing import Dict, List, Set, Tuple

from models.flow import FlowDefinition, FlowEdge, FlowNode, FlowDiff


def compute_node_config_hash(flow: FlowDefinition) -> str:
    """Deterministic hash over node ids, types, positions, data and edges.

    The hash captures the *structure and configuration* of a flow so that
    any edit (node add/remove, config change, edge add/remove) produces a
    different hash.  Position is included because moving a node visually
    still constitutes a definition change.
    """
    payload = {
        'nodes': [
            {
                'id': n.id,
                'type': n.type,
                'position': {'x': n.position.x, 'y': n.position.y},
                'data': n.data.model_dump(exclude_none=True),
            }
            for n in sorted(flow.nodes, key=lambda x: x.id)
        ],
        'edges': [
            {
                'id': e.id,
                'source': e.source,
                'target': e.target,
                'sourceHandle': e.sourceHandle,
            }
            for e in sorted(flow.edges, key=lambda x: x.id)
        ],
    }
    blob = json.dumps(payload, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False)
    return hashlib.sha256(blob.encode('utf-8')).hexdigest()[:16]


def _node_signature(n: FlowNode) -> dict:
    return {
        'type': n.type,
        'position': {'x': n.position.x, 'y': n.position.y},
        'data': n.data.model_dump(exclude_none=True),
    }


def _edge_signature(e: FlowEdge) -> dict:
    return {
        'source': e.source,
        'target': e.target,
        'sourceHandle': e.sourceHandle,
    }


def _stable_dict(d: dict) -> str:
    return json.dumps(d, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, default=str)


def diff_flows(old: FlowDefinition, new: FlowDefinition) -> FlowDiff:
    """Produce a stable structural diff between two flow definitions.

    Categorises changes into:
      * addedNodes / removedNodes
      * changedNodes (type, position, or data differs)
      * addedEdges / removedEdges / changedEdges (endpoints differ)
      * renamed (name changed)
    """
    old_nodes: Dict[str, FlowNode] = {n.id: n for n in old.nodes}
    new_nodes: Dict[str, FlowNode] = {n.id: n for n in new.nodes}

    old_ids: Set[str] = set(old_nodes.keys())
    new_ids: Set[str] = set(new_nodes.keys())

    added = sorted(new_ids - old_ids)
    removed = sorted(old_ids - new_ids)
    changed: List[str] = []
    for nid in sorted(old_ids & new_ids):
        if _stable_dict(_node_signature(old_nodes[nid])) != \
           _stable_dict(_node_signature(new_nodes[nid])):
            changed.append(nid)

    old_edges: Dict[str, FlowEdge] = {e.id: e for e in old.edges}
    new_edges: Dict[str, FlowEdge] = {e.id: e for e in new.edges}
    old_eids: Set[str] = set(old_edges.keys())
    new_eids: Set[str] = set(new_edges.keys())

    added_edges = sorted(new_eids - old_eids)
    removed_edges = sorted(old_eids - new_eids)
    changed_edges: List[str] = []
    for eid in sorted(old_eids & new_eids):
        if _stable_dict(_edge_signature(old_edges[eid])) != \
           _stable_dict(_edge_signature(new_edges[eid])):
            changed_edges.append(eid)

    renamed = old.name != new.name
    return FlowDiff(
        addedNodes=added,
        removedNodes=removed,
        changedNodes=changed,
        addedEdges=added_edges,
        removedEdges=removed_edges,
        changedEdges=changed_edges,
        renamed=renamed,
        nameBefore=old.name if renamed else None,
        nameAfter=new.name if renamed else None,
    )


def is_structurally_identical(old: FlowDefinition, new: FlowDefinition) -> bool:
    """True when nodes, edges, types, positions and data are identical
    (ignoring name and timestamps)."""
    d = diff_flows(old, new)
    return not (d.addedNodes or d.removedNodes or d.changedNodes
                or d.addedEdges or d.removedEdges or d.changedEdges)
