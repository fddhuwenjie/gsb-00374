"""Durable flow model.

A deliberately small, self-contained flow spec purpose-built for the durable
engine's guarantees (it is *not* the rich editor model in ``models/flow.py``).

A flow is an ordered graph of nodes reached via ``next`` pointers, starting at a
``start`` node and ending at an ``end`` node. Each node is a *boundary unit*:
when it completes, its resulting variables are flushed as a ``node_boundary``
event and that is the only place recovery may resume from.

Node types
----------
start            {"type":"start", "next": id}
end              {"type":"end"}
task             {"type":"task", "next": id, "ops":[...], "uninterruptible":bool,
                  "fail_times": int, "delay": float}
http             side-effect node; {"type":"http","next":id,"url":..,"method":..,
                  "fail_times":int, "retry": {...}}
file             side-effect node; {"type":"file","next":id,"path":..,"content":..,
                  "retry": {...}}
parallel         {"type":"parallel","next":id,"required":[branchId...],
                  "branches": {branchId: {"steps":[...], "fail_times":int,
                  "value": any}}, "retry": {...}}

``ops`` is a tiny instruction list applied to the variables dict:
    {"set": {"k": v}}      variables["k"] = v
    {"incr": "k"}          variables["k"] = variables.get("k", 0) + 1
    {"append": {"k": v}}   variables.setdefault("k", []).append(v)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


class FlowSpecError(Exception):
    pass


def apply_ops(variables: Dict[str, Any], ops: List[Dict[str, Any]]) -> None:
    for op in ops or []:
        if "set" in op:
            for k, v in op["set"].items():
                variables[k] = v
        elif "incr" in op:
            k = op["incr"]
            variables[k] = variables.get(k, 0) + 1
        elif "append" in op:
            for k, v in op["append"].items():
                variables.setdefault(k, []).append(v)
        else:
            raise FlowSpecError(f"Unknown op: {op}")


class DurableFlow:
    def __init__(self, spec: Dict[str, Any]):
        self.id: str = spec["id"]
        self.nodes: Dict[str, Dict[str, Any]] = spec["nodes"]
        self.spec = spec
        self._validate()

    def _validate(self) -> None:
        starts = [n for n in self.nodes.values() if n.get("type") == "start"]
        ends = [n for n in self.nodes.values() if n.get("type") == "end"]
        if len(starts) != 1:
            raise FlowSpecError(f"Flow must have exactly one start node, found {len(starts)}")
        if len(ends) < 1:
            raise FlowSpecError("Flow must have at least one end node")

    @property
    def start_id(self) -> str:
        for nid, node in self.nodes.items():
            if node.get("type") == "start":
                return nid
        raise FlowSpecError("No start node")

    def node(self, node_id: str) -> Dict[str, Any]:
        if node_id not in self.nodes:
            raise FlowSpecError(f"Unknown node: {node_id}")
        return self.nodes[node_id]

    def next_of(self, node_id: str) -> Optional[str]:
        return self.nodes[node_id].get("next")

    def to_dict(self) -> Dict[str, Any]:
        return self.spec
