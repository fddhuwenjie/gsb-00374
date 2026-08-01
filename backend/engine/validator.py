from typing import List, Dict, Set, Tuple
from collections import deque
from models.flow import FlowDefinition, FlowNode, FlowEdge, NodeType


class ValidationError(Exception):
    pass


class FlowValidator:
    def __init__(self, flow: FlowDefinition):
        self.flow = flow
        self.nodes: Dict[str, FlowNode] = {n.id: n for n in flow.nodes}
        self.edges: List[FlowEdge] = flow.edges
        self.outgoing: Dict[str, List[FlowEdge]] = {}
        self.incoming: Dict[str, List[FlowEdge]] = {}

        for edge in self.edges:
            if edge.source not in self.outgoing:
                self.outgoing[edge.source] = []
            self.outgoing[edge.source].append(edge)

            if edge.target not in self.incoming:
                self.incoming[edge.target] = []
            self.incoming[edge.target].append(edge)

    def validate(self) -> None:
        self._validate_single_start_end()
        self._validate_node_connectivity()
        self._validate_edge_consistency()
        self._validate_special_node_edges()
        self._validate_no_loops_except_at_loop_nodes()
        self._validate_reachability()

    def _validate_single_start_end(self) -> None:
        start_nodes = [n for n in self.flow.nodes if n.type == 'start']
        end_nodes = [n for n in self.flow.nodes if n.type == 'end']

        if len(start_nodes) == 0:
            raise ValidationError("Flow must have exactly one Start node, found none")
        if len(start_nodes) > 1:
            raise ValidationError(f"Flow must have exactly one Start node, found {len(start_nodes)}")

        if len(end_nodes) == 0:
            raise ValidationError("Flow must have exactly one End node, found none")
        if len(end_nodes) > 1:
            raise ValidationError(f"Flow must have exactly one End node, found {len(end_nodes)}")

    def _validate_node_connectivity(self) -> None:
        for node in self.flow.nodes:
            if node.type == 'start':
                if node.id in self.incoming and len(self.incoming[node.id]) > 0:
                    raise ValidationError(f"Start node '{node.id}' cannot have incoming edges")
                if node.id not in self.outgoing or len(self.outgoing[node.id]) == 0:
                    raise ValidationError(f"Start node '{node.id}' must have at least one outgoing edge")

            elif node.type == 'end':
                if node.id not in self.incoming or len(self.incoming[node.id]) == 0:
                    raise ValidationError(f"End node '{node.id}' must have at least one incoming edge")
                if node.id in self.outgoing and len(self.outgoing[node.id]) > 0:
                    raise ValidationError(f"End node '{node.id}' cannot have outgoing edges")

            else:
                if node.type in ['parallel', 'trycatch']:
                    if node.id not in self.incoming or len(self.incoming[node.id]) == 0:
                        raise ValidationError(f"Node '{node.id}' must have at least one incoming edge")
                    if node.id not in self.outgoing or len(self.outgoing[node.id]) == 0:
                        raise ValidationError(f"Node '{node.id}' must have at least one outgoing edge")
                elif node.data.anchorId:
                    pass
                else:
                    if node.id not in self.incoming or len(self.incoming[node.id]) == 0:
                        raise ValidationError(f"Node '{node.id}' must have at least one incoming edge")
                    if node.id not in self.outgoing or len(self.outgoing[node.id]) == 0:
                        raise ValidationError(f"Node '{node.id}' must have at least one outgoing edge")

    def _validate_edge_consistency(self) -> None:
        for edge in self.edges:
            if edge.source not in self.nodes:
                raise ValidationError(f"Edge '{edge.id}' references non-existent source node '{edge.source}'")
            if edge.target not in self.nodes:
                raise ValidationError(f"Edge '{edge.id}' references non-existent target node '{edge.target}'")

    def _validate_special_node_edges(self) -> None:
        for node in self.flow.nodes:
            node_edges = self.outgoing.get(node.id, [])

            if node.type == 'start':
                if len(node_edges) != 1:
                    raise ValidationError(
                        f"Start node '{node.id}' must have exactly one outgoing edge, "
                        f"found {len(node_edges)}"
                    )
                for edge in node_edges:
                    if edge.sourceHandle is not None:
                        raise ValidationError(
                            f"Start node '{node.id}' cannot have edges with sourceHandle."
                        )

            elif node.type == 'end':
                if len(node_edges) != 0:
                    raise ValidationError(
                        f"End node '{node.id}' cannot have outgoing edges, "
                        f"found {len(node_edges)}"
                    )

            elif node.type == 'condition':
                true_edges = [e for e in node_edges if e.sourceHandle == 'true']
                false_edges = [e for e in node_edges if e.sourceHandle == 'false']

                if len(true_edges) != 1:
                    raise ValidationError(
                        f"Condition node '{node.id}' must have exactly one 'true' outgoing edge, "
                        f"found {len(true_edges)}"
                    )
                if len(false_edges) != 1:
                    raise ValidationError(
                        f"Condition node '{node.id}' must have exactly one 'false' outgoing edge, "
                        f"found {len(false_edges)}"
                    )

            elif node.type == 'loop':
                loop_edges = [e for e in node_edges if e.sourceHandle == 'loop']
                normal_edges = [e for e in node_edges if e.sourceHandle is None or e.sourceHandle != 'loop']

                if len(loop_edges) != 1:
                    raise ValidationError(
                        f"Loop node '{node.id}' must have exactly one 'loop' outgoing edge (back to anchor), "
                        f"found {len(loop_edges)}"
                    )
                if len(normal_edges) != 1:
                    raise ValidationError(
                        f"Loop node '{node.id}' must have exactly one normal outgoing edge (exit path), "
                        f"found {len(normal_edges)}"
                    )

            elif node.type in ['parallel', 'trycatch', 'subflow', 'http', 'sql']:
                for edge in node_edges:
                    if edge.sourceHandle is not None:
                        raise ValidationError(
                            f"Node '{node.id}' of type '{node.type}' cannot have edges with sourceHandle. "
                            f"Only Condition and Loop nodes support labeled edges."
                        )

                if len(node_edges) != 1:
                    raise ValidationError(
                        f"Node '{node.id}' of type '{node.type}' must have exactly one outgoing edge, "
                        f"found {len(node_edges)}"
                    )

            elif node.data.anchorId:
                for edge in node_edges:
                    if edge.sourceHandle is not None:
                        raise ValidationError(
                            f"Node '{node.id}' of type '{node.type}' cannot have edges with sourceHandle. "
                            f"Only Condition and Loop nodes support labeled edges."
                        )

            else:
                for edge in node_edges:
                    if edge.sourceHandle is not None:
                        raise ValidationError(
                            f"Node '{node.id}' of type '{node.type}' cannot have edges with sourceHandle. "
                            f"Only Condition and Loop nodes support labeled edges."
                        )

                if len(node_edges) != 1:
                    raise ValidationError(
                        f"Node '{node.id}' of type '{node.type}' must have exactly one outgoing edge, "
                        f"found {len(node_edges)}"
                    )

    def _validate_no_loops_except_at_loop_nodes(self) -> None:
        non_loop_edges = []
        loop_back_edges = []

        for edge in self.edges:
            if edge.sourceHandle == 'loop':
                loop_back_edges.append(edge)
            else:
                non_loop_edges.append(edge)

        for edge in loop_back_edges:
            source_node = self.nodes[edge.source]
            if source_node.type != 'loop':
                raise ValidationError(
                    f"Only Loop nodes can have 'loop' handle edges. "
                    f"Node '{edge.source}' is type '{source_node.type}'."
                )

        self._validate_no_cycles_in_non_loop_edges(non_loop_edges)

        for edge in loop_back_edges:
            source = edge.source
            target = edge.target

            reachable = self._can_reach(target, source, non_loop_edges)
            if not reachable:
                raise ValidationError(
                    f"Invalid loop structure: Loop node '{source}' has back-edge to '{target}', "
                    f"but '{target}' cannot reach '{source}' through non-loop edges. "
                    f"This creates an unreachable loop structure."
                )

    def _validate_no_cycles_in_non_loop_edges(self, edges: List[FlowEdge]) -> None:
        in_degree: Dict[str, int] = {n.id: 0 for n in self.flow.nodes}
        adj: Dict[str, List[str]] = {n.id: [] for n in self.flow.nodes}

        for edge in edges:
            adj[edge.source].append(edge.target)
            in_degree[edge.target] += 1

        queue = deque([n.id for n in self.flow.nodes if in_degree[n.id] == 0])
        visited_count = 0

        while queue:
            node = queue.popleft()
            visited_count += 1
            for neighbor in adj[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if visited_count != len(self.flow.nodes):
            cycle_nodes = self._find_cycle_nodes(adj, in_degree)
            raise ValidationError(
                f"Cycle detected in flow (non-loop edges). Cycles are only allowed at Loop nodes. "
                f"Cycle involves nodes: {', '.join(cycle_nodes)}"
            )

    def _find_cycle_nodes(self, adj: Dict[str, List[str]], in_degree: Dict[str, int]) -> List[str]:
        cycle_nodes = []
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {n: WHITE for n in adj}

        def dfs(node: str) -> bool:
            color[node] = GRAY
            for neighbor in adj[node]:
                if color[neighbor] == GRAY:
                    cycle_nodes.append(neighbor)
                    return True
                if color[neighbor] == WHITE and dfs(neighbor):
                    if node not in cycle_nodes:
                        cycle_nodes.append(node)
                    return True
            color[node] = BLACK
            return False

        for node in adj:
            if color[node] == WHITE:
                dfs(node)
                if cycle_nodes:
                    break

        return cycle_nodes

    def _can_reach(self, start: str, target: str, edges: List[FlowEdge]) -> bool:
        adj: Dict[str, List[str]] = {n.id: [] for n in self.flow.nodes}
        for edge in edges:
            adj[edge.source].append(edge.target)

        visited: Set[str] = set()
        stack = [start]

        while stack:
            node = stack.pop()
            if node == target:
                return True
            if node in visited:
                continue
            visited.add(node)
            for neighbor in adj[node]:
                if neighbor not in visited:
                    stack.append(neighbor)

        return False

    def _validate_reachability(self) -> None:
        start_node = next(n for n in self.flow.nodes if n.type == 'start')
        end_node = next(n for n in self.flow.nodes if n.type == 'end')

        all_edges = []
        for edge in self.edges:
            all_edges.append(edge)

        reachable_from_start = self._get_reachable_nodes(start_node.id, all_edges)

        for node in self.flow.nodes:
            if node.data.anchorId:
                continue
            if node.id not in reachable_from_start:
                raise ValidationError(
                    f"Node '{node.id}' is not reachable from Start node. "
                    f"All nodes must be reachable in a valid flow."
                )

        can_reach_end = self._can_reach(start_node.id, end_node.id, all_edges)
        if not can_reach_end:
            raise ValidationError(
                f"End node is not reachable from Start node. "
                f"There must be at least one valid path from Start to End."
            )

    def _get_reachable_nodes(self, start: str, edges: List[FlowEdge]) -> Set[str]:
        adj: Dict[str, List[str]] = {n.id: [] for n in self.flow.nodes}
        for edge in edges:
            adj[edge.source].append(edge.target)

        visited: Set[str] = set()
        stack = [start]

        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            for neighbor in adj[node]:
                if neighbor not in visited:
                    stack.append(neighbor)

        return visited
