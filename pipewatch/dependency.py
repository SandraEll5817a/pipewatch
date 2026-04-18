"""Pipeline dependency ordering and validation."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class DependencyGraph:
    """Directed graph of pipeline dependencies."""
    edges: Dict[str, List[str]] = field(default_factory=dict)  # pipeline -> depends_on

    def add_pipeline(self, name: str, depends_on: Optional[List[str]] = None) -> None:
        self.edges[name] = depends_on or []

    def all_pipelines(self) -> List[str]:
        return list(self.edges.keys())

    def dependencies_of(self, name: str) -> List[str]:
        return self.edges.get(name, [])


@dataclass
class CycleError(Exception):
    cycle: List[str]

    def __str__(self) -> str:
        return f"Dependency cycle detected: {' -> '.join(self.cycle)}"


def topological_sort(graph: DependencyGraph) -> List[str]:
    """Return pipelines in dependency-first order. Raises CycleError if cycle found."""
    visited: set = set()
    temp: set = set()
    order: List[str] = []

    def visit(node: str, path: List[str]) -> None:
        if node in temp:
            cycle_start = path.index(node)
            raise CycleError(path[cycle_start:] + [node])
        if node in visited:
            return
        temp.add(node)
        for dep in graph.dependencies_of(node):
            visit(dep, path + [node])
        temp.discard(node)
        visited.add(node)
        order.append(node)

    for pipeline in graph.all_pipelines():
        if pipeline not in visited:
            visit(pipeline, [])

    return order


def build_graph_from_config(pipelines) -> DependencyGraph:
    """Build a DependencyGraph from a list of PipelineConfig objects."""
    graph = DependencyGraph()
    for p in pipelines:
        depends_on = getattr(p, 'depends_on', None) or []
        graph.add_pipeline(p.name, depends_on)
    return graph
