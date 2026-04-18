"""Tests for pipewatch.dependency."""
import pytest
from pipewatch.dependency import (
    DependencyGraph,
    CycleError,
    topological_sort,
    build_graph_from_config,
)


def _graph(*pipelines):
    """Build graph from (name, deps) tuples."""
    g = DependencyGraph()
    for name, deps in pipelines:
        g.add_pipeline(name, deps)
    return g


def test_topological_sort_no_deps():
    g = _graph(("a", []), ("b", []), ("c", []))
    result = topological_sort(g)
    assert set(result) == {"a", "b", "c"}


def test_topological_sort_linear_chain():
    g = _graph(("a", []), ("b", ["a"]), ("c", ["b"]))
    result = topological_sort(g)
    assert result.index("a") < result.index("b")
    assert result.index("b") < result.index("c")


def test_topological_sort_diamond():
    g = _graph(("a", []), ("b", ["a"]), ("c", ["a"]), ("d", ["b", "c"]))
    result = topological_sort(g)
    assert result.index("a") < result.index("b")
    assert result.index("a") < result.index("c")
    assert result.index("b") < result.index("d")
    assert result.index("c") < result.index("d")


def test_topological_sort_raises_on_cycle():
    g = _graph(("a", ["b"]), ("b", ["c"]), ("c", ["a"]))
    with pytest.raises(CycleError) as exc_info:
        topological_sort(g)
    assert "a" in str(exc_info.value) or "b" in str(exc_info.value)


def test_cycle_error_str():
    err = CycleError(["a", "b", "a"])
    assert "a -> b -> a" in str(err)


def test_build_graph_from_config():
    class FakePipeline:
        def __init__(self, name, depends_on=None):
            self.name = name
            self.depends_on = depends_on

    pipelines = [
        FakePipeline("etl_load", ["etl_extract"]),
        FakePipeline("etl_extract"),
    ]
    graph = build_graph_from_config(pipelines)
    assert graph.dependencies_of("etl_load") == ["etl_extract"]
    assert graph.dependencies_of("etl_extract") == []


def test_build_graph_no_depends_on_attr():
    class MinimalPipeline:
        def __init__(self, name):
            self.name = name

    graph = build_graph_from_config([MinimalPipeline("p1"), MinimalPipeline("p2")])
    assert graph.dependencies_of("p1") == []
