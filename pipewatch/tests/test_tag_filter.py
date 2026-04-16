"""Tests for pipewatch.tag_filter."""
import pytest
from unittest.mock import MagicMock

from pipewatch.tag_filter import TagFilter, filter_pipelines, pipelines_by_tag


def _pipeline(name: str, tags):
    p = MagicMock()
    p.name = name
    p.tags = tags
    return p


def test_tag_filter_no_constraints_matches_all():
    p = _pipeline("a", ["etl"])
    assert TagFilter().matches(p) is True


def test_tag_filter_require_present():
    p = _pipeline("a", ["etl", "daily"])
    assert TagFilter(require=["etl"]).matches(p) is True


def test_tag_filter_require_missing():
    p = _pipeline("a", ["etl"])
    assert TagFilter(require=["weekly"]).matches(p) is False


def test_tag_filter_exclude_present():
    p = _pipeline("a", ["etl", "disabled"])
    assert TagFilter(exclude=["disabled"]).matches(p) is False


def test_tag_filter_exclude_absent():
    p = _pipeline("a", ["etl"])
    assert TagFilter(exclude=["disabled"]).matches(p) is True


def test_tag_filter_require_and_exclude():
    p = _pipeline("a", ["etl", "daily"])
    assert TagFilter(require=["etl"], exclude=["disabled"]).matches(p) is True


def test_tag_filter_none_tags():
    p = _pipeline("a", None)
    assert TagFilter(require=["etl"]).matches(p) is False


def test_filter_pipelines_returns_matching():
    pipelines = [
        _pipeline("a", ["etl"]),
        _pipeline("b", ["etl", "daily"]),
        _pipeline("c", ["weekly"]),
    ]
    result = filter_pipelines(pipelines, require=["etl"])
    assert [p.name for p in result] == ["a", "b"]


def test_filter_pipelines_exclude():
    pipelines = [
        _pipeline("a", ["etl"]),
        _pipeline("b", ["etl", "disabled"]),
    ]
    result = filter_pipelines(pipelines, exclude=["disabled"])
    assert [p.name for p in result] == ["a"]


def test_pipelines_by_tag():
    pipelines = [
        _pipeline("a", ["etl", "daily"]),
        _pipeline("b", ["etl"]),
        _pipeline("c", ["weekly"]),
    ]
    index = pipelines_by_tag(pipelines)
    assert set(index["etl"]) == {"a", "b"}
    assert index["daily"] == ["a"]
    assert index["weekly"] == ["c"]


def test_pipelines_by_tag_empty():
    assert pipelines_by_tag([]) == {}
