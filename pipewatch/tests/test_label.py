"""Tests for pipewatch.label."""
from __future__ import annotations

import pytest

from pipewatch.config import PipelineConfig, ThresholdConfig
from pipewatch.label import LabelSelector, filter_by_labels, group_by_label, label_index


def _pipeline(name: str, labels: dict | None = None) -> PipelineConfig:
    return PipelineConfig(
        name=name,
        source="dummy",
        thresholds=ThresholdConfig(),
        labels=labels or {},
    )


def test_selector_no_constraints_matches_all():
    sel = LabelSelector()
    p = _pipeline("p1", {"env": "prod"})
    assert sel.matches(p) is True


def test_selector_require_present_and_matching():
    sel = LabelSelector(require={"env": "prod"})
    assert sel.matches(_pipeline("p1", {"env": "prod"})) is True


def test_selector_require_wrong_value():
    sel = LabelSelector(require={"env": "prod"})
    assert sel.matches(_pipeline("p1", {"env": "staging"})) is False


def test_selector_require_key_missing():
    sel = LabelSelector(require={"env": "prod"})
    assert sel.matches(_pipeline("p1", {})) is False


def test_selector_exclude_key_present():
    sel = LabelSelector(exclude_keys=["deprecated"])
    assert sel.matches(_pipeline("p1", {"deprecated": "true"})) is False


def test_selector_exclude_key_absent():
    sel = LabelSelector(exclude_keys=["deprecated"])
    assert sel.matches(_pipeline("p1", {"env": "prod"})) is True


def test_filter_by_labels_returns_matching():
    pipelines = [
        _pipeline("a", {"env": "prod"}),
        _pipeline("b", {"env": "staging"}),
        _pipeline("c", {"env": "prod"}),
    ]
    sel = LabelSelector(require={"env": "prod"})
    result = filter_by_labels(pipelines, sel)
    assert [p.name for p in result] == ["a", "c"]


def test_filter_by_labels_empty_list():
    assert filter_by_labels([], LabelSelector(require={"env": "prod"})) == []


def test_group_by_label_basic():
    pipelines = [
        _pipeline("a", {"team": "alpha"}),
        _pipeline("b", {"team": "beta"}),
        _pipeline("c", {"team": "alpha"}),
    ]
    groups = group_by_label(pipelines, "team")
    assert sorted(groups["alpha"], key=lambda p: p.name) == [pipelines[0], pipelines[2]]
    assert groups["beta"] == [pipelines[1]]


def test_group_by_label_missing_key_goes_to_empty_bucket():
    pipelines = [_pipeline("x", {}), _pipeline("y", {"team": "alpha"})]
    groups = group_by_label(pipelines, "team")
    assert "x" in [p.name for p in groups[""]]


def test_label_index_structure():
    pipelines = [
        _pipeline("a", {"env": "prod", "team": "alpha"}),
        _pipeline("b", {"env": "prod", "team": "beta"}),
    ]
    idx = label_index(pipelines)
    assert set(idx["env"]["prod"]) == {"a", "b"}
    assert idx["team"]["alpha"] == ["a"]
    assert idx["team"]["beta"] == ["b"]
