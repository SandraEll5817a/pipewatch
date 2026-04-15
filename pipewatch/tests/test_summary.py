"""Tests for pipewatch.summary module."""

import pytest

from pipewatch.metrics import ThresholdViolation
from pipewatch.runner import PipelineResult
from pipewatch.summary import (
    PipelineSummary,
    RunSummary,
    build_summary,
    format_summary_text,
)


def _make_violation(msg: str = "duration exceeded") -> ThresholdViolation:
    return ThresholdViolation(metric="duration_seconds", threshold=60.0, actual=90.0)


def _healthy_result(name: str = "pipe_a") -> PipelineResult:
    return PipelineResult(pipeline_name=name, metrics=None, violations=[])


def _unhealthy_result(name: str = "pipe_b") -> PipelineResult:
    return PipelineResult(
        pipeline_name=name,
        metrics=None,
        violations=[_make_violation()],
    )


# --- PipelineSummary ---

def test_pipeline_summary_to_dict_healthy():
    ps = PipelineSummary(pipeline_name="pipe_a", healthy=True, violation_count=0)
    d = ps.to_dict()
    assert d["pipeline"] == "pipe_a"
    assert d["healthy"] is True
    assert d["violation_count"] == 0
    assert d["violations"] == []


def test_pipeline_summary_to_dict_with_violations():
    ps = PipelineSummary(
        pipeline_name="pipe_b",
        healthy=False,
        violation_count=1,
        violation_messages=["duration_seconds exceeded 60.0 (actual 90.0)"],
    )
    d = ps.to_dict()
    assert d["healthy"] is False
    assert len(d["violations"]) == 1


# --- build_summary ---

def test_build_summary_counts():
    results = [_healthy_result("a"), _unhealthy_result("b"), _healthy_result("c")]
    summary = build_summary(results)
    assert summary.total_pipelines == 3
    assert summary.healthy_count == 2
    assert summary.unhealthy_count == 1


def test_build_summary_empty():
    summary = build_summary([])
    assert summary.total_pipelines == 0
    assert summary.healthy_count == 0
    assert summary.unhealthy_count == 0
    assert summary.pipelines == []


def test_build_summary_pipeline_entries():
    results = [_healthy_result("pipe_a"), _unhealthy_result("pipe_b")]
    summary = build_summary(results)
    names = [p.pipeline_name for p in summary.pipelines]
    assert "pipe_a" in names
    assert "pipe_b" in names


def test_build_summary_violation_messages_populated():
    results = [_unhealthy_result("pipe_b")]
    summary = build_summary(results)
    ps = summary.pipelines[0]
    assert ps.violation_count == 1
    assert len(ps.violation_messages) == 1


def test_build_summary_to_dict_structure():
    summary = build_summary([_healthy_result()])
    d = summary.to_dict()
    assert "timestamp" in d
    assert "total_pipelines" in d
    assert "healthy_count" in d
    assert "unhealthy_count" in d
    assert isinstance(d["pipelines"], list)


# --- format_summary_text ---

def test_format_summary_text_contains_pipeline_name():
    summary = build_summary([_healthy_result("my_pipeline")])
    text = format_summary_text(summary)
    assert "my_pipeline" in text


def test_format_summary_text_ok_label_for_healthy():
    summary = build_summary([_healthy_result("pipe_a")])
    text = format_summary_text(summary)
    assert "[OK]" in text


def test_format_summary_text_fail_label_for_unhealthy():
    summary = build_summary([_unhealthy_result("pipe_b")])
    text = format_summary_text(summary)
    assert "[FAIL]" in text


def test_format_summary_text_counts_present():
    results = [_healthy_result("a"), _unhealthy_result("b")]
    summary = build_summary(results)
    text = format_summary_text(summary)
    assert "2" in text
    assert "1" in text
