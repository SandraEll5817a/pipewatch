"""Tests for pipewatch.sla."""

from datetime import datetime, timezone

import pytest

from pipewatch.sla import SLAPolicy, SLAResult, SLAViolation, check_sla
from pipewatch.history import PipelineRun


def _run(
    duration: float = 10.0,
    error_rate: float = 0.0,
    rows: int = 1000,
    healthy: bool = True,
) -> PipelineRun:
    return PipelineRun(
        pipeline="pipe_a",
        ran_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        duration_seconds=duration,
        error_rate=error_rate,
        rows_processed=rows,
        healthy=healthy,
    )


def test_no_runs_returns_compliant():
    policy = SLAPolicy(max_duration_seconds=60.0)
    result = check_sla("pipe_a", [], policy)
    assert result.compliant is True
    assert result.violations == []


def test_within_all_thresholds_is_compliant():
    policy = SLAPolicy(
        max_duration_seconds=60.0,
        max_error_rate=0.05,
        min_rows_processed=500,
    )
    result = check_sla("pipe_a", [_run(duration=30.0, error_rate=0.01, rows=800)], policy)
    assert result.compliant is True
    assert result.violations == []


def test_duration_violation():
    policy = SLAPolicy(max_duration_seconds=20.0)
    result = check_sla("pipe_a", [_run(duration=45.0)], policy)
    assert result.compliant is False
    assert len(result.violations) == 1
    assert result.violations[0].metric == "duration_seconds"


def test_error_rate_violation():
    policy = SLAPolicy(max_error_rate=0.02)
    result = check_sla("pipe_a", [_run(error_rate=0.10)], policy)
    assert result.compliant is False
    assert result.violations[0].metric == "error_rate"


def test_rows_processed_violation():
    policy = SLAPolicy(min_rows_processed=1000)
    result = check_sla("pipe_a", [_run(rows=200)], policy)
    assert result.compliant is False
    assert result.violations[0].metric == "rows_processed"


def test_multiple_violations():
    policy = SLAPolicy(
        max_duration_seconds=5.0,
        max_error_rate=0.01,
        min_rows_processed=5000,
    )
    result = check_sla("pipe_a", [_run(duration=100.0, error_rate=0.5, rows=10)], policy)
    assert result.compliant is False
    assert len(result.violations) == 3


def test_uses_most_recent_run():
    old = PipelineRun(
        pipeline="pipe_a",
        ran_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        duration_seconds=200.0,
        error_rate=0.0,
        rows_processed=100,
        healthy=False,
    )
    recent = _run(duration=5.0)
    policy = SLAPolicy(max_duration_seconds=60.0)
    result = check_sla("pipe_a", [old, recent], policy)
    assert result.compliant is True


def test_violation_str():
    v = SLAViolation(pipeline="p", metric="duration_seconds", expected="<= 30", actual="90")
    assert "duration_seconds" in str(v)
    assert "90" in str(v)


def test_result_to_dict_structure():
    policy = SLAPolicy(max_duration_seconds=1.0)
    result = check_sla("pipe_a", [_run(duration=99.0)], policy)
    d = result.to_dict()
    assert d["pipeline"] == "pipe_a"
    assert d["compliant"] is False
    assert isinstance(d["violations"], list)
    assert d["violations"][0]["metric"] == "duration_seconds"
