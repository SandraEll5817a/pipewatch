"""Tests for pipewatch.trend module."""

from datetime import datetime, timezone
from typing import List

import pytest

from pipewatch.history import PipelineRun
from pipewatch.trend import TrendSummary, analyze_trend


def _run(
    healthy: bool = True,
    duration: float = 10.0,
    error_rate: float = 0.0,
    rows: float = 100.0,
) -> PipelineRun:
    return PipelineRun(
        timestamp=datetime.now(timezone.utc).isoformat(),
        healthy=healthy,
        duration_seconds=duration,
        error_rate=error_rate,
        rows_processed=rows,
        violations=[],
    )


def test_analyze_trend_empty_returns_insufficient_data():
    result = analyze_trend("pipe", [])
    assert result.trend_direction == "insufficient_data"
    assert result.sample_size == 0
    assert result.avg_duration_seconds is None
    assert result.failure_rate == 0.0


def test_analyze_trend_all_healthy():
    runs = [_run(healthy=True) for _ in range(6)]
    result = analyze_trend("pipe", runs)
    assert result.failure_rate == 0.0
    assert result.sample_size == 6
    assert result.trend_direction == "stable"


def test_analyze_trend_all_failing():
    runs = [_run(healthy=False) for _ in range(6)]
    result = analyze_trend("pipe", runs)
    assert result.failure_rate == 1.0
    assert result.trend_direction == "stable"


def test_analyze_trend_degrading():
    # first half healthy, second half failing
    runs = [_run(healthy=True)] * 4 + [_run(healthy=False)] * 4
    result = analyze_trend("pipe", runs)
    assert result.trend_direction == "degrading"


def test_analyze_trend_improving():
    # first half failing, second half healthy
    runs = [_run(healthy=False)] * 4 + [_run(healthy=True)] * 4
    result = analyze_trend("pipe", runs)
    assert result.trend_direction == "improving"


def test_analyze_trend_averages():
    runs = [
        _run(duration=20.0, error_rate=0.1, rows=200.0),
        _run(duration=40.0, error_rate=0.3, rows=400.0),
    ]
    result = analyze_trend("pipe", runs)
    assert result.avg_duration_seconds == 30.0
    assert result.avg_error_rate == 0.2
    assert result.avg_rows_processed == 300.0


def test_analyze_trend_insufficient_data_below_four():
    runs = [_run() for _ in range(3)]
    result = analyze_trend("pipe", runs)
    assert result.trend_direction == "insufficient_data"


def test_trend_summary_to_dict_keys():
    runs = [_run() for _ in range(5)]
    result = analyze_trend("my_pipe", runs)
    d = result.to_dict()
    assert d["pipeline"] == "my_pipe"
    assert "sample_size" in d
    assert "avg_duration_seconds" in d
    assert "trend_direction" in d
    assert "failure_rate" in d
