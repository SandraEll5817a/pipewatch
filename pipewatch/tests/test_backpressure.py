"""Tests for pipewatch.backpressure."""
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from pipewatch.backpressure import (
    BackpressureResult,
    _linear_slope,
    detect_backpressure,
)
from pipewatch.history import PipelineRun


def _run(rows: float) -> PipelineRun:
    return PipelineRun(
        pipeline="pipe",
        timestamp=datetime.now(timezone.utc).isoformat(),
        duration_seconds=10.0,
        error_rate=0.0,
        rows_processed=rows,
        healthy=True,
    )


def test_insufficient_data_returns_not_pressured():
    runs = [_run(100.0)] * 3  # fewer than min_runs=5
    result = detect_backpressure("pipe", runs)
    assert result.is_pressured is False
    assert result.message == "insufficient data"


def test_stable_throughput_not_pressured():
    runs = [_run(200.0)] * 8
    result = detect_backpressure("pipe", runs)
    assert result.is_pressured is False
    assert result.trend_slope == pytest.approx(0.0, abs=0.01)


def test_rising_throughput_not_pressured():
    runs = [_run(float(i * 10)) for i in range(1, 9)]
    result = detect_backpressure("pipe", runs)
    assert result.is_pressured is False
    assert result.trend_slope > 0


def test_declining_throughput_is_pressured():
    # rows drop steeply: 200, 190, 180 … 130 — slope ≈ -10
    runs = [_run(float(200 - i * 10)) for i in range(8)]
    result = detect_backpressure("pipe", runs, slope_threshold=-5.0)
    assert result.is_pressured is True
    assert "declining" in result.message


def test_current_lag_is_last_value():
    runs = [_run(float(100 + i)) for i in range(6)]
    result = detect_backpressure("pipe", runs)
    assert result.current_lag == pytest.approx(105.0)


def test_avg_lag_is_mean_of_series():
    runs = [_run(10.0), _run(20.0), _run(30.0), _run(40.0), _run(50.0)]
    result = detect_backpressure("pipe", runs)
    assert result.avg_lag == pytest.approx(30.0)


def test_to_dict_has_expected_keys():
    runs = [_run(100.0)] * 6
    result = detect_backpressure("pipe", runs)
    d = result.to_dict()
    for key in ("pipeline", "is_pressured", "current_lag", "avg_lag", "trend_slope", "message"):
        assert key in d


def test_str_ok():
    runs = [_run(100.0)] * 6
    result = detect_backpressure("pipe", runs)
    assert "[OK]" in str(result)


def test_str_pressured():
    runs = [_run(float(200 - i * 15)) for i in range(8)]
    result = detect_backpressure("pipe", runs, slope_threshold=-5.0)
    assert "[PRESSURED]" in str(result)


def test_linear_slope_constant():
    assert _linear_slope([5.0, 5.0, 5.0]) == pytest.approx(0.0)


def test_linear_slope_increasing():
    slope = _linear_slope([0.0, 1.0, 2.0, 3.0])
    assert slope == pytest.approx(1.0)


def test_linear_slope_single_value():
    assert _linear_slope([42.0]) == pytest.approx(0.0)
