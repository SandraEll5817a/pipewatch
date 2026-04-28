"""Tests for pipewatch.capacity."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pytest

from pipewatch.capacity import (
    MIN_RUNS,
    RISK_THRESHOLD,
    check_capacity,
    _linear_slope,
    _safe_avg,
)
from pipewatch.history import PipelineRun


def _run(rows: Optional[int], ok: bool = True) -> PipelineRun:
    return PipelineRun(
        pipeline="pipe",
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat(),
        duration_seconds=10.0,
        rows_processed=rows,
        error_rate=0.0,
        healthy=ok,
    )


# ---------------------------------------------------------------------------
# unit helpers
# ---------------------------------------------------------------------------

def test_safe_avg_empty():
    assert _safe_avg([]) == 0.0


def test_safe_avg_values():
    assert _safe_avg([2.0, 4.0, 6.0]) == pytest.approx(4.0)


def test_linear_slope_flat():
    assert _linear_slope([5.0, 5.0, 5.0]) == pytest.approx(0.0)


def test_linear_slope_rising():
    slope = _linear_slope([1.0, 2.0, 3.0])
    assert slope > 0


def test_linear_slope_single_value():
    assert _linear_slope([42.0]) == 0.0


# ---------------------------------------------------------------------------
# check_capacity
# ---------------------------------------------------------------------------

def test_insufficient_data_when_fewer_than_min_runs():
    runs = [_run(100)] * (MIN_RUNS - 1)
    result = check_capacity("pipe", runs, ceiling=1000)
    assert not result.at_risk
    assert result.message == "insufficient data"
    assert result.utilization == 0.0


def test_zero_ceiling_returns_insufficient():
    runs = [_run(100)] * MIN_RUNS
    result = check_capacity("pipe", runs, ceiling=0)
    assert not result.at_risk
    assert result.message == "insufficient data"


def test_low_utilization_not_at_risk():
    runs = [_run(100)] * MIN_RUNS
    result = check_capacity("pipe", runs, ceiling=10_000)
    assert not result.at_risk
    assert result.utilization == pytest.approx(0.01)


def test_high_utilization_triggers_at_risk():
    ceiling = 1000
    rows = int(ceiling * RISK_THRESHOLD) + 10
    runs = [_run(rows)] * MIN_RUNS
    result = check_capacity("pipe", runs, ceiling=ceiling)
    assert result.at_risk
    assert result.utilization >= RISK_THRESHOLD


def test_rapidly_rising_series_projects_saturation():
    # rows growing quickly toward ceiling
    runs = [_run(r) for r in [700, 800, 900]]
    result = check_capacity("pipe", runs, ceiling=1000)
    assert result.projected_saturation is not None
    assert result.projected_saturation >= 1


def test_flat_series_has_no_projected_saturation():
    runs = [_run(200)] * MIN_RUNS
    result = check_capacity("pipe", runs, ceiling=1000)
    assert result.projected_saturation is None


def test_runs_without_rows_are_ignored():
    runs = [_run(None)] * MIN_RUNS
    result = check_capacity("pipe", runs, ceiling=1000)
    assert result.message == "insufficient data"


def test_to_dict_contains_expected_keys():
    runs = [_run(200)] * MIN_RUNS
    result = check_capacity("pipe", runs, ceiling=1000)
    d = result.to_dict()
    for key in ("pipeline", "utilization", "projected_saturation",
                "ceiling", "recent_avg", "at_risk", "message"):
        assert key in d


def test_str_at_risk_contains_saturation():
    runs = [_run(r) for r in [700, 800, 900]]
    result = check_capacity("pipe", runs, ceiling=1000)
    if result.at_risk and result.projected_saturation is not None:
        assert "saturation" in str(result)


def test_str_ok_contains_ok():
    runs = [_run(100)] * MIN_RUNS
    result = check_capacity("pipe", runs, ceiling=10_000)
    assert "ok" in str(result)
