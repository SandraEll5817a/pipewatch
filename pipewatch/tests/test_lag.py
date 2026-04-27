"""Tests for pipewatch.lag."""

from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.lag import detect_lag, LagResult
from pipewatch.history import PipelineRun


def _dt(offset_seconds: float = 0) -> datetime:
    base = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    return base + timedelta(seconds=offset_seconds)


def _run(offset_seconds: float, healthy: bool = True) -> PipelineRun:
    return PipelineRun(
        pipeline="pipe-a",
        run_at=_dt(offset_seconds),
        healthy=healthy,
        duration_seconds=10.0,
        error_rate=0.0,
        rows_processed=100,
    )


NOW = _dt(0)
INTERVAL = 300.0  # 5 minutes


def test_no_runs_returns_not_lagging():
    result = detect_lag("pipe-a", [], INTERVAL, now=NOW)
    assert isinstance(result, LagResult)
    assert result.is_lagging is False
    assert result.last_run_at is None
    assert result.message == "no runs recorded"


def test_recent_run_is_not_lagging():
    runs = [_run(-60)]  # ran 60 seconds ago, interval is 300s
    result = detect_lag("pipe-a", runs, INTERVAL, now=NOW)
    assert result.is_lagging is False
    assert result.lag_seconds == 0.0


def test_run_just_within_tolerance_is_not_lagging():
    # tolerance=0.2 → threshold = 300 * 1.2 = 360s
    runs = [_run(-355)]  # 355s ago < 360s threshold
    result = detect_lag("pipe-a", runs, INTERVAL, tolerance=0.2, now=NOW)
    assert result.is_lagging is False


def test_run_beyond_tolerance_is_lagging():
    # 400s ago > 360s threshold
    runs = [_run(-400)]
    result = detect_lag("pipe-a", runs, INTERVAL, tolerance=0.2, now=NOW)
    assert result.is_lagging is True
    assert result.lag_seconds == pytest.approx(100.0)  # 400 - 300


def test_uses_most_recent_run():
    runs = [_run(-400), _run(-50), _run(-900)]
    result = detect_lag("pipe-a", runs, INTERVAL, now=NOW)
    assert result.is_lagging is False  # most recent was 50s ago


def test_str_ok():
    runs = [_run(-60)]
    result = detect_lag("pipe-a", runs, INTERVAL, now=NOW)
    assert "OK" in str(result)
    assert "pipe-a" in str(result)


def test_str_lagging():
    runs = [_run(-400)]
    result = detect_lag("pipe-a", runs, INTERVAL, tolerance=0.2, now=NOW)
    assert "LAGGING" in str(result)


def test_str_never_run():
    result = detect_lag("pipe-a", [], INTERVAL, now=NOW)
    assert "never run" in str(result)


def test_to_dict_keys():
    runs = [_run(-400)]
    result = detect_lag("pipe-a", runs, INTERVAL, now=NOW)
    d = result.to_dict()
    assert set(d.keys()) == {
        "pipeline",
        "lag_seconds",
        "expected_interval_seconds",
        "is_lagging",
        "last_run_at",
        "message",
    }


def test_to_dict_no_runs_last_run_at_is_none():
    result = detect_lag("pipe-a", [], INTERVAL, now=NOW)
    assert result.to_dict()["last_run_at"] is None


def test_zero_tolerance_exact_interval_not_lagging():
    runs = [_run(-300)]  # exactly at interval boundary
    result = detect_lag("pipe-a", runs, INTERVAL, tolerance=0.0, now=NOW)
    assert result.is_lagging is False


def test_zero_tolerance_one_second_over_is_lagging():
    runs = [_run(-301)]
    result = detect_lag("pipe-a", runs, INTERVAL, tolerance=0.0, now=NOW)
    assert result.is_lagging is True
