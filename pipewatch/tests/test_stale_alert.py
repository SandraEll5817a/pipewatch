"""Tests for pipewatch.stale_alert."""
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest

from pipewatch.history import PipelineRun
from pipewatch.stale_alert import (
    StaleAlertResult,
    check_stale,
    check_all_stale,
)


def _dt(offset_minutes: int = 0) -> datetime:
    base = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    return base + timedelta(minutes=offset_minutes)


def _run(offset_minutes: int = 0) -> PipelineRun:
    return PipelineRun(
        pipeline="pipe-a",
        ran_at=_dt(offset_minutes),
        duration_seconds=10.0,
        rows_processed=100,
        error_rate=0.0,
        healthy=True,
    )


NOW = _dt(0)


def test_stale_when_never_run():
    result = check_stale("pipe-a", [], stale_after_minutes=30, now=NOW)
    assert result.is_stale is True
    assert result.last_run_at is None
    assert result.age_minutes is None


def test_ok_when_within_threshold():
    runs = [_run(-10)]  # ran 10 minutes ago
    result = check_stale("pipe-a", runs, stale_after_minutes=30, now=NOW)
    assert result.is_stale is False
    assert result.age_minutes == pytest.approx(10.0)


def test_stale_when_overdue():
    runs = [_run(-60)]  # ran 60 minutes ago
    result = check_stale("pipe-a", runs, stale_after_minutes=30, now=NOW)
    assert result.is_stale is True
    assert result.age_minutes == pytest.approx(60.0)


def test_stale_exactly_at_boundary():
    runs = [_run(-30)]  # exactly at threshold — stale
    result = check_stale("pipe-a", runs, stale_after_minutes=30, now=NOW)
    assert result.is_stale is True


def test_uses_most_recent_run():
    runs = [_run(-90), _run(-5), _run(-45)]
    result = check_stale("pipe-a", runs, stale_after_minutes=30, now=NOW)
    assert result.is_stale is False
    assert result.age_minutes == pytest.approx(5.0)


def test_str_stale_never_run():
    result = StaleAlertResult(
        pipeline="p", is_stale=True, last_run_at=None,
        stale_after_minutes=15, age_minutes=None
    )
    assert "STALE" in str(result)
    assert "never" in str(result)


def test_str_stale_with_age():
    result = StaleAlertResult(
        pipeline="p", is_stale=True, last_run_at=_dt(-40),
        stale_after_minutes=30, age_minutes=40.0
    )
    assert "STALE" in str(result)
    assert "40.0m" in str(result)


def test_str_ok():
    result = StaleAlertResult(
        pipeline="p", is_stale=False, last_run_at=_dt(-5),
        stale_after_minutes=30, age_minutes=5.0
    )
    assert "OK" in str(result)
    assert "5.0m" in str(result)


def test_check_all_stale_returns_one_per_pipeline():
    pipeline_runs = {
        "pipe-a": [_run(-5)],
        "pipe-b": [],
        "pipe-c": [_run(-60)],
    }
    results = check_all_stale(pipeline_runs, stale_after_minutes=30, now=NOW)
    assert len(results) == 3
    by_name = {r.pipeline: r for r in results}
    assert by_name["pipe-a"].is_stale is False
    assert by_name["pipe-b"].is_stale is True
    assert by_name["pipe-c"].is_stale is True


def test_naive_datetime_treated_as_utc():
    naive_run = PipelineRun(
        pipeline="p",
        ran_at=datetime(2024, 6, 1, 11, 55, 0),  # naive, 5 min before NOW
        duration_seconds=1.0,
        rows_processed=0,
        error_rate=0.0,
        healthy=True,
    )
    result = check_stale("p", [naive_run], stale_after_minutes=30, now=NOW)
    assert result.is_stale is False
    assert result.age_minutes == pytest.approx(5.0)
