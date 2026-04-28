"""Tests for pipewatch.watchdog."""
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest

from pipewatch.history import PipelineRun
from pipewatch.watchdog import WatchdogResult, check_watchdog, check_all_watchdogs


NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _run(name: str, minutes_ago: float) -> PipelineRun:
    return PipelineRun(
        pipeline_name=name,
        ran_at=NOW - timedelta(minutes=minutes_ago),
        healthy=True,
        violation_count=0,
    )


def test_stale_when_never_run():
    result = check_watchdog("etl", 60, [], now=NOW)
    assert result.stale is True
    assert result.last_run_at is None
    assert result.minutes_overdue is None


def test_ok_when_within_interval():
    result = check_watchdog("etl", 60, [_run("etl", 30)], now=NOW)
    assert result.stale is False
    assert result.minutes_overdue is None


def test_stale_when_overdue():
    result = check_watchdog("etl", 60, [_run("etl", 90)], now=NOW)
    assert result.stale is True
    assert result.minutes_overdue == pytest.approx(30.0, abs=0.1)


def test_uses_most_recent_run():
    runs = [_run("etl", 120), _run("etl", 30)]
    result = check_watchdog("etl", 60, runs, now=NOW)
    assert result.stale is False


def test_ignores_other_pipelines():
    result = check_watchdog("etl", 60, [_run("other", 10)], now=NOW)
    assert result.stale is True


def test_exactly_at_interval_boundary():
    """A run exactly at the interval boundary should not be considered stale."""
    result = check_watchdog("etl", 60, [_run("etl", 60)], now=NOW)
    assert result.stale is False
    assert result.minutes_overdue is None


def test_str_ok():
    result = WatchdogResult("etl", 60, NOW, False, None)
    assert "OK" in str(result)


def test_str_stale_never_run():
    result = WatchdogResult("etl", 60, None, True, None)
    assert "never run" in str(result)


def test_str_stale_overdue():
    result = WatchdogResult("etl", 60, NOW, True, 15.0)
    assert "15.0 min overdue" in str(result)


def test_check_all_watchdogs():
    configs = [
        {"name": "etl", "interval_minutes": 60},
        {"name": "load", "interval_minutes": 30},
    ]
    runs = [_run("etl", 30), _run("load", 45)]
    results = check_all_watchdogs(configs, runs, now=NOW)
    assert len(results) == 2
    assert results[0].stale is False
    assert results[1].stale is True
