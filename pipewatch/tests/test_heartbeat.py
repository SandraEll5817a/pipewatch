"""Tests for pipewatch.heartbeat."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.heartbeat import (
    HeartbeatResult,
    check_heartbeat,
    check_all_heartbeats,
)
from pipewatch.history import PipelineRun


def _dt(offset_seconds: float = 0) -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(
        seconds=offset_seconds
    )


def _run(offset_seconds: float = 0, healthy: bool = True) -> PipelineRun:
    return PipelineRun(
        pipeline="pipe",
        ran_at=_dt(offset_seconds),
        healthy=healthy,
        duration_seconds=1.0,
        error_rate=0.0,
        rows_processed=100,
    )


NOW = _dt(0)


def test_never_run_is_dead():
    result = check_heartbeat("pipe", [], 300, grace_seconds=60, now=NOW)
    assert result.alive is False
    assert result.last_run_at is None
    assert result.seconds_since_last_run is None
    assert "never run" in result.message.lower()


def test_recent_run_is_alive():
    runs = [_run(-100)]
    result = check_heartbeat("pipe", runs, 300, grace_seconds=60, now=NOW)
    assert result.alive is True
    assert result.seconds_since_last_run == pytest.approx(100.0)


def test_overdue_run_is_dead():
    # last run 400s ago, interval=300, grace=60 => threshold=360 => dead
    runs = [_run(-400)]
    result = check_heartbeat("pipe", runs, 300, grace_seconds=60, now=NOW)
    assert result.alive is False
    assert result.seconds_since_last_run == pytest.approx(400.0)


def test_exactly_at_threshold_is_alive():
    # last run exactly at threshold (360s ago)
    runs = [_run(-360)]
    result = check_heartbeat("pipe", runs, 300, grace_seconds=60, now=NOW)
    assert result.alive is True


def test_uses_most_recent_run():
    runs = [_run(-500), _run(-100), _run(-300)]
    result = check_heartbeat("pipe", runs, 300, grace_seconds=60, now=NOW)
    assert result.seconds_since_last_run == pytest.approx(100.0)


def test_str_alive():
    runs = [_run(-50)]
    result = check_heartbeat("pipe", runs, 300, grace_seconds=60, now=NOW)
    assert "alive" in str(result)
    assert "pipe" in str(result)


def test_str_dead_never_run():
    result = check_heartbeat("pipe", [], 300, grace_seconds=60, now=NOW)
    assert "DEAD" in str(result)
    assert "never" in str(result).lower()


def test_str_dead_overdue():
    runs = [_run(-999)]
    result = check_heartbeat("pipe", runs, 300, grace_seconds=60, now=NOW)
    assert "DEAD" in str(result)


def test_to_dict_keys():
    runs = [_run(-50)]
    result = check_heartbeat("pipe", runs, 300, grace_seconds=60, now=NOW)
    d = result.to_dict()
    assert set(d.keys()) == {
        "pipeline",
        "expected_interval_seconds",
        "last_run_at",
        "seconds_since_last_run",
        "alive",
        "message",
    }


def test_to_dict_none_when_never_run():
    result = check_heartbeat("pipe", [], 300, now=NOW)
    d = result.to_dict()
    assert d["last_run_at"] is None
    assert d["seconds_since_last_run"] is None


def test_check_all_heartbeats_mixed():
    runs_by_pipeline = {
        "fast": [_run(-50)],
        "slow": [_run(-999)],
        "new": [],
    }
    pairs = [("fast", 300), ("slow", 300), ("new", 300)]
    results = check_all_heartbeats(pairs, runs_by_pipeline, grace_seconds=60, now=NOW)
    assert len(results) == 3
    by_name = {r.pipeline: r for r in results}
    assert by_name["fast"].alive is True
    assert by_name["slow"].alive is False
    assert by_name["new"].alive is False


def test_check_all_heartbeats_empty_pipelines():
    results = check_all_heartbeats([], {}, now=NOW)
    assert results == []
