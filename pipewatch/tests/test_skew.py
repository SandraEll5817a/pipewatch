"""Tests for pipewatch.skew."""
from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.history import PipelineRun
from pipewatch.skew import detect_skew, SkewResult


def _dt(offset_seconds: float = 0.0) -> datetime:
    base = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    return base + timedelta(seconds=offset_seconds)


def _run(name: str, offset: float) -> PipelineRun:
    return PipelineRun(
        pipeline=name,
        timestamp=_dt(offset),
        duration_seconds=10.0,
        rows_processed=100,
        error_rate=0.0,
        healthy=True,
    )


def test_no_skew_when_runs_simultaneous():
    runs_a = [_run("a", 0)]
    runs_b = [_run("b", 0)]
    result = detect_skew("a", "b", runs_a, runs_b, threshold_seconds=60.0)
    assert result.skew_seconds == 0.0
    assert result.is_skewed is False


def test_no_skew_within_threshold():
    runs_a = [_run("a", 0)]
    runs_b = [_run("b", 30)]
    result = detect_skew("a", "b", runs_a, runs_b, threshold_seconds=60.0)
    assert result.skew_seconds == pytest.approx(30.0)
    assert result.is_skewed is False


def test_skew_detected_above_threshold():
    runs_a = [_run("a", 0)]
    runs_b = [_run("b", 400)]
    result = detect_skew("a", "b", runs_a, runs_b, threshold_seconds=300.0)
    assert result.skew_seconds == pytest.approx(400.0)
    assert result.is_skewed is True


def test_skew_uses_most_recent_run():
    runs_a = [_run("a", -1000), _run("a", 0)]
    runs_b = [_run("b", -500), _run("b", 10)]
    result = detect_skew("a", "b", runs_a, runs_b, threshold_seconds=60.0)
    assert result.skew_seconds == pytest.approx(10.0)
    assert result.is_skewed is False


def test_not_skewed_when_a_missing():
    result = detect_skew("a", "b", [], [_run("b", 0)], threshold_seconds=60.0)
    assert result.is_skewed is False
    assert result.last_run_a is None


def test_not_skewed_when_b_missing():
    result = detect_skew("a", "b", [_run("a", 0)], [], threshold_seconds=60.0)
    assert result.is_skewed is False
    assert result.last_run_b is None


def test_not_skewed_when_both_missing():
    result = detect_skew("a", "b", [], [], threshold_seconds=60.0)
    assert result.is_skewed is False


def test_to_dict_keys():
    runs_a = [_run("a", 0)]
    runs_b = [_run("b", 50)]
    result = detect_skew("a", "b", runs_a, runs_b, threshold_seconds=100.0)
    d = result.to_dict()
    assert set(d.keys()) == {
        "pipeline_a", "pipeline_b", "skew_seconds",
        "threshold_seconds", "is_skewed", "last_run_a", "last_run_b",
    }


def test_str_skewed():
    runs_a = [_run("a", 0)]
    runs_b = [_run("b", 400)]
    result = detect_skew("a", "b", runs_a, runs_b, threshold_seconds=300.0)
    assert "SKEWED" in str(result)
    assert "a" in str(result)
    assert "b" in str(result)


def test_str_ok():
    runs_a = [_run("a", 0)]
    runs_b = [_run("b", 10)]
    result = detect_skew("a", "b", runs_a, runs_b, threshold_seconds=300.0)
    assert str(result).startswith("OK")
