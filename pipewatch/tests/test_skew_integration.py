"""Integration tests for skew detection using real history persistence."""
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.history import PipelineRun, save_history, load_history
from pipewatch.skew import detect_skew


def _dt(offset: float = 0.0) -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=offset)


def _run(name: str, offset: float, healthy: bool = True) -> PipelineRun:
    return PipelineRun(
        pipeline=name,
        timestamp=_dt(offset),
        duration_seconds=10.0,
        rows_processed=100,
        error_rate=0.0,
        healthy=healthy,
    )


def test_save_and_detect_no_skew(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    save_history("pipe_a", [_run("pipe_a", 0)])
    save_history("pipe_b", [_run("pipe_b", 30)])
    runs_a = load_history("pipe_a")
    runs_b = load_history("pipe_b")
    result = detect_skew("pipe_a", "pipe_b", runs_a, runs_b, threshold_seconds=60.0)
    assert result.is_skewed is False
    assert result.skew_seconds == pytest.approx(30.0)


def test_save_and_detect_skew(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    save_history("pipe_a", [_run("pipe_a", 0)])
    save_history("pipe_b", [_run("pipe_b", 500)])
    runs_a = load_history("pipe_a")
    runs_b = load_history("pipe_b")
    result = detect_skew("pipe_a", "pipe_b", runs_a, runs_b, threshold_seconds=300.0)
    assert result.is_skewed is True


def test_multiple_runs_uses_latest(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    save_history("pipe_a", [_run("pipe_a", -1000), _run("pipe_a", 0)])
    save_history("pipe_b", [_run("pipe_b", -900), _run("pipe_b", 20)])
    runs_a = load_history("pipe_a")
    runs_b = load_history("pipe_b")
    result = detect_skew("pipe_a", "pipe_b", runs_a, runs_b, threshold_seconds=60.0)
    assert result.skew_seconds == pytest.approx(20.0)
    assert result.is_skewed is False


def test_to_dict_roundtrip(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    save_history("pipe_a", [_run("pipe_a", 0)])
    save_history("pipe_b", [_run("pipe_b", 50)])
    runs_a = load_history("pipe_a")
    runs_b = load_history("pipe_b")
    result = detect_skew("pipe_a", "pipe_b", runs_a, runs_b, threshold_seconds=100.0)
    d = result.to_dict()
    assert d["pipeline_a"] == "pipe_a"
    assert d["pipeline_b"] == "pipe_b"
    assert d["is_skewed"] is False
    assert isinstance(d["last_run_a"], str)
    assert isinstance(d["last_run_b"], str)
