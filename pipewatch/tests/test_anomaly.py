"""Tests for pipewatch.anomaly."""
from __future__ import annotations

import pytest
from datetime import datetime, timezone
from pipewatch.anomaly import detect_anomalies, AnomalyResult, _z_score
from pipewatch.history import PipelineRun


def _run(duration=10.0, error_rate=0.01, rows=1000):
    return PipelineRun(
        pipeline="pipe1",
        ran_at=datetime.now(timezone.utc).isoformat(),
        duration_seconds=duration,
        error_rate=error_rate,
        rows_processed=rows,
        healthy=True,
        violations=[],
    )


def _history(n=10, duration=10.0, error_rate=0.01, rows=1000):
    return [_run(duration, error_rate, rows) for _ in range(n)]


def test_z_score_zero_stddev():
    assert _z_score(5.0, 5.0, 0.0) == 0.0


def test_z_score_basic():
    assert abs(_z_score(15.0, 10.0, 5.0) - 1.0) < 1e-9


def test_no_anomaly_when_within_normal_range():
    history = _history(10, duration=10.0)
    current = _run(duration=10.5)
    results = detect_anomalies("pipe1", history, current, z_threshold=2.5)
    duration_result = next(r for r in results if r.metric == "duration_seconds")
    assert not duration_result.is_anomaly


def test_anomaly_detected_on_spike():
    history = _history(10, duration=10.0)
    current = _run(duration=500.0)
    results = detect_anomalies("pipe1", history, current, z_threshold=2.5)
    duration_result = next(r for r in results if r.metric == "duration_seconds")
    assert duration_result.is_anomaly


def test_insufficient_samples_skips_metric():
    history = _history(3, duration=10.0)
    current = _run(duration=500.0)
    results = detect_anomalies("pipe1", history, current, min_samples=5)
    assert all(r.metric != "duration_seconds" for r in results)


def test_none_current_value_skipped():
    history = _history(10)
    current = _run(duration=None)
    results = detect_anomalies("pipe1", history, current)
    assert all(r.metric != "duration_seconds" for r in results)


def test_anomaly_result_str_anomaly():
    r = AnomalyResult("p", "duration_seconds", 500.0, 10.0, 5.0, 98.0, True)
    assert "ANOMALY" in str(r)


def test_anomaly_result_str_ok():
    r = AnomalyResult("p", "duration_seconds", 10.1, 10.0, 0.5, 0.2, False)
    assert "ok" in str(r)


def test_returns_result_for_all_three_metrics():
    history = _history(10)
    current = _run()
    results = detect_anomalies("pipe1", history, current)
    metrics = {r.metric for r in results}
    assert metrics == {"duration_seconds", "error_rate", "rows_processed"}
