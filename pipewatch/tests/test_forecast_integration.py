"""Integration tests: forecast_metric with realistic PipelineRun-like objects."""
import pytest
from unittest.mock import MagicMock
from pipewatch.forecast import forecast_metric, MIN_RUNS


def _run(error_rate, duration=2.0, rows=500):
    r = MagicMock()
    r.metrics.error_rate = error_rate
    r.metrics.duration = duration
    r.metrics.rows_processed = rows
    return r


def test_exact_min_runs_produces_result():
    runs = [_run(0.05)] * MIN_RUNS
    result = forecast_metric("pipe", runs, "error_rate", horizon=1)
    assert not result.insufficient_data


def test_one_below_min_runs_insufficient():
    runs = [_run(0.05)] * (MIN_RUNS - 1)
    result = forecast_metric("pipe", runs, "error_rate", horizon=1)
    assert result.insufficient_data


def test_duration_metric_forecast():
    runs = [_run(0.0, duration=float(i)) for i in range(1, 11)]
    result = forecast_metric("pipe", runs, "duration", horizon=1)
    assert not result.insufficient_data
    assert result.predicted_value > runs[-1].metrics.duration


def test_rows_processed_metric_forecast():
    runs = [_run(0.0, rows=100 * i) for i in range(1, 11)]
    result = forecast_metric("pipe", runs, "rows_processed", horizon=2)
    assert result.predicted_value > runs[-1].metrics.rows_processed


def test_horizon_increases_prediction_for_rising_trend():
    runs = [_run(i * 0.01) for i in range(10)]
    r1 = forecast_metric("pipe", runs, "error_rate", horizon=1)
    r5 = forecast_metric("pipe", runs, "error_rate", horizon=5)
    assert r5.predicted_value > r1.predicted_value


def test_to_dict_roundtrip():
    runs = [_run(0.1)] * 10
    result = forecast_metric("pipe", runs, "error_rate")
    d = result.to_dict()
    assert d["pipeline"] == "pipe"
    assert d["metric"] == "error_rate"
    assert isinstance(d["predicted_value"], float)
