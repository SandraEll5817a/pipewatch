"""Tests for pipewatch.forecast."""
import pytest
from unittest.mock import MagicMock
from pipewatch.forecast import forecast_metric, ForecastResult, _linear_forecast, _confidence


def _run(error_rate: float, duration: float = 1.0, rows: int = 100):
    r = MagicMock()
    r.metrics.error_rate = error_rate
    r.metrics.duration = duration
    r.metrics.rows_processed = rows
    return r


def test_insufficient_data_when_fewer_than_min_runs():
    runs = [_run(0.1)] * 4
    result = forecast_metric("p", runs, "error_rate")
    assert result.insufficient_data is True


def test_forecast_returns_result_with_enough_runs():
    runs = [_run(0.1)] * 10
    result = forecast_metric("p", runs, "error_rate")
    assert not result.insufficient_data
    assert result.pipeline == "p"
    assert result.metric == "error_rate"


def test_flat_series_predicts_same_value():
    runs = [_run(0.2)] * 10
    result = forecast_metric("p", runs, "error_rate", horizon=1)
    assert abs(result.predicted_value - 0.2) < 1e-6


def test_rising_series_predicts_higher():
    runs = [_run(i * 0.01) for i in range(10)]
    result = forecast_metric("p", runs, "error_rate", horizon=1)
    assert result.predicted_value > runs[-1].metrics.error_rate


def test_falling_series_predicts_lower():
    runs = [_run((9 - i) * 0.01) for i in range(10)]
    result = forecast_metric("p", runs, "error_rate", horizon=1)
    assert result.predicted_value < runs[-1].metrics.error_rate


def test_confidence_low_for_small_n():
    assert _confidence(5) == "low"


def test_confidence_medium():
    assert _confidence(10) == "medium"


def test_confidence_high():
    assert _confidence(20) == "high"


def test_to_dict_keys():
    result = ForecastResult("p", "error_rate", 0.05, "medium", 1)
    d = result.to_dict()
    assert set(d.keys()) == {"pipeline", "metric", "predicted_value", "confidence", "horizon", "insufficient_data"}


def test_str_insufficient():
    r = ForecastResult("p", "error_rate", 0.0, "low", 1, insufficient_data=True)
    assert "insufficient" in str(r)


def test_str_normal():
    r = ForecastResult("p", "error_rate", 0.05, "high", 1)
    assert "predicted" in str(r)
    assert "0.0500" in str(r)


def test_linear_forecast_constant():
    vals = [3.0] * 5
    assert abs(_linear_forecast(vals, 1) - 3.0) < 1e-9
