"""Tests for pipewatch.cli_forecast."""
import json
import pytest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from pipewatch.cli_forecast import forecast_command


def _make_app_config(pipelines=None):
    app = MagicMock()
    app.pipelines = pipelines or []
    return app


def _pipeline(name, error_rate_threshold=0.1):
    p = MagicMock()
    p.name = name
    p.thresholds.error_rate = error_rate_threshold
    p.thresholds.duration = None
    p.thresholds.rows_processed = None
    return p


def _run(pipeline, error_rate=0.05):
    r = MagicMock()
    r.pipeline = pipeline
    r.metrics.error_rate = error_rate
    r.metrics.duration = 1.0
    r.metrics.rows_processed = 100
    return r


@pytest.fixture
def runner():
    return CliRunner()


def test_show_exits_zero_when_no_violations(runner):
    app = _make_app_config([_pipeline("etl")])
    runs = [_run("etl", 0.01)] * 10
    with patch("pipewatch.cli_forecast.load_config", return_value=app), \
         patch("pipewatch.cli_forecast.load_history", return_value=runs):
        result = runner.invoke(forecast_command, ["show"])
    assert result.exit_code == 0


def test_show_exits_one_when_prediction_exceeds_threshold(runner):
    app = _make_app_config([_pipeline("etl", error_rate_threshold=0.05)])
    runs = [_run("etl", 0.3 + i * 0.05) for i in range(10)]
    with patch("pipewatch.cli_forecast.load_config", return_value=app), \
         patch("pipewatch.cli_forecast.load_history", return_value=runs):
        result = runner.invoke(forecast_command, ["show", "--metric", "error_rate"])
    assert result.exit_code == 1


def test_show_insufficient_data_exits_zero(runner):
    app = _make_app_config([_pipeline("etl")])
    runs = [_run("etl")] * 3  # fewer than MIN_RUNS
    with patch("pipewatch.cli_forecast.load_config", return_value=app), \
         patch("pipewatch.cli_forecast.load_history", return_value=runs):
        result = runner.invoke(forecast_command, ["show"])
    assert result.exit_code == 0
    assert "insufficient" in result.output


def test_show_unknown_pipeline_exits_one(runner):
    app = _make_app_config([_pipeline("etl")])
    with patch("pipewatch.cli_forecast.load_config", return_value=app), \
         patch("pipewatch.cli_forecast.load_history", return_value=[]):
        result = runner.invoke(forecast_command, ["show", "--pipeline", "ghost"])
    assert result.exit_code == 1


def test_show_filters_by_pipeline(runner):
    app = _make_app_config([_pipeline("etl"), _pipeline("loader")])
    runs = [_run("etl", 0.01)] * 10 + [_run("loader", 0.01)] * 10
    with patch("pipewatch.cli_forecast.load_config", return_value=app), \
         patch("pipewatch.cli_forecast.load_history", return_value=runs):
        result = runner.invoke(forecast_command, ["show", "--pipeline", "etl"])
    assert "loader" not in result.output
    assert "etl" in result.output
