"""Tests for pipewatch.runner."""

from unittest.mock import patch

import pytest

from pipewatch.config import AppConfig, PipelineConfig, ThresholdConfig
from pipewatch.metrics import PipelineMetrics
from pipewatch.runner import PipelineResult, run_all_checks, run_pipeline_check


def make_config(
    pipeline_name="orders",
    max_duration=300,
    max_error_rate=0.05,
    webhook_url=None,
    default_webhook="https://hooks.example.com/default",
):
    thresholds = ThresholdConfig(
        max_duration_seconds=max_duration,
        max_error_rate=max_error_rate,
    )
    pipeline = PipelineConfig(
        name=pipeline_name,
        thresholds=thresholds,
        webhook_url=webhook_url,
    )
    return AppConfig(pipelines=[pipeline], default_webhook_url=default_webhook)


def make_metrics(duration=100, error_rate=0.01, rows=1000):
    return PipelineMetrics(
        duration_seconds=duration,
        error_rate=error_rate,
        rows_processed=rows,
    )


# ---------------------------------------------------------------------------
# PipelineResult
# ---------------------------------------------------------------------------

def test_pipeline_result_healthy_when_no_violations():
    result = PipelineResult(pipeline="orders", violations=[])
    assert result.healthy is True


def test_pipeline_result_unhealthy_when_violations_exist():
    from pipewatch.metrics import ThresholdViolation
    v = ThresholdViolation(metric="error_rate", value=0.2, threshold=0.05)
    result = PipelineResult(pipeline="orders", violations=[v])
    assert result.healthy is False


# ---------------------------------------------------------------------------
# run_pipeline_check
# ---------------------------------------------------------------------------

def test_run_pipeline_check_healthy():
    config = make_config()
    metrics = make_metrics()
    result = run_pipeline_check("orders", metrics, config)
    assert result.healthy
    assert result.notified is False


def test_run_pipeline_check_violation_triggers_notification():
    config = make_config()
    metrics = make_metrics(error_rate=0.5)
    with patch("pipewatch.runner.notify_violations", return_value=True) as mock_notify:
        result = run_pipeline_check("orders", metrics, config)
    assert not result.healthy
    mock_notify.assert_called_once()
    assert result.notified is True


def test_run_pipeline_check_dry_run_skips_notification():
    config = make_config()
    metrics = make_metrics(error_rate=0.5)
    with patch("pipewatch.runner.notify_violations") as mock_notify:
        result = run_pipeline_check("orders", metrics, config, dry_run=True)
    mock_notify.assert_not_called()
    assert result.notified is False


def test_run_pipeline_check_unknown_pipeline_raises():
    config = make_config()
    with pytest.raises(ValueError, match="not found in config"):
        run_pipeline_check("unknown", make_metrics(), config)


# ---------------------------------------------------------------------------
# run_all_checks
# ---------------------------------------------------------------------------

def test_run_all_checks_returns_results_for_known_pipelines():
    config = make_config()
    metrics_map = {"orders": make_metrics()}
    results = run_all_checks(metrics_map, config)
    assert len(results) == 1
    assert results[0].pipeline == "orders"


def test_run_all_checks_skips_unknown_pipelines():
    config = make_config()
    metrics_map = {"orders": make_metrics(), "ghost": make_metrics()}
    results = run_all_checks(metrics_map, config)
    assert len(results) == 1
