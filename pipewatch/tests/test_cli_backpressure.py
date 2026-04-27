"""Tests for pipewatch.cli_backpressure."""
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from pipewatch.cli_backpressure import backpressure_command
from pipewatch.config import AppConfig, PipelineConfig, ThresholdConfig
from pipewatch.history import PipelineRun


def _make_app_config(names=("pipe_a",)):
    pipelines = [
        PipelineConfig(
            name=n,
            source="s3://bucket/path",
            thresholds=ThresholdConfig(),
        )
        for n in names
    ]
    return AppConfig(pipelines=pipelines, webhook_url="http://example.com/hook")


def _run(rows: float) -> PipelineRun:
    return PipelineRun(
        pipeline="pipe_a",
        timestamp=datetime.now(timezone.utc).isoformat(),
        duration_seconds=10.0,
        error_rate=0.0,
        rows_processed=rows,
        healthy=True,
    )


@pytest.fixture()
def runner():
    return CliRunner()


def test_check_exits_zero_when_no_pressure(runner):
    stable_runs = [_run(100.0)] * 8
    with patch("pipewatch.cli_backpressure.load_config", return_value=_make_app_config()), \
         patch("pipewatch.cli_backpressure.load_history", return_value=stable_runs):
        result = runner.invoke(backpressure_command, ["check"])
    assert result.exit_code == 0


def test_check_exits_one_when_pressured(runner):
    declining_runs = [_run(float(200 - i * 15)) for i in range(8)]
    with patch("pipewatch.cli_backpressure.load_config", return_value=_make_app_config()), \
         patch("pipewatch.cli_backpressure.load_history", return_value=declining_runs):
        result = runner.invoke(backpressure_command, ["check", "--slope-threshold", "-5.0"])
    assert result.exit_code == 1


def test_check_insufficient_data_exits_zero(runner):
    few_runs = [_run(100.0)] * 2
    with patch("pipewatch.cli_backpressure.load_config", return_value=_make_app_config()), \
         patch("pipewatch.cli_backpressure.load_history", return_value=few_runs):
        result = runner.invoke(backpressure_command, ["check"])
    assert result.exit_code == 0
    assert "insufficient data" in result.output


def test_check_pipeline_filter_unknown_exits_one(runner):
    with patch("pipewatch.cli_backpressure.load_config", return_value=_make_app_config()):
        result = runner.invoke(
            backpressure_command, ["check", "--pipeline", "nonexistent"]
        )
    assert result.exit_code == 1


def test_check_pipeline_filter_applies(runner):
    stable_runs = [_run(100.0)] * 8
    app = _make_app_config(names=("pipe_a", "pipe_b"))
    with patch("pipewatch.cli_backpressure.load_config", return_value=app), \
         patch("pipewatch.cli_backpressure.load_history", return_value=stable_runs):
        result = runner.invoke(
            backpressure_command, ["check", "--pipeline", "pipe_a"]
        )
    assert result.exit_code == 0
    assert "pipe_b" not in result.output
