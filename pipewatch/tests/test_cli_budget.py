"""Tests for pipewatch.cli_budget."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from pipewatch.cli_budget import budget_command
from pipewatch.config import AppConfig, PipelineConfig, ThresholdConfig
from pipewatch.history import PipelineRun


def _make_app_config(names=("pipe_a", "pipe_b")):
    pipelines = [
        PipelineConfig(name=n, thresholds=ThresholdConfig())
        for n in names
    ]
    return AppConfig(pipelines=pipelines, webhook_url="http://example.com/hook")


def _run(pipeline: str, duration: float = 10.0, error_rate: float = 0.0) -> PipelineRun:
    return PipelineRun(
        pipeline=pipeline,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        duration_seconds=duration,
        rows_processed=100,
        error_rate=error_rate,
        healthy=True,
    )


@pytest.fixture()
def runner():
    return CliRunner()


def test_check_exits_one_when_no_constraints(runner):
    app = _make_app_config()
    with patch("pipewatch.cli_budget.load_config", return_value=app):
        result = runner.invoke(budget_command, ["check"])
    assert result.exit_code == 1
    assert "No budget constraints" in result.output


def test_check_exits_zero_when_all_compliant(runner):
    app = _make_app_config(["pipe_a"])
    with (
        patch("pipewatch.cli_budget.load_config", return_value=app),
        patch("pipewatch.cli_budget.load_history", return_value=[_run("pipe_a", duration=5.0)]),
    ):
        result = runner.invoke(budget_command, ["check", "--max-duration", "60"])
    assert result.exit_code == 0
    assert "OK" in result.output


def test_check_exits_one_when_breach(runner):
    app = _make_app_config(["pipe_a"])
    with (
        patch("pipewatch.cli_budget.load_config", return_value=app),
        patch("pipewatch.cli_budget.load_history", return_value=[_run("pipe_a", duration=120.0)]),
    ):
        result = runner.invoke(budget_command, ["check", "--max-duration", "60"])
    assert result.exit_code == 1
    assert "BREACH" in result.output


def test_check_pipeline_filter_not_found(runner):
    app = _make_app_config(["pipe_a"])
    with patch("pipewatch.cli_budget.load_config", return_value=app):
        result = runner.invoke(budget_command, ["check", "--max-duration", "60", "--pipeline", "missing"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_check_pipeline_filter_limits_scope(runner):
    app = _make_app_config(["pipe_a", "pipe_b"])
    history_calls = []

    def fake_load_history(name):
        history_calls.append(name)
        return [_run(name, duration=5.0)]

    with (
        patch("pipewatch.cli_budget.load_config", return_value=app),
        patch("pipewatch.cli_budget.load_history", side_effect=fake_load_history),
    ):
        result = runner.invoke(budget_command, ["check", "--max-duration", "60", "--pipeline", "pipe_a"])

    assert result.exit_code == 0
    assert history_calls == ["pipe_a"]
