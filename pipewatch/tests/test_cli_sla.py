"""Tests for pipewatch.cli_sla."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from pipewatch.cli_sla import sla_command
from pipewatch.config import AppConfig, PipelineConfig, ThresholdConfig
from pipewatch.history import PipelineRun


def _make_app_config(max_duration: float = 60.0) -> AppConfig:
    return AppConfig(
        pipelines=[
            PipelineConfig(
                name="pipe_a",
                thresholds=ThresholdConfig(
                    max_duration_seconds=max_duration,
                    max_error_rate=0.05,
                    min_rows_processed=100,
                ),
            )
        ],
        webhook_url="http://example.com/hook",
    )


def _run(duration: float = 10.0, error_rate: float = 0.0, rows: int = 500) -> PipelineRun:
    return PipelineRun(
        pipeline="pipe_a",
        ran_at=datetime(2024, 6, 1, tzinfo=timezone.utc),
        duration_seconds=duration,
        error_rate=error_rate,
        rows_processed=rows,
        healthy=True,
    )


@pytest.fixture()
def runner():
    return CliRunner()


def test_check_exits_zero_when_compliant(runner, tmp_path):
    history_file = tmp_path / "history.json"
    history_file.write_text(json.dumps([]))
    app = _make_app_config()
    with patch("pipewatch.cli_sla.load_config", return_value=app), \
         patch("pipewatch.cli_sla.load_history", return_value=[_run(duration=5.0)]):
        result = runner.invoke(sla_command, ["check"])
    assert result.exit_code == 0
    assert "OK" in result.output


def test_check_exits_one_when_breach(runner):
    app = _make_app_config(max_duration=5.0)
    with patch("pipewatch.cli_sla.load_config", return_value=app), \
         patch("pipewatch.cli_sla.load_history", return_value=[_run(duration=120.0)]):
        result = runner.invoke(sla_command, ["check"])
    assert result.exit_code == 1
    assert "BREACH" in result.output


def test_check_single_pipeline_not_found(runner):
    app = _make_app_config()
    with patch("pipewatch.cli_sla.load_config", return_value=app), \
         patch("pipewatch.cli_sla.load_history", return_value=[]):
        result = runner.invoke(sla_command, ["check", "--pipeline", "nonexistent"])
    assert result.exit_code == 1


def test_check_single_pipeline_compliant(runner):
    app = _make_app_config()
    with patch("pipewatch.cli_sla.load_config", return_value=app), \
         patch("pipewatch.cli_sla.load_history", return_value=[_run(duration=1.0)]):
        result = runner.invoke(sla_command, ["check", "--pipeline", "pipe_a"])
    assert result.exit_code == 0
    assert "pipe_a" in result.output


def test_no_runs_is_compliant(runner):
    app = _make_app_config()
    with patch("pipewatch.cli_sla.load_config", return_value=app), \
         patch("pipewatch.cli_sla.load_history", return_value=[]):
        result = runner.invoke(sla_command, ["check"])
    assert result.exit_code == 0
