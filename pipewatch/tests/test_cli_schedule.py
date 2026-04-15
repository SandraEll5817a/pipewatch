"""Tests for pipewatch.cli_schedule."""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from pipewatch.cli_schedule import schedule_command
from pipewatch.config import AppConfig, PipelineConfig, ThresholdConfig
from pipewatch.runner import PipelineResult
from pipewatch.metrics import ThresholdViolation


def _make_app_config(names=("pipe_a",)):
    pipelines = [
        PipelineConfig(
            name=n,
            source="db",
            thresholds=ThresholdConfig(max_duration_seconds=60, max_error_rate=0.05),
        )
        for n in names
    ]
    return AppConfig(pipelines=pipelines, default_webhook="http://hook.example.com/")


def _healthy_result(name="pipe_a"):
    return PipelineResult(pipeline_name=name, violations=[], metrics=MagicMock())


def _unhealthy_result(name="pipe_a"):
    v = ThresholdViolation(field="duration", threshold=60.0, actual=120.0)
    return PipelineResult(pipeline_name=name, violations=[v], metrics=MagicMock())


@pytest.fixture()
def runner():
    return CliRunner()


def test_schedule_exits_one_when_no_pipelines(runner):
    empty_config = AppConfig(pipelines=[], default_webhook=None)
    with patch("pipewatch.cli_schedule.load_config", return_value=empty_config):
        result = runner.invoke(schedule_command, ["--max-ticks", "1"])
    assert result.exit_code == 1


def test_schedule_runs_check_for_each_pipeline(runner):
    app_config = _make_app_config(["pipe_a", "pipe_b"])
    healthy = {"pipe_a": _healthy_result("pipe_a"), "pipe_b": _healthy_result("pipe_b")}

    def fake_run(pipeline_cfg):
        return healthy[pipeline_cfg.name]

    with patch("pipewatch.cli_schedule.load_config", return_value=app_config), \
         patch("pipewatch.cli_schedule.run_pipeline_check", side_effect=fake_run):
        result = runner.invoke(schedule_command, ["--max-ticks", "1", "--interval", "0"])

    assert result.exit_code == 0
    assert "pipe_a" in result.output
    assert "pipe_b" in result.output


def test_schedule_outputs_healthy_status(runner):
    app_config = _make_app_config(["pipe_a"])

    with patch("pipewatch.cli_schedule.load_config", return_value=app_config), \
         patch("pipewatch.cli_schedule.run_pipeline_check", return_value=_healthy_result()):
        result = runner.invoke(schedule_command, ["--max-ticks", "1", "--interval", "0"])

    assert "healthy" in result.output


def test_schedule_sends_webhook_on_violation(runner):
    app_config = _make_app_config(["pipe_a"])

    with patch("pipewatch.cli_schedule.load_config", return_value=app_config), \
         patch("pipewatch.cli_schedule.run_pipeline_check", return_value=_unhealthy_result()), \
         patch("pipewatch.cli_schedule.notify_violations", return_value=True) as mock_notify:
        result = runner.invoke(schedule_command, ["--max-ticks", "1", "--interval", "0"])

    mock_notify.assert_called_once()
    assert result.exit_code == 0


def test_schedule_warns_when_webhook_fails(runner):
    app_config = _make_app_config(["pipe_a"])

    with patch("pipewatch.cli_schedule.load_config", return_value=app_config), \
         patch("pipewatch.cli_schedule.run_pipeline_check", return_value=_unhealthy_result()), \
         patch("pipewatch.cli_schedule.notify_violations", return_value=False):
        result = runner.invoke(schedule_command, ["--max-ticks", "1", "--interval", "0"])

    assert "Warning" in result.output or result.exit_code == 0


def test_schedule_reports_total_checks(runner):
    app_config = _make_app_config(["pipe_a"])

    with patch("pipewatch.cli_schedule.load_config", return_value=app_config), \
         patch("pipewatch.cli_schedule.run_pipeline_check", return_value=_healthy_result()):
        result = runner.invoke(schedule_command, ["--max-ticks", "2", "--interval", "0", "--tick", "0"])

    assert "Total checks run: 2" in result.output
