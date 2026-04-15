"""Tests for the pipewatch CLI commands."""

import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock

from pipewatch.cli import cli
from pipewatch.config import AppConfig, PipelineConfig, ThresholdConfig
from pipewatch.runner import PipelineResult
from pipewatch.metrics import ThresholdViolation


def _make_app_config(names=("pipeline_a", "pipeline_b")):
    pipelines = [
        PipelineConfig(
            name=n,
            thresholds=ThresholdConfig(max_duration_seconds=300, max_error_rate=0.05),
        )
        for n in names
    ]
    return AppConfig(pipelines=pipelines, webhook_url="http://example.com/hook")


def _healthy_results(app_config):
    return [
        PipelineResult(pipeline_name=p.name, violations=[])
        for p in app_config.pipelines
    ]


@pytest.fixture()
def runner():
    return CliRunner()


def test_check_exits_zero_when_all_healthy(runner):
    app_config = _make_app_config()
    with patch("pipewatch.cli.load_config", return_value=app_config), \
         patch("pipewatch.cli.run_all_checks", return_value=_healthy_results(app_config)):
        result = runner.invoke(cli, ["check", "--no-notify"])
    assert result.exit_code == 0
    assert "[OK]" in result.output


def test_check_exits_one_when_violations(runner):
    app_config = _make_app_config(["pipeline_a"])
    violation = ThresholdViolation(metric="duration", value=500, threshold=300)
    bad_results = [PipelineResult(pipeline_name="pipeline_a", violations=[violation])]
    with patch("pipewatch.cli.load_config", return_value=app_config), \
         patch("pipewatch.cli.run_all_checks", return_value=bad_results):
        result = runner.invoke(cli, ["check", "--no-notify"])
    assert result.exit_code == 1
    assert "[FAIL]" in result.output


def test_check_missing_config_exits_one(runner):
    with patch("pipewatch.cli.load_config", side_effect=FileNotFoundError):
        result = runner.invoke(cli, ["check", "-c", "missing.yaml"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_check_unknown_pipeline_exits_one(runner):
    app_config = _make_app_config(["pipeline_a"])
    with patch("pipewatch.cli.load_config", return_value=app_config), \
         patch("pipewatch.cli.run_all_checks", return_value=[]):
        result = runner.invoke(cli, ["check", "-p", "nonexistent"])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_list_shows_pipeline_names(runner):
    app_config = _make_app_config(["alpha", "beta", "gamma"])
    with patch("pipewatch.cli.load_config", return_value=app_config):
        result = runner.invoke(cli, ["list"])
    assert result.exit_code == 0
    assert "alpha" in result.output
    assert "beta" in result.output
    assert "gamma" in result.output
    assert "Pipelines (3)" in result.output


def test_list_config_error_exits_one(runner):
    with patch("pipewatch.cli.load_config", side_effect=ValueError("bad yaml")):
        result = runner.invoke(cli, ["list"])
    assert result.exit_code == 1
    assert "bad yaml" in result.output


def test_verbose_flag_shows_ok_violations(runner):
    app_config = _make_app_config(["pipeline_a"])
    results = _healthy_results(app_config)
    with patch("pipewatch.cli.load_config", return_value=app_config), \
         patch("pipewatch.cli.run_all_checks", return_value=results):
        result = runner.invoke(cli, ["check", "--no-notify", "--verbose"])
    assert result.exit_code == 0
