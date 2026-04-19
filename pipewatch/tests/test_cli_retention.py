"""Tests for pipewatch.cli_retention."""
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock
from click.testing import CliRunner
from pipewatch.cli_retention import retention_command
from pipewatch.config import AppConfig, PipelineConfig, ThresholdConfig
from pipewatch.history import PipelineRun


def _make_app_config():
    t = ThresholdConfig()
    p = PipelineConfig(name="pipe_a", source="s", thresholds=t)
    return AppConfig(pipelines=[p], webhook_url=None)


def _run(days_ago: float) -> PipelineRun:
    ts = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return PipelineRun(pipeline="pipe_a", timestamp=ts, healthy=True, violations=[])


@patch("pipewatch.cli_retention.save_history")
@patch("pipewatch.cli_retention.load_history")
@patch("pipewatch.cli_retention.load_config")
def test_prune_by_age(mock_cfg, mock_load, mock_save):
    mock_cfg.return_value = _make_app_config()
    mock_load.return_value = [_run(1), _run(20)]
    runner = CliRunner()
    result = runner.invoke(retention_command, ["prune", "--max-age-days", "7"])
    assert result.exit_code == 0
    mock_save.assert_called_once()
    saved_runs = mock_save.call_args[0][1]
    assert len(saved_runs) == 1


@patch("pipewatch.cli_retention.save_history")
@patch("pipewatch.cli_retention.load_history")
@patch("pipewatch.cli_retention.load_config")
def test_prune_dry_run_does_not_save(mock_cfg, mock_load, mock_save):
    mock_cfg.return_value = _make_app_config()
    mock_load.return_value = [_run(1), _run(20)]
    runner = CliRunner()
    result = runner.invoke(retention_command, ["prune", "--max-age-days", "7", "--dry-run"])
    assert result.exit_code == 0
    assert "Dry run" in result.output
    mock_save.assert_not_called()


@patch("pipewatch.cli_retention.load_config")
def test_prune_requires_policy(mock_cfg):
    mock_cfg.return_value = _make_app_config()
    runner = CliRunner()
    result = runner.invoke(retention_command, ["prune"])
    assert result.exit_code != 0


@patch("pipewatch.cli_retention.save_history")
@patch("pipewatch.cli_retention.load_history")
@patch("pipewatch.cli_retention.load_config")
def test_prune_nothing_to_prune(mock_cfg, mock_load, mock_save):
    mock_cfg.return_value = _make_app_config()
    mock_load.return_value = [_run(1)]
    runner = CliRunner()
    result = runner.invoke(retention_command, ["prune", "--max-age-days", "30"])
    assert result.exit_code == 0
    assert "Nothing to prune" in result.output
    mock_save.assert_not_called()
