"""Tests for pipewatch.cli_watchdog."""
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from pipewatch.cli_watchdog import watchdog_command
from pipewatch.history import PipelineRun
from pipewatch.watchdog import WatchdogResult


NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _make_pipeline(name, interval):
    p = MagicMock()
    p.name = name
    p.watchdog_interval_minutes = interval
    return p


def _make_app_config(pipelines):
    cfg = MagicMock()
    cfg.pipelines = pipelines
    return cfg


@pytest.fixture
def runner():
    return CliRunner()


def test_check_exits_zero_when_all_on_schedule(runner):
    app_config = _make_app_config([_make_pipeline("etl", 60)])
    healthy_result = WatchdogResult("etl", 60, NOW, False, None)
    with patch("pipewatch.cli_watchdog.load_config", return_value=app_config), \
         patch("pipewatch.cli_watchdog.load_history", return_value=[]), \
         patch("pipewatch.cli_watchdog.check_all_watchdogs", return_value=[healthy_result]):
        result = runner.invoke(watchdog_command, ["check"])
    assert result.exit_code == 0
    assert "All pipelines" in result.output


def test_check_exits_one_when_stale(runner):
    app_config = _make_app_config([_make_pipeline("etl", 60)])
    stale_result = WatchdogResult("etl", 60, None, True, None)
    with patch("pipewatch.cli_watchdog.load_config", return_value=app_config), \
         patch("pipewatch.cli_watchdog.load_history", return_value=[]), \
         patch("pipewatch.cli_watchdog.check_all_watchdogs", return_value=[stale_result]):
        result = runner.invoke(watchdog_command, ["check"])
    assert result.exit_code == 1
    assert "overdue" in result.output


def test_check_exits_zero_when_no_watchdog_config(runner):
    p = MagicMock()
    p.name = "etl"
    p.watchdog_interval_minutes = None
    app_config = _make_app_config([p])
    with patch("pipewatch.cli_watchdog.load_config", return_value=app_config), \
         patch("pipewatch.cli_watchdog.load_history", return_value=[]):
        result = runner.invoke(watchdog_command, ["check"])
    assert result.exit_code == 0
    assert "No pipelines" in result.output
