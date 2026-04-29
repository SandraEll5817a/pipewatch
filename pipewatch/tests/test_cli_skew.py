"""Tests for pipewatch.cli_skew."""
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from pipewatch.cli_skew import skew_command
from pipewatch.history import PipelineRun
from pipewatch.config import AppConfig, PipelineConfig, ThresholdConfig


def _dt(offset: float = 0.0) -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=offset)


def _run(name: str, offset: float) -> PipelineRun:
    return PipelineRun(
        pipeline=name,
        timestamp=_dt(offset),
        duration_seconds=5.0,
        rows_processed=50,
        error_rate=0.0,
        healthy=True,
    )


def _make_app_config() -> AppConfig:
    thresh = ThresholdConfig(max_duration_seconds=60, max_error_rate=0.1, min_rows_processed=1)
    pipeline = PipelineConfig(name="a", thresholds=thresh)
    return AppConfig(pipelines=[pipeline], webhook_url="http://example.com/hook")


@pytest.fixture()
def runner():
    return CliRunner()


def test_check_exits_zero_when_no_skew(runner):
    with patch("pipewatch.cli_skew.load_config", return_value=_make_app_config()), \
         patch("pipewatch.cli_skew.load_history", side_effect=[
             [_run("a", 0)], [_run("b", 10)]
         ]):
        result = runner.invoke(skew_command, ["check", "--threshold", "300", "a:b"])
    assert result.exit_code == 0
    assert "OK" in result.output


def test_check_exits_one_when_skewed(runner):
    with patch("pipewatch.cli_skew.load_config", return_value=_make_app_config()), \
         patch("pipewatch.cli_skew.load_history", side_effect=[
             [_run("a", 0)], [_run("b", 600)]
         ]):
        result = runner.invoke(skew_command, ["check", "--threshold", "300", "a:b"])
    assert result.exit_code == 1
    assert "SKEWED" in result.output


def test_check_exits_one_when_no_pairs(runner):
    with patch("pipewatch.cli_skew.load_config", return_value=_make_app_config()):
        result = runner.invoke(skew_command, ["check"])
    assert result.exit_code == 1


def test_check_exits_one_on_invalid_pair_format(runner):
    with patch("pipewatch.cli_skew.load_config", return_value=_make_app_config()):
        result = runner.invoke(skew_command, ["check", "ab"])
    assert result.exit_code == 1


def test_check_multiple_pairs_all_ok(runner):
    with patch("pipewatch.cli_skew.load_config", return_value=_make_app_config()), \
         patch("pipewatch.cli_skew.load_history", side_effect=[
             [_run("a", 0)], [_run("b", 5)],
             [_run("c", 0)], [_run("d", 5)],
         ]):
        result = runner.invoke(skew_command, ["check", "--threshold", "300", "a:b", "c:d"])
    assert result.exit_code == 0
    assert result.output.count("OK") == 2
