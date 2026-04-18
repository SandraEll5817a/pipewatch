"""Tests for pipewatch.cli_escalation."""
from unittest.mock import patch, MagicMock
import pytest
from click.testing import CliRunner

from pipewatch.cli_escalation import escalation_command
from pipewatch.escalation import EscalationState
from pipewatch.config import AppConfig, PipelineConfig, ThresholdConfig
from pipewatch.history import PipelineRun
from datetime import datetime, timezone


@pytest.fixture()
def runner():
    return CliRunner()


def _make_app(*names):
    pipelines = [
        PipelineConfig(name=n, source="s", thresholds=ThresholdConfig())
        for n in names
    ]
    return AppConfig(pipelines=pipelines)


def _run(healthy: bool) -> PipelineRun:
    return PipelineRun(
        pipeline="p",
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
        healthy=healthy,
        duration_seconds=1.0,
        error_rate=0.0,
        rows_processed=100,
    )


def test_check_exits_zero_when_no_escalation(runner):
    app = _make_app("p1")
    with patch("pipewatch.cli_escalation.load_config", return_value=app), \
         patch("pipewatch.cli_escalation.load_history", return_value=[_run(True), _run(True)]):
        result = runner.invoke(
            escalation_command,
            ["check", "--secondary-webhook", "https://example.com", "--threshold", "3"],
        )
    assert result.exit_code == 0


def test_check_exits_one_when_escalated(runner):
    app = _make_app("p1")
    failing_runs = [_run(False)] * 5
    with patch("pipewatch.cli_escalation.load_config", return_value=app), \
         patch("pipewatch.cli_escalation.load_history", return_value=failing_runs):
        result = runner.invoke(
            escalation_command,
            ["check", "--secondary-webhook", "https://example.com", "--threshold", "3"],
        )
    assert result.exit_code == 1
    assert "ESCALATED" in result.output


def test_check_output_contains_pipeline_name(runner):
    app = _make_app("my_etl")
    with patch("pipewatch.cli_escalation.load_config", return_value=app), \
         patch("pipewatch.cli_escalation.load_history", return_value=[]):
        result = runner.invoke(
            escalation_command,
            ["check", "--secondary-webhook", "https://example.com"],
        )
    assert "my_etl" in result.output
