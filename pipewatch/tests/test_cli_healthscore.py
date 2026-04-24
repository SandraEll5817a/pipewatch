import json
from unittest.mock import patch
from click.testing import CliRunner
from pipewatch.cli_healthscore import healthscore_command
from pipewatch.config import AppConfig, PipelineConfig, ThresholdConfig
from pipewatch.healthscore import HealthScore
from pipewatch.history import PipelineRun
from datetime import datetime, timezone


def _make_app_config(names):
    pipelines = [
        PipelineConfig(
            name=n,
            source="test",
            thresholds=ThresholdConfig(),
        )
        for n in names
    ]
    return AppConfig(pipelines=pipelines, webhook_url="http://example.com")


def _run(name, healthy=True):
    return PipelineRun(
        pipeline=name,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat(),
        duration_seconds=10.0,
        rows_processed=100,
        error_rate=0.0 if healthy else 1.0,
        healthy=healthy,
    )


@pytest.fixture
def runner():
    return CliRunner()


import pytest


def test_show_exits_zero_when_all_high_scores(runner):
    app = _make_app_config(["pipe_a"])
    runs = [_run("pipe_a", healthy=True) for _ in range(10)]
    score = HealthScore(score=95.0, run_count=10, grade="A")

    with patch("pipewatch.cli_healthscore.load_config", return_value=app), \
         patch("pipewatch.cli_healthscore.load_history", return_value=runs), \
         patch("pipewatch.cli_healthscore.compute_health_score", return_value=score):
        result = runner.invoke(healthscore_command, ["show"])

    assert result.exit_code == 0
    assert "pipe_a" in result.output
    assert "A" in result.output


def test_show_exits_one_when_low_score(runner):
    app = _make_app_config(["pipe_b"])
    runs = [_run("pipe_b", healthy=False) for _ in range(10)]
    score = HealthScore(score=40.0, run_count=10, grade="D")

    with patch("pipewatch.cli_healthscore.load_config", return_value=app), \
         patch("pipewatch.cli_healthscore.load_history", return_value=runs), \
         patch("pipewatch.cli_healthscore.compute_health_score", return_value=score):
        result = runner.invoke(healthscore_command, ["show"])

    assert result.exit_code == 1
    assert "[!]" in result.output


def test_show_filters_by_pipeline_name(runner):
    app = _make_app_config(["pipe_a", "pipe_b"])
    score = HealthScore(score=90.0, run_count=5, grade="A")

    with patch("pipewatch.cli_healthscore.load_config", return_value=app), \
         patch("pipewatch.cli_healthscore.load_history", return_value=[]), \
         patch("pipewatch.cli_healthscore.compute_health_score", return_value=score):
        result = runner.invoke(healthscore_command, ["show", "--pipeline", "pipe_a"])

    assert result.exit_code == 0
    assert "pipe_a" in result.output
    assert "pipe_b" not in result.output


def test_show_exits_one_for_unknown_pipeline(runner):
    app = _make_app_config(["pipe_a"])

    with patch("pipewatch.cli_healthscore.load_config", return_value=app):
        result = runner.invoke(healthscore_command, ["show", "--pipeline", "missing"])

    assert result.exit_code == 1
    assert "not found" in result.output
