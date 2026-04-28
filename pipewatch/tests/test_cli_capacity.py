"""Tests for pipewatch.cli_capacity."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pytest
from click.testing import CliRunner

from pipewatch.cli_capacity import capacity_command
from pipewatch.config import AppConfig, PipelineConfig, ThresholdConfig
from pipewatch.history import PipelineRun


def _make_app_config(pipelines) -> AppConfig:
    return AppConfig(pipelines=pipelines, webhook_url=None)


def _pipeline(name: str, max_rows: int = 1000) -> PipelineConfig:
    return PipelineConfig(
        name=name,
        thresholds=ThresholdConfig(
            max_duration_seconds=60,
            max_error_rate=0.05,
            min_rows_processed=0,
            max_rows=max_rows,
        ),
    )


def _run(name: str, rows: int) -> PipelineRun:
    return PipelineRun(
        pipeline=name,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat(),
        duration_seconds=10.0,
        rows_processed=rows,
        error_rate=0.0,
        healthy=True,
    )


@pytest.fixture()
def runner():
    return CliRunner()


def _write_config(tmp_path: Path, app_config: AppConfig) -> Path:
    import yaml  # type: ignore
    cfg_path = tmp_path / "pipewatch.yaml"
    data = {
        "pipelines": [
            {
                "name": p.name,
                "thresholds": {
                    "max_duration_seconds": p.thresholds.max_duration_seconds,
                    "max_error_rate": p.thresholds.max_error_rate,
                    "min_rows_processed": p.thresholds.min_rows_processed,
                    "max_rows": getattr(p.thresholds, "max_rows", 0),
                },
            }
            for p in app_config.pipelines
        ]
    }
    cfg_path.write_text(yaml.dump(data))
    return cfg_path


def _write_history(tmp_path: Path, runs: List[PipelineRun]) -> Path:
    hist_path = tmp_path / ".pipewatch_history.json"
    hist_path.write_text(json.dumps([r.__dict__ for r in runs]))
    return hist_path


def test_check_exits_zero_when_all_within_capacity(runner, tmp_path):
    app = _make_app_config([_pipeline("p1", max_rows=10_000)])
    cfg = _write_config(tmp_path, app)
    hist = _write_history(tmp_path, [_run("p1", 100)] * 3)
    result = runner.invoke(
        capacity_command, ["check", "--config", str(cfg), "--history", str(hist)]
    )
    assert result.exit_code == 0


def test_check_exits_one_when_at_risk(runner, tmp_path):
    app = _make_app_config([_pipeline("p1", max_rows=1000)])
    cfg = _write_config(tmp_path, app)
    hist = _write_history(tmp_path, [_run("p1", 950)] * 3)
    result = runner.invoke(
        capacity_command, ["check", "--config", str(cfg), "--history", str(hist)]
    )
    assert result.exit_code == 1


def test_check_skips_pipeline_without_max_rows(runner, tmp_path):
    p = PipelineConfig(
        name="p1",
        thresholds=ThresholdConfig(
            max_duration_seconds=60,
            max_error_rate=0.05,
            min_rows_processed=0,
        ),
    )
    app = _make_app_config([p])
    cfg = _write_config(tmp_path, app)
    hist = _write_history(tmp_path, [])
    result = runner.invoke(
        capacity_command, ["check", "--config", str(cfg), "--history", str(hist)]
    )
    assert "skipped" in result.output
    assert result.exit_code == 0


def test_check_unknown_pipeline_filter_exits_one(runner, tmp_path):
    app = _make_app_config([_pipeline("p1")])
    cfg = _write_config(tmp_path, app)
    hist = _write_history(tmp_path, [])
    result = runner.invoke(
        capacity_command,
        ["check", "--config", str(cfg), "--history", str(hist),
         "--pipeline", "ghost"],
    )
    assert result.exit_code == 1
