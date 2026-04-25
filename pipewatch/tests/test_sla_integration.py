"""Integration tests for SLA tracking with real history persistence."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.history import PipelineRun, save_history, load_history
from pipewatch.sla import SLAPolicy, check_sla


def _run(
    pipeline: str = "pipe_x",
    duration: float = 10.0,
    error_rate: float = 0.0,
    rows: int = 1000,
    healthy: bool = True,
) -> PipelineRun:
    return PipelineRun(
        pipeline=pipeline,
        ran_at=datetime(2024, 3, 15, 9, 0, 0, tzinfo=timezone.utc),
        duration_seconds=duration,
        error_rate=error_rate,
        rows_processed=rows,
        healthy=healthy,
    )


def test_save_and_check_sla_compliant(tmp_path):
    path = str(tmp_path / "history.json")
    runs = [_run(duration=30.0, error_rate=0.01, rows=500)]
    save_history(path, runs)

    loaded = load_history(path)
    policy = SLAPolicy(max_duration_seconds=60.0, max_error_rate=0.05, min_rows_processed=100)
    result = check_sla("pipe_x", loaded, policy)

    assert result.compliant is True
    assert result.violations == []


def test_save_and_check_sla_breach(tmp_path):
    path = str(tmp_path / "history.json")
    runs = [_run(duration=200.0, error_rate=0.5, rows=10)]
    save_history(path, runs)

    loaded = load_history(path)
    policy = SLAPolicy(max_duration_seconds=60.0, max_error_rate=0.05, min_rows_processed=100)
    result = check_sla("pipe_x", loaded, policy)

    assert result.compliant is False
    assert len(result.violations) == 3


def test_multiple_pipelines_isolated(tmp_path):
    path = str(tmp_path / "history.json")
    runs = [
        _run(pipeline="alpha", duration=10.0),
        _run(pipeline="beta", duration=500.0),
    ]
    save_history(path, runs)
    loaded = load_history(path)

    policy = SLAPolicy(max_duration_seconds=60.0)

    alpha_runs = [r for r in loaded if r.pipeline == "alpha"]
    beta_runs = [r for r in loaded if r.pipeline == "beta"]

    assert check_sla("alpha", alpha_runs, policy).compliant is True
    assert check_sla("beta", beta_runs, policy).compliant is False


def test_result_to_dict_roundtrip(tmp_path):
    runs = [_run(duration=999.0)]
    policy = SLAPolicy(max_duration_seconds=60.0)
    result = check_sla("pipe_x", runs, policy)
    d = result.to_dict()

    out = tmp_path / "sla_result.json"
    out.write_text(json.dumps(d))
    loaded = json.loads(out.read_text())

    assert loaded["pipeline"] == "pipe_x"
    assert loaded["compliant"] is False
    assert loaded["violations"][0]["metric"] == "duration_seconds"
