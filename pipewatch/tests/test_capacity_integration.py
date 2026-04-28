"""Integration tests: capacity check using real history persistence."""
from __future__ import annotations

from datetime import datetime, timezone

from pipewatch.capacity import check_capacity, MIN_RUNS
from pipewatch.history import PipelineRun, save_history, load_history


def _run(name: str, rows: int) -> PipelineRun:
    return PipelineRun(
        pipeline=name,
        timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat(),
        duration_seconds=5.0,
        rows_processed=rows,
        error_rate=0.0,
        healthy=True,
    )


def test_save_and_check_capacity_compliant(tmp_path):
    hist_path = str(tmp_path / "hist.json")
    runs = [_run("pipe", 200)] * MIN_RUNS
    save_history(hist_path, "pipe", runs)
    loaded = load_history(hist_path, "pipe")
    result = check_capacity("pipe", loaded, ceiling=10_000)
    assert not result.at_risk


def test_save_and_check_capacity_at_risk(tmp_path):
    hist_path = str(tmp_path / "hist.json")
    runs = [_run("pipe", 9_200)] * MIN_RUNS
    save_history(hist_path, "pipe", runs)
    loaded = load_history(hist_path, "pipe")
    result = check_capacity("pipe", loaded, ceiling=10_000)
    assert result.at_risk


def test_multiple_pipelines_isolated(tmp_path):
    hist_path = str(tmp_path / "hist.json")
    runs_a = [_run("a", 100)] * MIN_RUNS
    runs_b = [_run("b", 9_500)] * MIN_RUNS
    save_history(hist_path, "a", runs_a)
    save_history(hist_path, "b", runs_b)

    result_a = check_capacity("a", load_history(hist_path, "a"), ceiling=10_000)
    result_b = check_capacity("b", load_history(hist_path, "b"), ceiling=10_000)

    assert not result_a.at_risk
    assert result_b.at_risk


def test_to_dict_roundtrip(tmp_path):
    hist_path = str(tmp_path / "hist.json")
    runs = [_run("pipe", 500)] * MIN_RUNS
    save_history(hist_path, "pipe", runs)
    loaded = load_history(hist_path, "pipe")
    result = check_capacity("pipe", loaded, ceiling=1_000)
    d = result.to_dict()
    assert d["pipeline"] == "pipe"
    assert isinstance(d["utilization"], float)
    assert isinstance(d["at_risk"], bool)
