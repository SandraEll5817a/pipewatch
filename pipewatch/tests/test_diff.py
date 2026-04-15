"""Tests for pipewatch.diff."""

import pytest

from pipewatch.diff import MetricDiff, compute_diff
from pipewatch.history import PipelineRun


def _run(
    pipeline: str = "etl_main",
    run_id: str = "r1",
    duration: float = 10.0,
    error_rate: float = 0.01,
    rows: int = 1000,
) -> PipelineRun:
    return PipelineRun(
        run_id=run_id,
        pipeline=pipeline,
        timestamp="2024-01-01T00:00:00Z",
        healthy=True,
        duration_seconds=duration,
        error_rate=error_rate,
        rows_processed=rows,
        violations=[],
    )


def test_compute_diff_no_change():
    base = _run(run_id="r1")
    curr = _run(run_id="r2")
    diff = compute_diff(base, curr)
    assert diff.duration_delta == pytest.approx(0.0)
    assert diff.error_rate_delta == pytest.approx(0.0)
    assert diff.rows_processed_delta == 0


def test_compute_diff_slower_run():
    base = _run(run_id="r1", duration=10.0)
    curr = _run(run_id="r2", duration=15.0)
    diff = compute_diff(base, curr)
    assert diff.duration_delta == pytest.approx(5.0)


def test_compute_diff_improved_error_rate():
    base = _run(run_id="r1", error_rate=0.05)
    curr = _run(run_id="r2", error_rate=0.02)
    diff = compute_diff(base, curr)
    assert diff.error_rate_delta == pytest.approx(-0.03)


def test_compute_diff_rows_delta():
    base = _run(run_id="r1", rows=500)
    curr = _run(run_id="r2", rows=750)
    diff = compute_diff(base, curr)
    assert diff.rows_processed_delta == 250


def test_compute_diff_stores_run_ids():
    base = _run(run_id="base-001")
    curr = _run(run_id="curr-002")
    diff = compute_diff(base, curr)
    assert diff.baseline_run_id == "base-001"
    assert diff.current_run_id == "curr-002"


def test_compute_diff_pipeline_mismatch_raises():
    base = _run(pipeline="pipe_a")
    curr = _run(pipeline="pipe_b")
    with pytest.raises(ValueError, match="Pipeline mismatch"):
        compute_diff(base, curr)


def test_to_dict_keys():
    diff = compute_diff(_run(run_id="r1"), _run(run_id="r2"))
    d = diff.to_dict()
    assert set(d.keys()) == {
        "pipeline",
        "duration_delta",
        "error_rate_delta",
        "rows_processed_delta",
        "baseline_run_id",
        "current_run_id",
    }


def test_str_contains_pipeline_name():
    diff = compute_diff(_run(run_id="r1", duration=10.0), _run(run_id="r2", duration=12.0))
    s = str(diff)
    assert "etl_main" in s
    assert "+2.00s" in s


def test_none_fields_when_values_missing():
    base = PipelineRun(
        run_id="r1",
        pipeline="etl_main",
        timestamp="2024-01-01T00:00:00Z",
        healthy=True,
        duration_seconds=None,
        error_rate=None,
        rows_processed=None,
        violations=[],
    )
    curr = _run(run_id="r2")
    diff = compute_diff(base, curr)
    assert diff.duration_delta is None
    assert diff.error_rate_delta is None
    assert diff.rows_processed_delta is None
