"""Tests for pipewatch.correlation."""
from datetime import datetime, timezone, timedelta
from pipewatch.history import PipelineRun
from pipewatch.correlation import correlate_failures, CorrelationResult


def _run(pipeline: str, healthy: bool, offset_minutes: int = 0) -> PipelineRun:
    ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(
        minutes=offset_minutes
    )
    return PipelineRun(
        pipeline=pipeline,
        timestamp=ts,
        healthy=healthy,
        duration_seconds=10.0,
        error_rate=0.0,
        rows_processed=100,
    )


def test_empty_runs_returns_empty():
    assert correlate_failures([]) == []


def test_no_failures_returns_empty():
    runs = [_run("a", True), _run("b", True)]
    assert correlate_failures(runs) == []


def test_single_pipeline_no_pairs():
    runs = [_run("a", False), _run("a", False, offset_minutes=10)]
    assert correlate_failures(runs) == []


def test_co_failure_detected():
    # Both fail in same 5-min bucket
    runs = [
        _run("alpha", False, 0),
        _run("beta", False, 1),
        _run("alpha", False, 10),
        _run("beta", False, 11),
    ]
    results = correlate_failures(runs, min_rate=0.0)
    assert len(results) == 1
    r = results[0]
    assert r.pipeline_a == "alpha"
    assert r.pipeline_b == "beta"
    assert r.co_failures == 2


def test_min_rate_filters_low_correlation():
    runs = [
        _run("alpha", False, 0),
        _run("beta", False, 1),
        _run("alpha", False, 10),
        _run("beta", True, 11),
    ]
    results = correlate_failures(runs, min_rate=0.9)
    assert results == []


def test_rate_calculation():
    runs = [
        _run("x", False, 0),
        _run("y", False, 1),
        _run("x", False, 10),
        _run("y", True, 11),
        _run("x", False, 20),
        _run("y", True, 21),
    ]
    results = correlate_failures(runs, min_rate=0.0)
    assert len(results) == 1
    r = results[0]
    assert r.co_failures == 1
    assert r.total_windows > 0


def test_to_dict_keys():
    r = CorrelationResult("a", "b", 3, 5)
    d = r.to_dict()
    assert set(d.keys()) == {"pipeline_a", "pipeline_b", "co_failures", "total_windows", "rate"}
    assert d["rate"] == 0.6


def test_str_representation():
    r = CorrelationResult("pipe1", "pipe2", 2, 4)
    s = str(r)
    assert "pipe1" in s
    assert "pipe2" in s
    assert "50%" in s


def test_results_sorted_by_rate_descending():
    runs = [
        _run("a", False, 0), _run("b", False, 1), _run("c", False, 2),
        _run("a", False, 10), _run("b", False, 11), _run("c", True, 12),
        _run("a", False, 20), _run("b", False, 21), _run("c", True, 22),
    ]
    results = correlate_failures(runs, min_rate=0.0)
    rates = [r.rate for r in results]
    assert rates == sorted(rates, reverse=True)
