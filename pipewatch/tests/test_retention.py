"""Tests for pipewatch.retention."""
from datetime import datetime, timezone, timedelta
from pipewatch.history import PipelineRun
from pipewatch.retention import RetentionPolicy, apply_retention, apply_retention_all


def _run(days_ago: float, healthy: bool = True) -> PipelineRun:
    ts = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return PipelineRun(pipeline="pipe", timestamp=ts, healthy=healthy, violations=[])


def test_no_policy_keeps_all():
    runs = [_run(1), _run(5), _run(30)]
    kept, result = apply_retention("pipe", runs, RetentionPolicy())
    assert len(kept) == 3
    assert result.pruned_count == 0


def test_max_age_prunes_old_runs():
    runs = [_run(1), _run(8), _run(20)]
    kept, result = apply_retention("pipe", runs, RetentionPolicy(max_age_days=7))
    assert len(kept) == 1
    assert result.pruned_count == 2
    assert result.kept_count == 1


def test_max_runs_keeps_most_recent():
    runs = [_run(1), _run(2), _run(3), _run(4)]
    kept, result = apply_retention("pipe", runs, RetentionPolicy(max_runs=2))
    assert len(kept) == 2
    assert result.pruned_count == 2
    # most recent kept
    for r in kept:
        assert r.timestamp >= runs[2].timestamp


def test_both_policies_applied():
    runs = [_run(0.5), _run(1), _run(2), _run(10)]
    policy = RetentionPolicy(max_age_days=5, max_runs=2)
    kept, result = apply_retention("pipe", runs, policy)
    assert len(kept) == 2
    assert result.kept_count == 2


def test_result_str_contains_pipeline_name():
    runs = [_run(1)]
    _, result = apply_retention("my_pipe", runs, RetentionPolicy(max_runs=1))
    assert "my_pipe" in str(result)


def test_apply_retention_all_handles_multiple_pipelines():
    history = {
        "a": [_run(1), _run(20)],
        "b": [_run(2), _run(3)],
    }
    pruned, results = apply_retention_all(history, RetentionPolicy(max_age_days=7))
    assert len(pruned["a"]) == 1
    assert len(pruned["b"]) == 2
    assert len(results) == 2


def test_empty_runs_returns_empty():
    kept, result = apply_retention("pipe", [], RetentionPolicy(max_age_days=7))
    assert kept == []
    assert result.pruned_count == 0
    assert result.kept_count == 0
