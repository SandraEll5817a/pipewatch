"""Tests for pipewatch.healthscore."""
import pytest
from pipewatch.healthscore import compute_health_score, score_all, _grade
from pipewatch.history import PipelineRun
from datetime import datetime, timezone


def _run(healthy: bool, duration: float = 10.0) -> PipelineRun:
    return PipelineRun(
        pipeline="pipe",
        timestamp=datetime.now(timezone.utc).isoformat(),
        healthy=healthy,
        duration_seconds=duration,
        violation_count=0 if healthy else 1,
    )


def test_empty_runs_returns_zero_score():
    result = compute_health_score("pipe", [])
    assert result.score == 0.0
    assert result.grade == "F"
    assert result.total_runs == 0


def test_all_healthy_short_runs_score_near_100():
    runs = [_run(True, 5.0) for _ in range(10)]
    result = compute_health_score("pipe", runs)
    assert result.score > 90.0
    assert result.grade == "A"
    assert result.failed_runs == 0


def test_all_failing_score_is_zero():
    runs = [_run(False, 10.0) for _ in range(5)]
    result = compute_health_score("pipe", runs)
    assert result.score == 0.0
    assert result.grade == "F"
    assert result.failed_runs == 5


def test_half_failing_lowers_score():
    runs = [_run(True)] * 5 + [_run(False)] * 5
    result = compute_health_score("pipe", runs)
    assert 40.0 <= result.score <= 55.0


def test_long_duration_applies_penalty():
    fast = compute_health_score("pipe", [_run(True, 1.0)] * 5)
    slow = compute_health_score("pipe", [_run(True, 290.0)] * 5)
    assert fast.score > slow.score


def test_to_dict_keys():
    runs = [_run(True)]
    d = compute_health_score("pipe", runs).to_dict()
    assert set(d.keys()) == {"pipeline", "score", "total_runs", "failed_runs", "avg_duration_seconds", "grade"}


def test_str_contains_pipeline_name():
    runs = [_run(True)]
    s = str(compute_health_score("my_pipe", runs))
    assert "my_pipe" in s


def test_score_all_returns_one_entry_per_pipeline():
    history = {
        "a": [_run(True)],
        "b": [_run(False)],
    }
    scores = score_all(history)
    names = {s.pipeline for s in scores}
    assert names == {"a", "b"}


@pytest.mark.parametrize("score,expected", [
    (95, "A"), (80, "B"), (65, "C"), (45, "D"), (30, "F")
])
def test_grade_boundaries(score, expected):
    assert _grade(score) == expected
