"""Tests for pipewatch.jitter."""
from __future__ import annotations

import datetime
from typing import List

import pytest

from pipewatch.history import PipelineRun
from pipewatch.jitter import detect_jitter, JitterResult


_TS = datetime.datetime(2024, 6, 1, 12, 0, 0)


def _run(pipeline: str, duration: float) -> PipelineRun:
    return PipelineRun(
        pipeline=pipeline,
        timestamp=_TS.isoformat(),
        healthy=True,
        duration_seconds=duration,
        error_rate=0.0,
        rows_processed=100,
        violations=[],
    )


def _runs(pipeline: str, durations: List[float]) -> List[PipelineRun]:
    return [_run(pipeline, d) for d in durations]


# ---------------------------------------------------------------------------
# insufficient data
# ---------------------------------------------------------------------------

def test_insufficient_data_when_empty():
    result = detect_jitter("pipe", [])
    assert result.is_jittery is False
    assert result.sample_size == 0
    assert "insufficient" in result.message


def test_insufficient_data_below_min_runs():
    result = detect_jitter("pipe", _runs("pipe", [10.0, 20.0, 15.0]), min_runs=4)
    assert result.is_jittery is False
    assert result.sample_size == 3
    assert "insufficient" in result.message


def test_exactly_min_runs_is_evaluated():
    # Very stable: all the same duration → cv == 0 → not jittery
    result = detect_jitter("pipe", _runs("pipe", [10.0, 10.0, 10.0, 10.0]), min_runs=4)
    assert result.sample_size == 4
    assert result.is_jittery is False


# ---------------------------------------------------------------------------
# stable pipelines
# ---------------------------------------------------------------------------

def test_stable_when_cv_below_threshold():
    # durations very close together → low CV
    durations = [100.0, 101.0, 99.0, 100.5, 99.5]
    result = detect_jitter("pipe", _runs("pipe", durations), threshold_cv=0.5)
    assert result.is_jittery is False
    assert result.cv < 0.5
    assert "stable" in result.message


def test_stable_to_dict_keys():
    result = detect_jitter("pipe", _runs("pipe", [10.0] * 5))
    d = result.to_dict()
    for key in ("pipeline", "sample_size", "avg_duration", "stddev", "cv",
                "threshold_cv", "is_jittery", "message"):
        assert key in d


# ---------------------------------------------------------------------------
# jittery pipelines
# ---------------------------------------------------------------------------

def test_jittery_when_cv_exceeds_threshold():
    # durations wildly varying → high CV
    durations = [10.0, 200.0, 5.0, 300.0, 15.0]
    result = detect_jitter("pipe", _runs("pipe", durations), threshold_cv=0.5)
    assert result.is_jittery is True
    assert result.cv > 0.5
    assert "variance" in result.message


def test_str_contains_status_jittery():
    durations = [10.0, 200.0, 5.0, 300.0, 15.0]
    result = detect_jitter("pipe", _runs("pipe", durations), threshold_cv=0.5)
    assert "JITTERY" in str(result)


def test_str_contains_status_stable():
    result = detect_jitter("pipe", _runs("pipe", [50.0] * 5))
    assert "STABLE" in str(result)


# ---------------------------------------------------------------------------
# pipeline filtering
# ---------------------------------------------------------------------------

def test_only_considers_matching_pipeline():
    mixed = _runs("pipe_a", [10.0, 200.0, 5.0, 300.0, 15.0]) + _runs("pipe_b", [50.0] * 5)
    result = detect_jitter("pipe_b", mixed, threshold_cv=0.5)
    assert result.is_jittery is False
    assert result.sample_size == 5


def test_custom_threshold_cv():
    durations = [90.0, 110.0, 95.0, 105.0, 100.0]  # mild variance
    # With a very tight threshold this should be jittery
    result_tight = detect_jitter("p", _runs("p", durations), threshold_cv=0.01)
    assert result_tight.is_jittery is True
    # With a relaxed threshold it should be stable
    result_relaxed = detect_jitter("p", _runs("p", durations), threshold_cv=0.5)
    assert result_relaxed.is_jittery is False
