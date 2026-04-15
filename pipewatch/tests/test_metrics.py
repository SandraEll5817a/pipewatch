"""Tests for pipewatch.metrics threshold evaluation."""

from datetime import datetime

import pytest

from pipewatch.config import PipelineConfig, ThresholdConfig
from pipewatch.metrics import PipelineMetrics, ThresholdViolation, evaluate_thresholds


FIXED_TS = datetime(2024, 1, 1, 12, 0, 0)


def make_config(
    max_duration: float | None = None,
    max_error_rate: float | None = None,
    min_rows: int | None = None,
) -> PipelineConfig:
    return PipelineConfig(
        name="test_pipeline",
        thresholds=ThresholdConfig(
            max_duration_seconds=max_duration,
            max_error_rate=max_error_rate,
            min_rows_processed=min_rows,
        ),
    )


def make_metrics(**overrides) -> PipelineMetrics:
    defaults = dict(
        pipeline_name="test_pipeline",
        duration_seconds=30.0,
        error_rate=0.01,
        rows_processed=1000,
        timestamp=FIXED_TS,
    )
    defaults.update(overrides)
    return PipelineMetrics(**defaults)


def test_no_violations_when_within_thresholds():
    config = make_config(max_duration=60.0, max_error_rate=0.05, min_rows=500)
    metrics = make_metrics()
    assert evaluate_thresholds(metrics, config) == []


def test_duration_violation():
    config = make_config(max_duration=20.0)
    metrics = make_metrics(duration_seconds=25.0)
    violations = evaluate_thresholds(metrics, config)
    assert len(violations) == 1
    v = violations[0]
    assert v.metric == "duration_seconds"
    assert v.actual == 25.0
    assert v.threshold == 20.0


def test_error_rate_violation():
    config = make_config(max_error_rate=0.02)
    metrics = make_metrics(error_rate=0.05)
    violations = evaluate_thresholds(metrics, config)
    assert len(violations) == 1
    assert violations[0].metric == "error_rate"


def test_rows_processed_violation():
    config = make_config(min_rows=2000)
    metrics = make_metrics(rows_processed=500)
    violations = evaluate_thresholds(metrics, config)
    assert len(violations) == 1
    v = violations[0]
    assert v.metric == "rows_processed"
    assert v.actual == 500.0
    assert v.threshold == 2000.0


def test_multiple_violations():
    config = make_config(max_duration=10.0, max_error_rate=0.0, min_rows=5000)
    metrics = make_metrics(duration_seconds=50.0, error_rate=0.1, rows_processed=100)
    violations = evaluate_thresholds(metrics, config)
    assert len(violations) == 3
    metrics_names = {v.metric for v in violations}
    assert metrics_names == {"duration_seconds", "error_rate", "rows_processed"}


def test_violation_str_representation():
    v = ThresholdViolation(
        pipeline_name="my_pipe",
        metric="error_rate",
        threshold=0.02,
        actual=0.07,
        timestamp=FIXED_TS,
    )
    assert "my_pipe" in str(v)
    assert "error_rate" in str(v)


def test_no_violation_at_exact_threshold():
    config = make_config(max_duration=30.0, max_error_rate=0.01, min_rows=1000)
    metrics = make_metrics()  # values equal thresholds exactly
    assert evaluate_thresholds(metrics, config) == []
