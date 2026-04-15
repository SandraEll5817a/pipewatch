"""Tests for pipewatch.baseline."""
import json
from pathlib import Path

import pytest

from pipewatch.baseline import (
    BaselineComparison,
    PipelineBaseline,
    compare_to_baseline,
    load_baselines,
    save_baselines,
)


def _make_baseline(name: str = "etl") -> PipelineBaseline:
    return PipelineBaseline(
        pipeline_name=name,
        avg_duration_seconds=60.0,
        avg_error_rate=0.01,
        avg_row_count=1000.0,
    )


def test_load_baselines_missing_file(tmp_path):
    result = load_baselines(tmp_path / "nope.json")
    assert result == {}


def test_load_baselines_corrupt_json(tmp_path):
    p = tmp_path / "baseline.json"
    p.write_text("not json")
    assert load_baselines(p) == {}


def test_save_and_load_roundtrip(tmp_path):
    p = tmp_path / "baseline.json"
    baseline = _make_baseline()
    save_baselines({"etl": baseline}, path=p)
    loaded = load_baselines(p)
    assert "etl" in loaded
    assert loaded["etl"].avg_duration_seconds == 60.0
    assert loaded["etl"].avg_error_rate == 0.01


def test_save_multiple_baselines(tmp_path):
    p = tmp_path / "baseline.json"
    baselines = {
        "etl": _make_baseline("etl"),
        "ingest": _make_baseline("ingest"),
    }
    save_baselines(baselines, path=p)
    loaded = load_baselines(p)
    assert set(loaded.keys()) == {"etl", "ingest"}


def test_pipeline_baseline_to_dict_keys():
    b = _make_baseline()
    d = b.to_dict()
    assert set(d.keys()) == {
        "pipeline_name",
        "avg_duration_seconds",
        "avg_error_rate",
        "avg_row_count",
    }


def test_compare_no_regression_within_threshold():
    baseline = _make_baseline()
    result = compare_to_baseline(baseline, duration=62.0, error_rate=0.011, row_count=990.0)
    assert isinstance(result, BaselineComparison)
    assert result.regressed is False


def test_compare_duration_regression():
    baseline = _make_baseline()
    # 50% increase in duration — exceeds 20% threshold
    result = compare_to_baseline(baseline, duration=90.0, error_rate=0.01, row_count=1000.0)
    assert result.regressed is True
    assert result.duration_delta_pct == pytest.approx(50.0)


def test_compare_error_rate_regression():
    baseline = _make_baseline()
    # error rate triples
    result = compare_to_baseline(baseline, duration=60.0, error_rate=0.03, row_count=1000.0)
    assert result.regressed is True


def test_compare_zero_baseline_duration_returns_none_delta():
    baseline = PipelineBaseline(
        pipeline_name="z",
        avg_duration_seconds=0.0,
        avg_error_rate=0.0,
        avg_row_count=500.0,
    )
    result = compare_to_baseline(baseline, duration=10.0, error_rate=0.0, row_count=500.0)
    assert result.duration_delta_pct is None
    assert result.error_rate_delta_pct is None
    assert result.regressed is False


def test_compare_custom_threshold():
    baseline = _make_baseline()
    # 25% duration increase — below 30% custom threshold
    result = compare_to_baseline(
        baseline, duration=75.0, error_rate=0.01, row_count=1000.0,
        regression_threshold_pct=30.0,
    )
    assert result.regressed is False
