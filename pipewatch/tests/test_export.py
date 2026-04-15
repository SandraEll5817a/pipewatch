"""Tests for pipewatch.export module."""

import json
from pathlib import Path

import pytest

from pipewatch.metrics import ThresholdViolation
from pipewatch.runner import PipelineResult
from pipewatch.export import export_json, export_text, load_json_summary


def _healthy(name: str = "pipe_a") -> PipelineResult:
    return PipelineResult(pipeline_name=name, metrics=None, violations=[])


def _unhealthy(name: str = "pipe_b") -> PipelineResult:
    v = ThresholdViolation(metric="error_rate", threshold=0.05, actual=0.12)
    return PipelineResult(pipeline_name=name, metrics=None, violations=[v])


# --- export_json ---

def test_export_json_creates_file(tmp_path):
    out = tmp_path / "report.json"
    export_json([_healthy()], str(out))
    assert out.exists()


def test_export_json_valid_json(tmp_path):
    out = tmp_path / "report.json"
    export_json([_healthy(), _unhealthy()], str(out))
    data = json.loads(out.read_text())
    assert "total_pipelines" in data
    assert data["total_pipelines"] == 2


def test_export_json_returns_resolved_path(tmp_path):
    out = tmp_path / "sub" / "report.json"
    result = export_json([_healthy()], str(out))
    assert isinstance(result, Path)
    assert result.exists()


def test_export_json_creates_parent_dirs(tmp_path):
    out = tmp_path / "a" / "b" / "report.json"
    export_json([_healthy()], str(out))
    assert out.exists()


# --- export_text ---

def test_export_text_creates_file(tmp_path):
    out = tmp_path / "report.txt"
    export_text([_healthy()], str(out))
    assert out.exists()


def test_export_text_contains_pipeline_name(tmp_path):
    out = tmp_path / "report.txt"
    export_text([_healthy("my_pipe")], str(out))
    content = out.read_text()
    assert "my_pipe" in content


def test_export_text_contains_fail_label(tmp_path):
    out = tmp_path / "report.txt"
    export_text([_unhealthy("bad_pipe")], str(out))
    content = out.read_text()
    assert "[FAIL]" in content


# --- load_json_summary ---

def test_load_json_summary_returns_none_when_missing(tmp_path):
    result = load_json_summary(str(tmp_path / "nonexistent.json"))
    assert result is None


def test_load_json_summary_returns_none_on_corrupt_json(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("not valid json{{{")
    result = load_json_summary(str(bad))
    assert result is None


def test_load_json_summary_roundtrip(tmp_path):
    out = tmp_path / "report.json"
    export_json([_healthy("pipe_a"), _unhealthy("pipe_b")], str(out))
    data = load_json_summary(str(out))
    assert data is not None
    assert data["total_pipelines"] == 2
    assert data["healthy_count"] == 1
    assert data["unhealthy_count"] == 1
