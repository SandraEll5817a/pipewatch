"""Tests for pipewatch.history module."""

import json
from pathlib import Path

import pytest

from pipewatch.history import (
    PipelineRun,
    get_pipeline_history,
    load_history,
    record_run,
    save_history,
)


@pytest.fixture
def history_file(tmp_path: Path) -> Path:
    return tmp_path / "history.json"


def test_load_history_returns_empty_list_when_missing(history_file):
    assert load_history(history_file) == []


def test_load_history_returns_empty_list_on_corrupt_json(history_file):
    history_file.write_text("not-json{{{")
    assert load_history(history_file) == []


def test_save_and_load_roundtrip(history_file):
    runs = [
        PipelineRun(
            timestamp="2024-01-01T00:00:00+00:00",
            healthy=True,
            duration_seconds=5.0,
            error_rate=0.0,
            rows_processed=100.0,
            violations=[],
        )
    ]
    save_history(runs, history_file)
    loaded = load_history(history_file)
    assert len(loaded) == 1
    assert loaded[0].healthy is True
    assert loaded[0].duration_seconds == 5.0


def test_save_history_caps_at_max_entries(history_file):
    runs = [
        PipelineRun(
            timestamp=f"2024-01-{i:02d}T00:00:00+00:00",
            healthy=True,
            duration_seconds=float(i),
            error_rate=0.0,
            rows_processed=10.0,
            violations=[],
        )
        for i in range(1, 11)
    ]
    save_history(runs, history_file, max_entries=5)
    loaded = load_history(history_file)
    assert len(loaded) == 5
    # Most recent 5 kept
    assert loaded[0].duration_seconds == 6.0


def test_record_run_appends_to_history(history_file):
    record_run("pipe_a", healthy=True, duration_seconds=3.0, path=history_file)
    record_run("pipe_a", healthy=False, duration_seconds=7.0, path=history_file)
    runs = load_history(history_file)
    assert len(runs) == 2
    assert runs[1].healthy is False


def test_record_run_stores_violations(history_file):
    record_run(
        "pipe",
        healthy=False,
        violations=["duration exceeded 60s"],
        path=history_file,
    )
    runs = load_history(history_file)
    assert "duration exceeded 60s" in runs[0].violations


def test_get_pipeline_history_returns_all_runs(history_file):
    record_run("pipe", healthy=True, path=history_file)
    record_run("pipe", healthy=True, path=history_file)
    result = get_pipeline_history("pipe", path=history_file)
    assert len(result) == 2
