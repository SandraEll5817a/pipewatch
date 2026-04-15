"""Tests for pipewatch.history module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.history import (
    MAX_HISTORY_ENTRIES,
    clear_history,
    get_pipeline_history,
    load_history,
    record_run,
    save_history,
)


@pytest.fixture()
def history_file(tmp_path: Path) -> Path:
    return tmp_path / "test_history.json"


def test_load_history_returns_empty_list_when_missing(history_file: Path) -> None:
    assert load_history(history_file) == []


def test_load_history_returns_empty_list_on_corrupt_json(history_file: Path) -> None:
    history_file.write_text("not-json")
    assert load_history(history_file) == []


def test_save_and_load_roundtrip(history_file: Path) -> None:
    entries = [{"pipeline": "etl", "healthy": True, "violation_count": 0, "timestamp": "t"}]
    save_history(entries, history_file)
    loaded = load_history(history_file)
    assert loaded == entries


def test_save_history_caps_at_max_entries(history_file: Path) -> None:
    entries = [
        {"pipeline": "p", "healthy": True, "violation_count": 0, "timestamp": str(i)}
        for i in range(MAX_HISTORY_ENTRIES + 20)
    ]
    save_history(entries, history_file)
    loaded = load_history(history_file)
    assert len(loaded) == MAX_HISTORY_ENTRIES
    # should keep the most recent entries
    assert loaded[-1]["timestamp"] == str(MAX_HISTORY_ENTRIES + 19)


def test_record_run_appends_entry(history_file: Path) -> None:
    entry = record_run("orders", healthy=True, violation_count=0, path=history_file)
    assert entry["pipeline"] == "orders"
    assert entry["healthy"] is True
    assert entry["violation_count"] == 0
    assert "timestamp" in entry
    assert load_history(history_file) == [entry]


def test_record_run_accepts_custom_timestamp(history_file: Path) -> None:
    entry = record_run(
        "orders", healthy=False, violation_count=2, path=history_file, timestamp="2024-01-01T00:00:00+00:00"
    )
    assert entry["timestamp"] == "2024-01-01T00:00:00+00:00"


def test_get_pipeline_history_filters_by_name(history_file: Path) -> None:
    record_run("orders", healthy=True, violation_count=0, path=history_file, timestamp="t1")
    record_run("users", healthy=False, violation_count=1, path=history_file, timestamp="t2")
    record_run("orders", healthy=True, violation_count=0, path=history_file, timestamp="t3")

    result = get_pipeline_history("orders", path=history_file)
    assert len(result) == 2
    assert all(e["pipeline"] == "orders" for e in result)


def test_get_pipeline_history_respects_limit(history_file: Path) -> None:
    for i in range(5):
        record_run("orders", healthy=True, violation_count=0, path=history_file, timestamp=str(i))
    result = get_pipeline_history("orders", limit=3, path=history_file)
    assert len(result) == 3
    assert result[-1]["timestamp"] == "4"


def test_clear_history_removes_file(history_file: Path) -> None:
    record_run("orders", healthy=True, violation_count=0, path=history_file)
    assert history_file.exists()
    clear_history(history_file)
    assert not history_file.exists()


def test_clear_history_is_safe_when_no_file(history_file: Path) -> None:
    clear_history(history_file)  # should not raise
