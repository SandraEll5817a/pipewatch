"""Tests for pipewatch.snapshot."""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.snapshot import (
    MetricSnapshot,
    capture_snapshot,
    load_snapshots,
    save_snapshots,
    add_snapshot,
)

_NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


def _snap(**kwargs) -> MetricSnapshot:
    defaults = dict(pipeline="etl", duration_seconds=10.0, error_rate=0.0, rows_processed=100, healthy=True)
    defaults.update(kwargs)
    return capture_snapshot(now=_NOW, **defaults)


def test_capture_snapshot_sets_fields():
    s = _snap(pipeline="p1", duration_seconds=5.5, error_rate=0.1, rows_processed=50, healthy=False)
    assert s.pipeline == "p1"
    assert s.duration_seconds == 5.5
    assert s.error_rate == 0.1
    assert s.rows_processed == 50
    assert s.healthy is False
    assert "2024-06-01" in s.captured_at


def test_snapshot_to_dict_keys():
    s = _snap()
    d = s.to_dict()
    assert set(d.keys()) == {"pipeline", "captured_at", "duration_seconds", "error_rate", "rows_processed", "healthy"}


def test_snapshot_roundtrip():
    s = _snap(pipeline="roundtrip", rows_processed=999)
    assert MetricSnapshot.from_dict(s.to_dict()) == s


def test_load_snapshots_missing_file(tmp_path):
    assert load_snapshots(tmp_path / "missing.json") == []


def test_load_snapshots_corrupt_json(tmp_path):
    p = tmp_path / "snap.json"
    p.write_text("not json")
    assert load_snapshots(p) == []


def test_save_and_load_roundtrip(tmp_path):
    p = tmp_path / "snap.json"
    snaps = [_snap(pipeline=f"p{i}") for i in range(3)]
    save_snapshots(p, snaps)
    loaded = load_snapshots(p)
    assert len(loaded) == 3
    assert loaded[0].pipeline == "p0"


def test_save_caps_at_max_entries(tmp_path):
    p = tmp_path / "snap.json"
    snaps = [_snap(pipeline=f"p{i}") for i in range(10)]
    save_snapshots(p, snaps, max_entries=5)
    loaded = load_snapshots(p)
    assert len(loaded) == 5
    assert loaded[0].pipeline == "p5"


def test_add_snapshot_appends(tmp_path):
    p = tmp_path / "snap.json"
    add_snapshot(p, _snap(pipeline="a"))
    add_snapshot(p, _snap(pipeline="b"))
    loaded = load_snapshots(p)
    assert [s.pipeline for s in loaded] == ["a", "b"]
