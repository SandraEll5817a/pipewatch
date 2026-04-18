"""Tests for pipewatch.checkpoint."""
import json
import pytest
from pathlib import Path
from pipewatch.checkpoint import (
    Checkpoint,
    load_checkpoints,
    save_checkpoints,
    record_checkpoint,
    get_checkpoint,
    list_checkpoints,
)


@pytest.fixture
def cp_file(tmp_path) -> Path:
    return tmp_path / "checkpoints.json"


def test_load_checkpoints_missing_file(cp_file):
    assert load_checkpoints(cp_file) == {}


def test_load_checkpoints_corrupt_json(cp_file):
    cp_file.write_text("not json")
    assert load_checkpoints(cp_file) == {}


def test_save_and_load_roundtrip(cp_file):
    cp = Checkpoint(pipeline="etl", last_good_at="2024-01-01T00:00:00+00:00",
                    duration_seconds=12.5, rows_processed=1000)
    save_checkpoints({"etl": cp}, cp_file)
    loaded = load_checkpoints(cp_file)
    assert "etl" in loaded
    assert loaded["etl"].pipeline == "etl"
    assert loaded["etl"].duration_seconds == 12.5
    assert loaded["etl"].rows_processed == 1000


def test_record_checkpoint_creates_entry(cp_file):
    cp = record_checkpoint("ingest", 5.0, 500, path=cp_file)
    assert cp.pipeline == "ingest"
    assert cp.duration_seconds == 5.0
    assert cp.rows_processed == 500
    assert cp.last_good_at != ""


def test_record_checkpoint_overwrites_previous(cp_file):
    record_checkpoint("ingest", 5.0, 500, path=cp_file)
    record_checkpoint("ingest", 9.0, 900, path=cp_file)
    cp = get_checkpoint("ingest", path=cp_file)
    assert cp.duration_seconds == 9.0
    assert cp.rows_processed == 900


def test_get_checkpoint_missing_returns_none(cp_file):
    assert get_checkpoint("nonexistent", path=cp_file) is None


def test_list_checkpoints_returns_all(cp_file):
    record_checkpoint("a", 1.0, 10, path=cp_file)
    record_checkpoint("b", 2.0, 20, path=cp_file)
    items = list_checkpoints(cp_file)
    names = {c.pipeline for c in items}
    assert names == {"a", "b"}


def test_checkpoint_to_dict_keys():
    cp = Checkpoint(pipeline="p", last_good_at="ts", duration_seconds=1.0, rows_processed=5)
    d = cp.to_dict()
    assert set(d.keys()) == {"pipeline", "last_good_at", "duration_seconds", "rows_processed"}


def test_checkpoint_from_dict_roundtrip():
    original = Checkpoint(pipeline="x", last_good_at="2024-06-01T12:00:00+00:00",
                          duration_seconds=3.3, rows_processed=42)
    restored = Checkpoint.from_dict(original.to_dict())
    assert restored == original
