"""Tests for pipewatch.annotation."""
import json
from pathlib import Path

import pytest

from pipewatch.annotation import (
    Annotation,
    add_annotation,
    delete_annotation,
    get_annotations,
    load_annotations,
    save_annotations,
)


@pytest.fixture
def ann_file(tmp_path) -> Path:
    return tmp_path / "annotations.json"


def test_load_annotations_missing_file(ann_file):
    assert load_annotations(ann_file) == []


def test_load_annotations_corrupt_json(ann_file):
    ann_file.write_text("not json")
    assert load_annotations(ann_file) == []


def test_save_and_load_roundtrip(ann_file):
    ann = Annotation(pipeline="etl", run_id="run-1", note="looks good", author="alice")
    save_annotations([ann], ann_file)
    loaded = load_annotations(ann_file)
    assert len(loaded) == 1
    assert loaded[0].pipeline == "etl"
    assert loaded[0].run_id == "run-1"
    assert loaded[0].note == "looks good"
    assert loaded[0].author == "alice"


def test_add_annotation_appends(ann_file):
    add_annotation("etl", "run-1", "first note", path=ann_file)
    add_annotation("etl", "run-2", "second note", path=ann_file)
    all_anns = load_annotations(ann_file)
    assert len(all_anns) == 2


def test_get_annotations_filters_by_pipeline(ann_file):
    add_annotation("etl", "run-1", "note a", path=ann_file)
    add_annotation("other", "run-2", "note b", path=ann_file)
    result = get_annotations("etl", path=ann_file)
    assert len(result) == 1
    assert result[0].pipeline == "etl"


def test_delete_annotation_removes_first_match(ann_file):
    add_annotation("etl", "run-1", "to delete", path=ann_file)
    add_annotation("etl", "run-2", "keep", path=ann_file)
    removed = delete_annotation("etl", "run-1", path=ann_file)
    assert removed is not None
    assert removed.run_id == "run-1"
    remaining = load_annotations(ann_file)
    assert len(remaining) == 1
    assert remaining[0].run_id == "run-2"


def test_delete_annotation_returns_none_when_not_found(ann_file):
    result = delete_annotation("etl", "missing", path=ann_file)
    assert result is None


def test_annotation_to_dict_keys():
    ann = Annotation(pipeline="p", run_id="r", note="n", author="bob")
    d = ann.to_dict()
    assert set(d.keys()) == {"pipeline", "run_id", "note", "author", "created_at"}
