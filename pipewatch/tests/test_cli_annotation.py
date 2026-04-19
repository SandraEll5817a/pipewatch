"""Tests for pipewatch.cli_annotation."""
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipewatch.annotation import add_annotation, load_annotations
from pipewatch.cli_annotation import annotation_command


@pytest.fixture
def runner():
    return CliRunner()


def test_add_creates_annotation(runner, tmp_path):
    f = str(tmp_path / "ann.json")
    result = runner.invoke(
        annotation_command, ["add", "etl", "run-1", "all good", "--author", "alice", "--file", f]
    )
    assert result.exit_code == 0
    assert "etl/run-1" in result.output
    anns = load_annotations(Path(f))
    assert len(anns) == 1
    assert anns[0].note == "all good"


def test_list_shows_annotations(runner, tmp_path):
    f = Path(tmp_path / "ann.json")
    add_annotation("etl", "run-1", "my note", author="bob", path=f)
    result = runner.invoke(annotation_command, ["list", "etl", "--file", str(f)])
    assert result.exit_code == 0
    assert "my note" in result.output
    assert "bob" in result.output


def test_list_no_annotations(runner, tmp_path):
    f = str(tmp_path / "ann.json")
    result = runner.invoke(annotation_command, ["list", "etl", "--file", f])
    assert result.exit_code == 0
    assert "No annotations" in result.output


def test_delete_removes_annotation(runner, tmp_path):
    f = Path(tmp_path / "ann.json")
    add_annotation("etl", "run-1", "note", path=f)
    result = runner.invoke(annotation_command, ["delete", "etl", "run-1", "--file", str(f)])
    assert result.exit_code == 0
    assert load_annotations(f) == []


def test_delete_missing_exits_one(runner, tmp_path):
    f = str(tmp_path / "ann.json")
    result = runner.invoke(annotation_command, ["delete", "etl", "ghost", "--file", f])
    assert result.exit_code == 1


def test_clear_removes_all_for_pipeline(runner, tmp_path):
    f = Path(tmp_path / "ann.json")
    add_annotation("etl", "run-1", "a", path=f)
    add_annotation("etl", "run-2", "b", path=f)
    add_annotation("other", "run-3", "c", path=f)
    result = runner.invoke(annotation_command, ["clear", "etl", "--file", str(f)])
    assert result.exit_code == 0
    remaining = load_annotations(f)
    assert all(a.pipeline == "other" for a in remaining)
    assert len(remaining) == 1
