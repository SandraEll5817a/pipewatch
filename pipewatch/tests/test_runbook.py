"""Tests for pipewatch.runbook."""
import json
import pytest
from pathlib import Path
from pipewatch.runbook import (
    RunbookEntry,
    load_runbooks,
    save_runbooks,
    add_runbook,
    remove_runbook,
    get_runbooks_for,
)


@pytest.fixture()
def rb_file(tmp_path: Path) -> Path:
    return tmp_path / "runbooks.json"


def _entry(pipeline="orders", title="Check logs", url="http://wiki/orders") -> RunbookEntry:
    return RunbookEntry(pipeline=pipeline, title=title, url=url, notes="See grafana", tags=["etl"])


def test_load_runbooks_missing_file(rb_file):
    assert load_runbooks(rb_file) == {}


def test_load_runbooks_corrupt_json(rb_file):
    rb_file.write_text("not json")
    assert load_runbooks(rb_file) == {}


def test_save_and_load_roundtrip(rb_file):
    e = _entry()
    save_runbooks([e], rb_file)
    loaded = load_runbooks(rb_file)
    assert "orders" in loaded
    assert loaded["orders"][0].title == "Check logs"
    assert loaded["orders"][0].url == "http://wiki/orders"


def test_entry_to_dict_keys():
    e = _entry()
    d = e.to_dict()
    assert set(d.keys()) == {"pipeline", "title", "url", "notes", "tags"}


def test_entry_from_dict_roundtrip():
    e = _entry()
    assert RunbookEntry.from_dict(e.to_dict()) == e


def test_add_runbook_appends(rb_file):
    add_runbook(_entry(pipeline="orders", title="A"), rb_file)
    add_runbook(_entry(pipeline="orders", title="B"), rb_file)
    entries = get_runbooks_for("orders", rb_file)
    assert len(entries) == 2
    titles = {e.title for e in entries}
    assert titles == {"A", "B"}


def test_add_runbook_multiple_pipelines(rb_file):
    add_runbook(_entry(pipeline="orders"), rb_file)
    add_runbook(_entry(pipeline="users"), rb_file)
    assert len(get_runbooks_for("orders", rb_file)) == 1
    assert len(get_runbooks_for("users", rb_file)) == 1


def test_remove_runbook_returns_count(rb_file):
    add_runbook(_entry(title="Remove me"), rb_file)
    add_runbook(_entry(title="Keep me"), rb_file)
    removed = remove_runbook("orders", "Remove me", rb_file)
    assert removed == 1
    remaining = get_runbooks_for("orders", rb_file)
    assert len(remaining) == 1
    assert remaining[0].title == "Keep me"


def test_remove_runbook_no_match_returns_zero(rb_file):
    add_runbook(_entry(), rb_file)
    assert remove_runbook("orders", "Nonexistent", rb_file) == 0


def test_get_runbooks_for_missing_pipeline(rb_file):
    assert get_runbooks_for("ghost", rb_file) == []
