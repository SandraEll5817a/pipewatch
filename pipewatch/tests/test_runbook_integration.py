import pytest
from pipewatch.runbook import (
    load_runbooks,
    save_runbooks,
    add_runbook,
    remove_runbook,
    get_runbook,
)


def test_add_and_retrieve(tmp_path):
    db = str(tmp_path / "rb.json")
    entries = load_runbooks(db)
    add_runbook(entries, "etl_daily", "Daily ETL Fix", "https://wiki/daily", "Check logs first")
    save_runbooks(entries, db)

    loaded = load_runbooks(db)
    assert len(loaded) == 1
    assert loaded[0].pipeline == "etl_daily"
    assert loaded[0].title == "Daily ETL Fix"
    assert loaded[0].notes == "Check logs first"


def test_multiple_pipelines(tmp_path):
    db = str(tmp_path / "rb.json")
    entries = []
    add_runbook(entries, "pipe_a", "Guide A", "https://a")
    add_runbook(entries, "pipe_b", "Guide B", "https://b")
    save_runbooks(entries, db)

    loaded = load_runbooks(db)
    assert len(loaded) == 2
    pipelines = {e.pipeline for e in loaded}
    assert pipelines == {"pipe_a", "pipe_b"}


def test_remove_entry(tmp_path):
    db = str(tmp_path / "rb.json")
    entries = []
    add_runbook(entries, "pipe_a", "Guide A", "https://a")
    add_runbook(entries, "pipe_a", "Guide B", "https://b")
    save_runbooks(entries, db)

    entries = load_runbooks(db)
    entries = remove_runbook(entries, "pipe_a", "Guide A")
    save_runbooks(entries, db)

    loaded = load_runbooks(db)
    assert len(loaded) == 1
    assert loaded[0].title == "Guide B"


def test_get_runbook_returns_match(tmp_path):
    db = str(tmp_path / "rb.json")
    entries = []
    add_runbook(entries, "pipe_x", "Triage", "https://triage")
    save_runbooks(entries, db)

    loaded = load_runbooks(db)
    entry = get_runbook(loaded, "pipe_x", "Triage")
    assert entry is not None
    assert entry.url == "https://triage"


def test_get_runbook_returns_none_when_missing(tmp_path):
    db = str(tmp_path / "rb.json")
    entries = load_runbooks(db)
    result = get_runbook(entries, "nonexistent", "No Title")
    assert result is None
