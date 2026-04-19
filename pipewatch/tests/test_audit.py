"""Tests for pipewatch.audit."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.audit import (
    AuditEvent,
    append_audit,
    filter_audit,
    load_audit,
)


@pytest.fixture
def audit_file(tmp_path: Path) -> Path:
    return tmp_path / "audit.jsonl"


def _event(pipeline: str = "etl", event_type: str = "check", healthy: bool = True) -> AuditEvent:
    return AuditEvent(
        pipeline=pipeline,
        event_type=event_type,
        message="ok",
        healthy=healthy,
        timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    )


def test_load_audit_missing_file(audit_file: Path) -> None:
    assert load_audit(audit_file) == []


def test_load_audit_corrupt_returns_empty(audit_file: Path) -> None:
    audit_file.write_text("not json\n")
    assert load_audit(audit_file) == []


def test_append_and_load_roundtrip(audit_file: Path) -> None:
    e = _event()
    append_audit(e, audit_file)
    loaded = load_audit(audit_file)
    assert len(loaded) == 1
    assert loaded[0].pipeline == "etl"
    assert loaded[0].event_type == "check"
    assert loaded[0].healthy is True


def test_append_multiple_events(audit_file: Path) -> None:
    append_audit(_event("a"), audit_file)
    append_audit(_event("b"), audit_file)
    loaded = load_audit(audit_file)
    assert len(loaded) == 2
    assert {e.pipeline for e in loaded} == {"a", "b"}


def test_audit_caps_at_max_entries(audit_file: Path, monkeypatch) -> None:
    import pipewatch.audit as audit_mod
    monkeypatch.setattr(audit_mod, "MAX_ENTRIES", 3)
    for i in range(5):
        append_audit(_event(pipeline=str(i)), audit_file)
    loaded = load_audit(audit_file)
    assert len(loaded) == 3
    assert loaded[-1].pipeline == "4"


def test_filter_by_pipeline(audit_file: Path) -> None:
    events = [_event("pipe_a"), _event("pipe_b"), _event("pipe_a", event_type="alert")]
    result = filter_audit(events, pipeline="pipe_a")
    assert all(e.pipeline == "pipe_a" for e in result)
    assert len(result) == 2


def test_filter_by_event_type(audit_file: Path) -> None:
    events = [_event(event_type="check"), _event(event_type="alert"), _event(event_type="check")]
    result = filter_audit(events, event_type="alert")
    assert len(result) == 1
    assert result[0].event_type == "alert"


def test_to_dict_keys() -> None:
    e = _event()
    d = e.to_dict()
    assert set(d.keys()) == {"pipeline", "event_type", "message", "healthy", "timestamp"}


def test_from_dict_roundtrip() -> None:
    e = _event()
    restored = AuditEvent.from_dict(e.to_dict())
    assert restored.pipeline == e.pipeline
    assert restored.healthy == e.healthy
    assert restored.timestamp == e.timestamp
