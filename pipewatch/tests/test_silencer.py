"""Tests for pipewatch.silencer."""
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.silencer import (
    SilenceRule, load_silences, save_silences,
    add_silence, remove_silence, is_silenced,
)


def _dt(offset_hours: float) -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=offset_hours)


@pytest.fixture
def silence_file(tmp_path) -> Path:
    return tmp_path / "silences.json"


def test_load_silences_missing_file(silence_file):
    assert load_silences(silence_file) == []


def test_load_silences_corrupt_json(silence_file):
    silence_file.write_text("not json")
    assert load_silences(silence_file) == []


def test_save_and_load_roundtrip(silence_file):
    rule = SilenceRule(pipeline="etl_main", until=_dt(2).isoformat(), reason="maintenance")
    save_silences([rule], silence_file)
    loaded = load_silences(silence_file)
    assert len(loaded) == 1
    assert loaded[0].pipeline == "etl_main"
    assert loaded[0].reason == "maintenance"


def test_silence_rule_is_active_future(silence_file):
    rule = SilenceRule(pipeline="p", until=_dt(1).isoformat())
    assert rule.is_active() is True


def test_silence_rule_is_inactive_past(silence_file):
    rule = SilenceRule(pipeline="p", until=_dt(-1).isoformat())
    assert rule.is_active() is False


def test_add_silence_creates_rule(silence_file):
    rule = add_silence("etl_main", _dt(3), reason="deploy", path=silence_file)
    assert rule.pipeline == "etl_main"
    assert rule.reason == "deploy"
    rules = load_silences(silence_file)
    assert len(rules) == 1


def test_add_silence_replaces_existing(silence_file):
    add_silence("etl_main", _dt(1), path=silence_file)
    add_silence("etl_main", _dt(5), reason="extended", path=silence_file)
    rules = load_silences(silence_file)
    assert len(rules) == 1
    assert rules[0].reason == "extended"


def test_remove_silence_returns_true(silence_file):
    add_silence("etl_main", _dt(2), path=silence_file)
    assert remove_silence("etl_main", path=silence_file) is True
    assert load_silences(silence_file) == []


def test_remove_silence_returns_false_when_missing(silence_file):
    assert remove_silence("nonexistent", path=silence_file) is False


def test_is_silenced_active(silence_file):
    add_silence("etl_main", _dt(2), path=silence_file)
    assert is_silenced("etl_main", path=silence_file) is True


def test_is_silenced_expired(silence_file):
    rule = SilenceRule(pipeline="etl_main", until=_dt(-1).isoformat())
    save_silences([rule], silence_file)
    assert is_silenced("etl_main", path=silence_file) is False


def test_is_silenced_different_pipeline(silence_file):
    add_silence("etl_main", _dt(2), path=silence_file)
    assert is_silenced("other_pipeline", path=silence_file) is False
