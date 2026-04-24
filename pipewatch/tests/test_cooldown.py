"""Tests for pipewatch.cooldown."""

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.cooldown import (
    CooldownEntry,
    load_cooldowns,
    save_cooldowns,
    trigger_cooldown,
    is_in_cooldown,
    clear_cooldown,
    active_cooldowns,
)


def _dt(offset_seconds: int = 0) -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=offset_seconds)


@pytest.fixture
def cooldown_file(tmp_path):
    return tmp_path / "cooldowns.json"


def test_load_cooldowns_missing_file(cooldown_file):
    result = load_cooldowns(cooldown_file)
    assert result == {}


def test_load_cooldowns_corrupt_json(cooldown_file):
    cooldown_file.write_text("not valid json")
    result = load_cooldowns(cooldown_file)
    assert result == {}


def test_save_and_load_roundtrip(cooldown_file):
    entry = CooldownEntry(pipeline="etl_orders", triggered_at=_dt(), duration_seconds=300)
    save_cooldowns({"etl_orders": entry}, cooldown_file)
    loaded = load_cooldowns(cooldown_file)
    assert "etl_orders" in loaded
    assert loaded["etl_orders"].pipeline == "etl_orders"
    assert loaded["etl_orders"].duration_seconds == 300


def test_cooldown_entry_is_active_within_window():
    now = _dt()
    entry = CooldownEntry(pipeline="p", triggered_at=_dt(-100), duration_seconds=300)
    assert entry.is_active(now=now) is True


def test_cooldown_entry_inactive_after_window():
    now = _dt()
    entry = CooldownEntry(pipeline="p", triggered_at=_dt(-400), duration_seconds=300)
    assert entry.is_active(now=now) is False


def test_trigger_cooldown_creates_entry(cooldown_file):
    entry = trigger_cooldown("etl_users", 600, path=cooldown_file, now=_dt())
    assert entry.pipeline == "etl_users"
    assert entry.duration_seconds == 600
    loaded = load_cooldowns(cooldown_file)
    assert "etl_users" in loaded


def test_is_in_cooldown_true_when_active(cooldown_file):
    trigger_cooldown("etl_users", 600, path=cooldown_file, now=_dt())
    assert is_in_cooldown("etl_users", path=cooldown_file, now=_dt(100)) is True


def test_is_in_cooldown_false_when_expired(cooldown_file):
    trigger_cooldown("etl_users", 60, path=cooldown_file, now=_dt())
    assert is_in_cooldown("etl_users", path=cooldown_file, now=_dt(120)) is False


def test_is_in_cooldown_false_when_missing(cooldown_file):
    assert is_in_cooldown("nonexistent", path=cooldown_file) is False


def test_clear_cooldown_removes_entry(cooldown_file):
    trigger_cooldown("etl_orders", 300, path=cooldown_file, now=_dt())
    removed = clear_cooldown("etl_orders", path=cooldown_file)
    assert removed is True
    assert is_in_cooldown("etl_orders", path=cooldown_file) is False


def test_clear_cooldown_returns_false_when_missing(cooldown_file):
    assert clear_cooldown("ghost_pipeline", path=cooldown_file) is False


def test_active_cooldowns_returns_only_active(cooldown_file):
    trigger_cooldown("active_pipe", 600, path=cooldown_file, now=_dt())
    trigger_cooldown("expired_pipe", 10, path=cooldown_file, now=_dt(-100))
    results = active_cooldowns(path=cooldown_file, now=_dt())
    names = [e.pipeline for e in results]
    assert "active_pipe" in names
    assert "expired_pipe" not in names
