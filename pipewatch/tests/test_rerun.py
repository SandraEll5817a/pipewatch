"""Tests for pipewatch.rerun."""
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.rerun import (
    RerunPolicy,
    RerunEntry,
    RerunResult,
    check_rerun,
    record_rerun,
    load_rerun_state,
    save_rerun_state,
)


def _dt(offset_seconds: int = 0) -> datetime:
    base = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    return base + timedelta(seconds=offset_seconds)


def _policy(max_reruns: int = 3, cooldown_seconds: int = 60) -> RerunPolicy:
    return RerunPolicy(max_reruns=max_reruns, cooldown_seconds=cooldown_seconds)


def test_first_attempt_always_allowed():
    result = check_rerun("pipe_a", _policy(), state={}, now=_dt())
    assert result.allowed is True
    assert result.attempt == 1
    assert "first" in result.reason


def test_rerun_allowed_after_cooldown():
    state = {"pipe_a": RerunEntry("pipe_a", attempt=1, last_rerun_at=_dt(0))}
    result = check_rerun("pipe_a", _policy(cooldown_seconds=60), state, now=_dt(61))
    assert result.allowed is True
    assert result.attempt == 2


def test_rerun_denied_within_cooldown():
    state = {"pipe_a": RerunEntry("pipe_a", attempt=1, last_rerun_at=_dt(0))}
    result = check_rerun("pipe_a", _policy(cooldown_seconds=60), state, now=_dt(30))
    assert result.allowed is False
    assert "cooldown" in result.reason


def test_rerun_denied_when_max_reached():
    state = {"pipe_a": RerunEntry("pipe_a", attempt=3, last_rerun_at=_dt(0))}
    result = check_rerun("pipe_a", _policy(max_reruns=3), state, now=_dt(120))
    assert result.allowed is False
    assert "max reruns" in result.reason


def test_record_rerun_increments_attempt():
    state = {}
    entry = record_rerun("pipe_a", state, now=_dt())
    assert entry.attempt == 1
    entry2 = record_rerun("pipe_a", state, now=_dt(90))
    assert entry2.attempt == 2


def test_record_rerun_updates_state_in_place():
    state = {}
    record_rerun("pipe_a", state, now=_dt())
    assert "pipe_a" in state
    assert state["pipe_a"].attempt == 1


def test_rerun_result_str_allowed():
    r = RerunResult(pipeline="pipe_a", allowed=True, reason="cooldown elapsed", attempt=2)
    assert "allowed" in str(r)
    assert "pipe_a" in str(r)


def test_rerun_result_str_denied():
    r = RerunResult(pipeline="pipe_a", allowed=False, reason="max reruns (3) reached", attempt=3)
    assert "denied" in str(r)


def test_save_and_load_roundtrip(tmp_path):
    path = tmp_path / "rerun.json"
    state = {}
    record_rerun("pipe_x", state, now=_dt())
    record_rerun("pipe_y", state, now=_dt(10))
    save_rerun_state(state, path)
    loaded = load_rerun_state(path)
    assert "pipe_x" in loaded
    assert "pipe_y" in loaded
    assert loaded["pipe_x"].attempt == 1


def test_load_rerun_state_missing_file(tmp_path):
    result = load_rerun_state(tmp_path / "nonexistent.json")
    assert result == {}


def test_load_rerun_state_corrupt_json(tmp_path):
    path = tmp_path / "rerun.json"
    path.write_text("not valid json")
    result = load_rerun_state(path)
    assert result == {}


def test_independent_pipelines_do_not_interfere():
    state = {}
    record_rerun("pipe_a", state, now=_dt())
    result_b = check_rerun("pipe_b", _policy(), state, now=_dt())
    assert result_b.allowed is True
    assert result_b.attempt == 1
