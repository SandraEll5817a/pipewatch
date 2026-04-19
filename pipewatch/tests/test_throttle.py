"""Tests for pipewatch.throttle."""
import pytest
from pipewatch.throttle import (
    ThrottlePolicy,
    ThrottleEntry,
    is_throttled,
    record_sent,
    load_throttle,
    save_throttle,
)

BASE_TIME = 1_000_000.0


def _state():
    return {}


def test_not_throttled_when_no_entry():
    policy = ThrottlePolicy(window_seconds=300, max_per_window=1)
    assert not is_throttled("pipe", "duration", policy, _state(), now=BASE_TIME)


def test_throttled_after_single_send_with_max_one():
    policy = ThrottlePolicy(window_seconds=300, max_per_window=1)
    state = _state()
    record_sent("pipe", "duration", state, now=BASE_TIME)
    assert is_throttled("pipe", "duration", policy, state, now=BASE_TIME + 10)


def test_not_throttled_after_window_expires():
    policy = ThrottlePolicy(window_seconds=300, max_per_window=1)
    state = _state()
    record_sent("pipe", "duration", state, now=BASE_TIME)
    assert not is_throttled("pipe", "duration", policy, state, now=BASE_TIME + 301)


def test_throttled_only_for_same_key():
    policy = ThrottlePolicy(window_seconds=300, max_per_window=1)
    state = _state()
    record_sent("pipe", "duration", state, now=BASE_TIME)
    assert not is_throttled("pipe", "error_rate", policy, state, now=BASE_TIME + 10)


def test_max_per_window_two_allows_second():
    policy = ThrottlePolicy(window_seconds=300, max_per_window=2)
    state = _state()
    record_sent("pipe", "duration", state, now=BASE_TIME)
    assert not is_throttled("pipe", "duration", policy, state, now=BASE_TIME + 10)
    record_sent("pipe", "duration", state, now=BASE_TIME + 10)
    assert is_throttled("pipe", "duration", policy, state, now=BASE_TIME + 20)


def test_record_sent_increments_count():
    state = _state()
    record_sent("pipe", "duration", state, now=BASE_TIME)
    record_sent("pipe", "duration", state, now=BASE_TIME + 5)
    key = "pipe::duration"
    assert state[key].count == 2


def test_save_and_load_roundtrip(tmp_path):
    path = tmp_path / "throttle.json"
    state = _state()
    record_sent("pipe", "duration", state, now=BASE_TIME)
    save_throttle(state, path)
    loaded = load_throttle(path)
    assert "pipe::duration" in loaded
    assert loaded["pipe::duration"].pipeline == "pipe"


def test_load_throttle_missing_file(tmp_path):
    result = load_throttle(tmp_path / "nope.json")
    assert result == {}


def test_load_throttle_corrupt_json(tmp_path):
    path = tmp_path / "throttle.json"
    path.write_text("not json")
    assert load_throttle(path) == {}
