"""Integration tests: throttle interacts with save/load across calls."""
from pathlib import Path
from pipewatch.throttle import (
    ThrottlePolicy,
    is_throttled,
    load_throttle,
    record_sent,
    save_throttle,
)

BASE = 1_000_000.0


def test_persist_and_check_throttle(tmp_path):
    path = tmp_path / "t.json"
    policy = ThrottlePolicy(window_seconds=600, max_per_window=1)

    state = load_throttle(path)
    assert not is_throttled("etl", "duration", policy, state, now=BASE)
    record_sent("etl", "duration", state, now=BASE)
    save_throttle(state, path)

    state2 = load_throttle(path)
    assert is_throttled("etl", "duration", policy, state2, now=BASE + 60)


def test_window_expiry_across_loads(tmp_path):
    path = tmp_path / "t.json"
    policy = ThrottlePolicy(window_seconds=300, max_per_window=1)

    state = load_throttle(path)
    record_sent("etl", "error_rate", state, now=BASE)
    save_throttle(state, path)

    state2 = load_throttle(path)
    assert not is_throttled("etl", "error_rate", policy, state2, now=BASE + 400)


def test_multiple_pipelines_independent(tmp_path):
    path = tmp_path / "t.json"
    policy = ThrottlePolicy(window_seconds=300, max_per_window=1)

    state = load_throttle(path)
    record_sent("pipe_a", "duration", state, now=BASE)
    save_throttle(state, path)

    state2 = load_throttle(path)
    assert is_throttled("pipe_a", "duration", policy, state2, now=BASE + 10)
    assert not is_throttled("pipe_b", "duration", policy, state2, now=BASE + 10)
