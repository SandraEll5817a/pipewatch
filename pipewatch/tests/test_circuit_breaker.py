"""Tests for pipewatch.circuit_breaker."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from pipewatch.circuit_breaker import (
    CircuitBreakerPolicy,
    CircuitState,
    is_open,
    load_circuit_states,
    record_failure,
    record_success,
    save_circuit_states,
)


def _dt(offset_seconds: int = 0) -> datetime:
    return datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=offset_seconds)


def _policy(threshold: int = 3, timeout: int = 300) -> CircuitBreakerPolicy:
    return CircuitBreakerPolicy(failure_threshold=threshold, recovery_timeout=timeout)


def test_new_state_is_closed():
    s = CircuitState(pipeline="etl")
    assert s.state == "closed"
    assert s.consecutive_failures == 0


def test_circuit_closed_is_not_open():
    s = CircuitState(pipeline="etl")
    assert is_open(s, _policy()) is False


def test_record_failure_increments_count():
    s = CircuitState(pipeline="etl")
    record_failure(s, _policy(threshold=5))
    assert s.consecutive_failures == 1
    assert s.state == "closed"


def test_circuit_opens_at_threshold():
    s = CircuitState(pipeline="etl")
    policy = _policy(threshold=3)
    for _ in range(3):
        record_failure(s, policy)
    assert s.state == "open"
    assert s.opened_at is not None


def test_open_circuit_blocks_calls():
    now = _dt()
    s = CircuitState(pipeline="etl", consecutive_failures=3, state="open", opened_at=now.isoformat())
    with patch("pipewatch.circuit_breaker._utcnow", return_value=_dt(10)):
        assert is_open(s, _policy(timeout=300)) is True


def test_circuit_transitions_to_half_open_after_timeout():
    opened = _dt(0)
    s = CircuitState(pipeline="etl", consecutive_failures=3, state="open", opened_at=opened.isoformat())
    with patch("pipewatch.circuit_breaker._utcnow", return_value=_dt(301)):
        result = is_open(s, _policy(timeout=300))
    assert result is False
    assert s.state == "half-open"


def test_record_success_closes_circuit():
    s = CircuitState(pipeline="etl", consecutive_failures=3, state="open", opened_at=_dt().isoformat())
    record_success(s)
    assert s.state == "closed"
    assert s.consecutive_failures == 0
    assert s.opened_at is None


def test_save_and_load_roundtrip(tmp_path):
    path = tmp_path / "circuit.json"
    states = {"etl": CircuitState(pipeline="etl", consecutive_failures=2, state="closed")}
    save_circuit_states(states, path)
    loaded = load_circuit_states(path)
    assert "etl" in loaded
    assert loaded["etl"].consecutive_failures == 2


def test_load_missing_file_returns_empty(tmp_path):
    result = load_circuit_states(tmp_path / "missing.json")
    assert result == {}


def test_load_corrupt_json_returns_empty(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{bad json")
    result = load_circuit_states(path)
    assert result == {}


def test_state_roundtrip_via_dict():
    s = CircuitState(pipeline="p", consecutive_failures=1, state="open", opened_at="2024-01-01T00:00:00+00:00")
    restored = CircuitState.from_dict(s.to_dict())
    assert restored.pipeline == s.pipeline
    assert restored.consecutive_failures == s.consecutive_failures
    assert restored.state == s.state
