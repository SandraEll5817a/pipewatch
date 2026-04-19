"""Tests for pipewatch.quota."""
from datetime import datetime, timezone

import pytest

from pipewatch.quota import (
    QuotaPolicy,
    QuotaState,
    check_quota,
    check_all_quotas,
)


def _dt(hour: int = 10, minute: int = 0, day: int = 1) -> datetime:
    return datetime(2024, 6, day, hour, minute, 0, tzinfo=timezone.utc)


def test_first_run_always_allowed():
    policy = QuotaPolicy(max_runs_per_hour=5, max_runs_per_day=20)
    state = QuotaState(pipeline="p1")
    result, _ = check_quota("p1", policy, state, now=_dt())
    assert result.allowed is True


def test_blocked_when_hourly_limit_reached():
    policy = QuotaPolicy(max_runs_per_hour=3, max_runs_per_day=100)
    state = QuotaState("p1", runs_this_hour=3, runs_today=3, hour_window="2024-06-01T10", day_window="2024-06-01")
    result, _ = check_quota("p1", policy, state, now=_dt(hour=10))
    assert result.allowed is False
    assert "hourly" in result.reason


def test_allowed_after_hour_resets():
    policy = QuotaPolicy(max_runs_per_hour=3, max_runs_per_day=100)
    state = QuotaState("p1", runs_this_hour=3, runs_today=10, hour_window="2024-06-01T09", day_window="2024-06-01")
    result, new_state = check_quota("p1", policy, state, now=_dt(hour=10))
    assert result.allowed is True
    assert new_state.runs_this_hour == 1


def test_blocked_when_daily_limit_reached():
    policy = QuotaPolicy(max_runs_per_hour=100, max_runs_per_day=10)
    state = QuotaState("p1", runs_this_hour=1, runs_today=10, hour_window="2024-06-01T10", day_window="2024-06-01")
    result, _ = check_quota("p1", policy, state, now=_dt())
    assert result.allowed is False
    assert "daily" in result.reason


def test_allowed_after_day_resets():
    policy = QuotaPolicy(max_runs_per_hour=100, max_runs_per_day=10)
    state = QuotaState("p1", runs_this_hour=5, runs_today=10, hour_window="2024-06-01T10", day_window="2024-06-01")
    result, new_state = check_quota("p1", policy, state, now=_dt(day=2))
    assert result.allowed is True
    assert new_state.runs_today == 1


def test_state_increments_on_allowed():
    policy = QuotaPolicy(max_runs_per_hour=10, max_runs_per_day=50)
    state = QuotaState("p1", runs_this_hour=2, runs_today=5, hour_window="2024-06-01T10", day_window="2024-06-01")
    _, new_state = check_quota("p1", policy, state, now=_dt())
    assert new_state.runs_this_hour == 3
    assert new_state.runs_today == 6


def test_quota_result_str_allowed():
    policy = QuotaPolicy()
    state = QuotaState(pipeline="etl")
    result, _ = check_quota("etl", policy, state, now=_dt())
    assert "allowed" in str(result)


def test_check_all_quotas_returns_one_per_pipeline():
    policy = QuotaPolicy(max_runs_per_hour=5, max_runs_per_day=20)
    states = {}
    results = check_all_quotas(["a", "b", "c"], policy, states, now=_dt())
    assert len(results) == 3
    assert all(r.allowed for r in results)


def test_check_all_quotas_updates_states():
    policy = QuotaPolicy(max_runs_per_hour=5, max_runs_per_day=20)
    states = {}
    check_all_quotas(["pipe1"], policy, states, now=_dt())
    assert "pipe1" in states
    assert states["pipe1"].runs_this_hour == 1
