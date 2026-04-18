"""Tests for pipewatch.rate_limit."""
from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.rate_limit import RateLimitPolicy, RateLimitState, RateLimiter


def _dt(offset_seconds: int = 0) -> datetime:
    base = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    return base + timedelta(seconds=offset_seconds)


def test_first_notification_always_allowed():
    policy = RateLimitPolicy(min_interval_seconds=60, max_per_hour=5)
    state = RateLimitState(pipeline="p1")
    assert state.allowed(policy, _dt()) is True


def test_blocked_within_min_interval():
    policy = RateLimitPolicy(min_interval_seconds=300, max_per_hour=10)
    state = RateLimitState(pipeline="p1")
    state.record_sent(_dt(0))
    assert state.allowed(policy, _dt(100)) is False


def test_allowed_after_min_interval():
    policy = RateLimitPolicy(min_interval_seconds=300, max_per_hour=10)
    state = RateLimitState(pipeline="p1")
    state.record_sent(_dt(0))
    assert state.allowed(policy, _dt(301)) is True


def test_blocked_when_max_per_hour_reached():
    policy = RateLimitPolicy(min_interval_seconds=1, max_per_hour=3)
    state = RateLimitState(pipeline="p1")
    for i in range(3):
        state.record_sent(_dt(i * 10))
    assert state.allowed(policy, _dt(35)) is False


def test_resets_after_hour_window():
    policy = RateLimitPolicy(min_interval_seconds=1, max_per_hour=3)
    state = RateLimitState(pipeline="p1")
    for i in range(3):
        state.record_sent(_dt(i * 10))
    # advance past 1 hour
    assert state.allowed(policy, _dt(3601)) is True


def test_rate_limiter_tracks_per_pipeline():
    limiter = RateLimiter(policy=RateLimitPolicy(min_interval_seconds=300, max_per_hour=10))
    limiter.record_sent("pipe_a", _dt(0))
    assert limiter.is_allowed("pipe_a", _dt(100)) is False
    assert limiter.is_allowed("pipe_b", _dt(100)) is True


def test_rate_limiter_allows_after_interval():
    limiter = RateLimiter(policy=RateLimitPolicy(min_interval_seconds=60, max_per_hour=10))
    limiter.record_sent("pipe_a", _dt(0))
    assert limiter.is_allowed("pipe_a", _dt(61)) is True


def test_rate_limiter_independent_states():
    limiter = RateLimiter(policy=RateLimitPolicy(min_interval_seconds=10, max_per_hour=2))
    limiter.record_sent("x", _dt(0))
    limiter.record_sent("x", _dt(15))
    assert limiter.is_allowed("x", _dt(30)) is False
    assert limiter.is_allowed("y", _dt(30)) is True
