"""Tests for pipewatch.retry."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pipewatch.retry import RetryPolicy, RetryResult, with_retry


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _no_sleep(seconds: float) -> None:
    """Drop-in replacement for time.sleep that does nothing."""


def _make_flaky(fail_times: int):
    """Return a callable that raises ValueError *fail_times* then succeeds."""
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if calls["n"] <= fail_times:
            raise ValueError(f"fail #{calls['n']}")
        return "ok"

    return fn


# ---------------------------------------------------------------------------
# RetryPolicy.delays
# ---------------------------------------------------------------------------

def test_delays_length_is_max_attempts_minus_one():
    policy = RetryPolicy(max_attempts=4, backoff_seconds=1.0, backoff_multiplier=2.0)
    assert len(policy.delays()) == 3


def test_delays_respects_max_backoff():
    policy = RetryPolicy(
        max_attempts=5,
        backoff_seconds=10.0,
        backoff_multiplier=10.0,
        max_backoff_seconds=15.0,
    )
    for d in policy.delays():
        assert d <= 15.0


def test_delays_single_attempt_returns_empty():
    policy = RetryPolicy(max_attempts=1)
    assert policy.delays() == []


# ---------------------------------------------------------------------------
# with_retry — success paths
# ---------------------------------------------------------------------------

def test_success_on_first_attempt():
    result = with_retry(lambda: 42, RetryPolicy(max_attempts=3), _sleep_fn=_no_sleep)
    assert result.success is True
    assert result.value == 42
    assert result.attempts == 1


def test_success_after_retries():
    fn = _make_flaky(fail_times=2)
    policy = RetryPolicy(max_attempts=4, backoff_seconds=0.1)
    result = with_retry(fn, policy, _sleep_fn=_no_sleep)
    assert result.success is True
    assert result.attempts == 3


# ---------------------------------------------------------------------------
# with_retry — failure paths
# ---------------------------------------------------------------------------

def test_failure_after_all_attempts():
    fn = _make_flaky(fail_times=10)
    policy = RetryPolicy(max_attempts=3, backoff_seconds=0.1)
    result = with_retry(fn, policy, _sleep_fn=_no_sleep)
    assert result.success is False
    assert result.attempts == 3
    assert isinstance(result.last_exception, ValueError)


def test_sleep_called_between_attempts():
    sleep_mock = MagicMock()
    fn = _make_flaky(fail_times=2)
    policy = RetryPolicy(max_attempts=3, backoff_seconds=1.0)
    with_retry(fn, policy, _sleep_fn=sleep_mock)
    assert sleep_mock.call_count == 2


def test_only_specified_exceptions_retried():
    """A non-matching exception should propagate immediately without retrying."""
    sleep_mock = MagicMock()
    fn = _make_flaky(fail_times=10)
    policy = RetryPolicy(max_attempts=5, backoff_seconds=0.1, retryable_exceptions=(TypeError,))
    with pytest.raises(ValueError):
        with_retry(fn, policy, _sleep_fn=sleep_mock)
    sleep_mock.assert_not_called()
