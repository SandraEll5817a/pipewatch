"""Tests for pipewatch.escalation."""
from datetime import datetime, timezone, timedelta
import pytest

from pipewatch.escalation import (
    EscalationPolicy,
    EscalationState,
    should_escalate,
    update_state,
)


def _dt(hour: int = 12) -> datetime:
    return datetime(2024, 1, 1, hour, 0, 0, tzinfo=timezone.utc)


def _policy(**kwargs) -> EscalationPolicy:
    return EscalationPolicy(secondary_webhook="https://hooks.example.com/secondary", **kwargs)


def _state(failures: int = 0, last_escalated=None) -> EscalationState:
    return EscalationState(pipeline="my_pipeline", consecutive_failures=failures, last_escalated=last_escalated)


def test_no_escalation_below_threshold():
    result = should_escalate(_state(failures=2), _policy(failure_threshold=3), now=_dt())
    assert not result.escalated
    assert "2 consecutive" in result.reason


def test_escalation_at_threshold():
    result = should_escalate(_state(failures=3), _policy(failure_threshold=3), now=_dt())
    assert result.escalated
    assert result.secondary_webhook == "https://hooks.example.com/secondary"


def test_escalation_above_threshold():
    result = should_escalate(_state(failures=5), _policy(failure_threshold=3), now=_dt())
    assert result.escalated


def test_cooldown_suppresses_repeat_escalation():
    last = _dt(10)
    now = _dt(10) + timedelta(minutes=30)
    result = should_escalate(_state(failures=5, last_escalated=last), _policy(cooldown_minutes=60), now=now)
    assert not result.escalated
    assert "cooldown" in result.reason


def test_escalation_allowed_after_cooldown():
    last = _dt(8)
    now = _dt(10)  # 2 hours later
    result = should_escalate(_state(failures=5, last_escalated=last), _policy(cooldown_minutes=60), now=now)
    assert result.escalated


def test_update_state_increments_on_failure():
    s = _state(failures=2)
    new = update_state(s, healthy=False)
    assert new.consecutive_failures == 3
    assert new.pipeline == "my_pipeline"


def test_update_state_resets_on_healthy():
    s = _state(failures=5)
    new = update_state(s, healthy=True)
    assert new.consecutive_failures == 0


def test_str_escalated():
    result = should_escalate(_state(failures=4), _policy(failure_threshold=3), now=_dt())
    assert str(result).startswith("[ESCALATED]")


def test_str_not_escalated():
    result = should_escalate(_state(failures=1), _policy(failure_threshold=3), now=_dt())
    assert str(result).startswith("[OK]")
