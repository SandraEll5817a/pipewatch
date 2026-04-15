"""Tests for pipewatch.alert_policy."""

from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.alert_policy import AlertPolicy, AlertState, AlertPolicyManager


def _dt(offset_seconds: float = 0) -> datetime:
    base = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    return base + timedelta(seconds=offset_seconds)


# ---------------------------------------------------------------------------
# AlertState.should_alert
# ---------------------------------------------------------------------------

def test_first_alert_always_allowed():
    state = AlertState(pipeline_name="pipe1")
    policy = AlertPolicy(cooldown_seconds=300)
    assert state.should_alert(policy, now=_dt()) is True


def test_alert_suppressed_within_cooldown():
    state = AlertState(pipeline_name="pipe1")
    policy = AlertPolicy(cooldown_seconds=300)
    now = _dt()
    state.record_alert(now=now)
    assert state.should_alert(policy, now=_dt(100)) is False


def test_alert_allowed_after_cooldown():
    state = AlertState(pipeline_name="pipe1")
    policy = AlertPolicy(cooldown_seconds=300)
    now = _dt()
    state.record_alert(now=now)
    assert state.should_alert(policy, now=_dt(301)) is True


def test_max_alerts_per_hour_suppresses_after_limit():
    state = AlertState(pipeline_name="pipe1")
    policy = AlertPolicy(cooldown_seconds=0, max_alerts_per_hour=2)
    t = _dt()
    state.record_alert(now=t)
    state.record_alert(now=_dt(1))
    assert state.should_alert(policy, now=_dt(2)) is False


def test_max_alerts_per_hour_zero_means_unlimited():
    state = AlertState(pipeline_name="pipe1")
    policy = AlertPolicy(cooldown_seconds=0, max_alerts_per_hour=0)
    for i in range(20):
        state.record_alert(now=_dt(i))
    assert state.should_alert(policy, now=_dt(21)) is True


def test_hour_window_resets_after_3600_seconds():
    state = AlertState(pipeline_name="pipe1")
    policy = AlertPolicy(cooldown_seconds=0, max_alerts_per_hour=1)
    state.record_alert(now=_dt(0))
    # Still suppressed before window resets
    assert state.should_alert(policy, now=_dt(3599)) is False
    # Allowed after window resets
    assert state.should_alert(policy, now=_dt(3601)) is True


# ---------------------------------------------------------------------------
# AlertPolicyManager
# ---------------------------------------------------------------------------

def test_manager_tracks_separate_pipelines():
    policy = AlertPolicy(cooldown_seconds=300)
    manager = AlertPolicyManager(policy)
    t = _dt()
    manager.record_alert("pipe_a", now=t)
    # pipe_a is suppressed, pipe_b is not
    assert manager.should_alert("pipe_a", now=_dt(100)) is False
    assert manager.should_alert("pipe_b", now=_dt(100)) is True


def test_manager_allows_after_cooldown():
    policy = AlertPolicy(cooldown_seconds=60)
    manager = AlertPolicyManager(policy)
    manager.record_alert("pipe1", now=_dt(0))
    assert manager.should_alert("pipe1", now=_dt(61)) is True


def test_manager_creates_state_lazily():
    policy = AlertPolicy()
    manager = AlertPolicyManager(policy)
    assert manager.should_alert("new_pipe") is True
