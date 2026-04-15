"""Tests for pipewatch.schedule."""

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, call

import pytest

from pipewatch.schedule import (
    ScheduledPipeline,
    SchedulerConfig,
    build_schedule,
    run_scheduler,
)


def _dt(offset_seconds: float = 0.0) -> datetime:
    base = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    return base + timedelta(seconds=offset_seconds)


# --- ScheduledPipeline ---

def test_is_due_when_never_run():
    sp = ScheduledPipeline(pipeline_name="etl", interval_seconds=60)
    assert sp.is_due(now=_dt()) is True


def test_is_due_before_interval_elapsed():
    sp = ScheduledPipeline(pipeline_name="etl", interval_seconds=60, last_run=_dt(0))
    assert sp.is_due(now=_dt(30)) is False


def test_is_due_after_interval_elapsed():
    sp = ScheduledPipeline(pipeline_name="etl", interval_seconds=60, last_run=_dt(0))
    assert sp.is_due(now=_dt(61)) is True


def test_is_due_exactly_at_interval():
    sp = ScheduledPipeline(pipeline_name="etl", interval_seconds=60, last_run=_dt(0))
    assert sp.is_due(now=_dt(60)) is True


def test_mark_run_updates_last_run():
    sp = ScheduledPipeline(pipeline_name="etl", interval_seconds=60)
    now = _dt(100)
    sp.mark_run(now=now)
    assert sp.last_run == now


# --- build_schedule ---

def test_build_schedule_creates_correct_count():
    result = build_schedule(["pipe_a", "pipe_b", "pipe_c"], interval_seconds=300)
    assert len(result) == 3


def test_build_schedule_assigns_names_and_interval():
    result = build_schedule(["pipe_a"], interval_seconds=120)
    assert result[0].pipeline_name == "pipe_a"
    assert result[0].interval_seconds == 120
    assert result[0].last_run is None


# --- run_scheduler ---

def test_run_scheduler_calls_check_for_due_pipelines():
    pipelines = build_schedule(["alpha", "beta"], interval_seconds=60)
    config = SchedulerConfig(pipelines=pipelines, tick_seconds=1, max_ticks=1)
    check_fn = MagicMock()
    sleep_fn = MagicMock()

    total = run_scheduler(config, check_fn=check_fn, sleep_fn=sleep_fn)

    assert total == 2
    check_fn.assert_any_call("alpha")
    check_fn.assert_any_call("beta")


def test_run_scheduler_skips_pipelines_not_due():
    sp = ScheduledPipeline("etl", interval_seconds=300, last_run=_dt(0))
    config = SchedulerConfig(pipelines=[sp], tick_seconds=1, max_ticks=1)
    check_fn = MagicMock()
    sleep_fn = MagicMock()

    # Monkeypatch _utcnow inside schedule to return a time before interval elapses
    import pipewatch.schedule as sched_mod
    original = sched_mod._utcnow
    sched_mod._utcnow = lambda: _dt(10)
    try:
        total = run_scheduler(config, check_fn=check_fn, sleep_fn=sleep_fn)
    finally:
        sched_mod._utcnow = original

    assert total == 0
    check_fn.assert_not_called()


def test_run_scheduler_respects_max_ticks():
    pipelines = build_schedule(["pipe"], interval_seconds=0)
    config = SchedulerConfig(pipelines=pipelines, tick_seconds=5, max_ticks=3)
    check_fn = MagicMock()
    sleep_fn = MagicMock()

    total = run_scheduler(config, check_fn=check_fn, sleep_fn=sleep_fn)

    assert total == 3
    assert sleep_fn.call_count == 2  # sleeps between ticks, not after last


def test_run_scheduler_returns_total_checks():
    pipelines = build_schedule(["a", "b"], interval_seconds=0)
    config = SchedulerConfig(pipelines=pipelines, tick_seconds=1, max_ticks=2)
    check_fn = MagicMock()
    sleep_fn = MagicMock()

    total = run_scheduler(config, check_fn=check_fn, sleep_fn=sleep_fn)

    assert total == 4  # 2 pipelines x 2 ticks
