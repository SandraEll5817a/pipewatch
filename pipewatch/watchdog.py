"""Watchdog: detect pipelines that haven't run within their expected interval."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.history import PipelineRun


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class WatchdogResult:
    pipeline_name: str
    expected_interval_minutes: int
    last_run_at: Optional[datetime]
    stale: bool
    minutes_overdue: Optional[float]

    def __str__(self) -> str:
        if not self.stale:
            return f"{self.pipeline_name}: OK"
        if self.last_run_at is None:
            return f"{self.pipeline_name}: STALE (never run)"
        return (
            f"{self.pipeline_name}: STALE "
            f"({self.minutes_overdue:.1f} min overdue)"
        )


def check_watchdog(
    pipeline_name: str,
    interval_minutes: int,
    runs: List[PipelineRun],
    now: Optional[datetime] = None,
) -> WatchdogResult:
    """Return a WatchdogResult indicating whether the pipeline is overdue."""
    if now is None:
        now = _utcnow()

    pipeline_runs = [r for r in runs if r.pipeline_name == pipeline_name]
    if not pipeline_runs:
        return WatchdogResult(
            pipeline_name=pipeline_name,
            expected_interval_minutes=interval_minutes,
            last_run_at=None,
            stale=True,
            minutes_overdue=None,
        )

    last_run = max(pipeline_runs, key=lambda r: r.ran_at)
    elapsed = (now - last_run.ran_at).total_seconds() / 60.0
    overdue = elapsed > interval_minutes
    return WatchdogResult(
        pipeline_name=pipeline_name,
        expected_interval_minutes=interval_minutes,
        last_run_at=last_run.ran_at,
        stale=overdue,
        minutes_overdue=round(elapsed - interval_minutes, 2) if overdue else None,
    )


def check_all_watchdogs(
    watchdog_configs: List[dict],
    runs: List[PipelineRun],
    now: Optional[datetime] = None,
) -> List[WatchdogResult]:
    """Check all pipelines defined in watchdog_configs list of {name, interval_minutes}."""
    return [
        check_watchdog(c["name"], c["interval_minutes"], runs, now)
        for c in watchdog_configs
    ]
