"""Schedule-based pipeline check execution with cron-like interval support."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, List, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ScheduledPipeline:
    """Tracks the next scheduled run time for a pipeline."""

    pipeline_name: str
    interval_seconds: int
    last_run: Optional[datetime] = field(default=None)

    def is_due(self, now: Optional[datetime] = None) -> bool:
        """Return True if the pipeline is due for a check."""
        if self.last_run is None:
            return True
        now = now or _utcnow()
        elapsed = (now - self.last_run).total_seconds()
        return elapsed >= self.interval_seconds

    def mark_run(self, now: Optional[datetime] = None) -> None:
        """Record that the pipeline was just checked."""
        self.last_run = now or _utcnow()


@dataclass
class SchedulerConfig:
    """Configuration for the scheduler loop."""

    pipelines: List[ScheduledPipeline]
    tick_seconds: int = 10
    max_ticks: Optional[int] = None  # None means run forever


def build_schedule(pipeline_names: List[str], interval_seconds: int) -> List[ScheduledPipeline]:
    """Create a list of ScheduledPipeline entries from pipeline names."""
    return [
        ScheduledPipeline(pipeline_name=name, interval_seconds=interval_seconds)
        for name in pipeline_names
    ]


def run_scheduler(
    config: SchedulerConfig,
    check_fn: Callable[[str], None],
    sleep_fn: Callable[[float], None] = time.sleep,
) -> int:
    """Run the scheduler loop, calling check_fn for each due pipeline.

    Returns the total number of checks executed.
    """
    ticks = 0
    total_checks = 0

    while config.max_ticks is None or ticks < config.max_ticks:
        now = _utcnow()
        for scheduled in config.pipelines:
            if scheduled.is_due(now=now):
                check_fn(scheduled.pipeline_name)
                scheduled.mark_run(now=now)
                total_checks += 1
        ticks += 1
        if config.max_ticks is None or ticks < config.max_ticks:
            sleep_fn(config.tick_seconds)

    return total_checks
