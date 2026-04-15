"""Trend analysis for pipeline run history."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.history import PipelineRun


@dataclass
class TrendSummary:
    pipeline: str
    sample_size: int
    avg_duration_seconds: Optional[float]
    avg_error_rate: Optional[float]
    avg_rows_processed: Optional[float]
    failure_rate: float  # fraction of runs that had violations
    trend_direction: str  # 'improving', 'degrading', 'stable', 'insufficient_data'

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "sample_size": self.sample_size,
            "avg_duration_seconds": self.avg_duration_seconds,
            "avg_error_rate": self.avg_error_rate,
            "avg_rows_processed": self.avg_rows_processed,
            "failure_rate": self.failure_rate,
            "trend_direction": self.trend_direction,
        }


def _safe_avg(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return round(sum(values) / len(values), 4)


def _compute_trend_direction(runs: List[PipelineRun]) -> str:
    """Compare failure rate of first half vs second half of runs."""
    if len(runs) < 4:
        return "insufficient_data"

    mid = len(runs) // 2
    first_half = runs[:mid]
    second_half = runs[mid:]

    first_fail = sum(1 for r in first_half if not r.healthy) / len(first_half)
    second_fail = sum(1 for r in second_half if not r.healthy) / len(second_half)

    delta = second_fail - first_fail
    if delta > 0.1:
        return "degrading"
    if delta < -0.1:
        return "improving"
    return "stable"


def analyze_trend(pipeline: str, runs: List[PipelineRun]) -> TrendSummary:
    """Compute a TrendSummary from a list of historical PipelineRun entries."""
    if not runs:
        return TrendSummary(
            pipeline=pipeline,
            sample_size=0,
            avg_duration_seconds=None,
            avg_error_rate=None,
            avg_rows_processed=None,
            failure_rate=0.0,
            trend_direction="insufficient_data",
        )

    durations = [r.duration_seconds for r in runs if r.duration_seconds is not None]
    error_rates = [r.error_rate for r in runs if r.error_rate is not None]
    rows = [r.rows_processed for r in runs if r.rows_processed is not None]
    failures = sum(1 for r in runs if not r.healthy)

    return TrendSummary(
        pipeline=pipeline,
        sample_size=len(runs),
        avg_duration_seconds=_safe_avg(durations),
        avg_error_rate=_safe_avg(error_rates),
        avg_rows_processed=_safe_avg(rows),
        failure_rate=round(failures / len(runs), 4),
        trend_direction=_compute_trend_direction(runs),
    )
