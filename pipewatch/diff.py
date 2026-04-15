"""Compute and represent metric diffs between two pipeline runs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pipewatch.history import PipelineRun


@dataclass
class MetricDiff:
    """Difference between two pipeline run metrics."""

    pipeline: str
    duration_delta: Optional[float]  # seconds; positive = slower
    error_rate_delta: Optional[float]  # fraction; positive = more errors
    rows_processed_delta: Optional[int]  # positive = more rows
    baseline_run_id: Optional[str]
    current_run_id: Optional[str]

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "duration_delta": self.duration_delta,
            "error_rate_delta": self.error_rate_delta,
            "rows_processed_delta": self.rows_processed_delta,
            "baseline_run_id": self.baseline_run_id,
            "current_run_id": self.current_run_id,
        }

    def __str__(self) -> str:
        parts = [f"[{self.pipeline}]"]
        if self.duration_delta is not None:
            sign = "+" if self.duration_delta >= 0 else ""
            parts.append(f"duration {sign}{self.duration_delta:.2f}s")
        if self.error_rate_delta is not None:
            sign = "+" if self.error_rate_delta >= 0 else ""
            parts.append(f"error_rate {sign}{self.error_rate_delta:.4f}")
        if self.rows_processed_delta is not None:
            sign = "+" if self.rows_processed_delta >= 0 else ""
            parts.append(f"rows {sign}{self.rows_processed_delta}")
        return "  ".join(parts)


def _delta(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """Return b - a, or None if either value is missing."""
    if a is None or b is None:
        return None
    return b - a


def _delta_int(a: Optional[int], b: Optional[int]) -> Optional[int]:
    if a is None or b is None:
        return None
    return b - a


def compute_diff(baseline: PipelineRun, current: PipelineRun) -> MetricDiff:
    """Return a MetricDiff comparing *current* against *baseline*."""
    if baseline.pipeline != current.pipeline:
        raise ValueError(
            f"Pipeline mismatch: '{baseline.pipeline}' vs '{current.pipeline}'"
        )
    return MetricDiff(
        pipeline=current.pipeline,
        duration_delta=_delta(baseline.duration_seconds, current.duration_seconds),
        error_rate_delta=_delta(baseline.error_rate, current.error_rate),
        rows_processed_delta=_delta_int(
            baseline.rows_processed, current.rows_processed
        ),
        baseline_run_id=baseline.run_id,
        current_run_id=current.run_id,
    )
