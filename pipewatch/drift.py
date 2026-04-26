"""Drift detection for pipeline metrics.

Compares recent metric windows against a historical baseline window
to identify gradual degradation that may not trigger single-run
threshold violations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Sequence

from pipewatch.history import PipelineRun


@dataclass
class DriftResult:
    """Result of a drift analysis for a single metric."""

    pipeline: str
    metric: str
    baseline_avg: float
    recent_avg: float
    drift_pct: float          # positive = degraded, negative = improved
    drifted: bool
    threshold_pct: float

    def __str__(self) -> str:
        direction = "degraded" if self.drift_pct > 0 else "improved"
        return (
            f"{self.pipeline}/{self.metric}: {direction} by "
            f"{abs(self.drift_pct):.1f}% "
            f"(baseline={self.baseline_avg:.2f}, recent={self.recent_avg:.2f})"
        )

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "metric": self.metric,
            "baseline_avg": round(self.baseline_avg, 4),
            "recent_avg": round(self.recent_avg, 4),
            "drift_pct": round(self.drift_pct, 2),
            "drifted": self.drifted,
            "threshold_pct": self.threshold_pct,
        }


@dataclass
class DriftReport:
    """Aggregated drift results for a pipeline."""

    pipeline: str
    results: List[DriftResult] = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        return any(r.drifted for r in self.results)

    @property
    def drifted_metrics(self) -> List[DriftResult]:
        return [r for r in self.results if r.drifted]

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "has_drift": self.has_drift,
            "results": [r.to_dict() for r in self.results],
        }


def _safe_avg(values: Sequence[float]) -> Optional[float]:
    """Return mean of values, or None if the sequence is empty."""
    if not values:
        return None
    return sum(values) / len(values)


def _drift_pct(baseline: float, recent: float) -> float:
    """Percentage change from baseline to recent (positive = worse/higher)."""
    if baseline == 0.0:
        return 0.0
    return ((recent - baseline) / baseline) * 100.0


def detect_drift(
    pipeline: str,
    runs: List[PipelineRun],
    baseline_window: int = 20,
    recent_window: int = 5,
    drift_threshold_pct: float = 20.0,
) -> DriftReport:
    """Detect metric drift by comparing recent runs to a baseline window.

    Args:
        pipeline: Pipeline name (for labelling results).
        runs: Historical runs ordered oldest-first.
        baseline_window: Number of older runs used to establish the baseline.
        recent_window: Number of most-recent runs to compare against baseline.
        drift_threshold_pct: Percentage increase that constitutes drift.

    Returns:
        A DriftReport summarising per-metric drift.
    """
    report = DriftReport(pipeline=pipeline)

    if len(runs) < recent_window + 1:
        # Not enough data to compare windows
        return report

    recent_runs = runs[-recent_window:]
    # Baseline window sits just before the recent window
    baseline_runs = runs[-(recent_window + baseline_window):-recent_window]

    if not baseline_runs:
        return report

    metrics = {
        "duration_seconds": (
            [r.duration_seconds for r in baseline_runs],
            [r.duration_seconds for r in recent_runs],
        ),
        "error_rate": (
            [r.error_rate for r in baseline_runs],
            [r.error_rate for r in recent_runs],
        ),
        "rows_processed": (
            [r.rows_processed for r in baseline_runs if r.rows_processed is not None],
            [r.rows_processed for r in recent_runs if r.rows_processed is not None],
        ),
    }

    for metric_name, (baseline_vals, recent_vals) in metrics.items():
        b_avg = _safe_avg(baseline_vals)  # type: ignore[arg-type]
        r_avg = _safe_avg(recent_vals)    # type: ignore[arg-type]

        if b_avg is None or r_avg is None:
            continue

        pct = _drift_pct(b_avg, r_avg)
        drifted = pct >= drift_threshold_pct

        report.results.append(
            DriftResult(
                pipeline=pipeline,
                metric=metric_name,
                baseline_avg=b_avg,
                recent_avg=r_avg,
                drift_pct=pct,
                drifted=drifted,
                threshold_pct=drift_threshold_pct,
            )
        )

    return report
