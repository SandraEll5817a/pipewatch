"""Capacity planning: detect when a pipeline is approaching resource limits
based on recent throughput trends and configured ceilings."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.history import PipelineRun


@dataclass
class CapacityResult:
    pipeline: str
    utilization: float          # 0.0 – 1.0
    projected_saturation: Optional[int]  # runs until ceiling, None if stable
    ceiling: int                # configured max rows_processed
    recent_avg: float
    at_risk: bool
    message: str

    def __str__(self) -> str:
        pct = f"{self.utilization * 100:.1f}%"
        if self.at_risk:
            return (
                f"{self.pipeline}: at risk – {pct} utilization, "
                f"saturation in ~{self.projected_saturation} run(s)"
            )
        return f"{self.pipeline}: ok – {pct} utilization"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "utilization": round(self.utilization, 4),
            "projected_saturation": self.projected_saturation,
            "ceiling": self.ceiling,
            "recent_avg": round(self.recent_avg, 2),
            "at_risk": self.at_risk,
            "message": self.message,
        }


MIN_RUNS = 3
RISK_THRESHOLD = 0.80  # 80 % utilisation triggers at_risk


def _safe_avg(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _linear_slope(values: List[float]) -> float:
    """Least-squares slope over an evenly-spaced series."""
    n = len(values)
    if n < 2:
        return 0.0
    xs = list(range(n))
    x_mean = _safe_avg(xs)
    y_mean = _safe_avg(values)
    num = sum((xs[i] - x_mean) * (values[i] - y_mean) for i in range(n))
    den = sum((xs[i] - x_mean) ** 2 for i in range(n))
    return num / den if den else 0.0


def check_capacity(
    pipeline: str,
    runs: List[PipelineRun],
    ceiling: int,
    min_runs: int = MIN_RUNS,
    risk_threshold: float = RISK_THRESHOLD,
) -> CapacityResult:
    """Evaluate whether *pipeline* is approaching its *ceiling*."""
    rows = [r.rows_processed for r in runs if r.rows_processed is not None]
    if len(rows) < min_runs or ceiling <= 0:
        return CapacityResult(
            pipeline=pipeline,
            utilization=0.0,
            projected_saturation=None,
            ceiling=ceiling,
            recent_avg=0.0,
            at_risk=False,
            message="insufficient data",
        )

    recent = rows[-min_runs:]
    avg = _safe_avg(recent)
    utilization = avg / ceiling
    slope = _linear_slope(recent)

    projected: Optional[int] = None
    if slope > 0 and avg < ceiling:
        remaining = ceiling - avg
        projected = max(1, int(remaining / slope))

    at_risk = utilization >= risk_threshold or (
        projected is not None and projected <= 5
    )
    msg = str(CapacityResult(
        pipeline=pipeline, utilization=utilization,
        projected_saturation=projected, ceiling=ceiling,
        recent_avg=avg, at_risk=at_risk, message="",
    ))
    return CapacityResult(
        pipeline=pipeline,
        utilization=utilization,
        projected_saturation=projected,
        ceiling=ceiling,
        recent_avg=avg,
        at_risk=at_risk,
        message=msg,
    )
