"""SLA tracking: check whether pipelines meet their defined SLA windows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipewatch.history import PipelineRun


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class SLAPolicy:
    """Defines the SLA constraints for a pipeline."""
    max_duration_seconds: Optional[float] = None
    max_error_rate: Optional[float] = None
    min_rows_processed: Optional[int] = None


@dataclass
class SLAViolation:
    pipeline: str
    metric: str
    expected: str
    actual: str

    def __str__(self) -> str:
        return (
            f"[{self.pipeline}] SLA breach on '{self.metric}': "
            f"expected {self.expected}, got {self.actual}"
        )


@dataclass
class SLAResult:
    pipeline: str
    compliant: bool
    violations: List[SLAViolation] = field(default_factory=list)
    checked_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "compliant": self.compliant,
            "checked_at": self.checked_at.isoformat(),
            "violations": [
                {
                    "metric": v.metric,
                    "expected": v.expected,
                    "actual": v.actual,
                }
                for v in self.violations
            ],
        }


def check_sla(pipeline: str, runs: List[PipelineRun], policy: SLAPolicy) -> SLAResult:
    """Evaluate the most recent run against the SLA policy."""
    if not runs:
        return SLAResult(pipeline=pipeline, compliant=True)

    latest = max(runs, key=lambda r: r.ran_at)
    violations: List[SLAViolation] = []

    if policy.max_duration_seconds is not None:
        if latest.duration_seconds > policy.max_duration_seconds:
            violations.append(SLAViolation(
                pipeline=pipeline,
                metric="duration_seconds",
                expected=f"<= {policy.max_duration_seconds}",
                actual=str(latest.duration_seconds),
            ))

    if policy.max_error_rate is not None:
        if latest.error_rate > policy.max_error_rate:
            violations.append(SLAViolation(
                pipeline=pipeline,
                metric="error_rate",
                expected=f"<= {policy.max_error_rate}",
                actual=str(latest.error_rate),
            ))

    if policy.min_rows_processed is not None:
        if latest.rows_processed < policy.min_rows_processed:
            violations.append(SLAViolation(
                pipeline=pipeline,
                metric="rows_processed",
                expected=f">= {policy.min_rows_processed}",
                actual=str(latest.rows_processed),
            ))

    return SLAResult(
        pipeline=pipeline,
        compliant=len(violations) == 0,
        violations=violations,
    )
