"""Runtime budget tracking: flag pipelines that exceed time/row cost budgets."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pipewatch.history import PipelineRun

_DEFAULT_PATH = Path(".pipewatch_budgets.json")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class BudgetPolicy:
    max_duration_seconds: Optional[float] = None
    max_rows_processed: Optional[int] = None
    max_error_rate: Optional[float] = None


@dataclass
class BudgetViolation:
    metric: str
    limit: float
    actual: float

    def __str__(self) -> str:
        return f"{self.metric} exceeded budget: {self.actual:.2f} > {self.limit:.2f}"

    def to_dict(self) -> dict:
        return {"metric": self.metric, "limit": self.limit, "actual": self.actual}


@dataclass
class BudgetResult:
    pipeline: str
    compliant: bool
    violations: List[BudgetViolation] = field(default_factory=list)
    checked_at: datetime = field(default_factory=_utcnow)

    def __str__(self) -> str:
        if self.compliant:
            return f"{self.pipeline}: within budget"
        msgs = "; ".join(str(v) for v in self.violations)
        return f"{self.pipeline}: budget exceeded — {msgs}"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "compliant": self.compliant,
            "violations": [v.to_dict() for v in self.violations],
            "checked_at": self.checked_at.isoformat(),
        }


def check_budget(pipeline: str, runs: List[PipelineRun], policy: BudgetPolicy) -> BudgetResult:
    """Evaluate recent runs against a budget policy."""
    if not runs:
        return BudgetResult(pipeline=pipeline, compliant=True)

    recent = runs[-1]
    violations: List[BudgetViolation] = []

    if policy.max_duration_seconds is not None and recent.duration_seconds > policy.max_duration_seconds:
        violations.append(BudgetViolation("duration_seconds", policy.max_duration_seconds, recent.duration_seconds))

    if policy.max_rows_processed is not None and recent.rows_processed > policy.max_rows_processed:
        violations.append(BudgetViolation("rows_processed", float(policy.max_rows_processed), float(recent.rows_processed)))

    if policy.max_error_rate is not None and recent.error_rate > policy.max_error_rate:
        violations.append(BudgetViolation("error_rate", policy.max_error_rate, recent.error_rate))

    return BudgetResult(pipeline=pipeline, compliant=len(violations) == 0, violations=violations)


def check_all_budgets(
    pipelines: List[str],
    runs_by_pipeline: dict,
    policies: dict,
) -> List[BudgetResult]:
    results = []
    for name in pipelines:
        policy = policies.get(name)
        if policy is None:
            continue
        runs = runs_by_pipeline.get(name, [])
        results.append(check_budget(name, runs, policy))
    return results
