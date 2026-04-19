"""Retention policy: prune old pipeline run history by age or count."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from pipewatch.history import PipelineRun


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class RetentionPolicy:
    max_age_days: Optional[int] = None
    max_runs: Optional[int] = None


@dataclass
class RetentionResult:
    pipeline: str
    original_count: int
    pruned_count: int
    kept_count: int

    def __str__(self) -> str:
        return (
            f"{self.pipeline}: pruned {self.pruned_count} of "
            f"{self.original_count} runs, {self.kept_count} kept"
        )


def apply_retention(
    pipeline: str,
    runs: List[PipelineRun],
    policy: RetentionPolicy,
    now: Optional[datetime] = None,
) -> tuple[List[PipelineRun], RetentionResult]:
    """Return filtered runs and a result summary."""
    if now is None:
        now = _utcnow()

    kept = list(runs)

    if policy.max_age_days is not None:
        cutoff = now - timedelta(days=policy.max_age_days)
        kept = [r for r in kept if r.timestamp >= cutoff]

    if policy.max_runs is not None and len(kept) > policy.max_runs:
        kept = sorted(kept, key=lambda r: r.timestamp, reverse=True)
        kept = kept[: policy.max_runs]

    result = RetentionResult(
        pipeline=pipeline,
        original_count=len(runs),
        pruned_count=len(runs) - len(kept),
        kept_count=len(kept),
    )
    return kept, result


def apply_retention_all(
    history: dict[str, List[PipelineRun]],
    policy: RetentionPolicy,
    now: Optional[datetime] = None,
) -> tuple[dict[str, List[PipelineRun]], List[RetentionResult]]:
    results: List[RetentionResult] = []
    pruned_history: dict[str, List[PipelineRun]] = {}
    for pipeline, runs in history.items():
        kept, result = apply_retention(pipeline, runs, policy, now=now)
        pruned_history[pipeline] = kept
        results.append(result)
    return pruned_history, results
