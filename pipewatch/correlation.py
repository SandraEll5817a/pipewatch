"""Correlate pipeline failures across runs to find co-occurring issues."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict, Tuple
from pipewatch.history import PipelineRun


@dataclass
class CorrelationResult:
    pipeline_a: str
    pipeline_b: str
    co_failures: int
    total_windows: int

    @property
    def rate(self) -> float:
        if self.total_windows == 0:
            return 0.0
        return self.co_failures / self.total_windows

    def to_dict(self) -> Dict:
        return {
            "pipeline_a": self.pipeline_a,
            "pipeline_b": self.pipeline_b,
            "co_failures": self.co_failures,
            "total_windows": self.total_windows,
            "rate": round(self.rate, 4),
        }

    def __str__(self) -> str:
        return (
            f"{self.pipeline_a} <-> {self.pipeline_b}: "
            f"{self.co_failures}/{self.total_windows} co-failures "
            f"({self.rate:.0%})"
        )


def _failure_set_by_timestamp(
    runs: List[PipelineRun], bucket_seconds: int = 300
) -> Dict[int, set]:
    """Map time bucket -> set of failed pipeline names."""
    buckets: Dict[int, set] = {}
    for run in runs:
        if not run.healthy:
            ts = int(run.timestamp.timestamp()) // bucket_seconds
            buckets.setdefault(ts, set()).add(run.pipeline)
    return buckets


def correlate_failures(
    runs: List[PipelineRun],
    min_rate: float = 0.5,
    bucket_seconds: int = 300,
) -> List[CorrelationResult]:
    """Return pairs of pipelines that frequently fail together."""
    buckets = _failure_set_by_timestamp(runs, bucket_seconds)
    if not buckets:
        return []

    pipelines = sorted({r.pipeline for r in runs})
    total_windows = len(buckets)
    co_counts: Dict[Tuple[str, str], int] = {}

    for failed in buckets.values():
        failed_list = sorted(failed)
        for i, a in enumerate(failed_list):
            for b in failed_list[i + 1 :]:
                key = (a, b)
                co_counts[key] = co_counts.get(key, 0) + 1

    results = []
    for (a, b), count in co_counts.items():
        r = CorrelationResult(a, b, count, total_windows)
        if r.rate >= min_rate:
            results.append(r)

    results.sort(key=lambda x: x.rate, reverse=True)
    return results
