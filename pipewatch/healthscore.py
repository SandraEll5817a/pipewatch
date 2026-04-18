"""Pipeline health score computation based on recent run history."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List
from pipewatch.history import PipelineRun


@dataclass
class HealthScore:
    pipeline: str
    score: float  # 0.0 (worst) to 100.0 (best)
    total_runs: int
    failed_runs: int
    avg_duration_seconds: float
    grade: str

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "score": round(self.score, 2),
            "total_runs": self.total_runs,
            "failed_runs": self.failed_runs,
            "avg_duration_seconds": round(self.avg_duration_seconds, 2),
            "grade": self.grade,
        }

    def __str__(self) -> str:
        return (
            f"{self.pipeline}: {self.grade} ({self.score:.1f}/100) "
            f"— {self.failed_runs}/{self.total_runs} failures, "
            f"avg {self.avg_duration_seconds:.1f}s"
        )


def _grade(score: float) -> str:
    if score >= 90:
        return "A"
    if score >= 75:
        return "B"
    if score >= 60:
        return "C"
    if score >= 40:
        return "D"
    return "F"


def compute_health_score(pipeline: str, runs: List[PipelineRun]) -> HealthScore:
    if not runs:
        return HealthScore(
            pipeline=pipeline,
            score=0.0,
            total_runs=0,
            failed_runs=0,
            avg_duration_seconds=0.0,
            grade="F",
        )

    total = len(runs)
    failed = sum(1 for r in runs if not r.healthy)
    success_rate = (total - failed) / total
    avg_dur = sum(r.duration_seconds for r in runs) / total

    # Penalise long durations: cap contribution at 300s
    duration_penalty = min(avg_dur / 300.0, 1.0) * 10.0
    score = max(0.0, success_rate * 100.0 - duration_penalty)

    return HealthScore(
        pipeline=pipeline,
        score=score,
        total_runs=total,
        failed_runs=failed,
        avg_duration_seconds=avg_dur,
        grade=_grade(score),
    )


def score_all(history_by_pipeline: dict[str, List[PipelineRun]]) -> List[HealthScore]:
    return [
        compute_health_score(name, runs)
        for name, runs in history_by_pipeline.items()
    ]
