"""Point-in-time metric snapshots for pipeline runs."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class MetricSnapshot:
    pipeline: str
    captured_at: str
    duration_seconds: float
    error_rate: float
    rows_processed: int
    healthy: bool

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "MetricSnapshot":
        return cls(**data)


def capture_snapshot(
    pipeline: str,
    duration_seconds: float,
    error_rate: float,
    rows_processed: int,
    healthy: bool,
    now: Optional[datetime] = None,
) -> MetricSnapshot:
    ts = (now or _utcnow()).isoformat()
    return MetricSnapshot(
        pipeline=pipeline,
        captured_at=ts,
        duration_seconds=duration_seconds,
        error_rate=error_rate,
        rows_processed=rows_processed,
        healthy=healthy,
    )


def load_snapshots(path: Path) -> List[MetricSnapshot]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
        return [MetricSnapshot.from_dict(d) for d in data]
    except (json.JSONDecodeError, KeyError, TypeError):
        return []


def save_snapshots(path: Path, snapshots: List[MetricSnapshot], max_entries: int = 200) -> None:
    trimmed = snapshots[-max_entries:]
    path.write_text(json.dumps([s.to_dict() for s in trimmed], indent=2))


def add_snapshot(path: Path, snapshot: MetricSnapshot, max_entries: int = 200) -> List[MetricSnapshot]:
    existing = load_snapshots(path)
    existing.append(snapshot)
    save_snapshots(path, existing, max_entries)
    return existing
