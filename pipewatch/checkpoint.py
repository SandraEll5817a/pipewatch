"""Checkpoint tracking: record and query the last known good run per pipeline."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

DEFAULT_PATH = Path("pipewatch_checkpoints.json")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Checkpoint:
    pipeline: str
    last_good_at: str  # ISO-8601
    duration_seconds: float
    rows_processed: int

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "Checkpoint":
        return Checkpoint(
            pipeline=d["pipeline"],
            last_good_at=d["last_good_at"],
            duration_seconds=float(d["duration_seconds"]),
            rows_processed=int(d["rows_processed"]),
        )


def load_checkpoints(path: Path = DEFAULT_PATH) -> Dict[str, Checkpoint]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
        return {k: Checkpoint.from_dict(v) for k, v in data.items()}
    except (json.JSONDecodeError, KeyError):
        return {}


def save_checkpoints(checkpoints: Dict[str, Checkpoint], path: Path = DEFAULT_PATH) -> None:
    path.write_text(json.dumps({k: v.to_dict() for k, v in checkpoints.items()}, indent=2))


def record_checkpoint(
    pipeline: str,
    duration_seconds: float,
    rows_processed: int,
    path: Path = DEFAULT_PATH,
) -> Checkpoint:
    checkpoints = load_checkpoints(path)
    cp = Checkpoint(
        pipeline=pipeline,
        last_good_at=_utcnow().isoformat(),
        duration_seconds=duration_seconds,
        rows_processed=rows_processed,
    )
    checkpoints[pipeline] = cp
    save_checkpoints(checkpoints, path)
    return cp


def get_checkpoint(pipeline: str, path: Path = DEFAULT_PATH) -> Optional[Checkpoint]:
    return load_checkpoints(path).get(pipeline)


def list_checkpoints(path: Path = DEFAULT_PATH) -> List[Checkpoint]:
    return list(load_checkpoints(path).values())
