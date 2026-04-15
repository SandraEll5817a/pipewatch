"""Persistence layer for pipeline run history."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

DEFAULT_HISTORY_FILE = Path(".pipewatch_history.json")
MAX_HISTORY_ENTRIES = 200


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class PipelineRun:
    timestamp: str
    healthy: bool
    duration_seconds: Optional[float]
    error_rate: Optional[float]
    rows_processed: Optional[float]
    violations: List[str] = field(default_factory=list)


def load_history(path: Path = DEFAULT_HISTORY_FILE) -> List[PipelineRun]:
    """Load run history from a JSON file. Returns empty list on missing/corrupt file."""
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
        return [PipelineRun(**entry) for entry in data]
    except (json.JSONDecodeError, TypeError, KeyError):
        return []


def save_history(
    runs: List[PipelineRun],
    path: Path = DEFAULT_HISTORY_FILE,
    max_entries: int = MAX_HISTORY_ENTRIES,
) -> None:
    """Persist run history, capping at max_entries (most recent kept)."""
    capped = runs[-max_entries:]
    path.write_text(json.dumps([asdict(r) for r in capped], indent=2))


def record_run(
    pipeline: str,
    healthy: bool,
    duration_seconds: Optional[float] = None,
    error_rate: Optional[float] = None,
    rows_processed: Optional[float] = None,
    violations: Optional[List[str]] = None,
    path: Path = DEFAULT_HISTORY_FILE,
) -> PipelineRun:
    """Append a new run record to the history file and return it."""
    runs = load_history(path)
    run = PipelineRun(
        timestamp=_utcnow(),
        healthy=healthy,
        duration_seconds=duration_seconds,
        error_rate=error_rate,
        rows_processed=rows_processed,
        violations=violations or [],
    )
    runs.append(run)
    save_history(runs, path)
    return run


def get_pipeline_history(
    pipeline: str, path: Path = DEFAULT_HISTORY_FILE
) -> List[PipelineRun]:
    """Return all recorded runs (currently global; pipeline key reserved for future)."""
    return load_history(path)
