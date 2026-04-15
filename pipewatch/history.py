"""Pipeline run history tracking using a local JSON file store."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

DEFAULT_HISTORY_PATH = Path(".pipewatch_history.json")
MAX_HISTORY_ENTRIES = 100


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_history(path: Path = DEFAULT_HISTORY_PATH) -> List[dict]:
    """Load all history entries from the JSON store."""
    if not path.exists():
        return []
    with path.open("r") as fh:
        try:
            data = json.load(fh)
        except json.JSONDecodeError:
            return []
    return data if isinstance(data, list) else []


def save_history(entries: List[dict], path: Path = DEFAULT_HISTORY_PATH) -> None:
    """Persist history entries to the JSON store (capped at MAX_HISTORY_ENTRIES)."""
    capped = entries[-MAX_HISTORY_ENTRIES:"w") as fh:
        json.dump(capped, fh, indent=2)


def record_run(
    pipeline_name: str,
    healthy: bool,
    violation_count: int,
    path: Path = DEFAULT_HISTORY_PATH,
    *,
    timestamp: Optional[str] = None,
) -> dict:
    """ a run record for *pipeline_name* and return the new entry."""
    entry = {
        "pipeline": pipeline_name,
        "timestamp": timestamp or _utcnow(),
        "healthy": healthy,
        "violation_count": violation_count,
    }
    entries = load_history(path)
    entries.append(entry)
    save_history(entries, path)
    return entry


def get_pipeline_history(
    pipeline_name: str,
    limit: int = 10,
    path: Path = DEFAULT_HISTORY_PATH,
) -> List[dict]:
    """Return the most recent *limit* entries for a given pipeline."""
    all_entries = load_history(path)
    pipeline_entries = [
        e for e in all_entries if e.get("pipeline") == pipeline_name
    ]
    return pipeline_entries[-limit:]


def clear_history(path: Path = DEFAULT_HISTORY_PATH) -> None:
    """Remove all history entries."""
    if path.exists():
        os.remove(path)
