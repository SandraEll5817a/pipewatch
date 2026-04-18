"""Deduplication of alert notifications to avoid sending the same alert repeatedly."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

DEFAULT_DEDUP_FILE = Path(".pipewatch_dedup.json")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _fingerprint(pipeline: str, violation_keys: List[str]) -> str:
    raw = json.dumps({"pipeline": pipeline, "violations": sorted(violation_keys)}, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


@dataclass
class DedupEntry:
    fingerprint: str
    pipeline: str
    first_seen: str
    last_seen: str
    count: int = 1

    def to_dict(self) -> dict:
        return {
            "fingerprint": self.fingerprint,
            "pipeline": self.pipeline,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
            "count": self.count,
        }

    @staticmethod
    def from_dict(d: dict) -> "DedupEntry":
        return DedupEntry(
            fingerprint=d["fingerprint"],
            pipeline=d["pipeline"],
            first_seen=d["first_seen"],
            last_seen=d["last_seen"],
            count=d.get("count", 1),
        )


def load_dedup(path: Path = DEFAULT_DEDUP_FILE) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
        return {k: DedupEntry.from_dict(v) for k, v in data.items()}
    except (json.JSONDecodeError, KeyError):
        return {}


def save_dedup(entries: dict, path: Path = DEFAULT_DEDUP_FILE) -> None:
    path.write_text(json.dumps({k: v.to_dict() for k, v in entries.items()}, indent=2))


def is_duplicate(pipeline: str, violation_keys: List[str], ttl_seconds: int = 3600,
                 path: Path = DEFAULT_DEDUP_FILE) -> bool:
    """Return True if this alert was already sent within ttl_seconds."""
    fp = _fingerprint(pipeline, violation_keys)
    entries = load_dedup(path)
    now = _utcnow()

    if fp in entries:
        entry = entries[fp]
        last = datetime.fromisoformat(entry.last_seen)
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        elapsed = (now - last).total_seconds()
        if elapsed < ttl_seconds:
            entry.last_seen = now.isoformat()
            entry.count += 1
            save_dedup(entries, path)
            return True

    entries[fp] = DedupEntry(
        fingerprint=fp,
        pipeline=pipeline,
        first_seen=now.isoformat(),
        last_seen=now.isoformat(),
    )
    save_dedup(entries, path)
    return False
