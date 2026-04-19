"""Audit log: record and retrieve pipeline check events."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

DEFAULT_AUDIT_FILE = Path("pipewatch_audit.jsonl")
MAX_ENTRIES = 1000


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class AuditEvent:
    pipeline: str
    event_type: str  # "check", "alert", "silence", "baseline_set"
    message: str
    healthy: bool
    timestamp: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "event_type": self.event_type,
            "message": self.message,
            "healthy": self.healthy,
            "timestamp": self.timestamp.isoformat(),
        }

    @staticmethod
    def from_dict(d: dict) -> "AuditEvent":
        return AuditEvent(
            pipeline=d["pipeline"],
            event_type=d["event_type"],
            message=d["message"],
            healthy=d["healthy"],
            timestamp=datetime.fromisoformat(d["timestamp"]),
        )


def load_audit(path: Path = DEFAULT_AUDIT_FILE) -> List[AuditEvent]:
    if not path.exists():
        return []
    events: List[AuditEvent] = []
    try:
        for line in path.read_text().splitlines():
            line = line.strip()
            if line:
                events.append(AuditEvent.from_dict(json.loads(line)))
    except (json.JSONDecodeError, KeyError):
        return []
    return events


def append_audit(event: AuditEvent, path: Path = DEFAULT_AUDIT_FILE) -> None:
    existing = load_audit(path)
    existing.append(event)
    if len(existing) > MAX_ENTRIES:
        existing = existing[-MAX_ENTRIES:]
    path.write_text("\n".join(json.dumps(e.to_dict()) for e in existing) + "\n")


def filter_audit(
    events: List[AuditEvent],
    pipeline: Optional[str] = None,
    event_type: Optional[str] = None,
) -> List[AuditEvent]:
    result = events
    if pipeline:
        result = [e for e in result if e.pipeline == pipeline]
    if event_type:
        result = [e for e in result if e.event_type == event_type]
    return result
