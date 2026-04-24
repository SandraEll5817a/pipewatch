"""Pipeline cooldown tracking — suppress repeated alerts during a recovery window."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

_DEFAULT_PATH = Path("pipewatch_cooldowns.json")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class CooldownEntry:
    pipeline: str
    triggered_at: datetime
    duration_seconds: int

    def is_active(self, now: Optional[datetime] = None) -> bool:
        now = now or _utcnow()
        elapsed = (now - self.triggered_at).total_seconds()
        return elapsed < self.duration_seconds

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "triggered_at": self.triggered_at.isoformat(),
            "duration_seconds": self.duration_seconds,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CooldownEntry":
        return cls(
            pipeline=data["pipeline"],
            triggered_at=datetime.fromisoformat(data["triggered_at"]),
            duration_seconds=data["duration_seconds"],
        )


def load_cooldowns(path: Path = _DEFAULT_PATH) -> Dict[str, CooldownEntry]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text())
        return {k: CooldownEntry.from_dict(v) for k, v in raw.items()}
    except (json.JSONDecodeError, KeyError):
        return {}


def save_cooldowns(entries: Dict[str, CooldownEntry], path: Path = _DEFAULT_PATH) -> None:
    path.write_text(json.dumps({k: v.to_dict() for k, v in entries.items()}, indent=2))


def trigger_cooldown(
    pipeline: str,
    duration_seconds: int,
    path: Path = _DEFAULT_PATH,
    now: Optional[datetime] = None,
) -> CooldownEntry:
    entries = load_cooldowns(path)
    entry = CooldownEntry(
        pipeline=pipeline,
        triggered_at=now or _utcnow(),
        duration_seconds=duration_seconds,
    )
    entries[pipeline] = entry
    save_cooldowns(entries, path)
    return entry


def is_in_cooldown(
    pipeline: str,
    path: Path = _DEFAULT_PATH,
    now: Optional[datetime] = None,
) -> bool:
    entries = load_cooldowns(path)
    entry = entries.get(pipeline)
    if entry is None:
        return False
    return entry.is_active(now=now)


def clear_cooldown(pipeline: str, path: Path = _DEFAULT_PATH) -> bool:
    entries = load_cooldowns(path)
    if pipeline not in entries:
        return False
    del entries[pipeline]
    save_cooldowns(entries, path)
    return True


def active_cooldowns(
    path: Path = _DEFAULT_PATH,
    now: Optional[datetime] = None,
) -> List[CooldownEntry]:
    entries = load_cooldowns(path)
    return [e for e in entries.values() if e.is_active(now=now)]
