"""Notification throttling: suppress duplicate alerts for the same pipeline+violation within a window."""
from __future__ import annotations
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

_DEFAULT_PATH = Path(".pipewatch_throttle.json")


def _now() -> float:
    return time.time()


@dataclass
class ThrottleEntry:
    pipeline: str
    violation_key: str
    last_sent: float
    count: int = 1

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "violation_key": self.violation_key,
            "last_sent": self.last_sent,
            "count": self.count,
        }

    @staticmethod
    def from_dict(d: dict) -> "ThrottleEntry":
        return ThrottleEntry(
            pipeline=d["pipeline"],
            violation_key=d["violation_key"],
            last_sent=d["last_sent"],
            count=d.get("count", 1),
        )


@dataclass
class ThrottlePolicy:
    window_seconds: int = 300
    max_per_window: int = 1


def _entry_key(pipeline: str, violation_key: str) -> str:
    return f"{pipeline}::{violation_key}"


def load_throttle(path: Path = _DEFAULT_PATH) -> Dict[str, ThrottleEntry]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
        return {k: ThrottleEntry.from_dict(v) for k, v in data.items()}
    except Exception:
        return {}


def save_throttle(state: Dict[str, ThrottleEntry], path: Path = _DEFAULT_PATH) -> None:
    path.write_text(json.dumps({k: v.to_dict() for k, v in state.items()}, indent=2))


def is_throttled(
    pipeline: str,
    violation_key: str,
    policy: ThrottlePolicy,
    state: Dict[str, ThrottleEntry],
    now: Optional[float] = None,
) -> bool:
    now = now or _now()
    key = _entry_key(pipeline, violation_key)
    entry = state.get(key)
    if entry is None:
        return False
    age = now - entry.last_sent
    if age > policy.window_seconds:
        return False
    return entry.count >= policy.max_per_window


def record_sent(
    pipeline: str,
    violation_key: str,
    state: Dict[str, ThrottleEntry],
    now: Optional[float] = None,
) -> None:
    now = now or _now()
    key = _entry_key(pipeline, violation_key)
    if key in state:
        entry = state[key]
        entry.count += 1
        entry.last_sent = now
    else:
        state[key] = ThrottleEntry(pipeline=pipeline, violation_key=violation_key, last_sent=now)
