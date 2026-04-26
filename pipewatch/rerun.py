"""Rerun policy: track and decide whether a failed pipeline should be rerun."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

_DEFAULT_PATH = Path("pipewatch_rerun.json")
_MAX_ENTRIES = 500


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class RerunPolicy:
    max_reruns: int = 3
    cooldown_seconds: int = 60


@dataclass
class RerunEntry:
    pipeline: str
    attempt: int
    last_rerun_at: datetime

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "attempt": self.attempt,
            "last_rerun_at": self.last_rerun_at.isoformat(),
        }

    @staticmethod
    def from_dict(d: dict) -> "RerunEntry":
        return RerunEntry(
            pipeline=d["pipeline"],
            attempt=d["attempt"],
            last_rerun_at=datetime.fromisoformat(d["last_rerun_at"]),
        )


@dataclass
class RerunResult:
    pipeline: str
    allowed: bool
    reason: str
    attempt: int

    def __str__(self) -> str:
        status = "allowed" if self.allowed else "denied"
        return f"[{self.pipeline}] rerun {status} (attempt {self.attempt}): {self.reason}"


def load_rerun_state(path: Path = _DEFAULT_PATH) -> Dict[str, RerunEntry]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
        return {d["pipeline"]: RerunEntry.from_dict(d) for d in data}
    except (json.JSONDecodeError, KeyError):
        return {}


def save_rerun_state(state: Dict[str, RerunEntry], path: Path = _DEFAULT_PATH) -> None:
    entries = list(state.values())[-_MAX_ENTRIES:]
    path.write_text(json.dumps([e.to_dict() for e in entries], indent=2))


def check_rerun(
    pipeline: str,
    policy: RerunPolicy,
    state: Dict[str, RerunEntry],
    now: Optional[datetime] = None,
) -> RerunResult:
    now = now or _utcnow()
    entry = state.get(pipeline)

    if entry is None:
        return RerunResult(pipeline=pipeline, allowed=True, reason="first attempt", attempt=1)

    if entry.attempt >= policy.max_reruns:
        return RerunResult(
            pipeline=pipeline,
            allowed=False,
            reason=f"max reruns ({policy.max_reruns}) reached",
            attempt=entry.attempt,
        )

    elapsed = (now - entry.last_rerun_at).total_seconds()
    if elapsed < policy.cooldown_seconds:
        remaining = int(policy.cooldown_seconds - elapsed)
        return RerunResult(
            pipeline=pipeline,
            allowed=False,
            reason=f"cooldown active ({remaining}s remaining)",
            attempt=entry.attempt,
        )

    return RerunResult(
        pipeline=pipeline,
        allowed=True,
        reason="cooldown elapsed",
        attempt=entry.attempt + 1,
    )


def record_rerun(
    pipeline: str,
    state: Dict[str, RerunEntry],
    now: Optional[datetime] = None,
) -> RerunEntry:
    now = now or _utcnow()
    prev = state.get(pipeline)
    attempt = (prev.attempt + 1) if prev else 1
    entry = RerunEntry(pipeline=pipeline, attempt=attempt, last_rerun_at=now)
    state[pipeline] = entry
    return entry
