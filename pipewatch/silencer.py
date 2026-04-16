"""Silence (mute) specific pipelines from alerting for a duration."""
from __future__ import annotations

import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

DEFAULT_SILENCE_FILE = Path(".pipewatch_silences.json")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class SilenceRule:
    pipeline: str
    until: str  # ISO-8601
    reason: str = ""

    def is_active(self, now: Optional[datetime] = None) -> bool:
        now = now or _utcnow()
        until_dt = datetime.fromisoformat(self.until)
        if until_dt.tzinfo is None:
            until_dt = until_dt.replace(tzinfo=timezone.utc)
        return now < until_dt

    def to_dict(self) -> dict:
        return asdict(self)


def load_silences(path: Path = DEFAULT_SILENCE_FILE) -> List[SilenceRule]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
        return [SilenceRule(**r) for r in data]
    except (json.JSONDecodeError, TypeError, KeyError):
        return []


def save_silences(rules: List[SilenceRule], path: Path = DEFAULT_SILENCE_FILE) -> None:
    path.write_text(json.dumps([r.to_dict() for r in rules], indent=2))


def add_silence(pipeline: str, until: datetime, reason: str = "",
                path: Path = DEFAULT_SILENCE_FILE) -> SilenceRule:
    rules = load_silences(path)
    rules = [r for r in rules if r.pipeline != pipeline]  # replace existing
    rule = SilenceRule(pipeline=pipeline, until=until.isoformat(), reason=reason)
    rules.append(rule)
    save_silences(rules, path)
    return rule


def remove_silence(pipeline: str, path: Path = DEFAULT_SILENCE_FILE) -> bool:
    rules = load_silences(path)
    new_rules = [r for r in rules if r.pipeline != pipeline]
    if len(new_rules) == len(rules):
        return False
    save_silences(new_rules, path)
    return True


def is_silenced(pipeline: str, path: Path = DEFAULT_SILENCE_FILE,
               now: Optional[datetime] = None) -> bool:
    for rule in load_silences(path):
        if rule.pipeline == pipeline and rule.is_active(now):
            return True
    return False
