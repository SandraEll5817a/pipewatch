"""Runbook links: attach remediation URLs and notes to pipeline alert conditions."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class RunbookEntry:
    pipeline: str
    title: str
    url: Optional[str] = None
    notes: Optional[str] = None
    tags: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "title": self.title,
            "url": self.url,
            "notes": self.notes,
            "tags": self.tags,
        }

    @staticmethod
    def from_dict(d: dict) -> "RunbookEntry":
        return RunbookEntry(
            pipeline=d["pipeline"],
            title=d["title"],
            url=d.get("url"),
            notes=d.get("notes"),
            tags=d.get("tags", []),
        )


def load_runbooks(path: Path) -> Dict[str, List[RunbookEntry]]:
    """Load runbook entries grouped by pipeline name."""
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}
    result: Dict[str, List[RunbookEntry]] = {}
    for item in raw:
        entry = RunbookEntry.from_dict(item)
        result.setdefault(entry.pipeline, []).append(entry)
    return result


def save_runbooks(entries: List[RunbookEntry], path: Path) -> None:
    path.write_text(json.dumps([e.to_dict() for e in entries], indent=2))


def add_runbook(entry: RunbookEntry, path: Path) -> None:
    existing = load_runbooks(path)
    all_entries = [e for group in existing.values() for e in group]
    all_entries.append(entry)
    save_runbooks(all_entries, path)


def remove_runbook(pipeline: str, title: str, path: Path) -> int:
    """Remove matching entries; return count removed."""
    existing = load_runbooks(path)
    all_entries = [e for group in existing.values() for e in group]
    before = len(all_entries)
    kept = [e for e in all_entries if not (e.pipeline == pipeline and e.title == title)]
    save_runbooks(kept, path)
    return before - len(kept)


def get_runbooks_for(pipeline: str, path: Path) -> List[RunbookEntry]:
    return load_runbooks(path).get(pipeline, [])
