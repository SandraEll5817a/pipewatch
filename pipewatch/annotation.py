"""Pipeline run annotations — attach notes or labels to specific runs."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

_DEFAULT_PATH = Path("pipewatch_annotations.json")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Annotation:
    pipeline: str
    run_id: str
    note: str
    author: str = "unknown"
    created_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "run_id": self.run_id,
            "note": self.note,
            "author": self.author,
            "created_at": self.created_at.isoformat(),
        }

    @staticmethod
    def from_dict(d: dict) -> "Annotation":
        return Annotation(
            pipeline=d["pipeline"],
            run_id=d["run_id"],
            note=d["note"],
            author=d.get("author", "unknown"),
            created_at=datetime.fromisoformat(d["created_at"]),
        )


def load_annotations(path: Path = _DEFAULT_PATH) -> List[Annotation]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
        return [Annotation.from_dict(e) for e in data]
    except (json.JSONDecodeError, KeyError):
        return []


def save_annotations(annotations: List[Annotation], path: Path = _DEFAULT_PATH) -> None:
    path.write_text(json.dumps([a.to_dict() for a in annotations], indent=2))


def add_annotation(
    pipeline: str,
    run_id: str,
    note: str,
    author: str = "unknown",
    path: Path = _DEFAULT_PATH,
) -> Annotation:
    annotations = load_annotations(path)
    ann = Annotation(pipeline=pipeline, run_id=run_id, note=note, author=author)
    annotations.append(ann)
    save_annotations(annotations, path)
    return ann


def get_annotations(pipeline: str, path: Path = _DEFAULT_PATH) -> List[Annotation]:
    return [a for a in load_annotations(path) if a.pipeline == pipeline]


def delete_annotation(
    pipeline: str, run_id: str, path: Path = _DEFAULT_PATH
) -> Optional[Annotation]:
    annotations = load_annotations(path)
    removed: Optional[Annotation] = None
    kept = []
    for a in annotations:
        if a.pipeline == pipeline and a.run_id == run_id and removed is None:
            removed = a
        else:
            kept.append(a)
    if removed:
        save_annotations(kept, path)
    return removed
