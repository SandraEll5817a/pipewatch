"""Export run summaries to JSON or plain-text files."""

import json
from pathlib import Path
from typing import List, Optional

from pipewatch.runner import PipelineResult
from pipewatch.summary import RunSummary, build_summary, format_summary_text


def export_json(results: List[PipelineResult], output_path: str) -> Path:
    """Write a JSON summary of *results* to *output_path*.

    Returns the resolved Path that was written.
    """
    summary: RunSummary = build_summary(results)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(summary.to_dict(), fh, indent=2)
    return path.resolve()


def export_text(results: List[PipelineResult], output_path: str) -> Path:
    """Write a plain-text summary of *results* to *output_path*.

    Returns the resolved Path that was written.
    """
    summary: RunSummary = build_summary(results)
    text = format_summary_text(summary)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        fh.write(text)
        fh.write("\n")
    return path.resolve()


def load_json_summary(input_path: str) -> Optional[dict]:
    """Load a previously exported JSON summary.  Returns None if the file
    does not exist or cannot be parsed.
    """
    path = Path(input_path)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        return None
