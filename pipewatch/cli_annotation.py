"""CLI commands for managing pipeline run annotations."""
from pathlib import Path

import click

from pipewatch.annotation import (
    add_annotation,
    delete_annotation,
    get_annotations,
    load_annotations,
)

_DEFAULT = Path("pipewatch_annotations.json")


@click.group(name="annotation")
def annotation_command():
    """Manage annotations on pipeline runs."""


@annotation_command.command("add")
@click.argument("pipeline")
@click.argument("run_id")
@click.argument("note")
@click.option("--author", default="unknown", show_default=True)
@click.option("--file", "ann_file", default=str(_DEFAULT), show_default=True)
def add_cmd(pipeline: str, run_id: str, note: str, author: str, ann_file: str):
    """Add a note to a pipeline run."""
    ann = add_annotation(pipeline, run_id, note, author=author, path=Path(ann_file))
    click.echo(f"Annotation added for {ann.pipeline}/{ann.run_id} by {ann.author}.")


@annotation_command.command("list")
@click.argument("pipeline")
@click.option("--file", "ann_file", default=str(_DEFAULT), show_default=True)
def list_cmd(pipeline: str, ann_file: str):
    """List annotations for a pipeline."""
    anns = get_annotations(pipeline, path=Path(ann_file))
    if not anns:
        click.echo(f"No annotations for '{pipeline}'.")
        return
    for a in anns:
        click.echo(f"[{a.created_at.isoformat()}] {a.run_id} ({a.author}): {a.note}")


@annotation_command.command("delete")
@click.argument("pipeline")
@click.argument("run_id")
@click.option("--file", "ann_file", default=str(_DEFAULT), show_default=True)
def delete_cmd(pipeline: str, run_id: str, ann_file: str):
    """Delete an annotation by pipeline and run ID."""
    removed = delete_annotation(pipeline, run_id, path=Path(ann_file))
    if removed:
        click.echo(f"Removed annotation for {pipeline}/{run_id}.")
    else:
        click.echo(f"No annotation found for {pipeline}/{run_id}.")
        raise SystemExit(1)


@annotation_command.command("clear")
@click.argument("pipeline")
@click.option("--file", "ann_file", default=str(_DEFAULT), show_default=True)
def clear_cmd(pipeline: str, ann_file: str):
    """Remove all annotations for a pipeline."""
    p = Path(ann_file)
    all_anns = load_annotations(p)
    kept = [a for a in all_anns if a.pipeline != pipeline]
    removed = len(all_anns) - len(kept)
    from pipewatch.annotation import save_annotations
    save_annotations(kept, p)
    click.echo(f"Cleared {removed} annotation(s) for '{pipeline}'.")
