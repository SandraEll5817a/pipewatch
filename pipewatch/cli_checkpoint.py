"""CLI commands for managing pipeline checkpoints."""
from __future__ import annotations

from pathlib import Path

import click

from pipewatch.checkpoint import (
    list_checkpoints,
    get_checkpoint,
    record_checkpoint,
    load_checkpoints,
    save_checkpoints,
    DEFAULT_PATH,
)


@click.group(name="checkpoint")
def checkpoint_command() -> None:
    """Manage last-known-good checkpoints for pipelines."""


@checkpoint_command.command("list")
@click.option("--path", default=str(DEFAULT_PATH), show_default=True)
def list_cmd(path: str) -> None:
    """List all recorded checkpoints."""
    items = list_checkpoints(Path(path))
    if not items:
        click.echo("No checkpoints recorded.")
        return
    for cp in sorted(items, key=lambda c: c.pipeline):
        click.echo(
            f"{cp.pipeline}: last_good={cp.last_good_at} "
            f"duration={cp.duration_seconds:.2f}s rows={cp.rows_processed}"
        )


@checkpoint_command.command("show")
@click.argument("pipeline")
@click.option("--path", default=str(DEFAULT_PATH), show_default=True)
def show_cmd(pipeline: str, path: str) -> None:
    """Show checkpoint for a specific pipeline."""
    cp = get_checkpoint(pipeline, Path(path))
    if cp is None:
        click.echo(f"No checkpoint found for '{pipeline}'.")
        raise SystemExit(1)
    click.echo(f"pipeline:         {cp.pipeline}")
    click.echo(f"last_good_at:     {cp.last_good_at}")
    click.echo(f"duration_seconds: {cp.duration_seconds:.2f}")
    click.echo(f"rows_processed:   {cp.rows_processed}")


@checkpoint_command.command("record")
@click.argument("pipeline")
@click.option("--duration", type=float, required=True, help="Duration in seconds.")
@click.option("--rows", type=int, required=True, help="Rows processed.")
@click.option("--path", default=str(DEFAULT_PATH), show_default=True)
def record_cmd(pipeline: str, duration: float, rows: int, path: str) -> None:
    """Manually record a checkpoint for a pipeline."""
    cp = record_checkpoint(pipeline, duration, rows, Path(path))
    click.echo(f"Checkpoint recorded for '{pipeline}' at {cp.last_good_at}.")


@checkpoint_command.command("clear")
@click.argument("pipeline")
@click.option("--path", default=str(DEFAULT_PATH), show_default=True)
def clear_cmd(pipeline: str, path: str) -> None:
    """Remove a checkpoint for a pipeline."""
    p = Path(path)
    checkpoints = load_checkpoints(p)
    if pipeline not in checkpoints:
        click.echo(f"No checkpoint found for '{pipeline}'.")
        raise SystemExit(1)
    del checkpoints[pipeline]
    save_checkpoints(checkpoints, p)
    click.echo(f"Checkpoint for '{pipeline}' removed.")
