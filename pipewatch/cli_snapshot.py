"""CLI commands for viewing pipeline metric snapshots."""
from pathlib import Path

import click

from pipewatch.snapshot import load_snapshots

_DEFAULT_PATH = Path("pipewatch_snapshots.json")


@click.group(name="snapshot")
def snapshot_command() -> None:
    """Manage and view pipeline metric snapshots."""


@snapshot_command.command(name="list")
@click.option("--file", "snap_file", default=str(_DEFAULT_PATH), show_default=True, help="Snapshot file path.")
@click.option("--pipeline", "pipeline_filter", default=None, help="Filter by pipeline name.")
@click.option("--limit", default=20, show_default=True, help="Max entries to display.")
def list_snapshots(snap_file: str, pipeline_filter: str | None, limit: int) -> None:
    """List recent metric snapshots."""
    path = Path(snap_file)
    snapshots = load_snapshots(path)
    if pipeline_filter:
        snapshots = [s for s in snapshots if s.pipeline == pipeline_filter]
    snapshots = snapshots[-limit:]
    if not snapshots:
        click.echo("No snapshots found.")
        return
    click.echo(f"{'Pipeline':<20} {'Captured At':<28} {'Duration':>10} {'ErrRate':>8} {'Rows':>8} {'Healthy':>8}")
    click.echo("-" * 86)
    for s in snapshots:
        click.echo(
            f"{s.pipeline:<20} {s.captured_at:<28} {s.duration_seconds:>10.2f} "
            f"{s.error_rate:>8.4f} {s.rows_processed:>8} {'yes' if s.healthy else 'no':>8}"
        )


@snapshot_command.command(name="clear")
@click.option("--file", "snap_file", default=str(_DEFAULT_PATH), show_default=True)
@click.confirmation_option(prompt="Delete all snapshots?")
def clear_snapshots(snap_file: str) -> None:
    """Delete all stored snapshots."""
    path = Path(snap_file)
    if path.exists():
        path.unlink()
        click.echo(f"Snapshots cleared: {path}")
    else:
        click.echo("No snapshot file found.")
