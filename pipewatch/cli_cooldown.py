"""CLI commands for managing pipeline cooldowns."""

from __future__ import annotations

from pathlib import Path

import click

from pipewatch.cooldown import (
    active_cooldowns,
    clear_cooldown,
    is_in_cooldown,
    load_cooldowns,
    trigger_cooldown,
)

_DEFAULT_PATH = Path("pipewatch_cooldowns.json")


@click.group(name="cooldown")
def cooldown_command() -> None:
    """Manage pipeline alert cooldowns."""


@cooldown_command.command(name="trigger")
@click.argument("pipeline")
@click.option("--duration", default=300, show_default=True, help="Cooldown window in seconds.")
@click.option("--file", "path", default=str(_DEFAULT_PATH), show_default=True)
def trigger_cmd(pipeline: str, duration: int, path: str) -> None:
    """Trigger a cooldown for PIPELINE."""
    entry = trigger_cooldown(pipeline, duration, path=Path(path))
    click.echo(
        f"Cooldown triggered for '{pipeline}' — {duration}s from "
        f"{entry.triggered_at.isoformat()}"
    )


@cooldown_command.command(name="status")
@click.argument("pipeline")
@click.option("--file", "path", default=str(_DEFAULT_PATH), show_default=True)
def status_cmd(pipeline: str, path: str) -> None:
    """Check whether PIPELINE is currently in cooldown."""
    active = is_in_cooldown(pipeline, path=Path(path))
    if active:
        click.echo(f"'{pipeline}' is IN cooldown.")
        raise SystemExit(1)
    click.echo(f"'{pipeline}' is NOT in cooldown.")


@cooldown_command.command(name="list")
@click.option("--file", "path", default=str(_DEFAULT_PATH), show_default=True)
@click.option("--active-only", is_flag=True, default=False, help="Show only active cooldowns.")
def list_cmd(path: str, active_only: bool) -> None:
    """List all cooldown entries."""
    if active_only:
        entries = active_cooldowns(path=Path(path))
    else:
        entries = list(load_cooldowns(Path(path)).values())

    if not entries:
        click.echo("No cooldown entries found.")
        return

    for entry in entries:
        status = "ACTIVE" if entry.is_active() else "expired"
        click.echo(
            f"  {entry.pipeline:<30} {status:<8}  "
            f"triggered={entry.triggered_at.isoformat()}  "
            f"duration={entry.duration_seconds}s"
        )


@cooldown_command.command(name="clear")
@click.argument("pipeline")
@click.option("--file", "path", default=str(_DEFAULT_PATH), show_default=True)
def clear_cmd(pipeline: str, path: str) -> None:
    """Remove the cooldown entry for PIPELINE."""
    removed = clear_cooldown(pipeline, path=Path(path))
    if removed:
        click.echo(f"Cooldown cleared for '{pipeline}'.")
    else:
        click.echo(f"No cooldown entry found for '{pipeline}'.")
        raise SystemExit(1)
