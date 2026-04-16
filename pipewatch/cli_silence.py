"""CLI commands for managing pipeline silence rules."""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from pathlib import Path

import click

from pipewatch.silencer import (
    add_silence, remove_silence, load_silences, is_silenced,
    DEFAULT_SILENCE_FILE,
)


@click.group("silence")
def silence_command() -> None:
    """Manage pipeline alert silences."""


@silence_command.command("add")
@click.argument("pipeline")
@click.option("--hours", default=1.0, show_default=True, help="Duration in hours.")
@click.option("--reason", default="", help="Reason for silencing.")
@click.option("--file", "silence_file", default=str(DEFAULT_SILENCE_FILE),
              show_default=True, help="Silence file path.")
def add_silence_cmd(pipeline: str, hours: float, reason: str, silence_file: str) -> None:
    """Silence PIPELINE alerts for a given duration."""
    until = datetime.now(timezone.utc) + timedelta(hours=hours)
    rule = add_silence(pipeline, until, reason=reason, path=Path(silence_file))
    click.echo(f"Silenced '{rule.pipeline}' until {rule.until}" +
               (f" ({rule.reason})" if rule.reason else ""))


@silence_command.command("remove")
@click.argument("pipeline")
@click.option("--file", "silence_file", default=str(DEFAULT_SILENCE_FILE),
              show_default=True)
def remove_silence_cmd(pipeline: str, silence_file: str) -> None:
    """Remove a silence rule for PIPELINE."""
    removed = remove_silence(pipeline, path=Path(silence_file))
    if removed:
        click.echo(f"Silence removed for '{pipeline}'.")
    else:
        click.echo(f"No active silence found for '{pipeline}'.")
        raise SystemExit(1)


@silence_command.command("list")
@click.option("--file", "silence_file", default=str(DEFAULT_SILENCE_FILE),
              show_default=True)
def list_silences(silence_file: str) -> None:
    """List all silence rules (active and expired)."""
    rules = load_silences(Path(silence_file))
    if not rules:
        click.echo("No silence rules found.")
        return
    for rule in rules:
        status = "ACTIVE" if rule.is_active() else "EXPIRED"
        reason_str = f" — {rule.reason}" if rule.reason else ""
        click.echo(f"[{status}] {rule.pipeline} until {rule.until}{reason_str}")


@silence_command.command("check")
@click.argument("pipeline")
@click.option("--file", "silence_file", default=str(DEFAULT_SILENCE_FILE),
              show_default=True)
def check_silence(pipeline: str, silence_file: str) -> None:
    """Check whether PIPELINE is currently silenced."""
    if is_silenced(pipeline, path=Path(silence_file)):
        click.echo(f"'{pipeline}' is currently silenced.")
    else:
        click.echo(f"'{pipeline}' is not silenced.")
        raise SystemExit(1)
