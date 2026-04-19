"""CLI commands for viewing the audit log."""
from __future__ import annotations

from pathlib import Path

import click

from pipewatch.audit import DEFAULT_AUDIT_FILE, filter_audit, load_audit


@click.group(name="audit")
def audit_command() -> None:
    """View pipeline audit log."""


@audit_command.command(name="list")
@click.option("--pipeline", default=None, help="Filter by pipeline name.")
@click.option("--event-type", default=None, help="Filter by event type.")
@click.option("--limit", default=20, show_default=True, help="Max entries to show.")
@click.option("--file", "audit_file", default=str(DEFAULT_AUDIT_FILE), show_default=True)
def list_audit(
    pipeline: str | None,
    event_type: str | None,
    limit: int,
    audit_file: str,
) -> None:
    """List recent audit events."""
    events = load_audit(Path(audit_file))
    events = filter_audit(events, pipeline=pipeline, event_type=event_type)
    events = events[-limit:]
    if not events:
        click.echo("No audit events found.")
        return
    for e in events:
        status = "OK" if e.healthy else "FAIL"
        click.echo(f"[{e.timestamp.isoformat()}] {e.pipeline} | {e.event_type} | {status} | {e.message}")


@audit_command.command(name="clear")
@click.option("--file", "audit_file", default=str(DEFAULT_AUDIT_FILE), show_default=True)
@click.confirmation_option(prompt="Clear all audit events?")
def clear_audit(audit_file: str) -> None:
    """Clear the audit log."""
    p = Path(audit_file)
    if p.exists():
        p.unlink()
    click.echo("Audit log cleared.")
