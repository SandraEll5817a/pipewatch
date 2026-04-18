"""CLI commands for pipeline watchdog checks."""
from __future__ import annotations

import sys
from typing import List

import click

from pipewatch.config import load_config
from pipewatch.history import load_history
from pipewatch.watchdog import check_all_watchdogs


@click.group(name="watchdog")
def watchdog_command() -> None:
    """Monitor pipelines for missed or overdue runs."""


@watchdog_command.command(name="check")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--history", "history_path", default=".pipewatch_history.json", show_default=True)
def check_watchdogs(config_path: str, history_path: str) -> None:
    """Check all pipelines for overdue runs based on watchdog config."""
    app_config = load_config(config_path)
    runs = load_history(history_path)

    watchdog_configs = [
        {"name": p.name, "interval_minutes": p.watchdog_interval_minutes}
        for p in app_config.pipelines
        if getattr(p, "watchdog_interval_minutes", None) is not None
    ]

    if not watchdog_configs:
        click.echo("No pipelines have watchdog_interval_minutes configured.")
        sys.exit(0)

    results = check_all_watchdogs(watchdog_configs, runs)
    stale = [r for r in results if r.stale]

    for r in results:
        status = click.style("STALE", fg="red") if r.stale else click.style("OK", fg="green")
        click.echo(f"  [{status}] {r}")

    if stale:
        click.echo(f"\n{len(stale)} pipeline(s) overdue.")
        sys.exit(1)
    else:
        click.echo("\nAll pipelines running on schedule.")
        sys.exit(0)
