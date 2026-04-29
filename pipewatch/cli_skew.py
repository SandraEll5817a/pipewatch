"""CLI command for detecting timing skew between pipeline pairs."""
from __future__ import annotations

import sys
from typing import List, Tuple

import click

from pipewatch.config import load_config
from pipewatch.history import load_history
from pipewatch.skew import detect_skew


@click.group(name="skew")
def skew_command() -> None:
    """Detect timing skew between paired pipelines."""


@skew_command.command(name="check")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--threshold", default=300.0, show_default=True, help="Skew threshold in seconds.")
@click.argument("pairs", nargs=-1, metavar="A:B ...")
def check_skew(
    config_path: str,
    threshold: float,
    pairs: Tuple[str, ...],
) -> None:
    """Check skew between pipeline pairs supplied as A:B arguments."""
    if not pairs:
        click.echo("No pipeline pairs specified. Use A:B format.", err=True)
        sys.exit(1)

    app_config = load_config(config_path)
    skewed: List[str] = []

    for pair in pairs:
        if ":" not in pair:
            click.echo(f"Invalid pair format '{pair}'. Expected A:B.", err=True)
            sys.exit(1)
        name_a, name_b = pair.split(":", 1)
        runs_a = load_history(name_a)
        runs_b = load_history(name_b)
        result = detect_skew(name_a, name_b, runs_a, runs_b, threshold_seconds=threshold)
        status = "SKEWED" if result.is_skewed else "OK"
        click.echo(f"[{status}] {result}")
        if result.is_skewed:
            skewed.append(pair)

    if skewed:
        click.echo(f"\n{len(skewed)} skewed pair(s) detected.", err=True)
        sys.exit(1)

    click.echo("\nAll pairs within skew threshold.")
