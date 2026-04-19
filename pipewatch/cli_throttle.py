"""CLI commands for inspecting and clearing the notification throttle state."""
import click
from pathlib import Path
from pipewatch.throttle import load_throttle, save_throttle, ThrottleEntry

_DEFAULT_PATH = Path(".pipewatch_throttle.json")


@click.group(name="throttle")
def throttle_command():
    """Manage notification throttle state."""


@throttle_command.command(name="list")
@click.option("--path", default=str(_DEFAULT_PATH), show_default=True)
def list_throttle(path: str):
    """List current throttle entries."""
    state = load_throttle(Path(path))
    if not state:
        click.echo("No throttle entries.")
        return
    for key, entry in sorted(state.items()):
        click.echo(
            f"{entry.pipeline:30s}  violation={entry.violation_key:20s}  "
            f"count={entry.count}  last_sent={entry.last_sent:.0f}"
        )


@throttle_command.command(name="clear")
@click.option("--path", default=str(_DEFAULT_PATH), show_default=True)
@click.option("--pipeline", default=None, help="Clear only entries for this pipeline.")
def clear_throttle(path: str, pipeline: str):
    """Clear throttle state (all or for a specific pipeline)."""
    p = Path(path)
    state = load_throttle(p)
    if pipeline:
        before = len(state)
        state = {k: v for k, v in state.items() if v.pipeline != pipeline}
        removed = before - len(state)
        click.echo(f"Removed {removed} entries for pipeline '{pipeline}'.")
    else:
        state = {}
        click.echo("Cleared all throttle entries.")
    save_throttle(state, p)
