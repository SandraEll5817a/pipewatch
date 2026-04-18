"""CLI command for pipeline failure correlation analysis."""
import click
from pipewatch.config import load_config
from pipewatch.history import load_history
from pipewatch.correlation import correlate_failures


@click.group(name="correlation")
def correlation_command():
    """Analyse co-occurring pipeline failures."""


@correlation_command.command(name="check")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--history", "history_path", default="pipewatch_history.json", show_default=True)
@click.option("--min-rate", default=0.5, show_default=True, help="Minimum co-failure rate (0-1).")
@click.option("--bucket", default=300, show_default=True, help="Time bucket size in seconds.")
@click.option("--json", "as_json", is_flag=True, default=False)
def check_correlation(config_path, history_path, min_rate, bucket, as_json):
    """Show pipeline pairs that frequently fail together."""
    import json as _json

    load_config(config_path)  # validate config exists
    runs = load_history(history_path)

    if not runs:
        click.echo("No history available for correlation analysis.")
        raise SystemExit(0)

    results = correlate_failures(runs, min_rate=min_rate, bucket_seconds=bucket)

    if not results:
        click.echo("No correlated failures found above the threshold.")
        raise SystemExit(0)

    if as_json:
        click.echo(_json.dumps([r.to_dict() for r in results], indent=2))
    else:
        click.echo(f"Found {len(results)} correlated failure pair(s):\n")
        for r in results:
            click.echo(f"  {r}")

    raise SystemExit(0)
