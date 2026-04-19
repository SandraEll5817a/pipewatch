"""CLI commands for managing pipeline run retention."""
import click
from pipewatch.config import load_config
from pipewatch.history import load_history, save_history
from pipewatch.retention import RetentionPolicy, apply_retention_all


@click.group(name="retention")
def retention_command() -> None:
    """Manage run history retention."""


@retention_command.command(name="prune")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--max-age-days", default=None, type=int, help="Remove runs older than N days.")
@click.option("--max-runs", default=None, type=int, help="Keep only the N most recent runs per pipeline.")
@click.option("--dry-run", is_flag=True, default=False, help="Show what would be pruned without saving.")
def prune(
    config_path: str,
    max_age_days: int | None,
    max_runs: int | None,
    dry_run: bool,
) -> None:
    """Prune old pipeline run history."""
    if max_age_days is None and max_runs is None:
        click.echo("Specify --max-age-days and/or --max-runs.", err=True)
        raise SystemExit(1)

    cfg = load_config(config_path)
    policy = RetentionPolicy(max_age_days=max_age_days, max_runs=max_runs)

    history: dict = {}
    for pipeline in cfg.pipelines:
        history[pipeline.name] = load_history(pipeline.name)

    pruned_history, results = apply_retention_all(history, policy)

    total_pruned = sum(r.pruned_count for r in results)
    for result in results:
        if result.pruned_count > 0:
            click.echo(str(result))

    if total_pruned == 0:
        click.echo("Nothing to prune.")
        return

    if dry_run:
        click.echo(f"Dry run: would prune {total_pruned} run(s).")
        return

    for pipeline in cfg.pipelines:
        save_history(pipeline.name, pruned_history[pipeline.name])

    click.echo(f"Pruned {total_pruned} run(s).")
