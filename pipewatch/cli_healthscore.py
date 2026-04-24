import click
from pipewatch.config import load_config
from pipewatch.history import load_history
from pipewatch.healthscore import compute_health_score


@click.group(name="healthscore")
def healthscore_command():
    """View pipeline health scores."""


@healthscore_command.command(name="show")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--pipeline", default=None, help="Filter to a single pipeline")
@click.option("--min-runs", default=5, show_default=True, help="Minimum runs required")
def show_scores(config_path: str, pipeline: str | None, min_runs: int) -> None:
    """Display health scores for all (or one) pipeline."""
    app = load_config(config_path)
    pipelines = app.pipelines
    if pipeline:
        pipelines = [p for p in pipelines if p.name == pipeline]
        if not pipelines:
            click.echo(f"Pipeline '{pipeline}' not found.", err=True)
            raise SystemExit(1)

    any_low = False
    for p in pipelines:
        history = load_history(p.name)
        score = compute_health_score(history, min_runs=min_runs)
        flag = "" if score.score >= 80 else " [!]"
        if score.score < 80:
            any_low = True
        click.echo(
            f"{p.name}: {score} (runs={score.run_count}, "
            f"grade={score.grade}){flag}"
        )

    raise SystemExit(1 if any_low else 0)
