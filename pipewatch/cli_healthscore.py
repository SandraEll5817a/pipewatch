"""CLI command for displaying pipeline health scores."""
import click
from pipewatch.config import load_config
from pipewatch.history import load_history
from pipewatch.healthscore import compute_health_score


@click.group(name="health-score")
def healthscore_command() -> None:
    """Show health scores for monitored pipelines."""


@healthscore_command.command(name="show")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--history", "history_path", default=".pipewatch_history.json", show_default=True)
@click.option("--min-grade", default=None, help="Only show pipelines at or below this grade (e.g. B).")
def show_scores(config_path: str, history_path: str, min_grade: str | None) -> None:
    """Print health scores for all configured pipelines."""
    grade_order = ["A", "B", "C", "D", "F"]
    app = load_config(config_path)
    any_printed = False

    for pipeline in app.pipelines:
        runs = [
            r for r in load_history(history_path)
            if r.pipeline == pipeline.name
        ]
        score = compute_health_score(pipeline.name, runs)

        if min_grade:
            min_grade_upper = min_grade.upper()
            if min_grade_upper not in grade_order:
                raise click.BadParameter(f"Invalid grade '{min_grade}'. Choose from A B C D F.")
            threshold_idx = grade_order.index(min_grade_upper)
            if grade_order.index(score.grade) < threshold_idx:
                continue

        click.echo(str(score))
        any_printed = True

    if not any_printed:
        click.echo("No pipelines matched the filter.")
