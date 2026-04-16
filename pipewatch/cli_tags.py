"""CLI commands for tag-based pipeline inspection."""
import click
from pipewatch.config import load_config
from pipewatch.tag_filter import filter_pipelines, pipelines_by_tag


@click.group("tags")
def tags_command():
    """Inspect and filter pipelines by tag."""


@tags_command.command("list")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--require", multiple=True, help="Tags that must be present.")
@click.option("--exclude", multiple=True, help="Tags that must be absent.")
def list_by_tags(config_path, require, exclude):
    """List pipelines matching tag constraints."""
    app = load_config(config_path)
    matched = filter_pipelines(
        app.pipelines,
        require=list(require),
        exclude=list(exclude),
    )
    if not matched:
        click.echo("No pipelines match the given tag filters.")
        return
    for p in matched:
        tags = ", ".join(getattr(p, "tags", []) or [])
        click.echo(f"  {p.name}  [{tags}]")


@tags_command.command("index")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
def tag_index(config_path):
    """Show all tags and the pipelines that carry them."""
    app = load_config(config_path)
    index = pipelines_by_tag(app.pipelines)
    if not index:
        click.echo("No tags found.")
        return
    for tag in sorted(index):
        names = ", ".join(index[tag])
        click.echo(f"  {tag}: {names}")
