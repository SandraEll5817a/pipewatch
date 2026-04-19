import click
from pipewatch.runbook import load_runbooks, save_runbooks, add_runbook, remove_runbook, RunbookEntry


@click.group(name="runbook")
def runbook_command():
    """Manage pipeline runbooks."""


@runbook_command.command(name="add")
@click.argument("pipeline")
@click.argument("title")
@click.argument("url")
@click.option("--notes", default="", help="Optional notes for this runbook entry.")
@click.option("--db", default="runbooks.json", show_default=True)
def add_cmd(pipeline, title, url, notes, db):
    """Add a runbook entry for a pipeline."""
    entries = load_runbooks(db)
    entry = add_runbook(entries, pipeline, title, url, notes)
    save_runbooks(entries, db)
    click.echo(f"Added runbook '{entry.title}' for pipeline '{pipeline}'.")


@runbook_command.command(name="list")
@click.option("--pipeline", default=None, help="Filter by pipeline name.")
@click.option("--db", default="runbooks.json", show_default=True)
def list_cmd(pipeline, db):
    """List runbook entries."""
    entries = load_runbooks(db)
    if pipeline:
        entries = [e for e in entries if e.pipeline == pipeline]
    if not entries:
        click.echo("No runbook entries found.")
        return
    for e in entries:
        click.echo(f"[{e.pipeline}] {e.title} — {e.url}")
        if e.notes:
            click.echo(f"  Notes: {e.notes}")


@runbook_command.command(name="remove")
@click.argument("pipeline")
@click.argument("title")
@click.option("--db", default="runbooks.json", show_default=True)
def remove_cmd(pipeline, title, db):
    """Remove a runbook entry by pipeline and title."""
    entries = load_runbooks(db)
    before = len(entries)
    entries = remove_runbook(entries, pipeline, title)
    if len(entries) == before:
        click.echo(f"No matching runbook entry found for '{pipeline}' / '{title}'.")
        raise SystemExit(1)
    save_runbooks(entries, db)
    click.echo(f"Removed runbook entry '{title}' for pipeline '{pipeline}'.")


@runbook_command.command(name="clear")
@click.option("--pipeline", default=None, help="Clear only entries for this pipeline.")
@click.option("--db", default="runbooks.json", show_default=True)
def clear_cmd(pipeline, db):
    """Clear runbook entries."""
    entries = load_runbooks(db)
    if pipeline:
        entries = [e for e in entries if e.pipeline != pipeline]
    else:
        entries = []
    save_runbooks(entries, db)
    click.echo("Runbook entries cleared.")
