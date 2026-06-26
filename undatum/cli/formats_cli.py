"""CLI commands for inspecting supported data formats.

Backed by iterabledata's machine-readable catalog (``iterable.catalog``) and
capability reporting (``iterable.helpers.capabilities``), so the list always
reflects the formats the underlying engine can actually handle.
"""

import json
import logging
from typing import Annotated, Optional

import typer
from rich.table import Table

from .common import console, enable_verbose

logger = logging.getLogger(__name__)

formats_app = typer.Typer(help="Inspect supported data formats and their capabilities.")


def _bool_mark(value: Optional[bool]) -> str:
    if value is True:
        return "yes"
    if value is False:
        return "no"
    return "?"


# Capability columns shown (in order) when ``--capabilities`` is passed to list.
_CAPABILITY_COLUMNS = [
    ("bulk_read", "Bulk R"),
    ("bulk_write", "Bulk W"),
    ("streaming", "Stream"),
    ("totals", "Totals"),
    ("tables", "Tables"),
    ("nested", "Nested"),
]


@formats_app.command(name="list")
def formats_list(
    writable: Annotated[bool, typer.Option(help="Show only formats that support writing.")] = False,
    readable_only: Annotated[
        bool, typer.Option("--read-only", help="Show only read-only formats.")
    ] = False,
    capabilities: Annotated[
        bool,
        typer.Option(
            "--capabilities", "-c", help="Show the full runtime capability matrix per format."
        ),
    ] = False,
    as_json: Annotated[bool, typer.Option("--json", help="Output as JSON.")] = False,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """List all supported data formats and whether they are readable/writable.

    Capabilities come from iterabledata's runtime capability reporting
    (``iterable.helpers.capabilities``), so they reflect what the underlying
    engine (and installed optional dependencies) can actually do.

    Examples:
        # List every supported format
        undatum formats list

        # List only writable formats
        undatum formats list --writable

        # Show the full capability matrix (bulk, streaming, totals, tables, nested)
        undatum formats list --capabilities

        # Machine-readable output (includes the capabilities dict)
        undatum formats list --json
    """
    if verbose:
        enable_verbose()

    from iterable.catalog import describe_format, list_formats

    rows = []
    for fmt_id in list_formats():
        try:
            desc = describe_format(fmt_id)
        except Exception as e:  # noqa: BLE001
            logger.debug("Skipping format %s: %s", fmt_id, e)
            continue
        is_writable = bool(desc.get("writable"))
        if writable and not is_writable:
            continue
        if readable_only and is_writable:
            continue
        rows.append(
            {
                "id": desc.get("id", fmt_id),
                "writable": is_writable,
                "text": bool(desc.get("text")),
                "extra": desc.get("extra"),
                "description": desc.get("description") or "",
                "capabilities": desc.get("capabilities") or {},
            }
        )

    if as_json:
        console.print_json(json.dumps(rows))
        return

    table = Table(show_header=True, header_style="bold")
    table.add_column("Format")
    table.add_column("Read")
    table.add_column("Write")
    table.add_column("Text")
    if capabilities:
        for _key, label in _CAPABILITY_COLUMNS:
            table.add_column(label)
    else:
        table.add_column("Extra")
        table.add_column("Description")
    for row in rows:
        cells = [row["id"], "yes", _bool_mark(row["writable"]), _bool_mark(row["text"])]
        if capabilities:
            caps = row["capabilities"]
            cells.extend(_bool_mark(caps.get(key)) for key, _label in _CAPABILITY_COLUMNS)
        else:
            desc = row["description"]
            if len(desc) > 50:
                desc = desc[:50] + "..."
            cells.extend([row["extra"] or "", desc])
        table.add_row(*cells)
    console.print(table)
    console.print(f"\n[bold]{len(rows)}[/bold] formats")


@formats_app.command()
def describe(
    format_id: Annotated[str, typer.Argument(help="Format id or alias (e.g. 'csv', 'parquet').")],
    as_json: Annotated[bool, typer.Option("--json", help="Output as JSON.")] = False,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Show detailed metadata and capabilities for a single format.

    Examples:
        undatum formats describe parquet
        undatum formats describe geojson --json
    """
    if verbose:
        enable_verbose()

    from iterable.catalog import describe_format

    try:
        desc = describe_format(format_id)
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=1) from e

    if as_json:
        console.print_json(json.dumps(desc, default=str))
        return

    console.print(f"[bold]{desc.get('id')}[/bold]")
    if desc.get("description"):
        console.print(desc["description"])
    aliases = desc.get("aliases") or []
    if aliases:
        console.print(f"[dim]Aliases:[/dim] {', '.join(aliases)}")
    console.print(f"[dim]Writable:[/dim] {_bool_mark(desc.get('writable'))}")
    console.print(f"[dim]Text format:[/dim] {_bool_mark(desc.get('text'))}")
    if desc.get("extra"):
        console.print(f"[dim]Optional extra:[/dim] {desc['extra']}")
    if desc.get("doc_url"):
        console.print(f"[dim]Docs:[/dim] {desc['doc_url']}")

    limitations = desc.get("limitations") or []
    if limitations:
        console.print("[dim]Limitations:[/dim]")
        for item in limitations:
            console.print(f"  - {item}")

    caps = desc.get("capabilities") or {}
    if caps:
        table = Table(show_header=True, header_style="bold", title="Capabilities")
        table.add_column("Capability")
        table.add_column("Value")
        for key in sorted(caps):
            table.add_row(key, _bool_mark(caps[key]))
        console.print(table)


@formats_app.command()
def export(
    output: Annotated[
        str, typer.Option(help="Output file path. Prints to stdout if not given.")
    ] = None,
    no_capabilities: Annotated[
        bool, typer.Option("--no-capabilities", help="Exclude runtime capabilities.")
    ] = False,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Export the full format catalog as JSON.

    Examples:
        undatum formats export
        undatum formats export --output formats.json
    """
    if verbose:
        enable_verbose()

    from iterable.catalog import export_catalog

    payload = export_catalog(format="json", include_capabilities=not no_capabilities)
    if output:
        with open(output, "w", encoding="utf-8") as f:
            f.write(payload)
        console.print(f"[green]Catalog written to {output}[/green]")
    else:
        console.print_json(payload)
