"""CLI command for the optional local web UI."""

from __future__ import annotations

import os
import sys
from typing import Annotated

import typer

from ..tui.session import DEFAULT_SAMPLE_LIMIT
from ..web.deps import require_web_dependencies
from .common import enable_verbose


def web(
    input_file: Annotated[
        str | None,
        typer.Argument(help="Path to a data file. Opens an empty session if omitted."),
    ] = None,
    host: Annotated[
        str,
        typer.Option(help="Host to bind (default: 127.0.0.1)."),
    ] = "127.0.0.1",
    port: Annotated[
        int,
        typer.Option(help="Port to bind (default: 8765)."),
    ] = 8765,
    limit: Annotated[
        int,
        typer.Option(help="Sample rows to show in the grid (default 200, max 5000)."),
    ] = DEFAULT_SAMPLE_LIMIT,
    delimiter: Annotated[
        str | None,
        typer.Option(help="CSV delimiter character (auto-detected when omitted)."),
    ] = None,
    quotechar: Annotated[
        str | None,
        typer.Option(
            "--quotechar",
            help="CSV quote character (iterabledata default '\"' when omitted).",
        ),
    ] = None,
    encoding: Annotated[
        str | None,
        typer.Option(help="File encoding (e.g. 'utf8', 'latin1')."),
    ] = None,
    format_in: Annotated[
        str | None,
        typer.Option(help="Override input file format detection (e.g. 'csv', 'jsonl')."),
    ] = None,
    engine: Annotated[
        str | None,
        typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'python'."),
    ] = None,
    table: Annotated[
        str | None,
        typer.Option(
            "--table",
            "--sheet",
            help="Table or sheet name for multi-table sources (Excel, SQLite, lakehouse).",
        ),
    ] = None,
    start_page: Annotated[
        int,
        typer.Option(help="Sheet index (0-based) for Excel files."),
    ] = 0,
    trust: Annotated[
        bool,
        typer.Option(
            "--trust",
            help="Acknowledge pickle deserialization risk when reading pickle sources.",
        ),
    ] = False,
    on_error: Annotated[
        str | None,
        typer.Option(
            "--on-error",
            help="Parse-error policy: raise (default), skip, or warn.",
        ),
    ] = None,
    error_log: Annotated[
        str | None,
        typer.Option(
            "--error-log",
            help="Append parse errors as JSONL (use with --on-error skip or warn).",
        ),
    ] = None,
    flatten_nested: Annotated[
        bool,
        typer.Option(
            "--flatten-nested",
            help="Unfold nested dict / array-of-dict fields into dotted paths (e.g. city.lat).",
        ),
    ] = False,
    max_nested_depth: Annotated[
        int | None,
        typer.Option(
            "--max-nested-depth",
            help="With --flatten-nested, maximum nest depth to unfold (engine default 5).",
        ),
    ] = None,
    keep_nested_parents: Annotated[
        bool,
        typer.Option(
            "--keep-nested-parents/--no-keep-nested-parents",
            help="With --flatten-nested, keep parent dict/array fields alongside dotted children.",
        ),
    ] = True,
    open_browser: Annotated[
        bool | None,
        typer.Option("--open/--no-open", help="Open the system browser (default: on a TTY)."),
    ] = None,
    api_key: Annotated[
        str | None,
        typer.Option(help="Optional API key. Also read from UNDATUM_API_KEY."),
    ] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Open a local web UI to preview a sampled dataset.

    Requires the optional FastAPI extra. Binds to 127.0.0.1 by default.
    The grid shows a bounded sample (default 200 rows), not the whole file.

    Examples:
        undatum web data.csv
        undatum web data.parquet --limit 500 --no-open
        undatum web workbook.xlsx --table Sheet2
        undatum web nested.jsonl --flatten-nested --no-open
    """
    if verbose:
        enable_verbose()
    require_web_dependencies()
    from ..web.app import run_web

    options = {
        "delimiter": delimiter,
        "quotechar": quotechar,
        "encoding": encoding,
        "format_in": format_in,
        "engine": engine,
        "table": table,
        "start_page": start_page,
        "trust": trust,
        "on_error": on_error,
        "error_log": error_log,
        "flatten_nested": flatten_nested,
        "max_nested_depth": max_nested_depth,
        "keep_nested_parents": keep_nested_parents,
    }
    key = api_key or os.environ.get("UNDATUM_API_KEY")
    should_open = open_browser if open_browser is not None else bool(sys.stdout.isatty())
    run_web(
        path=input_file,
        options=options,
        limit=limit,
        host=host,
        port=port,
        open_browser=should_open,
        api_key=key,
    )
