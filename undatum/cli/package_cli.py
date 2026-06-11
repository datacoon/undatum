"""CLI commands for Frictionless Data Package generation."""

from typing import Annotated, Optional

import typer

from ..cmds.packager import Packager
from .common import enable_verbose

package_app = typer.Typer(help="Frictionless Data Package commands.")


@package_app.command("create")
def package_create(
    input_files: Annotated[list[str], typer.Argument(help="Input file(s) to package.")],
    output: Annotated[Optional[str], typer.Option(help="Output datapackage.json path.")] = None,
    package_dir: Annotated[
        Optional[str], typer.Option(help="Package directory to materialize.")
    ] = None,
    name: Annotated[Optional[str], typer.Option(help="Package name (slug).")] = None,
    title: Annotated[Optional[str], typer.Option(help="Package title.")] = None,
    description: Annotated[Optional[str], typer.Option(help="Package description.")] = None,
    keywords: Annotated[Optional[str], typer.Option(help="Comma-separated keywords.")] = None,
    licenses: Annotated[
        Optional[str],
        typer.Option(
            help="Licenses (semicolon-separated entries, e.g. 'name=MIT;name=ODC-PDDL-1.0')."
        ),
    ] = None,
    sources: Annotated[
        Optional[str],
        typer.Option(
            help="Sources (semicolon-separated entries, e.g. 'title=World Bank,path=https://...')."
        ),
    ] = None,
    contributors: Annotated[
        Optional[str],
        typer.Option(
            help="Contributors (semicolon-separated entries, e.g. 'title=Jane Doe,email=jane@example.com')."
        ),
    ] = None,
    version: Annotated[Optional[str], typer.Option(help="Package version string.")] = None,
    sample_size: Annotated[
        int, typer.Option(help="Number of sample records to include in metadata inference.")
    ] = 10,
    objects_limit: Annotated[
        int, typer.Option(help="Maximum number of objects to analyze for schema inference.")
    ] = 10000,
    engine: Annotated[
        str, typer.Option(help="Processing engine: 'auto' (default) or 'duckdb'.")
    ] = "auto",
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[
        Optional[str], typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")
    ] = None,
    tagname: Annotated[
        Optional[str], typer.Option(help="XML tag name that contains individual records.")
    ] = None,
    start_line: Annotated[
        int, typer.Option(help="Line number (0-based) to start reading from.")
    ] = 0,
    start_page: Annotated[
        int, typer.Option(help="Page number (0-based) to start from for Excel files.")
    ] = 0,
    format_in: Annotated[
        Optional[str],
        typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl', 'xml')."),
    ] = None,
    autodoc: Annotated[bool, typer.Option(help="Enable AI-powered metadata generation.")] = False,
    lang: Annotated[
        str, typer.Option(help="Language for AI-generated metadata (default: 'English').")
    ] = "English",
    ai_provider: Annotated[
        Optional[str],
        typer.Option(
            help="AI provider to use: 'openai', 'openrouter', 'ollama', 'lmstudio', or 'perplexity'."
        ),
    ] = None,
    ai_model: Annotated[
        Optional[str], typer.Option(help="Model name to use (provider-specific).")
    ] = None,
    ai_base_url: Annotated[
        Optional[str], typer.Option(help="Base URL for AI API (optional).")
    ] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Generate a Frictionless Data Package descriptor."""
    if verbose:
        enable_verbose()

    ai_config = {}
    if ai_model:
        ai_config["model"] = ai_model
    if ai_base_url:
        ai_config["base_url"] = ai_base_url

    options = {
        "output": output,
        "package_dir": package_dir,
        "name": name,
        "title": title,
        "description": description,
        "keywords": keywords,
        "licenses": licenses,
        "sources": sources,
        "contributors": contributors,
        "version": version,
        "sample_size": sample_size,
        "objects_limit": objects_limit,
        "engine": engine,
        "delimiter": delimiter,
        "encoding": encoding,
        "tagname": tagname,
        "start_line": start_line,
        "start_page": start_page,
        "format_in": format_in,
        "autodoc": autodoc,
        "lang": lang,
        "ai_provider": ai_provider,
        "ai_config": ai_config if ai_config else None,
    }
    Packager().create(input_files, options)
