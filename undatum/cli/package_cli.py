"""CLI commands for Frictionless Data Package generation."""

from typing import Annotated, Optional

import typer

from ..cmds.packager import Packager
from .common import enable_verbose

package_app = typer.Typer(help="Frictionless Data Package commands.")


def _build_package_options(
    *,
    output: Optional[str] = None,
    package_dir: Optional[str] = None,
    name: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    keywords: Optional[str] = None,
    licenses: Optional[str] = None,
    sources: Optional[str] = None,
    contributors: Optional[str] = None,
    version: Optional[str] = None,
    sample_size: int = 10,
    objects_limit: int = 10000,
    engine: str = "auto",
    delimiter: str = ",",
    encoding: Optional[str] = None,
    tagname: Optional[str] = None,
    start_line: int = 0,
    start_page: int = 0,
    format_in: Optional[str] = None,
    autodoc: bool = False,
    lang: str = "English",
    ai_provider: Optional[str] = None,
    ai_model: Optional[str] = None,
    ai_base_url: Optional[str] = None,
    zip_output: Optional[str] = None,
    quiet: bool = False,
) -> dict:
    ai_config = {}
    if ai_model:
        ai_config["model"] = ai_model
    if ai_base_url:
        ai_config["base_url"] = ai_base_url

    return {
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
        "zip": zip_output,
        "quiet": quiet,
    }


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
    zip_output: Annotated[
        Optional[str],
        typer.Option(
            "--zip",
            help="Create a ZIP archive of the package directory (requires --package-dir).",
        ),
    ] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Generate a Frictionless Data Package descriptor."""
    if verbose:
        enable_verbose()

    options = _build_package_options(
        output=output,
        package_dir=package_dir,
        name=name,
        title=title,
        description=description,
        keywords=keywords,
        licenses=licenses,
        sources=sources,
        contributors=contributors,
        version=version,
        sample_size=sample_size,
        objects_limit=objects_limit,
        engine=engine,
        delimiter=delimiter,
        encoding=encoding,
        tagname=tagname,
        start_line=start_line,
        start_page=start_page,
        format_in=format_in,
        autodoc=autodoc,
        lang=lang,
        ai_provider=ai_provider,
        ai_model=ai_model,
        ai_base_url=ai_base_url,
        zip_output=zip_output,
    )
    Packager().create(input_files, options)


@package_app.command("add-resource")
def package_add_resource(
    package_file: Annotated[
        str, typer.Argument(help="Existing datapackage.json to extend.")
    ],
    input_files: Annotated[list[str], typer.Argument(help="Input file(s) to add.")],
    package_dir: Annotated[
        Optional[str],
        typer.Option(help="Package directory containing data files (defaults to descriptor dir)."),
    ] = None,
    sample_size: Annotated[int, typer.Option(help="Sample size for metadata inference.")] = 10,
    objects_limit: Annotated[int, typer.Option(help="Maximum objects to analyze.")] = 10000,
    engine: Annotated[str, typer.Option(help="Processing engine.")] = "auto",
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[Optional[str], typer.Option(help="File encoding.")] = None,
    tagname: Annotated[Optional[str], typer.Option(help="XML record tag name.")] = None,
    start_line: Annotated[int, typer.Option(help="Line number (0-based) to start reading from.")] = 0,
    start_page: Annotated[int, typer.Option(help="Excel start page (0-based).")] = 0,
    format_in: Annotated[Optional[str], typer.Option(help="Override input format.")] = None,
    autodoc: Annotated[bool, typer.Option(help="Enable AI-powered metadata generation.")] = False,
    lang: Annotated[str, typer.Option(help="Language for AI metadata.")] = "English",
    ai_provider: Annotated[Optional[str], typer.Option(help="AI provider name.")] = None,
    ai_model: Annotated[Optional[str], typer.Option(help="AI model name.")] = None,
    ai_base_url: Annotated[Optional[str], typer.Option(help="AI API base URL.")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Add resources to an existing Frictionless Data Package descriptor."""
    if verbose:
        enable_verbose()

    options = _build_package_options(
        package_dir=package_dir,
        sample_size=sample_size,
        objects_limit=objects_limit,
        engine=engine,
        delimiter=delimiter,
        encoding=encoding,
        tagname=tagname,
        start_line=start_line,
        start_page=start_page,
        format_in=format_in,
        autodoc=autodoc,
        lang=lang,
        ai_provider=ai_provider,
        ai_model=ai_model,
        ai_base_url=ai_base_url,
    )
    Packager().add_resource(package_file, input_files, options)


@package_app.command("validate")
def package_validate(
    package_file: Annotated[str, typer.Argument(help="Path to datapackage.json.")],
    limit_rows: Annotated[
        Optional[int], typer.Option(help="Limit rows validated per resource.")
    ] = None,
    check_data: Annotated[
        bool, typer.Option(help="Validate resource data in addition to metadata.")
    ] = True,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Validate a Frictionless Data Package descriptor."""
    if verbose:
        enable_verbose()

    options = {
        "limit_rows": limit_rows,
        "check_data": check_data,
        "quiet": False,
    }
    Packager().validate(package_file, options)
