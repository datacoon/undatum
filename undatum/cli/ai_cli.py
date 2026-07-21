"""AI-assisted commands backed by iterabledata's ``iterable.ai`` stack.

These commands expose capabilities provided by the iterabledata foundation that
undatum did not surface before: block-based dataset documentation with metadata
enrichment and PII-safe sampling, natural-language to filter translation,
conversion planning, and transform suggestion. Provider support (OpenAI,
Anthropic, Gemini, Azure, OpenRouter, Ollama, LM Studio, Perplexity) comes from
iterabledata directly.

Provider/model/credentials default to undatum's existing AI configuration
(``undatum.yaml`` / ``~/.undatum/config.yaml`` / environment) and can be
overridden per-invocation with CLI options.
"""

import json
import logging
from typing import Annotated, Optional

import typer

from ..ai.config import get_ai_config
from .common import console, enable_verbose

logger = logging.getLogger(__name__)

ai_app = typer.Typer(help="AI-assisted documentation, filtering, planning, and suggestions.")


def _resolve_ai(
    provider: Optional[str],
    model: Optional[str],
    api_key: Optional[str],
    base_url: Optional[str],
) -> dict:
    """Merge CLI AI options with undatum's configured defaults.

    CLI values take precedence over config-file/environment values.
    """
    cfg = get_ai_config()
    resolved_provider = provider or cfg.get("provider") or "openai"
    resolved_model = model or cfg.get("model")
    resolved_api_key = api_key or cfg.get("api_key")
    # Provider-specific base URLs from undatum config.
    if base_url:
        resolved_base_url = base_url
    elif resolved_provider == "ollama":
        resolved_base_url = cfg.get("ollama_base_url") or cfg.get("base_url")
    elif resolved_provider == "lmstudio":
        resolved_base_url = cfg.get("lmstudio_base_url") or cfg.get("base_url")
    else:
        resolved_base_url = cfg.get("base_url")
    return {
        "provider": resolved_provider,
        "model": resolved_model,
        "api_key": resolved_api_key,
        "base_url": resolved_base_url,
    }


@ai_app.command()
def doc(
    filename: Annotated[str, typer.Argument(help="Input data file or source to document.")],
    output: Annotated[
        str, typer.Option(help="Output file path. Prints to stdout if not given.")
    ] = None,
    output_format: Annotated[
        str, typer.Option("--format", help="Output format: markdown, json, yaml, html, text.")
    ] = "markdown",
    blocks: Annotated[
        str,
        typer.Option(help="Comma-separated documentation blocks (e.g. 'general,schema,quality')."),
    ] = None,
    language: Annotated[str, typer.Option(help="Language for generated content.")] = "English",
    pii_detect: Annotated[
        bool, typer.Option(help="Detect PII fields (requires metacrafter).")
    ] = False,
    semantic_types: Annotated[
        bool, typer.Option(help="Detect semantic types (requires metacrafter).")
    ] = False,
    provider: Annotated[str, typer.Option(help="LLM provider override.")] = None,
    model: Annotated[str, typer.Option(help="Model name override.")] = None,
    api_key: Annotated[str, typer.Option(help="API key override.")] = None,
    base_url: Annotated[str, typer.Option(help="Base URL override (local providers).")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Generate AI dataset documentation using iterabledata's block-based engine.

    Supports metadata enrichment (keywords, geographic/temporal coverage,
    language, themes), PII-safe sampling, and many providers (OpenAI, Anthropic,
    Gemini, Azure, OpenRouter, Ollama, LM Studio, Perplexity).

    Examples:
        undatum ai doc data.csv
        undatum ai doc data.parquet --format json --blocks general,schema,quality
        undatum ai doc data.csv --provider anthropic --model claude-3-5-sonnet-latest
    """
    if verbose:
        enable_verbose()

    from iterable.ai import doc as ai_doc

    from ..ai.doc_enrichment import enrich_blocks_result, prepare_doc_source, restore_source_filename

    resolved = _resolve_ai(provider, model, api_key, base_url)
    block_list = [b.strip() for b in blocks.split(",") if b.strip()] if blocks else None

    doc_path, cleanup, field_hints, field_names = prepare_doc_source(filename)
    try:
        if block_list and "schema" in block_list:
            result = ai_doc.generate_blocks(
                doc_path,
                blocks=block_list,
                provider=resolved["provider"],
                model=resolved["model"],
                api_key=resolved["api_key"],
                base_url=resolved["base_url"],
                language=language,
                pii_detect=pii_detect,
                semantic_types=semantic_types,
            )
            restore_source_filename(result, filename, doc_path)
            enrich_blocks_result(
                result,
                field_hints,
                known_names=field_names,
                requested_blocks=block_list,
            )
            if output_format != "json":
                result = result["full_document_markdown"]
        else:
            result = ai_doc.generate(
                doc_path,
                provider=resolved["provider"],
                model=resolved["model"],
                api_key=resolved["api_key"],
                base_url=resolved["base_url"],
                format=output_format,
                language=language,
                pii_detect=pii_detect,
                semantic_types=semantic_types,
                blocks=block_list,
            )
    finally:
        cleanup()

    if isinstance(result, (dict, list)):
        text = json.dumps(result, indent=2, default=str)
    else:
        text = str(result)

    if output:
        with open(output, "w", encoding="utf-8") as f:
            f.write(text)
        console.print(f"[green]Documentation written to {output}[/green]")
    else:
        print(text)


@ai_app.command(name="filter")
def ai_filter(
    expression: Annotated[
        str, typer.Argument(help="Natural-language or DSL filter (e.g. 'rows where age > 30').")
    ],
    filename: Annotated[
        str, typer.Argument(help="Optional data file (used for schema context and --apply).")
    ] = None,
    apply: Annotated[
        bool, typer.Option(help="Apply the translated filter and output matching rows as JSONL.")
    ] = False,
    output: Annotated[str, typer.Option(help="Output file for --apply (default stdout).")] = None,
    provider: Annotated[str, typer.Option(help="LLM provider override.")] = None,
    model: Annotated[str, typer.Option(help="Model name override.")] = None,
    api_key: Annotated[str, typer.Option(help="API key override.")] = None,
    base_url: Annotated[str, typer.Option(help="Base URL override (local providers).")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Translate natural language (or simple DSL) into a filter expression.

    Simple DSL (e.g. ``age > 30``) is parsed without an LLM; natural language
    requires a configured provider. With ``--apply`` and a file, the filter is
    executed and matching rows are written as JSONL.

    Examples:
        undatum ai filter "status == 'active'"
        undatum ai filter "rows where amount over 1000" data.csv --provider openai
        undatum ai filter "age > 30" data.csv --apply --output adults.jsonl
    """
    if verbose:
        enable_verbose()

    from iterable.ai import filter as ai_flt

    schema = None
    if filename:
        try:
            from iterable.ops import schema as schema_ops

            schema = schema_ops.infer(filename, detect_constraints=False)
        except Exception as e:  # noqa: BLE001
            logger.debug("Schema inference failed for %s: %s", filename, e)

    resolved = _resolve_ai(provider, model, api_key, base_url)
    result = ai_flt.translate_filter(
        expression,
        schema,
        provider=resolved["provider"] if (provider or expression) else None,
        model=resolved["model"],
        api_key=resolved["api_key"],
        base_url=resolved["base_url"],
    )

    if not apply:
        console.print_json(json.dumps(result, default=str))
        return

    if not filename:
        console.print("[red]--apply requires a data file argument[/red]")
        raise typer.Exit(code=1)

    from ..common.s3_iterable import open_path

    ast = result["ast"]
    out_file = open(output, "w", encoding="utf-8") if output else None
    try:
        import sys

        sink = out_file or sys.stdout
        source = open_path(filename, mode="r")
        try:
            count = 0
            for row in ai_flt.apply_ast(source, ast):
                sink.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
                count += 1
        finally:
            if hasattr(source, "close"):
                source.close()
        logger.info("Filtered %d rows", count)
    finally:
        if out_file:
            out_file.close()


@ai_app.command()
def plan(
    source: Annotated[str, typer.Argument(help="Source file/path.")],
    target: Annotated[str, typer.Argument(help="Target file/path.")],
    use_llm: Annotated[
        bool, typer.Option(help="Use LLM reasoning in addition to catalog metadata.")
    ] = False,
    provider: Annotated[str, typer.Option(help="LLM provider override.")] = None,
    model: Annotated[str, typer.Option(help="Model name override.")] = None,
    api_key: Annotated[str, typer.Option(help="API key override.")] = None,
    base_url: Annotated[str, typer.Option(help="Base URL override (local providers).")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Produce a declarative conversion plan between two formats (no conversion).

    Examples:
        undatum ai plan data.csv data.parquet
        undatum ai plan data.json data.geojson --use-llm
    """
    if verbose:
        enable_verbose()

    from iterable.ai import plan as ai_plan

    resolved = _resolve_ai(provider, model, api_key, base_url)
    result = ai_plan.plan_conversion(
        source,
        target,
        use_llm=use_llm,
        provider=resolved["provider"],
        model=resolved["model"],
        api_key=resolved["api_key"],
        base_url=resolved["base_url"],
    )
    console.print_json(json.dumps(result, default=str))


@ai_app.command()
def suggest(
    filename: Annotated[str, typer.Argument(help="Input data file.")],
    goal: Annotated[
        str, typer.Argument(help="Natural-language description of the desired result.")
    ],
    provider: Annotated[str, typer.Option(help="LLM provider override.")] = None,
    model: Annotated[str, typer.Option(help="Model name override.")] = None,
    api_key: Annotated[str, typer.Option(help="API key override.")] = None,
    base_url: Annotated[str, typer.Option(help="Base URL override (local providers).")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
):
    """Suggest a declarative transform spec for a dataset and goal (no changes applied).

    Examples:
        undatum ai suggest data.csv "drop empty columns and rename id to user_id"
    """
    if verbose:
        enable_verbose()

    from iterable.ai import suggest as ai_suggest

    resolved = _resolve_ai(provider, model, api_key, base_url)
    result = ai_suggest.suggest_transform(
        filename,
        goal,
        provider=resolved["provider"],
        model=resolved["model"],
        api_key=resolved["api_key"],
        base_url=resolved["base_url"],
    )
    console.print_json(json.dumps(result, default=str))
