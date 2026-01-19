#!/usr/bin/env python
"""Core module providing CLI commands for the undatum package.

This module defines the main CLI interface using Typer, including all
command handlers for data conversion, analysis, validation, and more.
"""
import glob
import logging
from typing import Annotated, Optional

import typer

from .cmds.analyzer import Analyzer
from .cmds.cat import Cat
from .cmds.converter import Converter
from .cmds.counter import Counter
from .cmds.deduplicator import Deduplicator
from .cmds.doc import Documenter
from .cmds.differ import Differ
from .cmds.enumerator import Enumerator
from .cmds.excluder import Excluder
from .cmds.exploder import Exploder
from .cmds.filler import Filler
from .cmds.fixlengths import FixLengths
from .cmds.formatter import Formatter
from .cmds.head import Head
from .cmds.ingester import Ingester
from .cmds.joiner import Joiner
from .cmds.query import DataQuery
from .cmds.renamer import Renamer
from .cmds.replacer import Replacer
from .cmds.reverser import Reverser
from .cmds.sampler import Sampler
from .cmds.schemer import Schemer
from .cmds.searcher import Searcher
from .cmds.selector import Selector
from .cmds.slicer import Slicer
from .cmds.sniffer import Sniffer
from .cmds.sorter import Sorter
from .cmds.statistics import StatProcessor
from .cmds.table import TableFormatter
from .cmds.tail import Tail
from .cmds.textproc import TextProcessor
from .cmds.transformer import Transformer
from .cmds.transposer import Transposer
from .cmds.validator import Validator

DEFAULT_BATCH_SIZE = 1000

app = typer.Typer()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)


def enable_verbose():
    """Enable verbose logging."""
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO)

@app.command()
def convert(
    input_file: Annotated[str, typer.Argument(help="Path to input file to convert.")],
    output: Annotated[str, typer.Argument(help="Path to output file.")],
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    compression: Annotated[str, typer.Option(help="Compression type (e.g., 'brotli', 'gzip', 'xz').")] = 'brotli',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = 'utf8',
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    flatten_data: Annotated[bool, typer.Option(help="Flatten nested data structures into flat records.")] = False,
    prefix_strip: Annotated[bool, typer.Option(help="Strip XML namespace prefixes from element names.")] = True,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names to include in output.")] = None,
    start_line: Annotated[int, typer.Option(help="Line number (0-based) to start reading from.")] = 0,
    skip_end_rows: Annotated[int, typer.Option(help="Number of rows to skip at the end of the file.")] = 0,
    start_page: Annotated[int, typer.Option(help="Page number (0-based) to start from for Excel files.")] = 0,
    tagname: Annotated[str, typer.Option(help="XML tag name that contains individual records.")] = None,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl', 'xml').")] = None,
    format_out: Annotated[str, typer.Option(help="Override output file format (e.g., 'csv', 'jsonl', 'parquet').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False
):
    """Convert one file to another format.

    Supports conversion between XML, CSV, JSON, JSONL, BSON, Parquet, ORC, and AVRO formats.
    """
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'compression': compression,
        'flatten': flatten_data,
        'encoding': encoding,
        'prefix_strip': prefix_strip,
        'start_line': start_line,
        'skip_end_rows': skip_end_rows,
        'start_page': start_page,
        'tagname': tagname,
        'fields': fields,
        'format_in': format_in,
        'format_out': format_out,
        'zipfile': zipfile
    }
    acmd = Converter()
    acmd.convert(input_file, output, options)


@app.command()
def uniq(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names to extract unique values from.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype: Annotated[str, typer.Option(help="Override file type detection (e.g., 'csv', 'jsonl').")] = None,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")] = "auto"
):
    """Extract all unique values from specified field(s).

    Returns unique values or unique combinations if multiple fields are specified.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'fields': fields,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': filetype,
        'engine': engine
    }
    acmd = Selector()
    acmd.uniq(input_file, options)


@app.command()
def headers(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    fields: Annotated[str, typer.Option(help="Field filter (kept for API compatibility, not currently used).")] = None,  # pylint: disable=unused-argument
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    limit: Annotated[int, typer.Option(help="Maximum number of records to scan for field detection.")] = 10000,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl', 'xml').")] = None,
    format_out: Annotated[str, typer.Option(help="Override output format (e.g., 'csv', 'json').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    filter_expr: Annotated[str, typer.Option(help="Filter expression (kept for API compatibility, not currently used).")] = None  # pylint: disable=unused-argument
):
    """Returns fieldnames of the file. Supports XML, CSV, JSON, BSON.

    Scans the input file and returns all detected field/column names.
    """
    if verbose:
        enable_verbose()
    # fields and filter_expr kept for API compatibility but not currently used
    options = {
        'output': output,
        'delimiter': delimiter,
        'encoding': encoding,
        'limit': limit,
        'format_in': format_in,
        'format_out': format_out,
        'zipfile': zipfile
    }
    acmd = Selector()
    acmd.headers(input_file, options)

@app.command()
def stats(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    dictshare: Annotated[int, typer.Option(help="Dictionary share threshold (0-100) for type detection.")] = None,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None,
    format_out: Annotated[str, typer.Option(help="Override output format (e.g., 'json', 'yaml').")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    checkdates: Annotated[bool, typer.Option(help="Enable automatic date field detection.")] = True,
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    progress: Annotated[bool, typer.Option(help="Show progress bar (default: True).")] = True,
    no_progress: Annotated[bool, typer.Option(help="Disable progress bar (for non-interactive use).")] = False,
    engine: Annotated[str, typer.Option(help="Engine to use for statistics computation: 'auto' (detect), 'duckdb' (DuckDB engine), or 'iterable' (row-by-row).")] = 'auto'
):
    """Generate detailed statistics about a dataset.

    Provides field types, uniqueness counts, min/max/average lengths,
    and optional date field detection.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'dictshare': dictshare,
        'zipfile': zipfile,
        'format_in': format_in,
        'format_out': format_out,
        'delimiter': delimiter,
        'checkdates': checkdates,
        'encoding': encoding,
        'verbose': verbose,
        'progress': progress if not no_progress else False,
        'no_progress': no_progress,
        'engine': engine
    }
    acmd = StatProcessor(nodates=not checkdates)
    acmd.stats(input_file, options)


@app.command()
def flatten(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = 'utf8',
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'jsonl', 'xml').")] = None,
    filter_expr: Annotated[str, typer.Option(help="Filter expression to apply before flattening.")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False
):
    """Flatten nested data records into one value per row.

    Converts nested structures (arrays, objects) into flat records.
    """
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'output': output,
        'encoding': encoding,
        'format_in': format_in,
        'filter': filter_expr
    }
    acmd = TextProcessor()
    acmd.flatten(input_file, options)


@app.command()
def frequency(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names to calculate frequency for.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype: Annotated[str, typer.Option(help="Override file type detection (e.g., 'csv', 'jsonl').")] = None,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")] = "auto"
):
    """Calculate frequency distribution for specified fields.

    Counts occurrences of each unique value in the specified field(s).
    """
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'fields': fields,
        'output': output,
        'encoding': encoding,
        'filetype': filetype,
        'engine': engine
    }
    acmd = Selector()
    acmd.frequency(input_file, options)


@app.command()
def select(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names to select and reorder.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None,
    format_out: Annotated[str, typer.Option(help="Override output format (e.g., 'csv', 'jsonl').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    filter_expr: Annotated[str, typer.Option(help="Filter expression to apply (e.g., \"`status` == 'active'\").")] = None,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")] = "auto"
):
    """Select or reorder columns from file.

    Supports CSV, JSONL, and BSON formats. Can also filter records.
    """
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'fields': fields,
        'output': output,
        'encoding': encoding,
        'format_in': format_in,
        'format_out': format_out,
        'zipfile': zipfile,
        'filter': filter_expr,
        'engine': engine
    }
    acmd = Selector()
    acmd.select(input_file, options)


@app.command()
def split(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path prefix. If not specified, uses input filename.")] = None,
    fields: Annotated[str, typer.Option(help="Comma-separated field names to split by (creates one file per unique value combination).")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = "utf8",
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    gzipfile: Annotated[str, typer.Option(help="Gzip compression option for output files.")] = None,
    chunksize: Annotated[int, typer.Option(help="Number of records per chunk when splitting by size (default: 10000).")] = 10000,
    filter_expr: Annotated[str, typer.Option(help="Filter expression to apply before splitting.")] = None,
    dirname: Annotated[str, typer.Option(help="Directory path to write output files to.")] = None
):
    """Split a data file into multiple chunks.

    Can split by chunk size or by unique field values.
    """
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'fields': fields,
        'output': output,
        'encoding': encoding,
        'format_in': format_in,
        'zipfile': zipfile,
        'gzipfile': gzipfile,
        'chunksize': chunksize,
        'filter': filter_expr,
        'dirname': dirname
    }
    acmd = Selector()
    acmd.split(input_file, options)


@app.command()
def validate(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names to validate.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = 'utf8',
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    rule: Annotated[str, typer.Option(help="Validation rule name (e.g., 'common.email', 'common.url', 'ru.org.inn', 'ru.org.ogrn').")] = None,
    filter_expr: Annotated[str, typer.Option(help="Filter expression to apply before validation.")] = None,
    mode: Annotated[str, typer.Option(help="Output mode: 'invalid' (default, show invalid records), 'stats' (show statistics), or 'valid' (show valid records).")] = "invalid"
):
    """Validate fields against built-in or custom validation rules.

    Available rules: common.email, common.url, ru.org.inn, ru.org.ogrn
    """
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'fields': fields,
        'output': output,
        'encoding': encoding,
        'format_in': format_in,
        'zipfile': zipfile,
        'filter': filter_expr,
        'rule': rule,
        'mode': mode
    }
    acmd = Validator()
    acmd.validate(input_file, options)


@app.command()
def apply(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names (kept for compatibility).")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ",",
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = 'utf8',
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    script: Annotated[str, typer.Option(help="Path to Python script file containing transformation function.")] = None,
    filter_expr: Annotated[str, typer.Option(help="Filter expression to apply before transformation.")] = None
):
    """Apply a transformation script to each record in the file.

    Executes a Python script that transforms each record.
    """
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'fields': fields,
        'output': output,
        'encoding': encoding,
        'format_in': format_in,
        'zipfile': zipfile,
        'filter': filter_expr,
        'script': script
    }
    acmd = Transformer()
    acmd.script(input_file, options)


@app.command()
def scheme(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = 'utf8',
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    stype: Annotated[str, typer.Option(help="Schema type: 'cerberus' (default) or other schema formats.")] = 'cerberus'
):
    """[DEPRECATED] Generate data schema from file.

    ⚠️  This command is deprecated. Use 'undatum schema --format cerberus' instead.

    Creates a schema definition based on the structure of the input data.
    This command will be removed in a future version.
    """
    import warnings
    warnings.warn(
        "The 'scheme' command is deprecated. Use 'undatum schema --format cerberus' instead.",
        DeprecationWarning,
        stacklevel=2
    )

    if verbose:
        enable_verbose()

    # Redirect to schema command with cerberus format
    # Build AI configuration (not used for scheme, but needed for schema command)
    options = {
        'outtype': 'json',  # Cerberus format outputs JSON
        'format': 'cerberus',
        'output': output,
        'autodoc': False,
        'engine': 'auto'
    }
    acmd = Schemer()
    acmd.extract_schema(input_file, options)


@app.command()
def analyze(
    input_file: Annotated[str, typer.Argument(help="Path to input file to analyze.")],
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")] = "auto",
    use_pandas: Annotated[bool, typer.Option(help="Use pandas for data processing (may use more memory).")] = False,
    outtype: Annotated[str, typer.Option(help="Output format: 'text' (default), 'json', or 'yaml'.")] = "text",
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    autodoc: Annotated[bool, typer.Option(help="Enable AI-powered automatic field and dataset documentation.")] = False,
    lang: Annotated[str, typer.Option(help="Language for AI-generated documentation (default: 'English').")] = "English",
    ai_provider: Annotated[str, typer.Option(help="AI provider to use: 'openai', 'openrouter', 'ollama', 'lmstudio', or 'perplexity'.")] = None,
    ai_model: Annotated[str, typer.Option(help="Model name to use (provider-specific, e.g., 'gpt-4o-mini' for OpenAI).")] = None,
    ai_base_url: Annotated[str, typer.Option(help="Base URL for AI API (optional, uses provider-specific defaults if not specified).")] = None
):
    """Analyzes given data file and returns human readable insights.

    Provides detailed analysis of file structure, encoding, fields, data types,
    and optionally AI-generated field descriptions and dataset summaries.
    """
    if verbose:
        enable_verbose()

    # Build AI configuration
    ai_config = {}
    if ai_model:
        ai_config['model'] = ai_model
    if ai_base_url:
        ai_config['base_url'] = ai_base_url

    options = {
        'engine': engine,
        'use_pandas': use_pandas,
        'outtype': outtype,
        'output': output,
        'autodoc': autodoc,
        'lang': lang,
        'ai_provider': ai_provider,
        'ai_config': ai_config if ai_config else None
    }
    acmd = Analyzer()
    acmd.analyze(input_file, options)


def _run_doc_command(
    input_file: str,
    format: str,
    output: Optional[str],
    sample_size: int,
    verbose: bool,
    engine: str,
    delimiter: str,
    encoding: Optional[str],
    tagname: Optional[str],
    start_line: int,
    start_page: int,
    format_in: Optional[str],
    autodoc: bool,
    lang: str,
    ai_provider: Optional[str],
    ai_model: Optional[str],
    ai_base_url: Optional[str],
    semantic_types: bool,
    pii_detect: bool,
    pii_mask_samples: bool
):
    if verbose:
        enable_verbose()

    ai_config = {}
    if ai_model:
        ai_config['model'] = ai_model
    if ai_base_url:
        ai_config['base_url'] = ai_base_url

    options = {
        'format': format,
        'output': output,
        'sample_size': sample_size,
        'engine': engine,
        'delimiter': delimiter,
        'encoding': encoding,
        'tagname': tagname,
        'start_line': start_line,
        'start_page': start_page,
        'format_in': format_in,
        'autodoc': autodoc,
        'lang': lang,
        'ai_provider': ai_provider,
        'ai_config': ai_config if ai_config else None,
        'semantic_types': semantic_types,
        'pii_detect': pii_detect,
        'pii_mask_samples': pii_mask_samples
    }
    acmd = Documenter()
    acmd.document(input_file, options)


@app.command()
def doc(
    input_file: Annotated[str, typer.Argument(help="Path to input file to document.")],
    format: Annotated[str, typer.Option(help="Output format: 'markdown' (default), 'json', 'yaml', or 'text'.")] = "markdown",
    output: Annotated[Optional[str], typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    sample_size: Annotated[int, typer.Option(help="Number of sample records to include (default: 10).")] = 10,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default) or 'duckdb'.")] = "auto",
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[Optional[str], typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    tagname: Annotated[Optional[str], typer.Option(help="XML tag name that contains individual records.")] = None,
    start_line: Annotated[int, typer.Option(help="Line number (0-based) to start reading from.")] = 0,
    start_page: Annotated[int, typer.Option(help="Page number (0-based) to start from for Excel files.")] = 0,
    format_in: Annotated[Optional[str], typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None,
    autodoc: Annotated[bool, typer.Option(help="Enable AI-powered automatic field and dataset documentation.")] = False,
    lang: Annotated[str, typer.Option(help="Language for AI-generated documentation (default: 'English').")] = "English",
    ai_provider: Annotated[Optional[str], typer.Option(help="AI provider to use: 'openai', 'openrouter', 'ollama', 'lmstudio', or 'perplexity'.")] = None,
    ai_model: Annotated[Optional[str], typer.Option(help="Model name to use (provider-specific, e.g., 'gpt-4o-mini' for OpenAI).")] = None,
    ai_base_url: Annotated[Optional[str], typer.Option(help="Base URL for AI API (optional, uses provider-specific defaults if not specified).")] = None,
    semantic_types: Annotated[bool, typer.Option(help="Enable semantic type annotations using Metacrafter.")] = False,
    pii_detect: Annotated[bool, typer.Option(help="Enable PII detection using Metacrafter.")] = False,
    pii_mask_samples: Annotated[bool, typer.Option(help="Redact detected PII values in sample records.")] = False
):
    """Generate documentation for a dataset."""
    _run_doc_command(
        input_file=input_file,
        format=format,
        output=output,
        sample_size=sample_size,
        verbose=verbose,
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
        semantic_types=semantic_types,
        pii_detect=pii_detect,
        pii_mask_samples=pii_mask_samples
    )


@app.command()
def document(
    input_file: Annotated[str, typer.Argument(help="Path to input file to document.")],
    format: Annotated[str, typer.Option(help="Output format: 'markdown' (default), 'json', 'yaml', or 'text'.")] = "markdown",
    output: Annotated[Optional[str], typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    sample_size: Annotated[int, typer.Option(help="Number of sample records to include (default: 10).")] = 10,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default) or 'duckdb'.")] = "auto",
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[Optional[str], typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    tagname: Annotated[Optional[str], typer.Option(help="XML tag name that contains individual records.")] = None,
    start_line: Annotated[int, typer.Option(help="Line number (0-based) to start reading from.")] = 0,
    start_page: Annotated[int, typer.Option(help="Page number (0-based) to start from for Excel files.")] = 0,
    format_in: Annotated[Optional[str], typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None,
    autodoc: Annotated[bool, typer.Option(help="Enable AI-powered automatic field and dataset documentation.")] = False,
    lang: Annotated[str, typer.Option(help="Language for AI-generated documentation (default: 'English').")] = "English",
    ai_provider: Annotated[Optional[str], typer.Option(help="AI provider to use: 'openai', 'openrouter', 'ollama', 'lmstudio', or 'perplexity'.")] = None,
    ai_model: Annotated[Optional[str], typer.Option(help="Model name to use (provider-specific, e.g., 'gpt-4o-mini' for OpenAI).")] = None,
    ai_base_url: Annotated[Optional[str], typer.Option(help="Base URL for AI API (optional, uses provider-specific defaults if not specified).")] = None,
    semantic_types: Annotated[bool, typer.Option(help="Enable semantic type annotations using Metacrafter.")] = False,
    pii_detect: Annotated[bool, typer.Option(help="Enable PII detection using Metacrafter.")] = False,
    pii_mask_samples: Annotated[bool, typer.Option(help="Redact detected PII values in sample records.")] = False
):
    """Generate documentation for a dataset (alias for doc)."""
    _run_doc_command(
        input_file=input_file,
        format=format,
        output=output,
        sample_size=sample_size,
        verbose=verbose,
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
        semantic_types=semantic_types,
        pii_detect=pii_detect,
        pii_mask_samples=pii_mask_samples
    )


@app.command()
def schema(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    outtype: Annotated[str, typer.Option(help="Output format: 'text' (default), 'json', or 'yaml'.")] = "text",
    format: Annotated[str, typer.Option(help="Schema format: 'yaml' (default), 'json', 'cerberus', 'jsonschema', 'avro', or 'parquet'. Overrides outtype when specified.")] = None,
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    autodoc: Annotated[bool, typer.Option(help="Enable AI-powered automatic field documentation.")] = False,
    lang: Annotated[str, typer.Option(help="Language for AI-generated documentation (default: 'English').")] = "English",
    ai_provider: Annotated[str, typer.Option(help="AI provider to use: 'openai', 'openrouter', 'ollama', 'lmstudio', or 'perplexity'.")] = None,
    ai_model: Annotated[str, typer.Option(help="Model name to use (provider-specific, e.g., 'gpt-4o-mini' for OpenAI).")] = None,
    ai_base_url: Annotated[str, typer.Option(help="Base URL for AI API (optional, uses provider-specific defaults if not specified).")] = None,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")] = "auto"
):
    """Extract schema from a data file.

    Generates a schema definition describing the structure and types of fields in the data.
    Supports multiple output formats including YAML, JSON, Cerberus, JSON Schema, Avro, and Parquet.
    """
    if verbose:
        enable_verbose()

    # Build AI configuration
    ai_config = {}
    if ai_model:
        ai_config['model'] = ai_model
    if ai_base_url:
        ai_config['base_url'] = ai_base_url

    options = {
        'outtype': outtype,
        'format': format,
        'output': output,
        'autodoc': autodoc,
        'lang': lang,
        'ai_provider': ai_provider,
        'ai_config': ai_config if ai_config else None,
        'engine': engine
    }
    acmd = Schemer()
    acmd.extract_schema(input_file, options)


@app.command()
def schema_bulk(
    input_file: Annotated[str, typer.Argument(help="Glob pattern or directory path for input files (e.g., 'data/*.csv' or 'data/').")],
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    outtype: Annotated[str, typer.Option(help="Output format: 'text' (default), 'json', or 'yaml'.")] = "text",
    format: Annotated[str, typer.Option(help="Schema format: 'yaml' (default), 'json', 'cerberus', 'jsonschema', 'avro', or 'parquet'. Overrides outtype when specified.")] = None,
    output: Annotated[str, typer.Option(help="Output directory path for schema files.")] = None,
    mode: Annotated[str, typer.Option(help="Extraction mode: 'distinct' (extract unique schemas, default) or 'perfile' (one schema per file).")] = "distinct",
    autodoc: Annotated[bool, typer.Option(help="Enable AI-powered automatic field documentation.")] = False,
    lang: Annotated[str, typer.Option(help="Language for AI-generated documentation (default: 'English').")] = "English",
    ai_provider: Annotated[str, typer.Option(help="AI provider to use: 'openai', 'openrouter', 'ollama', 'lmstudio', or 'perplexity'.")] = None,
    ai_model: Annotated[str, typer.Option(help="Model name to use (provider-specific, e.g., 'gpt-4o-mini' for OpenAI).")] = None,
    ai_base_url: Annotated[str, typer.Option(help="Base URL for AI API (optional, uses provider-specific defaults if not specified).")] = None,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")] = "auto"
):
    """Extract schemas from multiple files.

    Processes multiple files and extracts their schemas, either as distinct unique schemas
    or one schema per file.
    """
    if verbose:
        enable_verbose()

    # Build AI configuration
    ai_config = {}
    if ai_model:
        ai_config['model'] = ai_model
    if ai_base_url:
        ai_config['base_url'] = ai_base_url

    options = {
        'outtype': outtype,
        'format': format,
        'output': output,
        'mode': mode,
        'autodoc': autodoc,
        'lang': lang,
        'ai_provider': ai_provider,
        'ai_config': ai_config if ai_config else None,
        'engine': engine
    }
    acmd = Schemer()
    acmd.extract_schema_bulk(input_file, options)


@app.command()
def ingest(
    input_file: Annotated[str, typer.Argument(help="Path to input file or glob pattern (e.g., 'data/*.jsonl').")],
    uri: Annotated[str, typer.Argument(help="Database connection URI (e.g., 'mongodb://localhost:27017', 'postgresql://user:pass@host:5432/db', or 'https://elasticsearch:9200').")],
    db: Annotated[str, typer.Argument(help="Database name.")],
    table: Annotated[str, typer.Argument(help="Collection or table name.")],
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    batch: Annotated[int, typer.Option(help="Batch size for ingestion (number of records per batch, default: 1000).")] = DEFAULT_BATCH_SIZE,
    dbtype: Annotated[str, typer.Option(help="Database type: 'mongodb' (default), 'postgresql', 'duckdb', 'mysql', 'sqlite', 'elasticsearch', or 'elastic'.")] = "mongodb",
    totals: Annotated[bool, typer.Option(help="Show total record counts during ingestion.")] = False,
    drop: Annotated[bool, typer.Option(help="Drop existing collection/table before ingestion.")] = False,
    timeout: Annotated[int, typer.Option(help="Connection timeout in seconds (default: -30).")] = -30,
    skip: Annotated[int, typer.Option(help="Number of records to skip at the beginning.")] = None,
    api_key: Annotated[str, typer.Option(help="API key for database authentication (Elasticsearch).")] = None,
    doc_id: Annotated[str, typer.Option(help="Field name to use as document ID (Elasticsearch, default: 'id').")] = None,
    mode: Annotated[str, typer.Option(help="Ingestion mode for PostgreSQL/DuckDB/MySQL/SQLite: 'append' (default), 'replace', or 'upsert'.")] = "append",
    create_table: Annotated[bool, typer.Option(help="Auto-create table from data schema (PostgreSQL/DuckDB/MySQL/SQLite).")] = False,
    upsert_key: Annotated[str, typer.Option(help="Field name(s) to use for conflict resolution in upsert mode (PostgreSQL/DuckDB/MySQL/SQLite, comma-separated for multiple keys).")] = None,
    use_appender: Annotated[bool, typer.Option(help="Use Appender API for DuckDB (streaming insertion, default: False).")] = False
):
    """Ingest data into a database.

    Supports MongoDB, PostgreSQL, DuckDB, MySQL, SQLite, and Elasticsearch databases.
    Reads data from files and inserts them into the specified database collection or table.

    For PostgreSQL:
    - Use COPY FROM for maximum performance (10-100x faster than INSERT)
    - Supports append, replace, and upsert modes
    - Can auto-create tables from data schema
    - Uses connection pooling for efficient connection management

    For DuckDB:
    - Fast bulk loading with optimized batch inserts
    - Supports append, replace, and upsert modes
    - Can auto-create tables from data schema
    - Appender API available for streaming insertion
    - Works with file-based or in-memory databases

    For MySQL:
    - Multi-row INSERT for efficient batch operations
    - Supports append, replace, and upsert modes
    - Can auto-create tables from data schema
    - Uses connection pooling for efficient connection management

    For SQLite:
    - Optimized batch inserts with PRAGMA optimizations
    - Supports append, replace, and upsert modes
    - Can auto-create tables from data schema
    - Works with file-based or in-memory databases
    """
    if verbose:
        enable_verbose()

    # Parse upsert_key if provided (can be comma-separated)
    upsert_key_parsed = None
    if upsert_key:
        upsert_key_parsed = [k.strip() for k in upsert_key.split(',')]
        if len(upsert_key_parsed) == 1:
            upsert_key_parsed = upsert_key_parsed[0]

    options = {
        'dbtype': dbtype,
        'skip': skip,
        'drop': drop,
        'totals': totals,
        'doc_id': doc_id,
        'api_key': api_key,
        'timeout': timeout,
        'mode': mode,
        'create_table': create_table,
        'upsert_key': upsert_key_parsed,
        'use_appender': use_appender
    }
    acmd = Ingester(batch)
    files = glob.glob(input_file.strip("'"))
    acmd.ingest(files, uri, db, table, options)


@app.command()
def query(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names (kept for compatibility).")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None,
    format_out: Annotated[str, typer.Option(help="Override output format (e.g., 'csv', 'jsonl').")] = None,
    zipfile: Annotated[bool, typer.Option(help="Treat input file as a ZIP archive.")] = False,
    query_expr: Annotated[str, typer.Option(help="MistQL query expression to execute on the data.")] = None
):
    """Query data using MistQL query language.

    .. note:: Experimental feature. Requires 'mistql' package: pip install mistql

    Executes MistQL queries on the input data and returns the results.
    """
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'fields': fields,
        'output': output,
        'encoding': encoding,
        'format_in': format_in,
        'format_out': format_out,
        'zipfile': zipfile,
        'query': query_expr
    }
    acmd = DataQuery()
    acmd.query(input_file, options)


@app.command()
def count(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype: Annotated[str, typer.Option(help="Override file type detection (e.g., 'csv', 'jsonl').")] = None,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")] = "auto"
):
    """Count the number of rows in a data file.

    Returns the total number of data rows (excluding header if present).
    With DuckDB engine, counting is instant for supported formats.
    """
    if verbose:
        enable_verbose()
    options = {
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': filetype,
        'engine': engine
    }
    acmd = Counter()
    acmd.count(input_file, options)


@app.command()
def head(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    n: Annotated[int, typer.Option(help="Number of rows to extract (default: 10).")] = 10,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Extract the first N rows from a data file.

    Useful for quick data inspection.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'n': n,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = Head()
    acmd.head(input_file, options)


@app.command()
def tail(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    n: Annotated[int, typer.Option(help="Number of rows to extract (default: 10).")] = 10,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Extract the last N rows from a data file.

    Uses efficient buffering for large files.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'n': n,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = Tail()
    acmd.tail(input_file, options)


@app.command()
def enum(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    field: Annotated[str, typer.Option(help="Field name for the generated values (default: 'row_id').")] = 'row_id',
    type: Annotated[str, typer.Option(help="Type of value: 'number' (default), 'uuid', or 'constant'.")] = 'number',
    start: Annotated[int, typer.Option(help="Starting number for numeric enumeration (default: 1).")] = 1,
    value: Annotated[str, typer.Option(help="Constant value to use when type is 'constant'.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Add row numbers, UUIDs, or constant values to records.

    Useful for adding unique identifiers or sequential numbers to data.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'field': field,
        'type': type,
        'start': start,
        'value': value,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = Enumerator()
    acmd.enum(input_file, options)


@app.command()
def reverse(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype: Annotated[str, typer.Option(help="Override file type detection (e.g., 'csv', 'jsonl').")] = None,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")] = "auto"
):
    """Reverse the order of rows in a data file.

    For large files, may require buffering. DuckDB engine provides optimization for supported formats.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': filetype,
        'engine': engine
    }
    acmd = Reverser()
    acmd.reverse(input_file, options)


@app.command()
def table(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    limit: Annotated[int, typer.Option(help="Maximum number of rows to display (default: 20).")] = 20,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names to display.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Display data in a formatted, aligned table for inspection.

    Uses the rich library to create a nicely formatted table output.
    """
    if verbose:
        enable_verbose()
    options = {
        'limit': limit,
        'fields': fields,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = TableFormatter()
    acmd.table(input_file, options)


@app.command()
def fixlengths(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    strategy: Annotated[str, typer.Option(help="Strategy: 'pad' (default) or 'truncate'.")] = 'pad',
    value: Annotated[str, typer.Option(help="Value to use for padding (default: empty string).")] = '',
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Ensure all rows have the same number of fields.

    Pads shorter rows or truncates longer rows to normalize field counts.
    Useful for data cleaning workflows.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'strategy': strategy,
        'value': value,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = FixLengths()
    acmd.fixlengths(input_file, options)


@app.command()
def sort(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    by: Annotated[str, typer.Option(help="Comma-separated list of field names to sort by.")] = None,
    desc: Annotated[bool, typer.Option(help="Sort in descending order.")] = False,
    numeric: Annotated[str, typer.Option(help="Comma-separated list of field names to sort numerically.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype: Annotated[str, typer.Option(help="Override file type detection (e.g., 'csv', 'jsonl').")] = None,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")] = "auto"
):
    """Sort rows by one or more columns.

    Supports multiple sort keys, ascending/descending order, and numeric sorting.
    Uses external merge sort for large files.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'by': by,
        'desc': desc,
        'numeric': numeric,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': filetype,
        'engine': engine
    }
    acmd = Sorter()
    acmd.sort(input_file, options)


@app.command()
def sample(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    n: Annotated[int, typer.Option(help="Number of rows to sample.")] = None,
    percent: Annotated[float, typer.Option(help="Percentage of rows to sample (0-100).")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Randomly select rows from a data file.

    Uses reservoir sampling algorithm that doesn't require loading all data into memory.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'n': n,
        'percent': percent,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = Sampler()
    acmd.sample(input_file, options)


@app.command()
def search(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    pattern: Annotated[str, typer.Option(help="Regex pattern to search for.")] = None,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names to search in (default: all fields).")] = None,
    ignore_case: Annotated[bool, typer.Option(help="Case-insensitive search.")] = False,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Filter rows using regex patterns.

    Searches across specified fields or all fields, outputting rows that match the pattern.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'pattern': pattern,
        'fields': fields,
        'ignore_case': ignore_case,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = Searcher()
    acmd.search(input_file, options)


@app.command()
def dedup(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    key_fields: Annotated[str, typer.Option(help="Comma-separated list of field names to use for deduplication (default: all fields).")] = None,
    keep: Annotated[str, typer.Option(help="Which duplicate to keep: 'first' (default) or 'last'.")] = 'first',
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype: Annotated[str, typer.Option(help="Override file type detection (e.g., 'csv', 'jsonl').")] = None,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")] = "auto"
):
    """Remove duplicate rows.

    Can deduplicate by all fields or specified key fields. Supports keeping first or last occurrence.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'key_fields': key_fields,
        'keep': keep,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': filetype,
        'engine': engine
    }
    acmd = Deduplicator()
    acmd.dedup(input_file, options)


@app.command()
def fill(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    fields: Annotated[str, typer.Option(help="Comma-separated list of field names to fill (default: all fields).")] = None,
    strategy: Annotated[str, typer.Option(help="Fill strategy: 'constant' (default), 'forward', or 'backward'.")] = 'constant',
    value: Annotated[str, typer.Option(help="Constant value to use for filling (required for 'constant' strategy).")] = '',
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Fill empty or null values with specified values or strategies.

    Supports constant filling, forward-fill (use previous value), and backward-fill (use next value).
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'fields': fields,
        'strategy': strategy,
        'value': value,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = Filler()
    acmd.fill(input_file, options)


@app.command()
def rename(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    map: Annotated[str, typer.Option(help="Field name mapping: 'old_name:new_name,old2:new2'.")] = None,
    pattern: Annotated[str, typer.Option(help="Regex pattern to match field names (for regex-based renaming).")] = None,
    replacement: Annotated[str, typer.Option(help="Replacement string for regex pattern (default: empty string).")] = '',
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Rename fields by exact mapping or regex patterns.

    Supports multiple field renames in one operation.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'map': map,
        'pattern': pattern,
        'replacement': replacement,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = Renamer()
    acmd.rename(input_file, options)


@app.command()
def explode(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    field: Annotated[str, typer.Option(help="Field name to split by separator.")] = None,
    separator: Annotated[str, typer.Option(help="Separator character (default: comma).")] = ',',
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Split a column by separator into multiple rows.

    Creates one row per value in the specified field, duplicating other fields.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'field': field,
        'separator': separator,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = Exploder()
    acmd.explode(input_file, options)


@app.command()
def replace(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    field: Annotated[str, typer.Option(help="Field name to perform replacement in.")] = None,
    pattern: Annotated[str, typer.Option(help="Pattern to search for (string or regex).")] = None,
    replacement: Annotated[str, typer.Option(help="Replacement string.")] = '',
    regex: Annotated[bool, typer.Option(help="Treat pattern as regex.")] = False,
    global_replace: Annotated[bool, typer.Option(help="Replace all occurrences (default: replace first only).")] = False,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Perform string replacement in specified fields.

    Supports simple string replacement and regex-based replacement.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'field': field,
        'pattern': pattern,
        'replacement': replacement,
        'regex': regex,
        'global': global_replace,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = Replacer()
    acmd.replace(input_file, options)


@app.command()
def cat(
    input_files: Annotated[list[str], typer.Argument(help="Path(s) to input file(s). Multiple files can be specified.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    mode: Annotated[str, typer.Option(help="Concatenation mode: 'rows' (default) or 'columns'.")] = 'rows',
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Concatenate files by rows or columns.

    Row mode: appends files vertically. Column mode: combines files side-by-side.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'mode': mode,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = Cat()
    acmd.cat(input_files, options)


@app.command()
def join(
    file1: Annotated[str, typer.Argument(help="Path to first input file.")],
    file2: Annotated[str, typer.Argument(help="Path to second input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    on: Annotated[str, typer.Option(help="Comma-separated list of key field names to join on.")] = None,
    type: Annotated[str, typer.Option(help="Join type: 'inner' (default), 'left', 'right', or 'full'.")] = 'inner',
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype1: Annotated[str, typer.Option(help="Override file type detection for first file.")] = None,
    filetype2: Annotated[str, typer.Option(help="Override file type detection for second file.")] = None,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")] = "auto"
):
    """Perform relational join between two files.

    Supports inner, left, right, and full outer joins. Uses hash-based join for streaming formats
    and DuckDB SQL join for supported formats.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'on': on,
        'type': type,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype1': filetype1,
        'filetype2': filetype2,
        'engine': engine
    }
    acmd = Joiner()
    acmd.join(file1, file2, options)


@app.command()
def diff(
    file1: Annotated[str, typer.Argument(help="Path to first input file.")],
    file2: Annotated[str, typer.Argument(help="Path to second input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    key: Annotated[str, typer.Option(help="Comma-separated list of key field names to compare on.")] = None,
    format: Annotated[str, typer.Option(help="Output format: 'json' (default) or 'unified'.")] = 'json',
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Compare two files and show differences.

    Outputs added, removed, and changed rows based on key fields.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'key': key,
        'format': format,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = Differ()
    acmd.diff(file1, file2, options)


@app.command()
def exclude(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    exclude_file: Annotated[str, typer.Argument(help="Path to exclusion file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    on: Annotated[str, typer.Option(help="Comma-separated list of key field names to exclude on.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Remove rows from input file where keys match exclusion file.

    Uses hash-based lookup for performance.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'on': on,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = Excluder()
    acmd.exclude(input_file, exclude_file, options)


@app.command()
def transpose(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Swap rows and columns.

    Transposes the data table, handling headers appropriately.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = Transposer()
    acmd.transpose(input_file, options)


@app.command()
def sniff(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    format: Annotated[str, typer.Option(help="Output format: 'text' (default), 'json', or 'yaml'.")] = 'text',
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Detect file properties (delimiter, encoding, types, record count).

    Analyzes the file and reports detected properties.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'format': format,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = Sniffer()
    acmd.sniff(input_file, options)


@app.command()
def slice(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    start: Annotated[int, typer.Option(help="Start index (inclusive).")] = None,
    end: Annotated[int, typer.Option(help="End index (inclusive).")] = None,
    indices: Annotated[str, typer.Option(help="Comma-separated list of specific indices to extract.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character.")] = ',',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    filetype: Annotated[str, typer.Option(help="Override file type detection (e.g., 'csv', 'jsonl').")] = None,
    engine: Annotated[str, typer.Option(help="Processing engine: 'auto' (default), 'duckdb', or 'iterable'.")] = "auto"
):
    """Extract specific rows by range or index list.

    Supports range-based slicing (--start/--end) or index-based slicing (--indices).
    Uses DuckDB for efficient random access when supported.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'start': start,
        'end': end,
        'indices': indices,
        'delimiter': delimiter,
        'encoding': encoding,
        'filetype': filetype,
        'engine': engine
    }
    acmd = Slicer()
    acmd.slice(input_file, options)


@app.command()
def fmt(
    input_file: Annotated[str, typer.Argument(help="Path to input file.")],
    output: Annotated[str, typer.Option(help="Optional output file path. If not specified, prints to stdout.")] = None,
    delimiter: Annotated[str, typer.Option(help="CSV delimiter character (default: comma).")] = ',',
    quote: Annotated[str, typer.Option(help="Quote style: 'minimal' (default), 'always', 'none', or 'nonnumeric'.")] = 'minimal',
    escape: Annotated[str, typer.Option(help="Escape character: 'double' (default), 'backslash', or 'none'.")] = 'double',
    line_ending: Annotated[str, typer.Option(help="Line ending: 'unix' (default), 'windows', 'crlf', or 'mac'.")] = 'unix',
    encoding: Annotated[str, typer.Option(help="File encoding (e.g., 'utf8', 'latin1').")] = None,
    verbose: Annotated[bool, typer.Option(help="Enable verbose logging output.")] = False,
    format_in: Annotated[str, typer.Option(help="Override input file format detection (e.g., 'csv', 'jsonl').")] = None
):
    """Reformat CSV data with specific formatting options.

    Controls delimiter, quote style, escape character, and line endings.
    """
    if verbose:
        enable_verbose()
    options = {
        'output': output,
        'delimiter': delimiter,
        'quote': quote,
        'escape': escape,
        'line_ending': line_ending,
        'encoding': encoding,
        'filetype': format_in
    }
    acmd = Formatter()
    acmd.fmt(input_file, options)


if __name__ == '__main__':
    app()
